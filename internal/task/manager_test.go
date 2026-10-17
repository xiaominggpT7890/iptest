package task

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	"iptest/internal/config"
	"iptest/internal/metrics"
	"iptest/internal/store"
	"iptest/internal/worker"
)

func TestReconcileRunningPreservesData(t *testing.T) {
	db, mgr := testManager(t)
	id := seedInterruptedTask(t, db)
	other := testTask(t, db)
	if err := db.UpdateTaskState(context.Background(), other, store.StateCompleted, map[string]interface{}{"finished_at": int64(123)}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetReportCache(context.Background(), other, []byte(`{"saved":true}`)); err != nil {
		t.Fatal(err)
	}

	if err := mgr.ReconcileRunning(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRecoveredTask(t, db, id)
	row, err := db.GetTask(context.Background(), other)
	if err != nil || row == nil || row.State != store.StateCompleted || row.FinishedAt.Int64 != 123 {
		t.Fatalf("completed task changed: row=%+v err=%v", row, err)
	}
	cache, err := db.GetReportCache(context.Background(), other)
	if err != nil || string(cache) != `{"saved":true}` {
		t.Fatalf("completed task cache changed: cache=%s err=%v", cache, err)
	}
	if mgr.IsRunning(id) {
		t.Fatal("recovered task was started without a user request")
	}
	if err := mgr.Start(id); err != nil {
		t.Fatalf("recovered task cannot resume: %v", err)
	}
	if err := mgr.Pause(id); err != nil {
		t.Fatalf("resumed task cannot pause: %v", err)
	}
}

func TestReconcileRunningReportsDatabaseFailure(t *testing.T) {
	db, mgr := testManager(t)
	id := seedInterruptedTask(t, db)
	if _, err := db.Exec(`CREATE TRIGGER fail_recovery BEFORE UPDATE OF state ON tasks WHEN NEW.state='paused' BEGIN SELECT RAISE(ABORT, 'recovery blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ReconcileRunning(context.Background()); err == nil {
		t.Fatal("failed startup recovery was reported as successful")
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row == nil || row.State != store.StateRunning || row.TotalRequests != 7 {
		t.Fatalf("failed recovery changed task: row=%+v err=%v", row, err)
	}
}

func TestReconcileRunningRejectsActiveManager(t *testing.T) {
	db, mgr := testManager(t)
	id := testTask(t, db)
	if err := mgr.Start(id); err != nil {
		t.Fatal(err)
	}
	if err := mgr.ReconcileRunning(context.Background()); err == nil {
		t.Fatal("recovery accepted an active runner")
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row == nil || row.State != store.StateRunning || !mgr.IsRunning(id) {
		t.Fatalf("live task was altered: row=%+v err=%v", row, err)
	}
}

func TestPauseRecoversInterruptedTask(t *testing.T) {
	db, mgr := testManager(t)
	id := seedInterruptedTask(t, db)
	if err := mgr.Pause(id); err != nil {
		t.Fatalf("orphan RUNNING task cannot pause: %v", err)
	}
	assertRecoveredTask(t, db, id)
}

func TestConcurrentStartCreatesOnlyOneRun(t *testing.T) {
	db, mgr := testManager(t)
	id := testTask(t, db)
	const attempts = 20
	ready := make(chan struct{})
	results := make(chan error, attempts)
	var wg sync.WaitGroup
	for i := 0; i < attempts; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-ready
			results <- mgr.Start(id)
		}()
	}
	close(ready)
	wg.Wait()
	close(results)
	successes := 0
	for err := range results {
		if err == nil {
			successes++
		}
	}
	if successes != 1 {
		t.Fatalf("concurrent start succeeded %d times, want 1", successes)
	}
	mgr.mu.Lock()
	rt := mgr.running[id]
	mgr.mu.Unlock()
	if rt == nil {
		t.Fatal("successful start has no runner")
	}
	if err := mgr.Pause(id); err != nil {
		t.Fatal(err)
	}
	select {
	case <-rt.snapshotDone:
	default:
		t.Fatal("pause returned before snapshot writer stopped")
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row == nil || row.State != store.StatePaused || mgr.IsRunning(id) {
		t.Fatalf("pause did not finish the sole runner: row=%+v err=%v", row, err)
	}
}

func TestStartFailuresLeaveTaskRestartable(t *testing.T) {
	for _, failure := range []string{"reload", "state_write"} {
		t.Run(failure, func(t *testing.T) {
			db, mgr := testManager(t)
			id := testTask(t, db)
			var failSQL, repairSQL string
			if failure == "reload" {
				failSQL = `DROP TABLE samples`
				repairSQL = store.Schema
			} else {
				failSQL = `CREATE TRIGGER fail_start BEFORE UPDATE OF state ON tasks WHEN NEW.state='running' BEGIN SELECT RAISE(ABORT, 'start blocked'); END`
				repairSQL = `DROP TRIGGER fail_start`
			}
			if _, err := db.Exec(failSQL); err != nil {
				t.Fatal(err)
			}
			if err := mgr.Start(id); err == nil {
				t.Fatal("database failure was reported as a successful start")
			}
			row, err := db.GetTask(context.Background(), id)
			if err != nil || row == nil || row.State != store.StateCreated || mgr.IsRunning(id) || mgr.CurrentSnapshot(id) != nil {
				t.Fatalf("failed start left running state: row=%+v err=%v", row, err)
			}
			if _, err := db.Exec(repairSQL); err != nil {
				t.Fatal(err)
			}
			if err := mgr.Start(id); err != nil {
				t.Fatalf("failed start left task blocked after database repair: %v", err)
			}
			if err := mgr.Pause(id); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestBlockedStartDoesNotBlockLiveSnapshots(t *testing.T) {
	db, mgr := testManager(t)
	activeID := testTask(t, db)
	blockedID := testTask(t, db)
	if err := mgr.Start(activeID); err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	waits := db.Stats().WaitCount
	started := make(chan error, 1)
	go func() { started <- mgr.Start(blockedID) }()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for db.Stats().WaitCount == waits {
		select {
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatal("start did not wait for the exhausted database connection pool")
		}
	}
	snapshot := make(chan *metrics.Snapshot, 1)
	go func() { snapshot <- mgr.CurrentSnapshot(activeID) }()
	select {
	case snap := <-snapshot:
		if snap == nil {
			t.Fatal("live task has no snapshot")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("another task's blocked SQL prevented a live snapshot")
	}
	if err := conn.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-started:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("start did not continue after the database connection was released")
	}
}

func TestFinalMetricsWaitForOutstandingSnapshot(t *testing.T) {
	db, mgr := testManager(t)
	id := testTask(t, db)
	if err := db.UpdateTaskState(context.Background(), id, store.StateRunning, nil); err != nil {
		t.Fatal(err)
	}
	agg := metrics.NewAggregator(10)
	agg.ObserveSuccess("1.1.1.1", "US", 1)
	agg.ObserveSuccess("8.8.8.8", "US", 1)
	writer := store.NewBatchWriter(db, 10, id)
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	snapshotCancelled := make(chan struct{})
	snapshotDone := make(chan struct{})
	var snapshotOnce sync.Once
	finishSnapshot := func() { snapshotOnce.Do(func() { close(snapshotDone) }) }
	defer finishSnapshot()
	rt := &runningTask{
		cancel: cancel, runner: worker.NewRunner(runCtx, id, &config.TaskConfig{}, db, writer, agg),
		writer: writer, agg: agg, startedAt: time.Now(), done: make(chan struct{}),
		snapshotCancel: func() { close(snapshotCancelled) }, snapshotDone: snapshotDone,
	}
	mgr.mu.Lock()
	mgr.running[id] = rt
	mgr.mu.Unlock()
	go mgr.supervise(id, rt)
	select {
	case <-snapshotCancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not cancel its snapshot loop")
	}
	select {
	case <-rt.done:
		t.Fatal("runner completed with a snapshot write still pending")
	default:
	}
	if err := db.UpdateTaskMetrics(context.Background(), id, 1, 1, 0, 1, 1); err != nil {
		t.Fatal(err)
	}
	finishSnapshot()
	select {
	case <-rt.done:
	case <-time.After(5 * time.Second):
		t.Fatal("runner did not finish after its snapshot loop exited")
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row == nil || row.State != store.StateCompleted || row.TotalRequests != 2 || row.UniqueIPs != 2 {
		t.Fatalf("stale snapshot overwrote final metrics: row=%+v err=%v", row, err)
	}
}

func testManager(t *testing.T) (*store.DB, *Manager) {
	t.Helper()
	db, err := store.Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	mgr := NewManager(db)
	t.Cleanup(func() {
		mgr.Shutdown()
		if err := db.Close(); err != nil {
			t.Error(err)
		}
	})
	return db, mgr
}

func testTask(t *testing.T, db *store.DB) int64 {
	t.Helper()
	cfg := &config.TaskConfig{
		Proxy:     config.ProxyConfig{Scheme: "socks5", Template: "test-{num}:test@127.0.0.1:1"},
		TargetURL: "http://127.0.0.1/geo", Sessid: config.SessidRange{Min: 1, Max: 100},
		DurationSeconds: 3600, RequestTimeoutMs: 1000,
		StickinessProbe: config.StickinessProbeConfig{Enabled: true, SessidCount: 1, PollScheduleSeconds: []int{3600}, LoopSchedule: true},
	}
	id, err := db.CreateTask(context.Background(), "test", cfg.ToJSON())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func seedInterruptedTask(t *testing.T, db *store.DB) int64 {
	t.Helper()
	id := testTask(t, db)
	ctx := context.Background()
	if err := db.UpdateTaskState(ctx, id, store.StateRunning, map[string]interface{}{"started_at": int64(1000), "elapsed_seconds": int64(123)}); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateTaskMetrics(ctx, id, 7, 6, 1, 3, 2); err != nil {
		t.Fatal(err)
	}
	writer := store.NewBatchWriter(db, 10, id)
	writer.Submit(store.Sample{TaskID: id, WorkerType: "probe", Sessid: 1, IP: "1.1.1.1", Country: "US", OK: true, TS: 1000})
	writer.Close()
	if _, err := db.UpsertProbeSession(ctx, id, 1, "1.1.1.1", 1000); err != nil {
		t.Fatal(err)
	}
	if err := db.InsertMetricsSnapshot(ctx, id, store.MetricsPoint{TS: 1000, TotalReqs: 7, Success: 6, Failed: 1, UniqueIPs: 3}); err != nil {
		t.Fatal(err)
	}
	if err := db.SetReportCache(ctx, id, []byte(`{"old":true}`)); err != nil {
		t.Fatal(err)
	}
	return id
}

func assertRecoveredTask(t *testing.T, db *store.DB, id int64) {
	t.Helper()
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row == nil {
		t.Fatalf("read recovered task: row=%+v err=%v", row, err)
	}
	if row.State != store.StatePaused || !row.PausedAt.Valid || row.StartedAt.Int64 != 1000 || row.ElapsedSeconds != 123 || row.TotalRequests != 7 || row.TotalSuccess != 6 || row.TotalFailed != 1 || row.UniqueIPs != 3 || row.UniqueCountries != 2 {
		t.Fatalf("recovery damaged task metadata or metrics: %+v", row)
	}
	for _, table := range []string{"samples", "probe_sessions", "metrics_snapshots"} {
		var count int
		if err := db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE task_id=?", table), id).Scan(&count); err != nil || count != 1 {
			t.Fatalf("%s data changed: count=%d err=%v", table, count, err)
		}
	}
	cache, err := db.GetReportCache(context.Background(), id)
	if err != nil || len(cache) != 0 {
		t.Fatalf("recovered task has a stale report cache: cache=%s err=%v", cache, err)
	}
}
