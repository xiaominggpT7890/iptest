package task

import (
	"context"
	"strings"
	"testing"
	"time"

	"iptest/internal/config"
	"iptest/internal/store"
)

func TestSequentialPauseAndResumeKeepsPreciseCursor(t *testing.T) {
	db, mgr := testManager(t)
	id := sequentialTask(t, db, 700, 1_000_000)
	writer := store.NewBatchWriter(db, 10, id)
	writer.Submit(store.Sample{TaskID: id, WorkerType: "probe", Sessid: 999999, IP: "1.1.1.1", OK: true, TS: 1000})
	writer.Close()
	if err := db.UpdateTaskMetrics(context.Background(), id, 1, 1, 0, 1, 0); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Start(id); err != nil {
		t.Fatal(err)
	}
	waitTaskCondition(t, func() bool { return mgr.CurrentSweepNext(id) != nil })
	if err := mgr.Pause(id); err != nil {
		t.Fatal(err)
	}
	first, err := db.GetTask(context.Background(), id)
	if err != nil || !first.SweepNextSessid.Valid || first.SweepNextSessid.Int64 <= 700 || first.SweepNextSessid.Int64 > 1_000_000 {
		t.Fatalf("pause did not preserve precise cursor: row=%+v err=%v", first, err)
	}
	var count, minimum, maximum, lastID int64
	if err := db.QueryRow(`SELECT COUNT(*), MIN(sessid), MAX(sessid), MAX(id) FROM samples WHERE task_id=? AND worker_type='sweep'`, id).Scan(&count, &minimum, &maximum, &lastID); err != nil {
		t.Fatal(err)
	}
	if minimum != 700 || maximum != first.SweepNextSessid.Int64-1 || count != first.SweepNextSessid.Int64-700 {
		t.Fatalf("sequential samples contain gaps or followed probe history: count=%d min=%d max=%d next=%d", count, minimum, maximum, first.SweepNextSessid.Int64)
	}
	if err := mgr.Start(id); err != nil {
		t.Fatal(err)
	}
	waitTaskCondition(t, func() bool {
		next := mgr.CurrentSweepNext(id)
		return next != nil && *next > first.SweepNextSessid.Int64
	})
	if err := mgr.Pause(id); err != nil {
		t.Fatal(err)
	}
	var resumedMinimum, duplicates int64
	if err := db.QueryRow(`SELECT MIN(sessid) FROM samples WHERE task_id=? AND worker_type='sweep' AND id>?`, id, lastID).Scan(&resumedMinimum); err != nil {
		t.Fatal(err)
	}
	if err := db.QueryRow(`SELECT COUNT(*)-COUNT(DISTINCT sessid) FROM samples WHERE task_id=? AND worker_type='sweep'`, id).Scan(&duplicates); err != nil {
		t.Fatal(err)
	}
	if resumedMinimum != first.SweepNextSessid.Int64 || duplicates != 0 {
		t.Fatalf("resume repeated or skipped numbers: first resumed=%d want=%d duplicates=%d", resumedMinimum, first.SweepNextSessid.Int64, duplicates)
	}
}

func TestSequentialWrapKeepsTaskRunning(t *testing.T) {
	db, mgr := testManager(t)
	id := sequentialTask(t, db, 7, 9)
	if err := mgr.Start(id); err != nil {
		t.Fatal(err)
	}
	waitTaskCondition(t, func() bool {
		var count int
		return db.QueryRow(`SELECT COUNT(*) FROM samples WHERE task_id=? AND worker_type='sweep'`, id).Scan(&count) == nil && count >= 9
	})
	if !mgr.IsRunning(id) {
		t.Fatal("sequential sweep stopped instead of starting another round")
	}
	if err := mgr.Pause(id); err != nil {
		t.Fatal(err)
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row.SweepNextSessid.Int64 < 7 || row.SweepNextSessid.Int64 > 9 {
		t.Fatalf("wrapped cursor outside configured range: row=%+v err=%v", row, err)
	}
	var outside int
	if err := db.QueryRow(`SELECT COUNT(*) FROM samples WHERE task_id=? AND worker_type='sweep' AND (sessid<7 OR sessid>9)`, id).Scan(&outside); err != nil || outside != 0 {
		t.Fatalf("sweep sent out-of-range requests: count=%d err=%v", outside, err)
	}
}

func TestSequentialReservationFailureIsReportedAsFailed(t *testing.T) {
	db, mgr := testManager(t)
	id := sequentialTask(t, db, 10, 1_000_000)
	if _, err := db.Exec(`CREATE TRIGGER fail_sweep_reserve BEFORE UPDATE OF sweep_next_sessid ON tasks BEGIN SELECT RAISE(ABORT, 'reserve blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Start(id); err != nil {
		t.Fatal(err)
	}
	waitTaskCondition(t, func() bool { return !mgr.IsRunning(id) })
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row.State != store.StateFailed || !strings.Contains(row.LastError.String, "reserve sweep sessid") || row.SweepNextSessid.Valid || row.TotalRequests != 0 {
		t.Fatalf("reservation failure was hidden: row=%+v err=%v", row, err)
	}
}

func TestSequentialPauseReportsCursorSaveFailure(t *testing.T) {
	db, mgr := testManager(t)
	id := sequentialTask(t, db, 10, 1_000_000)
	if err := mgr.Start(id); err != nil {
		t.Fatal(err)
	}
	waitTaskCondition(t, func() bool { return mgr.CurrentSweepNext(id) != nil })
	if _, err := db.Exec(`CREATE TRIGGER fail_sweep_save BEFORE UPDATE OF sweep_next_sessid ON tasks WHEN NEW.sweep_next_sessid<=OLD.sweep_next_sessid BEGIN SELECT RAISE(ABORT, 'save blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := mgr.Pause(id); err == nil || !strings.Contains(err.Error(), "save blocked") {
		t.Fatalf("cursor save failure was reported as successful: %v", err)
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || !row.SweepNextSessid.Valid || row.SweepNextSessid.Int64 < 1010 {
		t.Fatalf("failed save lost crash-safe reserved boundary: row=%+v err=%v", row, err)
	}
}

func sequentialTask(t *testing.T, db *store.DB, minimum, maximum int64) int64 {
	t.Helper()
	cfg := &config.TaskConfig{
		Proxy:     config.ProxyConfig{Template: "test-{num}:test@127.0.0.1:1"},
		TargetURL: "http://127.0.0.1/geo", Sessid: config.SessidRange{Min: minimum, Max: maximum, Mode: config.SessidSequential},
		DurationSeconds: 3600, Concurrency: 4, RequestTimeoutMs: 1000,
		PoolSweep: config.PoolSweepConfig{Enabled: true},
	}
	if err := cfg.Validate(); err != nil {
		t.Fatal(err)
	}
	id, err := db.CreateTask(context.Background(), "sequential", cfg.ToJSON())
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func waitTaskCondition(t *testing.T, condition func() bool) {
	t.Helper()
	deadline := time.NewTimer(5 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for !condition() {
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("task condition did not become true")
		}
	}
}
