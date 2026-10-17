package store

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestConcurrentWritersDoNotLoseRows(t *testing.T) {
	db, err := Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	baseTaskID, err := db.CreateTask(ctx, "base", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}

	writer := NewBatchWriter(db, 5000, baseTaskID)

	const goroutines = 24
	const writesPerGoroutine = 50
	errs := make(chan error, goroutines)
	var wg sync.WaitGroup
	for worker := 0; worker < goroutines; worker++ {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for i := 0; i < writesPerGoroutine; i++ {
				if _, err := db.CreateTask(ctx, fmt.Sprintf("task-%d-%d", worker, i), []byte(`{}`)); err != nil {
					errs <- err
					return
				}
				writer.Submit(Sample{
					TaskID: baseTaskID, WorkerType: "sweep", Sessid: int64(worker*writesPerGoroutine + i),
					IP: "8.8.8.8", Country: "United States", LatencyMs: 1, OK: true, TS: time.Now().UnixMilli(),
				})
			}
		}(worker)
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent write failed: %v", err)
	}
	writer.Close()

	assertCount(t, db, "tasks", 1+goroutines*writesPerGoroutine)
	assertCount(t, db, "samples", goroutines*writesPerGoroutine)
}

func TestConcurrentProbeUpsertsAreSerialized(t *testing.T) {
	db, err := Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	taskID, err := db.CreateTask(ctx, "probe", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}

	const sessions = 200
	var wg sync.WaitGroup
	errs := make(chan error, sessions)
	for i := 0; i < sessions; i++ {
		wg.Add(1)
		go func(sessid int64) {
			defer wg.Done()
			if _, err := db.UpsertProbeSession(ctx, taskID, sessid, "1.1.1.1", time.Now().UnixMilli()); err != nil {
				errs <- err
			}
		}(int64(i + 1))
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("probe upsert failed: %v", err)
	}

	assertCount(t, db, "probe_sessions", sessions)
}

func TestSubmitContextUnblocksWhenTaskIsCancelled(t *testing.T) {
	writer := &BatchWriter{ch: make(chan Sample, 1), stop: make(chan struct{})}
	writer.ch <- Sample{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	started := time.Now()
	if writer.SubmitContext(ctx, Sample{}) {
		t.Fatal("cancelled submission was accepted")
	}
	if elapsed := time.Since(started); elapsed > 100*time.Millisecond {
		t.Fatalf("cancelled submission blocked for %s", elapsed)
	}
}

func assertCount(t *testing.T, db *DB, table string, want int) {
	t.Helper()
	var got int
	if err := db.QueryRow(`SELECT COUNT(*) FROM ` + table).Scan(&got); err != nil {
		t.Fatal(err)
	}
	if got != want {
		t.Fatalf("%s count=%d, want %d", table, got, want)
	}
}
