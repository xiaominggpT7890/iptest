package worker

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"iptest/internal/store"
)

func TestSweepCursorCyclesInOrder(t *testing.T) {
	db, id := cursorDB(t)
	cursor := &sweepCursor{db: db, taskID: id, minimum: 12, maximum: 15}
	for i, want := range []int64{12, 13, 14, 15, 12, 13, 14, 15, 12} {
		got, err := cursor.take(context.Background())
		if err != nil || got != want {
			t.Fatalf("take %d=%d, want %d, error=%v", i, got, want, err)
		}
	}
	if next := cursor.current(); next == nil || *next != 13 {
		t.Fatalf("next=%v, want 13", next)
	}
	if err := cursor.save(context.Background()); err != nil {
		t.Fatal(err)
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || !row.SweepNextSessid.Valid || row.SweepNextSessid.Int64 != 13 {
		t.Fatalf("precise cursor was not saved: row=%+v err=%v", row, err)
	}
}

func TestSweepCursorConcurrentCycles(t *testing.T) {
	for _, rounds := range []int64{1, 20} {
		t.Run(fmt.Sprintf("rounds_%d", rounds), func(t *testing.T) {
			db, id := cursorDB(t)
			cursor := &sweepCursor{db: db, taskID: id, minimum: 100, maximum: 196}
			const capacity int64 = 97
			var jobs atomic.Int64
			counts := make(map[int64]int64)
			var mu sync.Mutex
			var wg sync.WaitGroup
			errs := make(chan error, 32)
			for i := 0; i < 32; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for jobs.Add(1) <= capacity*rounds {
						sessid, err := cursor.take(context.Background())
						if err != nil {
							errs <- err
							return
						}
						mu.Lock()
						counts[sessid]++
						mu.Unlock()
					}
				}()
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				t.Fatal(err)
			}
			if len(counts) != int(capacity) {
				t.Fatalf("allocated %d distinct numbers, want %d", len(counts), capacity)
			}
			for sessid, count := range counts {
				if sessid < 100 || sessid > 196 || count != rounds {
					t.Fatalf("sessid=%d count=%d, want exactly %d per number", sessid, count, rounds)
				}
			}
			if next := cursor.current(); next == nil || *next != 100 {
				t.Fatalf("completed round did not wrap to minimum: next=%v", next)
			}
		})
	}
}

func TestSweepCursorResumeAndCrashReservation(t *testing.T) {
	db, id := cursorDB(t)
	first := &sweepCursor{db: db, taskID: id, minimum: 10, maximum: 3000}
	if got, err := first.take(context.Background()); err != nil || got != 10 {
		t.Fatalf("first=%d err=%v", got, err)
	}
	crashed := &sweepCursor{db: db, taskID: id, minimum: 10, maximum: 3000}
	if got, err := crashed.take(context.Background()); err != nil || got != 1010 {
		t.Fatalf("crash restarted at %d instead of skipping unused reservation: %v", got, err)
	}
	if err := first.save(context.Background()); err == nil {
		t.Fatal("stale runner rewound a newer reservation")
	}
	if err := crashed.save(context.Background()); err != nil {
		t.Fatal(err)
	}
	resumed := &sweepCursor{db: db, taskID: id, minimum: 10, maximum: 3000}
	if got, err := resumed.take(context.Background()); err != nil || got != 1011 {
		t.Fatalf("normal resume=%d, want precise next 1011: %v", got, err)
	}
}

func TestSweepCursorDatabaseErrors(t *testing.T) {
	db, id := cursorDB(t)
	cursor := &sweepCursor{db: db, taskID: id, minimum: 10, maximum: 3000}
	if _, err := db.Exec(`CREATE TRIGGER fail_reserve BEFORE UPDATE OF sweep_next_sessid ON tasks BEGIN SELECT RAISE(ABORT, 'cursor blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if _, err := cursor.take(context.Background()); err == nil || cursor.current() != nil {
		t.Fatal("failed reservation allocated a number or exposed progress")
	}
	if _, err := db.Exec(`DROP TRIGGER fail_reserve`); err != nil {
		t.Fatal(err)
	}
	if got, err := cursor.take(context.Background()); err != nil || got != 10 {
		t.Fatalf("retry=%d err=%v", got, err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_save BEFORE UPDATE OF sweep_next_sessid ON tasks WHEN NEW.sweep_next_sessid=11 BEGIN SELECT RAISE(ABORT, 'cursor save blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := cursor.save(context.Background()); err == nil {
		t.Fatal("failed precise cursor save was reported as successful")
	}
	row, err := db.GetTask(context.Background(), id)
	if err != nil || row.SweepNextSessid.Int64 != 1010 {
		t.Fatalf("failed save lost the safe reservation boundary: row=%+v err=%v", row, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := cursor.take(ctx); err == nil {
		t.Fatal("cancelled task claimed another number")
	}
}

func TestSweepReservationWaitsForTaskContext(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(fmt.Sprintf("cancelled_%v", cancelled), func(t *testing.T) {
			db, id := cursorDB(t)
			db.SetMaxOpenConns(1)
			conn, err := db.Conn(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			defer conn.Close()
			waits := db.Stats().WaitCount
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			cursor := &sweepCursor{db: db, taskID: id, minimum: 10, maximum: 3000}
			type allocation struct {
				sessid int64
				err    error
			}
			result := make(chan allocation, 1)
			go func() {
				sessid, err := cursor.take(ctx)
				result <- allocation{sessid, err}
			}()
			deadline := time.NewTimer(5 * time.Second)
			defer deadline.Stop()
			tick := time.NewTicker(time.Millisecond)
			defer tick.Stop()
			for db.Stats().WaitCount == waits {
				select {
				case <-tick.C:
				case <-deadline.C:
					t.Fatal("reservation did not wait for the held database connection")
				}
			}
			select {
			case got := <-result:
				t.Fatalf("active task failed instead of waiting for its connection: %+v", got)
			default:
			}
			if cancelled {
				cancel()
			} else if err := conn.Close(); err != nil {
				t.Fatal(err)
			}
			select {
			case got := <-result:
				if cancelled {
					if !errors.Is(got.err, context.Canceled) || cursor.current() != nil {
						t.Fatalf("cancelled queued reservation advanced or failed incorrectly: %+v", got)
					}
				} else if got.err != nil || got.sessid != 10 {
					t.Fatalf("queued reservation did not continue at minimum: %+v", got)
				}
			case <-deadline.C:
				t.Fatal("reservation did not respond to connection release or task cancellation")
			}
			if cancelled {
				if err := conn.Close(); err != nil {
					t.Fatal(err)
				}
				if got, err := cursor.take(context.Background()); err != nil || got != 10 {
					t.Fatalf("cancelled reservation consumed or stranded the first number: got=%d err=%v", got, err)
				}
			}
		})
	}
}

func cursorDB(t *testing.T) (*store.DB, int64) {
	t.Helper()
	db, err := store.Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	id, err := db.CreateTask(context.Background(), "test", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	return db, id
}
