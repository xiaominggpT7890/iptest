package store

import (
	"context"
	"database/sql"
	"strings"
	"sync"
	"testing"
)

func TestSweepCursorMigrationPreservesOldTaskData(t *testing.T) {
	path := t.TempDir() + "/legacy.db"
	legacy, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	oldSchema := strings.Replace(Schema, "report_cache_ts INTEGER,\n    sweep_next_sessid INTEGER", "report_cache_ts INTEGER", 1)
	if strings.Contains(oldSchema, "sweep_next_sessid") {
		t.Fatal("legacy schema still contains new cursor column")
	}
	if _, err := legacy.Exec(oldSchema); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`INSERT INTO tasks(id,name,config_json,state,created_at,elapsed_seconds,total_requests,total_success,unique_ips) VALUES(19,'old','{}','paused',1000,123,17,16,10)`); err != nil {
		t.Fatal(err)
	}
	if _, err := legacy.Exec(`INSERT INTO samples(task_id,worker_type,sessid,ip,ok,ts) VALUES(19,'sweep',55,'1.1.1.1',1,1000)`); err != nil {
		t.Fatal(err)
	}
	if err := legacy.Close(); err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		db, err := Open(path)
		if err != nil {
			t.Fatal(err)
		}
		row, err := db.GetTask(context.Background(), 19)
		if err != nil || row == nil || row.ElapsedSeconds != 123 || row.TotalRequests != 17 || row.TotalSuccess != 16 || row.UniqueIPs != 10 || row.SweepNextSessid.Valid {
			db.Close()
			t.Fatalf("migration damaged old task: row=%+v err=%v", row, err)
		}
		assertCount(t, db, "samples", 1)
		var columns int
		if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='sweep_next_sessid'`).Scan(&columns); err != nil || columns != 1 {
			db.Close()
			t.Fatalf("cursor migration is not idempotent: columns=%d err=%v", columns, err)
		}
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

func TestSweepCursorResetOnlyForRangeOrModeChange(t *testing.T) {
	db, err := Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	for _, tt := range []struct {
		name      string
		old, next string
		reset     bool
	}{
		{"name", `{"sessid_range":{"min":10,"max":99,"mode":"sequential"}}`, `{"sessid_range":{"min":10,"max":99,"mode":"sequential"}}`, false},
		{"other setting", `{"sessid_range":{"min":10,"max":99,"mode":"sequential"},"concurrency":10}`, `{"sessid_range":{"min":10,"max":99,"mode":"sequential"},"concurrency":20}`, false},
		{"legacy random", `{"sessid_range":{"min":10,"max":99}}`, `{"sessid_range":{"min":10,"max":99,"mode":"random"}}`, false},
		{"minimum", `{"sessid_range":{"min":10,"max":99,"mode":"sequential"}}`, `{"sessid_range":{"min":11,"max":99,"mode":"sequential"}}`, true},
		{"maximum", `{"sessid_range":{"min":10,"max":99,"mode":"sequential"}}`, `{"sessid_range":{"min":10,"max":100,"mode":"sequential"}}`, true},
		{"mode", `{"sessid_range":{"min":10,"max":99,"mode":"random"}}`, `{"sessid_range":{"min":10,"max":99,"mode":"sequential"}}`, true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			id, err := db.CreateTask(ctx, "old", []byte(tt.old))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := db.Exec(`UPDATE tasks SET sweep_next_sessid=42 WHERE id=?`, id); err != nil {
				t.Fatal(err)
			}
			if err := db.UpdateTask(ctx, id, "updated", []byte(tt.next)); err != nil {
				t.Fatal(err)
			}
			row, err := db.GetTask(ctx, id)
			if err != nil || row.SweepNextSessid.Valid == tt.reset || (!tt.reset && row.SweepNextSessid.Int64 != 42) {
				t.Fatalf("cursor reset=%v, row=%+v err=%v", tt.reset, row, err)
			}
		})
	}
}

func TestSweepCursorClearCopyAndUpdateFailure(t *testing.T) {
	db, err := Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	ctx := context.Background()
	cfg := []byte(`{"sessid_range":{"min":10,"max":99,"mode":"sequential"}}`)
	id, err := db.CreateTask(ctx, "source", cfg)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`UPDATE tasks SET sweep_next_sessid=42 WHERE id=?`, id); err != nil {
		t.Fatal(err)
	}
	copyID, err := db.CreateTask(ctx, "copy", cfg)
	if err != nil {
		t.Fatal(err)
	}
	start, _, err := db.ReserveSweepBlock(ctx, copyID, 10, 99, 1000)
	if err != nil || start != 10 {
		t.Fatalf("copy did not start at minimum: start=%d err=%v", start, err)
	}
	if _, err := db.Exec(`CREATE TRIGGER fail_edit BEFORE UPDATE OF config_json ON tasks BEGIN SELECT RAISE(ABORT, 'edit blocked'); END`); err != nil {
		t.Fatal(err)
	}
	if err := db.UpdateTask(ctx, id, "changed", []byte(`{"sessid_range":{"min":11,"max":99,"mode":"sequential"}}`)); err == nil {
		t.Fatal("failed edit was reported as successful")
	}
	row, err := db.GetTask(ctx, id)
	if err != nil || string(row.ConfigJSON) != string(cfg) || row.SweepNextSessid.Int64 != 42 {
		t.Fatalf("failed edit or copy changed source cursor/config: row=%+v err=%v", row, err)
	}
	if err := db.ClearTaskData(ctx, id); err != nil {
		t.Fatal(err)
	}
	row, err = db.GetTask(ctx, id)
	if err != nil || row.SweepNextSessid.Valid {
		t.Fatalf("clear did not reset cursor: row=%+v err=%v", row, err)
	}
	start, _, err = db.ReserveSweepBlock(ctx, id, 10, 99, 1000)
	if err != nil || start != 10 {
		t.Fatalf("clear did not restart at minimum: start=%d err=%v", start, err)
	}
}

func TestConcurrentSweepReservationsDoNotOverlapWithinRound(t *testing.T) {
	db, err := Open(t.TempDir() + "/iptest.db")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	id, err := db.CreateTask(context.Background(), "test", []byte(`{}`))
	if err != nil {
		t.Fatal(err)
	}
	type block struct {
		start, end int64
		err        error
	}
	blocks := make(chan block, 8)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			start, end, err := db.ReserveSweepBlock(context.Background(), id, 100, 399, 10)
			blocks <- block{start, end, err}
		}()
	}
	wg.Wait()
	close(blocks)
	seen := make(map[int64]bool)
	for b := range blocks {
		if b.err != nil || b.end-b.start != 10 || b.start < 100 || b.end > 400 {
			t.Fatalf("invalid reservation: %+v", b)
		}
		for n := b.start; n < b.end; n++ {
			if seen[n] {
				t.Fatalf("concurrent reservations both allocated %d", n)
			}
			seen[n] = true
		}
	}
}
