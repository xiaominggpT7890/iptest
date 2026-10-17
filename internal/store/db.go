package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"modernc.org/sqlite"
)

// Sample 是一次代理请求结果
type Sample struct {
	TaskID     int64
	WorkerType string // "sweep" | "probe"
	Sessid     int64
	IP         string
	Country    string
	Region     string
	City       string
	LatencyMs  int64
	OK         bool
	ErrKind    string
	ErrMsg     string
	TS         int64 // unix ms
}

// DB 包装 *sql.DB,并启用 WAL
type DB struct {
	*sql.DB
	writeGate chan struct{}
	stop      chan struct{}
	stopOnce  sync.Once
	loopWG    sync.WaitGroup
}

type writeTransaction interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryRowContext(context.Context, string, ...interface{}) *sql.Row
}

func Open(path string) (*DB, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=foreign_keys(ON)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(8)
	db.SetMaxIdleConns(4)
	if _, err := db.Exec(Schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	for _, sql := range []string{
		`ALTER TABLE tasks ADD COLUMN report_cache TEXT`,
		`ALTER TABLE tasks ADD COLUMN report_cache_ts INTEGER`,
	} {
		_, _ = db.Exec(sql)
	}
	var cursorColumn int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pragma_table_info('tasks') WHERE name='sweep_next_sessid'`).Scan(&cursorColumn); err != nil {
		db.Close()
		return nil, fmt.Errorf("inspect sweep cursor migration: %w", err)
	}
	if cursorColumn == 0 {
		if _, err := db.Exec(`ALTER TABLE tasks ADD COLUMN sweep_next_sessid INTEGER`); err != nil {
			db.Close()
			return nil, fmt.Errorf("migrate sweep cursor: %w", err)
		}
	}

	d := &DB{
		DB:        db,
		writeGate: make(chan struct{}, 1),
		stop:      make(chan struct{}),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	_, _, _, _ = d.checkpoint(ctx, "TRUNCATE")
	cancel()
	_, _ = db.Exec(`ANALYZE;`)

	d.loopWG.Add(1)
	go d.checkpointLoop()
	return d, nil
}

func (d *DB) Close() error {
	d.stopOnce.Do(func() { close(d.stop) })
	d.loopWG.Wait()
	return d.DB.Close()
}

func (d *DB) checkpointLoop() {
	defer d.loopWG.Done()
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for {
		select {
		case <-d.stop:
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			_, _, _, _ = d.checkpoint(ctx, "PASSIVE")
			cancel()
		}
	}
}

func (d *DB) checkpoint(ctx context.Context, mode string) (busy, logFrames, checkpointed int, err error) {
	if err = d.acquireWrite(ctx); err != nil {
		return 0, 0, 0, err
	}
	defer d.releaseWrite()
	err = d.QueryRowContext(ctx, `PRAGMA wal_checkpoint(`+mode+`)`).Scan(&busy, &logFrames, &checkpointed)
	return
}

func (d *DB) execWriteContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	if err := d.acquireWrite(ctx); err != nil {
		return nil, err
	}
	defer d.releaseWrite()
	return d.execBusyRetry(ctx, query, args...)
}

func (d *DB) withWriteTx(ctx context.Context, fn func(writeTransaction) error) error {
	if err := d.acquireWrite(ctx); err != nil {
		return err
	}
	defer d.releaseWrite()

	conn, err := d.Conn(ctx)
	if err != nil {
		return err
	}
	defer conn.Close()

	if _, err := execConnBusyRetry(ctx, conn, `BEGIN IMMEDIATE`); err != nil {
		return err
	}
	if err := fn(conn); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return err
	}
	if _, err := conn.ExecContext(ctx, `COMMIT`); err != nil {
		_, _ = conn.ExecContext(context.Background(), `ROLLBACK`)
		return err
	}
	return nil
}

func (d *DB) execBusyRetry(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		result, err := d.ExecContext(ctx, query, args...)
		if err == nil || !isBusyError(err) {
			return result, err
		}
		lastErr = err
		if err := waitBusyRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func execConnBusyRetry(ctx context.Context, conn *sql.Conn, query string, args ...interface{}) (sql.Result, error) {
	var lastErr error
	for attempt := 0; attempt < 4; attempt++ {
		result, err := conn.ExecContext(ctx, query, args...)
		if err == nil || !isBusyError(err) {
			return result, err
		}
		lastErr = err
		if err := waitBusyRetry(ctx, attempt); err != nil {
			return nil, err
		}
	}
	return nil, lastErr
}

func (d *DB) acquireWrite(ctx context.Context) error {
	select {
	case d.writeGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (d *DB) releaseWrite() {
	<-d.writeGate
}

func isBusyError(err error) bool {
	var sqliteErr *sqlite.Error
	return errors.As(err, &sqliteErr) && sqliteErr.Code()&0xff == 5
}

func waitBusyRetry(ctx context.Context, attempt int) error {
	timer := time.NewTimer(time.Duration(25*(1<<attempt)) * time.Millisecond)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// BatchWriter 批量写入 sample
type BatchWriter struct {
	db        *DB
	ch        chan Sample
	wg        sync.WaitGroup
	stop      chan struct{}
	flushDur  time.Duration
	batchMax  int
	taskID    int64
	broken    bool
	brokenMu  sync.Mutex
	closeOnce sync.Once
}

type flushResult int

const (
	flushOK flushResult = iota
	flushRetry
	flushBroken
)

func NewBatchWriter(db *DB, buf int, taskID int64) *BatchWriter {
	if buf <= 0 {
		buf = 100000
	}
	bw := &BatchWriter{
		db:       db,
		ch:       make(chan Sample, buf),
		stop:     make(chan struct{}),
		flushDur: 200 * time.Millisecond,
		batchMax: 1000,
		taskID:   taskID,
	}
	bw.wg.Add(1)
	go bw.loop()
	return bw
}

// Submit 提交一个样本；队列满时施加背压，任务取消后 SubmitContext 可立即退出。
func (b *BatchWriter) Submit(s Sample) {
	b.SubmitContext(context.Background(), s)
}

func (b *BatchWriter) SubmitContext(ctx context.Context, s Sample) bool {
	b.brokenMu.Lock()
	broken := b.broken
	b.brokenMu.Unlock()
	if broken {
		return false
	}
	select {
	case b.ch <- s:
		return true
	case <-ctx.Done():
		return false
	case <-b.stop:
		return false
	}
}

func (b *BatchWriter) setBroken() {
	b.brokenMu.Lock()
	b.broken = true
	b.brokenMu.Unlock()
}

func (b *BatchWriter) Close() {
	b.closeOnce.Do(func() { close(b.stop) })
	b.wg.Wait()
}

func (b *BatchWriter) loop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushDur)
	defer ticker.Stop()
	batch := make([]Sample, 0, b.batchMax)
	retryPending := false
	for {
		select {
		case <-b.stop:
			b.drain(batch)
			return
		case s := <-b.ch:
			batch = append(batch, s)
			if len(batch) >= b.batchMax && !retryPending {
				n := min(len(batch), b.batchMax)
				switch b.doFlush(batch[:n]) {
				case flushOK:
					batch = batch[n:]
				case flushBroken:
					batch = batch[:0]
				case flushRetry:
					retryPending = true
				}
			}
		case <-ticker.C:
			if len(batch) > 0 {
				n := min(len(batch), b.batchMax)
				switch b.doFlush(batch[:n]) {
				case flushOK:
					batch = batch[n:]
					retryPending = false
				case flushBroken:
					batch = batch[:0]
					retryPending = false
				case flushRetry:
					retryPending = true
				}
			}
		}
	}
}

func (b *BatchWriter) drain(batch []Sample) {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	drained := false
	for !drained {
		select {
		case <-ctx.Done():
			fmt.Printf("[batch_writer] task=%d shutdown timed out with %d queued samples\n", b.taskID, len(batch)+len(b.ch))
			return
		case s := <-b.ch:
			batch = append(batch, s)
		default:
			drained = true
		}
	}

	for len(batch) > 0 {
		n := min(len(batch), b.batchMax)
		result := flushRetry
		for attempt := 0; attempt < 3 && result == flushRetry; attempt++ {
			result = b.doFlushContext(ctx, batch[:n])
			if result == flushRetry {
				timer := time.NewTimer(time.Duration(100*(attempt+1)) * time.Millisecond)
				select {
				case <-timer.C:
				case <-ctx.Done():
					timer.Stop()
					fmt.Printf("[batch_writer] task=%d shutdown timed out with %d unflushed samples\n", b.taskID, len(batch))
					return
				}
			}
		}
		if result == flushBroken {
			b.setBroken()
			return
		}
		if result == flushRetry {
			fmt.Printf("[batch_writer] task=%d shutdown with %d unflushed samples\n", b.taskID, len(batch))
			return
		}
		batch = batch[n:]
	}
}

// doFlush 区分成功、可重试错误和任务已删除，避免临时锁冲突导致样本丢失。
func (b *BatchWriter) doFlush(batch []Sample) flushResult {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	return b.doFlushContext(ctx, batch)
}

func (b *BatchWriter) doFlushContext(ctx context.Context, batch []Sample) flushResult {
	if len(batch) == 0 {
		return flushOK
	}
	// 用单条 multi-VALUES insert,效率最高
	var sb strings.Builder
	sb.WriteString("INSERT INTO samples (task_id, worker_type, sessid, ip, country, region, city, latency_ms, ok, err_kind, err_msg, ts) VALUES ")
	args := make([]interface{}, 0, len(batch)*12)
	for i, s := range batch {
		if i > 0 {
			sb.WriteByte(',')
		}
		sb.WriteString("(?,?,?,?,?,?,?,?,?,?,?,?)")
		okVal := 0
		if s.OK {
			okVal = 1
		}
		args = append(args,
			s.TaskID, s.WorkerType, s.Sessid,
			nullIfEmpty(s.IP), nullIfEmpty(s.Country), nullIfEmpty(s.Region), nullIfEmpty(s.City),
			s.LatencyMs, okVal,
			nullIfEmpty(s.ErrKind), nullIfEmpty(s.ErrMsg),
			s.TS,
		)
	}
	if _, err := b.db.execWriteContext(ctx, sb.String(), args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY") {
			fmt.Printf("[batch_writer] task=%d flushed %d samples FAILED (task deleted), discarding %d\n", b.taskID, len(batch), len(batch))
			b.setBroken()
			return flushBroken
		}
		fmt.Printf("[batch_writer] flush %d failed: %v\n", len(batch), err)
		return flushRetry
	}
	return flushOK
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
