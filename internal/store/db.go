package store

import (
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "modernc.org/sqlite"
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

	_, _ = db.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`)
	_, _ = db.Exec(`ANALYZE;`)

	d := &DB{DB: db}
	go d.checkpointLoop()
	return d, nil
}

func (d *DB) checkpointLoop() {
	ticker := time.NewTicker(2 * time.Minute)
	defer ticker.Stop()
	for range ticker.C {
		_, _ = d.Exec(`PRAGMA wal_checkpoint(TRUNCATE);`)
	}
}

// BatchWriter 批量写入 sample
type BatchWriter struct {
	db       *DB
	ch       chan Sample
	wg       sync.WaitGroup
	stop     chan struct{}
	flushDur time.Duration
	batchMax int
	taskID   int64
	broken   bool
	brokenMu sync.Mutex
}

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

// Submit 非阻塞提交一个样本; 满了会阻塞;
// 如果 writer 已 broken（任务被删除），直接丢弃
func (b *BatchWriter) Submit(s Sample) {
	b.brokenMu.Lock()
	broken := b.broken
	b.brokenMu.Unlock()
	if broken {
		return
	}
	b.ch <- s
}

func (b *BatchWriter) setBroken() {
	b.brokenMu.Lock()
	b.broken = true
	b.brokenMu.Unlock()
}

func (b *BatchWriter) Close() {
	close(b.stop)
	b.wg.Wait()
}

func (b *BatchWriter) loop() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.flushDur)
	defer ticker.Stop()
	batch := make([]Sample, 0, b.batchMax)
	for {
		select {
		case <-b.stop:
			b.drain(batch)
			return
		case s := <-b.ch:
			batch = append(batch, s)
			if len(batch) >= b.batchMax {
				if b.doFlush(batch) {
					batch = batch[:0]
				} else {
					batch = nil
				}
			}
		case <-ticker.C:
			if len(batch) > 0 {
				if b.doFlush(batch) {
					batch = batch[:0]
				} else {
					batch = nil
				}
			}
		}
	}
}

func (b *BatchWriter) drain(batch []Sample) {
	for {
		select {
		case s := <-b.ch:
			batch = append(batch, s)
			if len(batch) >= b.batchMax {
				if !b.doFlush(batch) {
					b.setBroken()
				}
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				b.doFlush(batch)
			}
			return
		}
	}
}

// doFlush 写入一批样本, 返回 true 表示成功, false 表示 FK 约束失败(任务已删除)
func (b *BatchWriter) doFlush(batch []Sample) bool {
	if len(batch) == 0 {
		return true
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
	if _, err := b.db.Exec(sb.String(), args...); err != nil {
		if strings.Contains(err.Error(), "FOREIGN KEY") {
			fmt.Printf("[batch_writer] task=%d flushed %d samples FAILED (task deleted), discarding %d\n", b.taskID, len(batch), len(batch))
			b.setBroken()
			return false
		}
		fmt.Printf("[batch_writer] flush %d failed: %v\n", len(batch), err)
	}
	return true
}

func nullIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}
