package task

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync"
	"time"

	"iptest/internal/config"
	"iptest/internal/metrics"
	"iptest/internal/store"
	"iptest/internal/worker"
)

// Manager 负责任务的生命周期管理:启动/暂停/恢复/停止/删除
type Manager struct {
	db *store.DB

	mu         sync.Mutex
	running    map[int64]*runningTask // taskID -> handle
	operations map[int64]*sync.Mutex
	recoveryMu sync.RWMutex
}

type runningTask struct {
	cancel         context.CancelFunc
	runner         *worker.Runner
	writer         *store.BatchWriter
	agg            *metrics.Aggregator
	startedAt      time.Time
	priorElapsed   int64 // 之前累计运行秒数
	done           chan struct{}
	snapshotCancel context.CancelFunc
	snapshotDone   chan struct{}
	finishErr      error
}

func NewManager(db *store.DB) *Manager {
	return &Manager{
		db:         db,
		running:    make(map[int64]*runningTask),
		operations: make(map[int64]*sync.Mutex),
	}
}

// ReconcileRunning 恢复上一个进程遗留的运行状态，必须在接收请求前调用。
func (m *Manager) ReconcileRunning(ctx context.Context) error {
	m.recoveryMu.Lock()
	defer m.recoveryMu.Unlock()
	m.mu.Lock()
	active := len(m.running) > 0
	m.mu.Unlock()
	if active {
		return fmt.Errorf("cannot reconcile while tasks are running")
	}
	_, err := m.db.ExecContext(ctx, `UPDATE tasks SET state=?, paused_at=?, report_cache=NULL, report_cache_ts=NULL WHERE state=?`,
		store.StatePaused, time.Now().UnixMilli(), store.StateRunning)
	return err
}

func (m *Manager) lockTask(taskID int64) func() {
	m.recoveryMu.RLock()
	m.mu.Lock()
	operation := m.operations[taskID]
	if operation == nil {
		operation = &sync.Mutex{}
		m.operations[taskID] = operation
	}
	m.mu.Unlock()
	operation.Lock()
	return func() {
		operation.Unlock()
		m.recoveryMu.RUnlock()
	}
}

// CurrentSnapshot 返回正在运行任务的当前实时快照(若已停止返回 nil)
func (m *Manager) CurrentSnapshot(taskID int64) *metrics.Snapshot {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	s := rt.agg.Snapshot()
	return &s
}

// CurrentDetailed 返回正在运行任务的详细快照(含国家/TopIP),不走 SQL
func (m *Manager) CurrentDetailed(taskID int64, topN int) *metrics.DetailedSnapshot {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		return nil
	}
	d := rt.agg.Detailed(topN)
	return &d
}

// CurrentSweepNext 返回顺序探测的实时下一号码。
func (m *Manager) CurrentSweepNext(taskID int64) *int64 {
	m.mu.Lock()
	rt := m.running[taskID]
	m.mu.Unlock()
	if rt == nil {
		return nil
	}
	return rt.runner.CurrentSweepNext()
}

// ElapsedSeconds 含本次运行 + 历史累积
func (m *Manager) ElapsedSeconds(taskID int64) int64 {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		return -1
	}
	return rt.priorElapsed + int64(time.Since(rt.startedAt).Seconds())
}

// Start 启动一个任务:created/paused/stopped 状态都可以启动
func (m *Manager) Start(taskID int64) error {
	unlock := m.lockTask(taskID)
	defer unlock()
	m.mu.Lock()
	if _, ok := m.running[taskID]; ok {
		m.mu.Unlock()
		return fmt.Errorf("task %d already running", taskID)
	}
	m.mu.Unlock()

	ctx := context.Background()
	row, err := m.db.GetTask(ctx, taskID)
	if err != nil {
		return err
	}
	if row == nil {
		return fmt.Errorf("task %d not found", taskID)
	}
	if row.State == store.StateRunning {
		return fmt.Errorf("task is already in running state")
	}

	cfg, err := config.ParseTaskConfig(row.ConfigJSON)
	if err != nil {
		return fmt.Errorf("invalid config: %w", err)
	}

	winSize := cfg.PoolSweep.HitRateWindowSize
	if winSize <= 0 {
		winSize = 1000
	}
	agg := metrics.NewAggregator(winSize)

	// 恢复历史指标
	if err := m.reloadAggregator(ctx, row, agg); err != nil {
		return fmt.Errorf("reload metrics: %w", err)
	}

	extra := map[string]interface{}{
		"started_at":      time.Now().UnixMilli(),
		"paused_at":       nil,
		"finished_at":     nil,
		"last_error":      nil,
		"report_cache":    nil,
		"report_cache_ts": nil,
	}
	if err := m.db.UpdateTaskState(ctx, taskID, store.StateRunning, extra); err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(context.Background())
	writer := store.NewBatchWriter(m.db, 100000, taskID)
	runner := worker.NewRunner(runCtx, taskID, cfg, m.db, writer, agg)
	snapshotCtx, snapshotCancel := context.WithCancel(context.Background())

	rt := &runningTask{
		cancel:         cancel,
		runner:         runner,
		writer:         writer,
		agg:            agg,
		startedAt:      time.Now(),
		priorElapsed:   row.ElapsedSeconds,
		done:           make(chan struct{}),
		snapshotCancel: snapshotCancel,
		snapshotDone:   make(chan struct{}),
	}

	m.mu.Lock()
	m.running[taskID] = rt
	m.mu.Unlock()

	// 后台运行
	go m.supervise(taskID, rt)
	// 后台指标快照
	go m.snapshotLoop(snapshotCtx, taskID, rt)

	return nil
}

func (m *Manager) supervise(taskID int64, rt *runningTask) {
	rt.runner.Run() // 阻塞直到 ctx 取消或自然完成
	rt.snapshotCancel()
	<-rt.snapshotDone
	rt.writer.Close()

	// 持久化最终指标
	snap := rt.agg.Snapshot()
	elapsed := rt.priorElapsed + int64(time.Since(rt.startedAt).Seconds())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	rt.finishErr = errors.Join(rt.runner.Err(), rt.runner.SaveSweepCursor(ctx), m.db.UpdateTaskMetrics(ctx, taskID, snap.TotalReqs, snap.Success, snap.Failed, snap.UniqueIPs, snap.UniqueCountries))

	// 判定最终状态
	row, err := m.db.GetTask(ctx, taskID)
	rt.finishErr = errors.Join(rt.finishErr, err)
	// supervise 退出后,supervise 自己不知道是 user-pause 还是 finished
	// 这里规则:如果 db 里已经被改成 paused/stopped/deleted,就别覆盖
	// 否则 = 自然完成 (达到时长或命中率)
	if row != nil && row.State == store.StateRunning {
		state := store.StateCompleted
		extra := map[string]interface{}{
			"finished_at":     time.Now().UnixMilli(),
			"elapsed_seconds": elapsed,
		}
		if rt.finishErr != nil {
			state = store.StateFailed
			extra["last_error"] = rt.finishErr.Error()
		}
		err := m.db.UpdateTaskState(ctx, taskID, state, extra)
		rt.finishErr = errors.Join(rt.finishErr, err)
	}
	if rt.finishErr != nil {
		log.Printf("[task] task=%d final persistence failed: %v", taskID, rt.finishErr)
	}
	m.mu.Lock()
	if m.running[taskID] == rt {
		delete(m.running, taskID)
	}
	close(rt.done)
	m.mu.Unlock()
}

func (m *Manager) snapshotLoop(ctx context.Context, taskID int64, rt *runningTask) {
	defer close(rt.snapshotDone)
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	lastTotal := rt.agg.Snapshot().TotalReqs
	var lastTS = time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case t := <-ticker.C:
			snap := rt.agg.Snapshot()
			dt := t.Sub(lastTS).Seconds()
			var qps float64
			if dt > 0 {
				qps = float64(snap.TotalReqs-lastTotal) / dt
			}
			lastTotal = snap.TotalReqs
			lastTS = t
			writeCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
			_ = m.db.InsertMetricsSnapshot(writeCtx, taskID, store.MetricsPoint{
				TS:            t.UnixMilli(),
				TotalReqs:     snap.TotalReqs,
				Success:       snap.Success,
				Failed:        snap.Failed,
				UniqueIPs:     snap.UniqueIPs,
				HitRateWindow: snap.HitRateWindow,
				QPS:           qps,
			})
			_ = m.db.UpdateTaskMetrics(writeCtx, taskID, snap.TotalReqs, snap.Success, snap.Failed, snap.UniqueIPs, snap.UniqueCountries)
			cancel()
		}
	}
}

func (m *Manager) reloadAggregator(ctx context.Context, row *store.TaskRow, agg *metrics.Aggregator) error {
	rows, err := m.db.QueryContext(ctx, `SELECT ip, COALESCE(country,'') FROM samples WHERE task_id=? AND ok=1 AND ip IS NOT NULL`, row.ID)
	if err != nil {
		return err
	}
	defer rows.Close()
	ips := make(map[string]int64, 1024)
	countries := make(map[string]int64, 32)
	ipCountry := make(map[string]string, 1024)
	for rows.Next() {
		var ip, country string
		if err := rows.Scan(&ip, &country); err != nil {
			return err
		}
		if _, seen := ips[ip]; !seen {
			ipCountry[ip] = country
			if country != "" {
				countries[country]++
			}
		}
		ips[ip]++
	}
	if err := rows.Err(); err != nil {
		return err
	}
	agg.Reload(ips, countries, ipCountry, row.TotalRequests, row.TotalSuccess, row.TotalFailed)
	return nil
}

// Pause 暂停
func (m *Manager) Pause(taskID int64) error {
	unlock := m.lockTask(taskID)
	defer unlock()
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		row, err := m.db.GetTask(context.Background(), taskID)
		if err != nil {
			return err
		}
		if row == nil || row.State != store.StateRunning {
			return fmt.Errorf("task %d not running", taskID)
		}
		return m.db.UpdateTaskState(context.Background(), taskID, store.StatePaused, map[string]interface{}{
			"paused_at":       time.Now().UnixMilli(),
			"report_cache":    nil,
			"report_cache_ts": nil,
		})
	}
	rt.cancel()
	<-rt.done
	elapsed := rt.priorElapsed + int64(time.Since(rt.startedAt).Seconds())
	err := m.db.UpdateTaskState(context.Background(), taskID, store.StatePaused, map[string]interface{}{
		"paused_at":       time.Now().UnixMilli(),
		"elapsed_seconds": elapsed,
	})
	return errors.Join(rt.finishErr, err)
}

// Stop: 终止后不能再恢复
func (m *Manager) Stop(taskID int64) error {
	unlock := m.lockTask(taskID)
	defer unlock()
	return m.stopLocked(taskID)
}

func (m *Manager) stopLocked(taskID int64) error {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if ok {
		rt.cancel()
		<-rt.done
	}
	err := m.db.UpdateTaskState(context.Background(), taskID, store.StateStopped, map[string]interface{}{
		"finished_at": time.Now().UnixMilli(),
	})
	if ok {
		return errors.Join(rt.finishErr, err)
	}
	return err
}

// Delete: 必须先暂停/停止
func (m *Manager) Delete(taskID int64) error {
	unlock := m.lockTask(taskID)
	defer unlock()
	m.mu.Lock()
	_, ok := m.running[taskID]
	m.mu.Unlock()
	if ok {
		if err := m.stopLocked(taskID); err != nil {
			return err
		}
	}
	return m.db.DeleteTask(context.Background(), taskID)
}

// Clear: 清空任务数据(保留任务元数据)
func (m *Manager) Clear(taskID int64) error {
	unlock := m.lockTask(taskID)
	defer unlock()
	m.mu.Lock()
	_, ok := m.running[taskID]
	m.mu.Unlock()
	if ok {
		return fmt.Errorf("stop the task before clearing data")
	}
	return m.db.ClearTaskData(context.Background(), taskID)
}

// Shutdown 停止所有正在运行的任务
func (m *Manager) Shutdown() {
	m.mu.Lock()
	ids := make([]int64, 0, len(m.running))
	for id := range m.running {
		ids = append(ids, id)
	}
	m.mu.Unlock()
	for _, id := range ids {
		_ = m.Pause(id)
	}
}

// IsRunning 是否有该任务在跑
func (m *Manager) IsRunning(taskID int64) bool {
	m.mu.Lock()
	_, ok := m.running[taskID]
	m.mu.Unlock()
	return ok
}
