package task

import (
	"context"
	"fmt"
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

	mu      sync.Mutex
	running map[int64]*runningTask // taskID -> handle
}

type runningTask struct {
	cancel       context.CancelFunc
	runner       *worker.Runner
	writer       *store.BatchWriter
	agg          *metrics.Aggregator
	startedAt    time.Time
	priorElapsed int64 // 之前累计运行秒数
	done         chan struct{}
}

func NewManager(db *store.DB) *Manager {
	return &Manager{
		db:      db,
		running: make(map[int64]*runningTask),
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

	writer := store.NewBatchWriter(m.db, 100000, taskID)
	winSize := cfg.PoolSweep.HitRateWindowSize
	if winSize <= 0 {
		winSize = 1000
	}
	agg := metrics.NewAggregator(winSize)

	// 恢复历史指标
	if err := m.reloadAggregator(taskID, agg); err != nil {
		return fmt.Errorf("reload metrics: %w", err)
	}

	runCtx, cancel := context.WithCancel(context.Background())
	runner := worker.NewRunner(runCtx, taskID, cfg, m.db, writer, agg)

	rt := &runningTask{
		cancel:       cancel,
		runner:       runner,
		writer:       writer,
		agg:          agg,
		startedAt:    time.Now(),
		priorElapsed: row.ElapsedSeconds,
		done:         make(chan struct{}),
	}

	m.mu.Lock()
	m.running[taskID] = rt
	m.mu.Unlock()

	// 状态更新为 running
	extra := map[string]interface{}{
		"started_at":       time.Now().UnixMilli(),
		"paused_at":        nil,
		"last_error":       nil,
		"report_cache":     nil,
		"report_cache_ts":  nil,
	}
	if err := m.db.UpdateTaskState(ctx, taskID, store.StateRunning, extra); err != nil {
		return err
	}

	// 后台运行
	go m.supervise(taskID, rt)
	// 后台指标快照
	go m.snapshotLoop(taskID, rt)

	return nil
}

func (m *Manager) supervise(taskID int64, rt *runningTask) {
	rt.runner.Run() // 阻塞直到 ctx 取消或自然完成
	rt.writer.Close()

	// 持久化最终指标
	snap := rt.agg.Snapshot()
	elapsed := rt.priorElapsed + int64(time.Since(rt.startedAt).Seconds())
	ctx := context.Background()
	_ = m.db.UpdateTaskMetrics(ctx, taskID, snap.TotalReqs, snap.Success, snap.Failed, snap.UniqueIPs, snap.UniqueCountries)

	m.mu.Lock()
	delete(m.running, taskID)
	m.mu.Unlock()

	// 判定最终状态
	row, _ := m.db.GetTask(ctx, taskID)
	if row == nil {
		close(rt.done)
		return
	}
	// supervise 退出后,supervise 自己不知道是 user-pause 还是 finished
	// 这里规则:如果 db 里已经被改成 paused/stopped/deleted,就别覆盖
	// 否则 = 自然完成 (达到时长或命中率)
	if row.State == store.StateRunning {
		_ = m.db.UpdateTaskState(ctx, taskID, store.StateCompleted, map[string]interface{}{
			"finished_at":     time.Now().UnixMilli(),
			"elapsed_seconds": elapsed,
		})
	}
	close(rt.done)
}

func (m *Manager) snapshotLoop(taskID int64, rt *runningTask) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()
	ctx := context.Background()
	var lastTotal int64
	var lastTS = time.Now()
	for {
		select {
		case <-rt.done:
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
			_ = m.db.InsertMetricsSnapshot(ctx, taskID, store.MetricsPoint{
				TS:            t.UnixMilli(),
				TotalReqs:     snap.TotalReqs,
				Success:       snap.Success,
				Failed:        snap.Failed,
				UniqueIPs:     snap.UniqueIPs,
				HitRateWindow: snap.HitRateWindow,
				QPS:           qps,
			})
			_ = m.db.UpdateTaskMetrics(ctx, taskID, snap.TotalReqs, snap.Success, snap.Failed, snap.UniqueIPs, snap.UniqueCountries)
		}
	}
}

func (m *Manager) reloadAggregator(taskID int64, agg *metrics.Aggregator) error {
	ctx := context.Background()
	row, err := m.db.GetTask(ctx, taskID)
	if err != nil || row == nil {
		return err
	}
	rows, err := m.db.QueryContext(ctx, `SELECT ip, COALESCE(country,'') FROM samples WHERE task_id=? AND ok=1 AND ip IS NOT NULL`, taskID)
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
	agg.Reload(ips, countries, ipCountry, row.TotalRequests, row.TotalSuccess, row.TotalFailed)
	return nil
}

// Pause 暂停
func (m *Manager) Pause(taskID int64) error {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if !ok {
		return fmt.Errorf("task %d not running", taskID)
	}
	rt.cancel()
	<-rt.done
	elapsed := rt.priorElapsed + int64(time.Since(rt.startedAt).Seconds())
	return m.db.UpdateTaskState(context.Background(), taskID, store.StatePaused, map[string]interface{}{
		"paused_at":       time.Now().UnixMilli(),
		"elapsed_seconds": elapsed,
	})
}

// Stop: 终止后不能再恢复
func (m *Manager) Stop(taskID int64) error {
	m.mu.Lock()
	rt, ok := m.running[taskID]
	m.mu.Unlock()
	if ok {
		rt.cancel()
		<-rt.done
	}
	return m.db.UpdateTaskState(context.Background(), taskID, store.StateStopped, map[string]interface{}{
		"finished_at": time.Now().UnixMilli(),
	})
}

// Delete: 必须先暂停/停止
func (m *Manager) Delete(taskID int64) error {
	m.mu.Lock()
	_, ok := m.running[taskID]
	m.mu.Unlock()
	if ok {
		if err := m.Stop(taskID); err != nil {
			return err
		}
	}
	return m.db.DeleteTask(context.Background(), taskID)
}

// Clear: 清空任务数据(保留任务元数据)
func (m *Manager) Clear(taskID int64) error {
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
