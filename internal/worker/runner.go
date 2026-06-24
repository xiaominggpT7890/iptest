package worker

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"iptest/internal/config"
	"iptest/internal/metrics"
	"iptest/internal/proxy"
	"iptest/internal/store"
)

// Runner 跑一个任务的全部 worker
type Runner struct {
	TaskID int64
	Cfg    *config.TaskConfig
	DB     *store.DB
	Writer *store.BatchWriter
	Agg    *metrics.Aggregator

	// 外部传入的取消信号
	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	// 探池停止标志
	stopHitRate float64
}

func NewRunner(parent context.Context, taskID int64, cfg *config.TaskConfig, db *store.DB, writer *store.BatchWriter, agg *metrics.Aggregator) *Runner {
	ctx, cancel := context.WithCancel(parent)
	return &Runner{
		TaskID:      taskID,
		Cfg:         cfg,
		DB:          db,
		Writer:      writer,
		Agg:         agg,
		ctx:         ctx,
		cancel:      cancel,
		stopHitRate: cfg.PoolSweep.StopWhenHitRateGE,
	}
}

func (r *Runner) Stop() {
	r.cancel()
}

func (r *Runner) Wait() {
	r.wg.Wait()
}

// Run 启动所有 worker;阻塞直到全部结束
func (r *Runner) Run() {
	// 时长定时器
	if r.Cfg.DurationSeconds > 0 {
		t := time.AfterFunc(time.Duration(r.Cfg.DurationSeconds)*time.Second, func() {
			r.cancel()
		})
		defer t.Stop()
	}

	if r.Cfg.PoolSweep.Enabled {
		r.startPoolSweeper()
	}
	if r.Cfg.StickinessProbe.Enabled {
		r.startStickinessProbe()
	}

	// 命中率监控:每 2 秒检查一次,达到阈值停整个任务
	if r.Cfg.PoolSweep.Enabled && r.stopHitRate > 0 {
		r.wg.Add(1)
		go r.hitRateWatchdog()
	}

	r.wg.Wait()
}

func (r *Runner) hitRateWatchdog() {
	defer r.wg.Done()
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			if r.Agg.HitRate() >= r.stopHitRate {
				r.cancel()
				return
			}
		}
	}
}

func (r *Runner) startPoolSweeper() {
	conc := r.Cfg.Concurrency
	timeout := time.Duration(r.Cfg.RequestTimeoutMs) * time.Millisecond
	builder := &proxy.Builder{
		Scheme:       r.Cfg.Proxy.Scheme,
		UserTemplate: r.Cfg.Proxy.UserTemplate,
		Password:     r.Cfg.Proxy.Password,
		Host:         r.Cfg.Proxy.Host,
		Port:         r.Cfg.Proxy.Port,
		Timeout:      timeout,
	}
	minSess, maxSess := r.Cfg.Sessid.Min, r.Cfg.Sessid.Max
	for i := 0; i < conc; i++ {
		r.wg.Add(1)
		go func(id int) {
			defer r.wg.Done()
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(id)*7919))
			for {
				if r.ctx.Err() != nil {
					return
				}
				sessid := minSess + rng.Int63n(maxSess-minSess+1)
				r.fireOnce(builder, sessid, "sweep")
			}
		}(i)
	}
}

func (r *Runner) startStickinessProbe() {
	timeout := time.Duration(r.Cfg.RequestTimeoutMs) * time.Millisecond
	builder := &proxy.Builder{
		Scheme:       r.Cfg.Proxy.Scheme,
		UserTemplate: r.Cfg.Proxy.UserTemplate,
		Password:     r.Cfg.Proxy.Password,
		Host:         r.Cfg.Proxy.Host,
		Port:         r.Cfg.Proxy.Port,
		Timeout:      timeout,
	}
	// 从范围里抽出 sessidCount 个 sessid,固定使用
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	count := r.Cfg.StickinessProbe.SessidCount
	minS, maxS := r.Cfg.Sessid.Min, r.Cfg.Sessid.Max
	picked := make([]int64, 0, count)
	used := make(map[int64]struct{}, count)
	for len(picked) < count {
		s := minS + rng.Int63n(maxS-minS+1)
		if _, ok := used[s]; ok {
			continue
		}
		used[s] = struct{}{}
		picked = append(picked, s)
	}
	schedule := r.Cfg.StickinessProbe.PollScheduleSeconds
	loop := r.Cfg.StickinessProbe.LoopSchedule

	for _, sessid := range picked {
		r.wg.Add(1)
		go func(sessid int64) {
			defer r.wg.Done()
			// 立即先来一次
			r.fireOnce(builder, sessid, "probe")
			idx := 0
			for {
				if idx >= len(schedule) {
					if !loop {
						return
					}
					idx = 0
				}
				wait := time.Duration(schedule[idx]) * time.Second
				idx++
				select {
				case <-r.ctx.Done():
					return
				case <-time.After(wait):
				}
				r.fireOnce(builder, sessid, "probe")
			}
		}(sessid)
	}
}

// fireOnce 发起一次请求并落库
func (r *Runner) fireOnce(builder *proxy.Builder, sessid int64, workerType string) {
	startTS := time.Now()
	ctx, cancel := context.WithTimeout(r.ctx, builder.Timeout)
	defer cancel()

	client, err := builder.NewClient(sessid)
	if err != nil {
		r.recordFailure(sessid, workerType, startTS, proxy.ErrOther, err.Error())
		return
	}
	defer client.CloseIdleConnections()

	geo, ferr := proxy.Fetch(ctx, client, r.Cfg.TargetURL)
	latency := time.Since(startTS).Milliseconds()
	now := time.Now().UnixMilli()
	if ferr != nil {
		r.Agg.ObserveFailure(ferr.Kind, latency)
		r.Writer.Submit(store.Sample{
			TaskID:     r.TaskID,
			WorkerType: workerType,
			Sessid:     sessid,
			LatencyMs:  latency,
			OK:         false,
			ErrKind:    ferr.Kind,
			ErrMsg:     ferr.Msg,
			TS:         now,
		})
		return
	}
	r.Agg.ObserveSuccess(geo.IP, geo.Country, latency)
	r.Writer.Submit(store.Sample{
		TaskID:     r.TaskID,
		WorkerType: workerType,
		Sessid:     sessid,
		IP:         geo.IP,
		Country:    geo.Country,
		Region:     geo.Region,
		City:       geo.City,
		LatencyMs:  latency,
		OK:         true,
		TS:         now,
	})
	if workerType == "probe" {
		_, _ = r.DB.UpsertProbeSession(context.Background(), r.TaskID, sessid, geo.IP, now)
	}
}

func (r *Runner) recordFailure(sessid int64, workerType string, startTS time.Time, kind, msg string) {
	latency := time.Since(startTS).Milliseconds()
	r.Agg.ObserveFailure(kind, latency)
	r.Writer.Submit(store.Sample{
		TaskID:     r.TaskID,
		WorkerType: workerType,
		Sessid:     sessid,
		LatencyMs:  latency,
		OK:         false,
		ErrKind:    kind,
		ErrMsg:     msg,
		TS:         time.Now().UnixMilli(),
	})
}
