package api

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"net/http"
	"os"
	"runtime"
	"strconv"
	"strings"
	"time"

	"iptest/internal/config"
	"iptest/internal/metrics"
	"iptest/internal/store"
	"iptest/internal/task"

	"github.com/gin-gonic/gin"
)

var (
	osReadFile = os.ReadFile
	osReadDir  = os.ReadDir
)

//go:embed all:static
var staticFS embed.FS

type Server struct {
	db  *store.DB
	mgr *task.Manager
}

func NewServer(db *store.DB, mgr *task.Manager) *Server {
	return &Server{db: db, mgr: mgr}
}

func (s *Server) Router() *gin.Engine {
	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(gin.Recovery())

	api := r.Group("/api")
	api.GET("/tasks", s.listTasks)
	api.POST("/tasks", s.createTask)
	api.GET("/tasks/:id", s.getTask)
	api.PUT("/tasks/:id", s.updateTask)
	api.POST("/tasks/:id/clone", s.cloneTask)
	api.DELETE("/tasks/:id", s.deleteTask)
	api.POST("/tasks/:id/start", s.startTask)
	api.POST("/tasks/:id/pause", s.pauseTask)
	api.POST("/tasks/:id/stop", s.stopTask)
	api.POST("/tasks/:id/clear", s.clearTask)
	api.GET("/tasks/:id/report", s.report)
	api.GET("/tasks/:id/samples", s.recentSamples)
	api.GET("/tasks/:id/probes", s.probeSessions)
	api.GET("/tasks/:id/metrics", s.metricsHistory)
	api.GET("/tasks/:id/live", s.liveSSE)
	api.GET("/system", s.systemStatus)

	// 静态 UI
	sub, _ := fs.Sub(staticFS, "static")
	r.GET("/", func(c *gin.Context) {
		c.FileFromFS("./", http.FS(sub))
	})
	r.StaticFS("/ui", http.FS(sub))
	// 兜底 — 让 /xxx 路径(刷新页面)也能拿到 index.html
	r.NoRoute(func(c *gin.Context) {
		c.FileFromFS("./", http.FS(sub))
	})
	return r
}

// ------------------ handlers ------------------

func (s *Server) listTasks(c *gin.Context) {
	rows, err := s.db.ListTasks(c.Request.Context())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	items := make([]gin.H, 0, len(rows))
	for _, r := range rows {
		if next := s.mgr.CurrentSweepNext(r.ID); next != nil {
			r.SweepNextSessid = sql.NullInt64{Int64: *next, Valid: true}
		}
		rowJSON, _ := json.Marshal(r)
		var m map[string]interface{}
		_ = json.Unmarshal(rowJSON, &m)
		if s.mgr.IsRunning(r.ID) {
			if snap := s.mgr.CurrentSnapshot(r.ID); snap != nil {
				m["running"] = snap
			}
			m["elapsed_seconds_live"] = s.mgr.ElapsedSeconds(r.ID)
		}
		items = append(items, m)
	}
	c.JSON(200, items)
}

func (s *Server) createTask(c *gin.Context) {
	var body struct {
		Name   string             `json:"name"`
		Config *config.TaskConfig `json:"config"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if body.Name == "" {
		body.Name = fmt.Sprintf("task-%d", time.Now().Unix())
	}
	if body.Config == nil {
		c.JSON(400, gin.H{"error": "config required"})
		return
	}
	if err := body.Config.Validate(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	cfgJSON, _ := json.Marshal(body.Config)
	id, err := s.db.CreateTask(c.Request.Context(), body.Name, cfgJSON)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"id": id})
}

func (s *Server) getTask(c *gin.Context) {
	id := parseID(c)
	row, err := s.db.GetTask(c.Request.Context(), id)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if row == nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	if next := s.mgr.CurrentSweepNext(id); next != nil {
		row.SweepNextSessid = sql.NullInt64{Int64: *next, Valid: true}
	}
	c.JSON(200, row)
}

func (s *Server) deleteTask(c *gin.Context) {
	id := parseID(c)
	if err := s.mgr.Delete(id); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

func (s *Server) cloneTask(c *gin.Context) {
	id := parseID(c)
	var body struct {
		Name string `json:"name"`
	}
	_ = c.ShouldBindJSON(&body)
	row, err := s.db.GetTask(c.Request.Context(), id)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	if row == nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	name := body.Name
	if name == "" {
		name = row.Name + " (copy)"
	}
	newID, err := s.db.CreateTask(c.Request.Context(), name, row.ConfigJSON)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"id": newID})
}

func (s *Server) updateTask(c *gin.Context) {
	id := parseID(c)
	if s.mgr.IsRunning(id) {
		c.JSON(400, gin.H{"error": "task is running; pause it before editing"})
		return
	}
	var body struct {
		Name   string             `json:"name"`
		Config *config.TaskConfig `json:"config"`
	}
	if err := c.ShouldBindJSON(&body); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if body.Config == nil {
		c.JSON(400, gin.H{"error": "config required"})
		return
	}
	if err := body.Config.Validate(); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	row, err := s.db.GetTask(c.Request.Context(), id)
	if err != nil || row == nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}
	cfgJSON, _ := json.Marshal(body.Config)
	name := body.Name
	if name == "" {
		name = row.Name
	}
	if err := s.db.UpdateTask(c.Request.Context(), id, name, cfgJSON); err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

func (s *Server) startTask(c *gin.Context) {
	id := parseID(c)
	if err := s.mgr.Start(id); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

func (s *Server) pauseTask(c *gin.Context) {
	id := parseID(c)
	if err := s.mgr.Pause(id); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

func (s *Server) stopTask(c *gin.Context) {
	id := parseID(c)
	if err := s.mgr.Stop(id); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

func (s *Server) clearTask(c *gin.Context) {
	id := parseID(c)
	if err := s.mgr.Clear(id); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, gin.H{"ok": true})
}

func (s *Server) report(c *gin.Context) {
	id := parseID(c)
	ctx := c.Request.Context()
	row, err := s.db.GetTask(ctx, id)
	if err != nil || row == nil {
		c.JSON(404, gin.H{"error": "not found"})
		return
	}

	terminal := row.State == store.StateCompleted || row.State == store.StateStopped ||
		row.State == store.StateFailed
	if terminal {
		if cached, _ := s.db.GetReportCache(ctx, id); len(cached) > 0 {
			c.Data(200, "application/json; charset=utf-8", cached)
			return
		}
	}

	if s.mgr.IsRunning(id) {
		if d := s.mgr.CurrentDetailed(id, 50); d != nil {
			c.JSON(200, s.fastReport(row, d))
			return
		}
	}

	countries, _ := s.db.CountryDistribution(ctx, id)
	errs, _ := s.db.ErrorDistribution(ctx, id)
	tops, _ := s.db.TopIPs(ctx, id, 50)
	hist, ge3, ge10, _ := s.db.StickinessHistogram(ctx, id)
	probes, _ := s.db.ListProbeSessions(ctx, id)

	var avgLat float64
	var p95Lat, p99Lat int64
	_ = s.db.QueryRowContext(ctx, `SELECT COALESCE(AVG(latency_ms),0) FROM samples WHERE task_id=? AND ok=1`, id).Scan(&avgLat)

	var sampleSize int64
	const desired int64 = 20000
	if row.TotalSuccess <= desired {
		_ = s.db.QueryRowContext(ctx,
			`SELECT latency_ms FROM samples WHERE task_id=? AND ok=1 ORDER BY latency_ms LIMIT 1 OFFSET CAST(? AS INTEGER)`,
			id, row.TotalSuccess*95/100).Scan(&p95Lat)
		_ = s.db.QueryRowContext(ctx,
			`SELECT latency_ms FROM samples WHERE task_id=? AND ok=1 ORDER BY latency_ms LIMIT 1 OFFSET CAST(? AS INTEGER)`,
			id, row.TotalSuccess*99/100).Scan(&p99Lat)
	} else {
		sampleSize = desired
		off95 := sampleSize * 95 / 100
		off99 := sampleSize * 99 / 100
		_ = s.db.QueryRowContext(ctx,
			`SELECT latency_ms FROM (SELECT latency_ms FROM samples WHERE task_id=? AND ok=1 ORDER BY id DESC LIMIT ?) ORDER BY latency_ms LIMIT 1 OFFSET ?`,
			id, sampleSize, off95).Scan(&p95Lat)
		_ = s.db.QueryRowContext(ctx,
			`SELECT latency_ms FROM (SELECT latency_ms FROM samples WHERE task_id=? AND ok=1 ORDER BY id DESC LIMIT ?) ORDER BY latency_ms LIMIT 1 OFFSET ?`,
			id, sampleSize, off99).Scan(&p99Lat)
	}

	successCount := row.TotalSuccess
	uniqueIPs := row.UniqueIPs
	var avgPerIP float64
	if uniqueIPs > 0 {
		avgPerIP = float64(successCount) / float64(uniqueIPs)
	}
	var dupRate float64
	if successCount > 0 {
		dupRate = float64(successCount-uniqueIPs) / float64(successCount)
	}
	var maxDupCount int64
	if len(tops) > 0 {
		maxDupCount = tops[0].Count
	}

	resp := gin.H{
		"task":               row,
		"countries":          countries,
		"errors":             errs,
		"top_ips":            tops,
		"stickiness":         hist,
		"stickiness_ge3min":  ge3,
		"stickiness_ge10min": ge10,
		"probe_sessions":     probes,
		"avg_latency":        avgLat,
		"p95_latency":        p95Lat,
		"p99_latency":        p99Lat,
		"dup_rate":           dupRate,
		"avg_per_ip":         avgPerIP,
		"max_dup_count":      maxDupCount,
	}
	if terminal {
		body, _ := json.Marshal(resp)
		_ = s.db.SetReportCache(ctx, id, body)
		c.Data(200, "application/json; charset=utf-8", body)
		return
	}
	c.JSON(200, resp)
}

func (s *Server) recentSamples(c *gin.Context) {
	id := parseID(c)
	limit, _ := strconv.Atoi(c.Query("limit"))
	if limit <= 0 {
		limit = 200
	}
	if limit > 2000 {
		limit = 2000
	}
	page, err := s.db.RecentSamples(c.Request.Context(), id, limit)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, page)
}

func (s *Server) probeSessions(c *gin.Context) {
	id := parseID(c)
	list, err := s.db.ListProbeSessions(c.Request.Context(), id)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, list)
}

func (s *Server) metricsHistory(c *gin.Context) {
	id := parseID(c)
	list, err := s.db.MetricsHistory(c.Request.Context(), id)
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, list)
}

// liveSSE 实时推送当前任务快照
func (s *Server) liveSSE(c *gin.Context) {
	id := parseID(c)
	c.Writer.Header().Set("Content-Type", "text/event-stream")
	c.Writer.Header().Set("Cache-Control", "no-cache")
	c.Writer.Header().Set("Connection", "keep-alive")
	c.Writer.Header().Set("X-Accel-Buffering", "no")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	flusher, ok := c.Writer.(http.Flusher)
	if !ok {
		c.JSON(500, gin.H{"error": "streaming unsupported"})
		return
	}

	for {
		select {
		case <-c.Request.Context().Done():
			return
		case <-ticker.C:
			snap := s.mgr.CurrentSnapshot(id)
			elapsed := s.mgr.ElapsedSeconds(id)
			running := snap != nil
			data := gin.H{
				"running":         running,
				"elapsed_seconds": elapsed,
				"snapshot":        snap,
			}
			b, _ := json.Marshal(data)
			fmt.Fprintf(c.Writer, "data: %s\n\n", b)
			flusher.Flush()
		}
	}
}

func parseID(c *gin.Context) int64 {
	v, _ := strconv.ParseInt(c.Param("id"), 10, 64)
	return v
}

func (s *Server) fastReport(row *store.TaskRow, d *metrics.DetailedSnapshot) gin.H {
	if next := s.mgr.CurrentSweepNext(row.ID); next != nil {
		row.SweepNextSessid = sql.NullInt64{Int64: *next, Valid: true}
	}
	ctx := context.Background()
	hist, ge3, ge10, _ := s.db.StickinessHistogram(ctx, row.ID)
	probes, _ := s.db.ListProbeSessions(ctx, row.ID)

	errs := make([]store.ErrKindStat, 0, len(d.ErrKinds))
	for k, v := range d.ErrKinds {
		errs = append(errs, store.ErrKindStat{Kind: k, Count: v})
	}
	for i := 1; i < len(errs); i++ {
		for j := i; j > 0 && errs[j].Count > errs[j-1].Count; j-- {
			errs[j], errs[j-1] = errs[j-1], errs[j]
		}
	}

	row.TotalRequests = d.TotalReqs
	row.TotalSuccess = d.Success
	row.TotalFailed = d.Failed
	row.UniqueIPs = d.UniqueIPs
	row.UniqueCountries = d.UniqueCountries

	return gin.H{
		"task":               row,
		"countries":          d.Countries,
		"errors":             errs,
		"top_ips":            d.TopIPs,
		"stickiness":         hist,
		"stickiness_ge3min":  ge3,
		"stickiness_ge10min": ge10,
		"probe_sessions":     probes,
		"avg_latency":        d.AvgLatencyMs,
		"p95_latency":        0,
		"p99_latency":        0,
		"dup_rate":           d.DupRate,
		"avg_per_ip":         d.AvgPerIP,
		"max_dup_count":      d.MaxDupCnt,
		"_fast":              true,
	}
}

func (s *Server) systemStatus(c *gin.Context) {
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	la, _ := readLoadAvg()
	fd := countOpenFDs()
	c.JSON(200, gin.H{
		"goroutines":    runtime.NumGoroutine(),
		"num_cpu":       runtime.NumCPU(),
		"go_version":    runtime.Version(),
		"heap_alloc_mb": ms.HeapAlloc / 1024 / 1024,
		"heap_sys_mb":   ms.HeapSys / 1024 / 1024,
		"sys_mb":        ms.Sys / 1024 / 1024,
		"gc_runs":       ms.NumGC,
		"load_avg":      la,
		"open_fds":      fd,
		"ts":            time.Now().UnixMilli(),
	})
}

func readLoadAvg() ([3]float64, error) {
	var la [3]float64
	b, err := osReadFile("/proc/loadavg")
	if err != nil {
		return la, err
	}
	fields := strings.Fields(string(b))
	if len(fields) >= 3 {
		la[0], _ = strconv.ParseFloat(fields[0], 64)
		la[1], _ = strconv.ParseFloat(fields[1], 64)
		la[2], _ = strconv.ParseFloat(fields[2], 64)
	}
	return la, nil
}

func countOpenFDs() int {
	entries, err := osReadDir("/proc/self/fd")
	if err != nil {
		return -1
	}
	return len(entries)
}
