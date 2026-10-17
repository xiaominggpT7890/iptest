package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

type TaskState string

const (
	StateCreated   TaskState = "created"
	StateRunning   TaskState = "running"
	StatePaused    TaskState = "paused"
	StateCompleted TaskState = "completed"
	StateStopped   TaskState = "stopped"
	StateFailed    TaskState = "failed"
)

type TaskRow struct {
	ID              int64           `json:"id"`
	Name            string          `json:"name"`
	ConfigJSON      json.RawMessage `json:"config"`
	State           TaskState       `json:"state"`
	CreatedAt       int64           `json:"created_at"`
	StartedAt       sql.NullInt64   `json:"-"`
	PausedAt        sql.NullInt64   `json:"-"`
	FinishedAt      sql.NullInt64   `json:"-"`
	ElapsedSeconds  int64           `json:"elapsed_seconds"`
	TotalRequests   int64           `json:"total_requests"`
	TotalSuccess    int64           `json:"total_success"`
	TotalFailed     int64           `json:"total_failed"`
	UniqueIPs       int64           `json:"unique_ips"`
	UniqueCountries int64           `json:"unique_countries"`
	LastError       sql.NullString  `json:"-"`
	SweepNextSessid sql.NullInt64   `json:"-"`
}

func (t TaskRow) MarshalJSON() ([]byte, error) {
	type Alias TaskRow
	return json.Marshal(struct {
		Alias
		StartedAt       *int64 `json:"started_at,omitempty"`
		PausedAt        *int64 `json:"paused_at,omitempty"`
		FinishedAt      *int64 `json:"finished_at,omitempty"`
		LastError       string `json:"last_error,omitempty"`
		SweepNextSessid *int64 `json:"sweep_next_sessid"`
	}{
		Alias:           Alias(t),
		StartedAt:       nullInt64Ptr(t.StartedAt),
		PausedAt:        nullInt64Ptr(t.PausedAt),
		FinishedAt:      nullInt64Ptr(t.FinishedAt),
		LastError:       t.LastError.String,
		SweepNextSessid: nullInt64Ptr(t.SweepNextSessid),
	})
}

func nullInt64Ptr(n sql.NullInt64) *int64 {
	if !n.Valid {
		return nil
	}
	v := n.Int64
	return &v
}

func (db *DB) CreateTask(ctx context.Context, name string, configJSON []byte) (int64, error) {
	res, err := db.execWriteContext(ctx,
		`INSERT INTO tasks (name, config_json, state, created_at) VALUES (?, ?, 'created', ?)`,
		name, string(configJSON), time.Now().UnixMilli())
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func (db *DB) GetTask(ctx context.Context, id int64) (*TaskRow, error) {
	row := db.QueryRowContext(ctx, `SELECT id, name, config_json, state, created_at, started_at, paused_at, finished_at, elapsed_seconds, total_requests, total_success, total_failed, unique_ips, unique_countries, last_error, sweep_next_sessid FROM tasks WHERE id=?`, id)
	t := &TaskRow{}
	var cfg string
	if err := row.Scan(&t.ID, &t.Name, &cfg, &t.State, &t.CreatedAt, &t.StartedAt, &t.PausedAt, &t.FinishedAt, &t.ElapsedSeconds, &t.TotalRequests, &t.TotalSuccess, &t.TotalFailed, &t.UniqueIPs, &t.UniqueCountries, &t.LastError, &t.SweepNextSessid); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}
	t.ConfigJSON = json.RawMessage(cfg)
	return t, nil
}

func (db *DB) ListTasks(ctx context.Context) ([]*TaskRow, error) {
	rows, err := db.QueryContext(ctx, `SELECT id, name, config_json, state, created_at, started_at, paused_at, finished_at, elapsed_seconds, total_requests, total_success, total_failed, unique_ips, unique_countries, last_error, sweep_next_sessid FROM tasks ORDER BY id DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	list := []*TaskRow{}
	for rows.Next() {
		t := &TaskRow{}
		var cfg string
		if err := rows.Scan(&t.ID, &t.Name, &cfg, &t.State, &t.CreatedAt, &t.StartedAt, &t.PausedAt, &t.FinishedAt, &t.ElapsedSeconds, &t.TotalRequests, &t.TotalSuccess, &t.TotalFailed, &t.UniqueIPs, &t.UniqueCountries, &t.LastError, &t.SweepNextSessid); err != nil {
			return nil, err
		}
		t.ConfigJSON = json.RawMessage(cfg)
		list = append(list, t)
	}
	return list, nil
}

func (db *DB) UpdateTaskState(ctx context.Context, id int64, state TaskState, extra map[string]interface{}) error {
	parts := []string{"state=?"}
	args := []interface{}{string(state)}
	for k, v := range extra {
		parts = append(parts, k+"=?")
		args = append(args, v)
	}
	args = append(args, id)
	_, err := db.execWriteContext(ctx, "UPDATE tasks SET "+strings.Join(parts, ",")+" WHERE id=?", args...)
	return err
}

func (db *DB) UpdateTaskMetrics(ctx context.Context, id int64, total, success, failed, uniqueIPs, uniqueCountries int64) error {
	_, err := db.execWriteContext(ctx, `UPDATE tasks SET total_requests=?, total_success=?, total_failed=?, unique_ips=?, unique_countries=? WHERE id=?`,
		total, success, failed, uniqueIPs, uniqueCountries, id)
	return err
}

func (db *DB) DeleteTask(ctx context.Context, id int64) error {
	_, err := db.execWriteContext(ctx, `DELETE FROM tasks WHERE id=?`, id)
	return err
}

func (db *DB) UpdateTask(ctx context.Context, id int64, name string, configJSON []byte) error {
	return db.withWriteTx(ctx, func(tx writeTransaction) error {
		var previous string
		if err := tx.QueryRowContext(ctx, `SELECT config_json FROM tasks WHERE id=?`, id).Scan(&previous); err != nil {
			return err
		}
		oldRange, err := sweepRange([]byte(previous))
		if err != nil {
			return err
		}
		newRange, err := sweepRange(configJSON)
		if err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `UPDATE tasks SET name=?, config_json=?, sweep_next_sessid=CASE WHEN ? THEN NULL ELSE sweep_next_sessid END, report_cache=NULL, report_cache_ts=NULL WHERE id=?`,
			name, string(configJSON), oldRange != newRange, id)
		return err
	})
}

func (db *DB) ClearTaskData(ctx context.Context, id int64) error {
	return db.withWriteTx(ctx, func(tx writeTransaction) error {
		if _, err := tx.ExecContext(ctx, `DELETE FROM samples WHERE task_id=?`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM probe_sessions WHERE task_id=?`, id); err != nil {
			return err
		}
		if _, err := tx.ExecContext(ctx, `DELETE FROM metrics_snapshots WHERE task_id=?`, id); err != nil {
			return err
		}
		_, err := tx.ExecContext(ctx, `UPDATE tasks SET total_requests=0,total_success=0,total_failed=0,unique_ips=0,unique_countries=0,elapsed_seconds=0,report_cache=NULL,report_cache_ts=NULL,sweep_next_sessid=NULL WHERE id=?`, id)
		return err
	})
}

// GetReportCache 返回缓存的报表 JSON(可能为 nil)
func (db *DB) GetReportCache(ctx context.Context, taskID int64) ([]byte, error) {
	var cache sql.NullString
	err := db.QueryRowContext(ctx, `SELECT report_cache FROM tasks WHERE id=?`, taskID).Scan(&cache)
	if err != nil {
		return nil, err
	}
	if !cache.Valid {
		return nil, nil
	}
	return []byte(cache.String), nil
}

// SetReportCache 写入缓存
func (db *DB) SetReportCache(ctx context.Context, taskID int64, body []byte) error {
	_, err := db.execWriteContext(ctx, `UPDATE tasks SET report_cache=?, report_cache_ts=? WHERE id=?`,
		string(body), time.Now().UnixMilli(), taskID)
	return err
}

// InvalidateReportCache 清空缓存(任务启动/清空数据时调用)
func (db *DB) InvalidateReportCache(ctx context.Context, taskID int64) error {
	_, err := db.execWriteContext(ctx, `UPDATE tasks SET report_cache=NULL, report_cache_ts=NULL WHERE id=?`, taskID)
	return err
}

// 报表查询

type CountryStat struct {
	Country string `json:"country"`
	Count   int64  `json:"count"`
}

func (db *DB) CountryDistribution(ctx context.Context, taskID int64) ([]CountryStat, error) {
	rows, err := db.QueryContext(ctx,
		`SELECT country, COUNT(*) FROM (
		   SELECT DISTINCT ip, COALESCE(country,'UNKNOWN') AS country
		   FROM samples WHERE task_id=? AND ok=1 AND ip IS NOT NULL
		 ) GROUP BY country ORDER BY 2 DESC`,
		taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []CountryStat{}
	for rows.Next() {
		var c CountryStat
		if err := rows.Scan(&c.Country, &c.Count); err != nil {
			return nil, err
		}
		out = append(out, c)
	}
	return out, nil
}

type ErrKindStat struct {
	Kind  string `json:"kind"`
	Count int64  `json:"count"`
}

func (db *DB) ErrorDistribution(ctx context.Context, taskID int64) ([]ErrKindStat, error) {
	rows, err := db.QueryContext(ctx, `SELECT COALESCE(err_kind,'unknown'), COUNT(*) FROM samples WHERE task_id=? AND ok=0 GROUP BY err_kind ORDER BY 2 DESC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ErrKindStat{}
	for rows.Next() {
		var s ErrKindStat
		if err := rows.Scan(&s.Kind, &s.Count); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

type IPTop struct {
	IP    string `json:"ip"`
	Count int64  `json:"count"`
}

func (db *DB) TopIPs(ctx context.Context, taskID int64, limit int) ([]IPTop, error) {
	rows, err := db.QueryContext(ctx, `SELECT ip, COUNT(*) FROM samples WHERE task_id=? AND ok=1 AND ip IS NOT NULL GROUP BY ip ORDER BY 2 DESC LIMIT ?`, taskID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []IPTop{}
	for rows.Next() {
		var s IPTop
		if err := rows.Scan(&s.IP, &s.Count); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

type MetricsPoint struct {
	TS            int64   `json:"ts"`
	TotalReqs     int64   `json:"total_reqs"`
	Success       int64   `json:"success"`
	Failed        int64   `json:"failed"`
	UniqueIPs     int64   `json:"unique_ips"`
	HitRateWindow float64 `json:"hit_rate"`
	QPS           float64 `json:"qps"`
}

func (db *DB) MetricsHistory(ctx context.Context, taskID int64) ([]MetricsPoint, error) {
	rows, err := db.QueryContext(ctx, `SELECT ts,total_reqs,success,failed,unique_ips,hit_rate_window,qps FROM metrics_snapshots WHERE task_id=? ORDER BY ts ASC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []MetricsPoint{}
	for rows.Next() {
		var p MetricsPoint
		if err := rows.Scan(&p.TS, &p.TotalReqs, &p.Success, &p.Failed, &p.UniqueIPs, &p.HitRateWindow, &p.QPS); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, nil
}

func (db *DB) InsertMetricsSnapshot(ctx context.Context, taskID int64, p MetricsPoint) error {
	_, err := db.execWriteContext(ctx, `INSERT INTO metrics_snapshots (task_id,ts,total_reqs,success,failed,unique_ips,hit_rate_window,qps) VALUES (?,?,?,?,?,?,?,?)`,
		taskID, p.TS, p.TotalReqs, p.Success, p.Failed, p.UniqueIPs, p.HitRateWindow, p.QPS)
	return err
}

// 粘性 session
type ProbeSession struct {
	Sessid             int64  `json:"sessid"`
	FirstIP            string `json:"first_ip"`
	CurrentIP          string `json:"current_ip"`
	IPChanges          int64  `json:"ip_changes"`
	PollCount          int64  `json:"poll_count"`
	FirstSeen          int64  `json:"first_seen"`
	LastSeen           int64  `json:"last_seen"`
	LongestHoldSeconds int64  `json:"longest_hold_seconds"`
	CurrentIPSinceMs   int64  `json:"current_ip_since"`
}

func (db *DB) ListProbeSessions(ctx context.Context, taskID int64) ([]ProbeSession, error) {
	rows, err := db.QueryContext(ctx, `SELECT sessid, COALESCE(first_ip,''), COALESCE(current_ip,''), ip_changes, poll_count, COALESCE(first_seen,0), COALESCE(last_seen,0), longest_hold_seconds, COALESCE(current_ip_since,0) FROM probe_sessions WHERE task_id=? ORDER BY sessid ASC`, taskID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []ProbeSession{}
	for rows.Next() {
		var s ProbeSession
		if err := rows.Scan(&s.Sessid, &s.FirstIP, &s.CurrentIP, &s.IPChanges, &s.PollCount, &s.FirstSeen, &s.LastSeen, &s.LongestHoldSeconds, &s.CurrentIPSinceMs); err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	return out, nil
}

// UpsertProbeSession 更新粘性会话状态; 返回是否换IP
func (db *DB) UpsertProbeSession(ctx context.Context, taskID, sessid int64, ip string, ts int64) (changed bool, err error) {
	err = db.withWriteTx(ctx, func(tx writeTransaction) error {
		var (
			curIP     sql.NullString
			curSince  sql.NullInt64
			longest   int64
			ipChanges int64
			pollCount int64
			firstSeen sql.NullInt64
		)
		txErr := tx.QueryRowContext(ctx, `SELECT current_ip, current_ip_since, longest_hold_seconds, ip_changes, poll_count, first_seen FROM probe_sessions WHERE task_id=? AND sessid=?`, taskID, sessid).Scan(&curIP, &curSince, &longest, &ipChanges, &pollCount, &firstSeen)
		exists := txErr == nil
		if txErr != nil && !errors.Is(txErr, sql.ErrNoRows) {
			return txErr
		}

		if !exists {
			_, txErr = tx.ExecContext(ctx,
				`INSERT INTO probe_sessions (task_id, sessid, first_ip, current_ip, ip_changes, poll_count, first_seen, last_seen, longest_hold_seconds, current_ip_since) VALUES (?,?,?,?,0,1,?,?,0,?)`,
				taskID, sessid, ip, ip, ts, ts, ts)
			return txErr
		}

		changed = curIP.Valid && curIP.String != ip && ip != ""
		if changed {
			// 锁定之前 IP 的保持时长
			if curSince.Valid {
				hold := (ts - curSince.Int64) / 1000
				if hold > longest {
					longest = hold
				}
			}
			ipChanges++
			_, txErr = tx.ExecContext(ctx,
				`UPDATE probe_sessions SET current_ip=?, current_ip_since=?, last_seen=?, poll_count=poll_count+1, ip_changes=?, longest_hold_seconds=? WHERE task_id=? AND sessid=?`,
				ip, ts, ts, ipChanges, longest, taskID, sessid)
		} else {
			// 同 IP, 更新最长持续时长
			if curSince.Valid && ip != "" {
				hold := (ts - curSince.Int64) / 1000
				if hold > longest {
					longest = hold
				}
			}
			ipStr := curIP.String
			if !curIP.Valid && ip != "" {
				ipStr = ip
			}
			_, txErr = tx.ExecContext(ctx,
				`UPDATE probe_sessions SET current_ip=?, last_seen=?, poll_count=poll_count+1, longest_hold_seconds=? WHERE task_id=? AND sessid=?`,
				ipStr, ts, longest, taskID, sessid)
		}
		return txErr
	})
	return changed, err
}

// SamplesPage 用于前端分页查看
type SamplesPage struct {
	Items []map[string]interface{} `json:"items"`
	Total int64                    `json:"total"`
}

func (db *DB) RecentSamples(ctx context.Context, taskID int64, limit int) (*SamplesPage, error) {
	var total int64
	if err := db.QueryRowContext(ctx, `SELECT COUNT(*) FROM samples WHERE task_id=?`, taskID).Scan(&total); err != nil {
		return nil, err
	}
	rows, err := db.QueryContext(ctx, `SELECT id, worker_type, sessid, COALESCE(ip,''), COALESCE(country,''), COALESCE(city,''), latency_ms, ok, COALESCE(err_kind,''), COALESCE(err_msg,''), ts FROM samples WHERE task_id=? ORDER BY id DESC LIMIT ?`, taskID, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	items := []map[string]interface{}{}
	for rows.Next() {
		var (
			id, sessid, latency, ts                    int64
			worker, ip, country, city, errKind, errMsg string
			ok                                         int64
		)
		if err := rows.Scan(&id, &worker, &sessid, &ip, &country, &city, &latency, &ok, &errKind, &errMsg, &ts); err != nil {
			return nil, err
		}
		items = append(items, map[string]interface{}{
			"id": id, "worker": worker, "sessid": sessid, "ip": ip, "country": country,
			"city": city, "latency_ms": latency, "ok": ok == 1, "err_kind": errKind,
			"err_msg": errMsg, "ts": ts,
		})
	}
	return &SamplesPage{Items: items, Total: total}, nil
}

// StickinessHistogram 计算 longest_hold_seconds 分桶
type StickinessBucket struct {
	Label string `json:"label"`
	Min   int64  `json:"min_seconds"`
	Max   int64  `json:"max_seconds"` // -1 = inf
	Count int64  `json:"count"`
}

func (db *DB) StickinessHistogram(ctx context.Context, taskID int64) ([]StickinessBucket, int64, int64, error) {
	buckets := []StickinessBucket{
		{"<1min", 0, 60, 0},
		{"1-3min", 60, 180, 0},
		{"3-10min", 180, 600, 0},
		{"10-30min", 600, 1800, 0},
		{">=30min", 1800, -1, 0},
	}
	rows, err := db.QueryContext(ctx, `SELECT longest_hold_seconds FROM probe_sessions WHERE task_id=?`, taskID)
	if err != nil {
		return nil, 0, 0, err
	}
	defer rows.Close()
	var ge3, ge10, total int64
	for rows.Next() {
		var hold int64
		if err := rows.Scan(&hold); err != nil {
			return nil, 0, 0, err
		}
		total++
		if hold >= 180 {
			ge3++
		}
		if hold >= 600 {
			ge10++
		}
		for i := range buckets {
			b := &buckets[i]
			if b.Max == -1 {
				if hold >= b.Min {
					b.Count++
					break
				}
			} else if hold >= b.Min && hold < b.Max {
				b.Count++
				break
			}
		}
	}
	return buckets, ge3, ge10, nil
}

// helper
func clampLimit(n, def, max int) int {
	if n <= 0 {
		return def
	}
	if n > max {
		return max
	}
	return n
}

var _ = fmt.Sprintf
var _ = clampLimit
