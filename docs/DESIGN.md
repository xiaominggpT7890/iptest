# iptest — 设计文档

## 一、问题与目标

待测对象是一个支持 sessid 粘性的 SOCKS5 代理：

```
user-{account}-region-{region}-sessid-{N}-sesstime-{ttl}:{password}@gate3.abcsocks.cloud:9102
```

`sessid` 是核心变量：**同一个 sessid 在 sesstime 秒内理论上走同一个出口 IP**。我们要测的就是"理论"是否成立、以及池子的真实质量。

| 想知道的问题 | 测试方法 |
|---|---|
| 池子有多大？ | 用海量不同 sessid 请求，去重统计独立 IP |
| 地区分布？ | 通过 cityinfo 接口拿 country code，汇总 |
| 池子是否还在增长？ | 滑动窗口最近 N 次请求中"已见过 IP"的占比 |
| 同 sessid 能保持多久同一个 IP？ | 固定 N 个 sessid，按时间表轮询，记录切换时刻 |
| 失败率？错误分布？ | 区分 timeout / socks_err / http_status / parse / other |

---

## 二、核心抽象：两类 Worker

把"探池"和"粘性观测"统一抽象成两类 Worker，互不干扰，可同时启用：

### A. PoolSweeper（探池）

- 启动 N 个 goroutine（`config.concurrency`）
- 每个 goroutine 在 `sessid_range` 内**随机抽取** → 通过 SOCKS5 请求 cityinfo → 落库 → 立刻下一轮
- 不维护任何会话生命周期，纯暴力探池
- 回答：**池子有多大、地区分布、命中率曲线**

### B. StickinessProbe（粘性探针）

- 启动时从 `sessid_range` 随机挑出 `sessid_count` 个固定 sessid
- 每个 sessid 一个 goroutine，按 `poll_schedule_seconds` 时间表轮询
- 每次请求维护 `probe_sessions` 表：first_ip / current_ip / ip_changes / poll_count / longest_hold_seconds
- 回答：**同 sessid 能保持多久同一 IP、3min/10min 留存率**

### C. 自动停止：HitRate Watchdog

- 监控 PoolSweeper 的滑动窗口命中率（环形 buffer，O(1)）
- 达到 `stop_when_hit_rate_above` 即整体取消 ctx → 所有 worker 退出

---

## 三、并发架构

```
┌──────────────────────────────────────────────────────────────┐
│                Web UI (Alpine.js + Chart.js)                 │
└───────────────────────────┬──────────────────────────────────┘
                            │ REST + SSE (1s 推送)
┌───────────────────────────▼──────────────────────────────────┐
│                  HTTP Server (Gin, embed 前端)               │
└───────────────────────────┬──────────────────────────────────┘
                            │
                ┌───────────▼────────────┐
                │     Task Manager       │  ◄──► SQLite (元数据)
                │  (状态机 + ctx 控制)    │
                └───────────┬────────────┘
        ┌───────────────────┼───────────────────┐
        ▼                                       ▼
┌──────────────────┐                  ┌──────────────────┐
│   PoolSweeper    │                  │ StickinessProbe  │
│ (N goroutines)   │                  │ (N goroutines)   │
└────────┬─────────┘                  └────────┬─────────┘
         │ Sample{taskID, sessid, ip, country, latency, ok, err, ts}
         └─────────────────┬──────────────────┘
                           ▼
                ┌─────────────────────┐
                │ Sample Channel      │  buf=100000
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │ Batch DB Writer     │  单 goroutine
                │ 每 200ms 或 1000 条 │
                │ 1 个事务 multi-INSERT│
                └──────────┬──────────┘
                           ▼
                ┌─────────────────────┐
                │ SQLite (WAL)        │
                │ sync=NORMAL,fk=ON   │
                └─────────────────────┘
                           ▲
                ┌──────────┴──────────┐
                │ Metrics Aggregator  │  内存中聚合, 1s 写一条快照
                │ (sync.RWMutex + ringbuf) │
                └──────────┬──────────┘
                           ▼
                       SSE 推送
```

### 并发关键设计

| 问题 | 方案 |
|---|---|
| SOCKS5 高并发 | `golang.org/x/net/proxy` + 每 sessid 独立 `http.Transport`，TLS skip verify，`MaxIdleConnsPerHost=2`，单请求超时 |
| goroutine 数量 | PoolSweeper 总数 = `config.concurrency`；探针每 sessid 1 个，固定 |
| SQLite 写瓶颈 | 单 batch writer goroutine，每 200ms 或 1000 条 flush 一次，multi-row VALUES INSERT，单事务 |
| IP 去重 | 内存中 `map[string]int64`（带读写锁）；恢复时从 DB 重建 |
| 滑动窗口命中率 | 容量 N 的环形数组 + `map[ip]count`，O(1) 命中计算 |
| 文件描述符 | systemd `LimitNOFILE=1048576` |
| 暂停瞬时生效 | `context.WithCancel` 贯穿；Pause 取消后等所有 worker 退出再标记状态 |
| 实时 UI | SSE 每秒推送内存快照（不查 DB） |

---

## 四、任务生命周期（状态机）

```
  ┌─────────┐  start   ┌─────────┐  pause   ┌────────┐
  │ created │─────────►│ running │─────────►│ paused │
  └─────────┘          └────┬────┘          └───┬────┘
                            │                   │ start
                            │ stop/finish       ▼
                            │              ┌─────────┐
                            │              │ running │
                            │              └────┬────┘
                            ▼                   │
                       ┌──────────┐             │
                       │completed │◄────────────┘
                       │ /stopped │
                       └──────────┘
                       
  在任何非 running 状态都可 delete / clear-data
```

### 暂停/恢复语义

- **暂停**：取消 ctx → 等所有 in-flight 请求落库 → 累加 elapsed_seconds → 状态 paused
- **恢复**：从 DB 加载历史 samples，重建 IP/Country 集合 → 重新启动 worker → 剩余时长 = `duration - elapsed`
- **粘性探针的"接力"**：probe_sessions 表里记录每个 sessid 的 first_ip/current_ip，恢复时不重置，继续累加

### 删除 / 清空

- `DELETE /api/tasks/:id` → 级联删除 samples/probe_sessions/metrics_snapshots（外键 `ON DELETE CASCADE`）
- `POST /api/tasks/:id/clear` → 保留任务元数据，仅清空数据表 + 重置任务汇总指标

---

## 五、数据模型（SQLite）

```sql
-- 任务元数据
CREATE TABLE tasks (
    id, name, config_json, state,
    created_at, started_at, paused_at, finished_at, elapsed_seconds,
    total_requests, total_success, total_failed,    -- 汇总指标
    unique_ips, unique_countries,
    last_error
);

-- 每次请求样本(高写入表)
CREATE TABLE samples (
    id, task_id FK, worker_type ('sweep'|'probe'), sessid,
    ip, country, region, city,
    latency_ms, ok, err_kind, err_msg, ts
);
INDEX (task_id, ts), (task_id, ip), (task_id, sessid)

-- 粘性探针的 sessid 时间线汇总
CREATE TABLE probe_sessions (
    id, task_id FK, sessid,
    first_ip, current_ip, current_ip_since,
    ip_changes, poll_count,
    first_seen, last_seen,
    longest_hold_seconds,
    UNIQUE(task_id, sessid)
);

-- 指标快照(每秒一条, 用于绘图)
CREATE TABLE metrics_snapshots (
    id, task_id FK, ts,
    total_reqs, success, failed,
    unique_ips, hit_rate_window, qps
);
```

PRAGMA：`journal_mode=WAL`, `synchronous=NORMAL`, `cache_size=-64000`, `temp_store=MEMORY`, `foreign_keys=ON`

---

## 六、任务配置 JSON 结构

```jsonc
{
  "proxy": {
    "host": "gate3.abcsocks.cloud",
    "port": 9102,
    "account": "zphdrj4g",
    "password": "U9NR5Dr4",
    "region": "us",
    "sesstime": 600
  },
  "target_url": "https://api.skyvora.cc/appapi/geo/cityinfo",
  "sessid_range": { "min": 1000000, "max": 9000000 },
  "duration_seconds": 1800,           // 0 = 不限时
  "concurrency": 500,                 // PoolSweeper 并发
  "request_timeout_ms": 15000,

  "pool_sweep": {
    "enabled": true,
    "stop_when_hit_rate_above": 0.9,  // 0 = 关闭自动停
    "hit_rate_window_size": 1000
  },

  "stickiness_probe": {
    "enabled": true,
    "sessid_count": 30,
    "poll_schedule_seconds": [60, 60, 60, 120, 300, 600],
    "loop_schedule": true             // 时间表跑完后循环
  }
}
```

校验规则（`config.Validate`）：
- proxy.host/port/account/password 必填
- sessid_range.max > min
- concurrency ≤ 30000
- 至少启用 pool_sweep 或 stickiness_probe 之一

---

## 七、HTTP API

| Method | Path | 说明 |
|---|---|---|
| GET    | `/api/tasks` | 列出所有任务（含运行中实时快照） |
| POST   | `/api/tasks` | 创建任务 `{name, config}` |
| GET    | `/api/tasks/:id` | 单任务详情 |
| DELETE | `/api/tasks/:id` | 删除任务（级联） |
| POST   | `/api/tasks/:id/start` | 启动/恢复 |
| POST   | `/api/tasks/:id/pause` | 暂停 |
| POST   | `/api/tasks/:id/stop` | 终止（不能再恢复） |
| POST   | `/api/tasks/:id/clear` | 清空数据（保留任务） |
| GET    | `/api/tasks/:id/report` | 完整报表(国家/错误/粘性/TOP IP/延迟分位数) |
| GET    | `/api/tasks/:id/samples?limit=N` | 最近 N 条原始样本 |
| GET    | `/api/tasks/:id/probes` | 粘性 sessid 详情列表 |
| GET    | `/api/tasks/:id/metrics` | 历史指标快照(画曲线用) |
| GET    | `/api/tasks/:id/live` | SSE 实时推送内存快照 |

---

## 八、性能预算

**目标**：1万–2万并发，16G 内存机器

| 资源 | 估算 |
|---|---|
| 单请求耗时 | 200–800ms（取决于代理质量；实测平均 1.3s） |
| 5000 并发 × 平均 1s = 5000 QPS | DB 单 writer 完全跟得上（实测单事务 multi-INSERT > 5万行/秒） |
| 内存：IP 去重 map | ~50MB / 100万 IP |
| 内存：goroutine 栈 | 8KB × 5000 = 40MB |
| 文件描述符 | 每 in-flight 请求 ~2 fd（socks + tls）→ systemd 设 1M nofile |

**瓶颈实际来自**：
1. 代理网关本身吞吐
2. 单台机器到代理网关的网络延迟
3. cityinfo 接口的响应速度

---

## 九、容错与边界

- **代理失败**：分类落库（timeout / socks_err / http_status / parse / other），单个请求失败不影响整体
- **批量写入失败**：打印错误但不阻塞 worker（数据可能丢一批，但内存聚合器还在）
- **进程崩溃**：再次启动后 task 状态会停留在 running（DB 未更新），人工 stop 即可恢复 paused 后再 start
- **粘性 sessid 表竞争**：每次 upsert 用事务包裹，避免并发同 sessid 写花
- **SSE 客户端断线**：通过 `c.Request.Context().Done()` 检测，goroutine 自动退出
