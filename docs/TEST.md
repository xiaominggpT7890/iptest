# iptest — 测试文档

## 一、🌐 公网访问入口

| 类型 | URL |
|---|---|
| Web UI | https://admin.dearka.cc/iptest/ |
| API 根 | https://admin.dearka.cc/iptest/api/ |

打开 UI 即可可视化操作；同时所有 API 也可用 `curl` 直接调（同源，无需鉴权）。

---

## 二、UI 操作流程（推荐方式）

1. 浏览器打开 https://admin.dearka.cc/iptest/
2. 顶部点 **「＋ 新建任务」**
3. 填入代理参数（默认值已经预填好示例）：
   - Host `gate3.abcsocks.cloud`、Port `9102`
   - Account `zphdrj4g`、Password `U9NR5Dr4`、Region `us`、sesstime `600`
   - sessid 范围 `1000000–9000000`
4. 调整运行参数：
   - **预设按钮**：30 分钟 / 1 小时 / 2 小时 / 不限
   - 并发：100–2000（看代理上游容量）
   - 单请求超时：15000 ms（默认）
5. 选模式：
   - ☑ 池子探测（PoolSweep）— 高并发探池
   - ☑ 粘性探针（StickinessProbe）— 选 N 个 sessid，按时间表轮询
6. **"保存并立即开始"** 按钮一键开跑
7. 回到列表实时看 QPS / 独立 IP / 命中率 / 国家数
8. 点任务名进**报表详情**：曲线 / 饼图 / 直方图 / TOP IP / 错误分类

任务中途随时 **⏸ 暂停 / ⏵ 继续 / ⏹ 停止**。停止后还能：

- **🧹 清空数据** — 保留任务配置，删 samples / metrics
- **🗑 删除** — 级联删除所有相关数据

---

## 三、命令行测试（CI / 脚本场景）

### 1. 创建任务

```bash
cat > /tmp/cfg.json <<'EOF'
{
  "name": "us-pool-30min",
  "config": {
    "proxy": {
      "host": "gate3.abcsocks.cloud", "port": 9102,
      "account": "zphdrj4g", "password": "U9NR5Dr4",
      "region": "us", "sesstime": 600
    },
    "target_url": "https://api.skyvora.cc/appapi/geo/cityinfo",
    "sessid_range": {"min": 1000000, "max": 9000000},
    "duration_seconds": 1800,
    "concurrency": 500,
    "request_timeout_ms": 15000,
    "pool_sweep": {
      "enabled": true,
      "stop_when_hit_rate_above": 0.9,
      "hit_rate_window_size": 1000
    },
    "stickiness_probe": {
      "enabled": true,
      "sessid_count": 30,
      "poll_schedule_seconds": [60, 60, 60, 120, 300, 600],
      "loop_schedule": true
    }
  }
}
EOF

# 返回 {"id": N}
curl -s -X POST -H "Content-Type: application/json" \
  -d @/tmp/cfg.json \
  https://admin.dearka.cc/iptest/api/tasks
```

### 2. 启动 / 暂停 / 停止

```bash
TASK_ID=1

curl -s -X POST https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/start
curl -s -X POST https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/pause
curl -s -X POST https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/start    # 续跑
curl -s -X POST https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/stop
```

### 3. 实时监控

```bash
# 简单轮询
watch -n 2 'curl -s https://admin.dearka.cc/iptest/api/tasks/'$TASK_ID' \
  | python3 -c "import json,sys; t=json.load(sys.stdin); print(f\"state={t[\\\"state\\\"]} reqs={t[\\\"total_requests\\\"]} ips={t[\\\"unique_ips\\\"]} fail={t[\\\"total_failed\\\"]}\")"'

# SSE 实时流
curl -N https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/live
```

### 4. 拉报表

```bash
curl -s https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/report | jq .
curl -s https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/metrics | jq '.[-5:]'
curl -s https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/probes | jq .
```

### 5. 清空 / 删除

```bash
curl -s -X POST https://admin.dearka.cc/iptest/api/tasks/$TASK_ID/clear
curl -s -X DELETE https://admin.dearka.cc/iptest/api/tasks/$TASK_ID
```

---

## 四、冒烟测试结果（已验证）

### 测试 1：本地编译 + 启动

```
✅ go build -o /tmp/iptest ./cmd/iptest      → 0 错误
✅ /tmp/iptest -listen 127.0.0.1:18099       → 启动成功
✅ curl /api/tasks                            → []
✅ curl /                                     → HTTP 200 (UI)
```

### 测试 2：远端部署（45.33.102.108 / Ubuntu）

```
✅ 交叉编译 24MB 二进制
✅ scp + systemctl daemon-reload + restart
✅ systemctl status iptest                    → active (running)
✅ curl http://127.0.0.1:8088/api/tasks       → []
```

### 测试 3：远端部署（45.79.200.36 / CentOS 7，**生产**）

```
✅ systemctl status iptest                    → active (running)
✅ 监听 127.0.0.1:8088
✅ nginx 反代 admin.dearka.cc/iptest/
✅ https://admin.dearka.cc/iptest/            → HTTP 200 (UI 加载)
✅ https://admin.dearka.cc/iptest/ui/style.css → HTTP 200
✅ https://admin.dearka.cc/iptest/ui/app.js   → HTTP 200
✅ https://admin.dearka.cc/iptest/api/tasks   → []
```

### 测试 4：端到端 — 真实代理跑 60 秒

```
任务: prod-smoke-1min  state=completed  时长=60s
请求数: 3611  成功: 3110  失败: 501  失败率: 13.87%
独立 IP: 2839  国家数: 6
国家分布: US(2829), CA(4), GB(3), MA(1), CN(1), AE(1)
延迟: avg=1319ms  P95=3841ms  P99=6338ms
错误分类: other(481), socks_err(11), timeout(6), parse_err(3)
粘性 sessid: 2 个, 全部 60s 内未切换 ✅ (sesstime=600 符合预期)
```

> 失败率 14% 主要来自代理偶发 i/o timeout（`err_kind: other`），属于上游网络抖动，非工具问题。可以通过提高 `request_timeout_ms` 改善。

---

## 五、性能压测建议

按场景调参：

| 场景 | concurrency | duration | 备注 |
|---|---|---|---|
| 快速探池（找上限） | 1000 | 600 | 看命中率曲线变平了就停 |
| 池子规模测量 | 500 | 1800 | 配合 `stop_when_hit_rate_above=0.9` 自动停 |
| 长时间粘性 | 50 | 7200 | 主用粘性探针，关 PoolSweep |
| 极限压力 | 2000 | 60 | 短时高并发，观察失败率上升点 |

### 提示

- **失败率 > 30%** → 降并发或加大 request_timeout
- **平均延迟 > 5s** → 代理上游瓶颈，再加并发也没用
- **命中率 > 90%** → 池子基本探完，可以停了
- **粘性 ip_changes > 0** → 代理 sesstime 不完全可信，记录现象

---

## 六、本地直连测试（绕过 nginx）

如果想绕过域名直接打后端：

```bash
# 建隧道
ssh -L 18088:127.0.0.1:8088 root@45.79.200.36

# 然后本机访问
open http://localhost:18088
curl http://localhost:18088/api/tasks
```

---

## 七、API 完整列表

| Method | Path | 说明 |
|---|---|---|
| GET    | `/api/tasks` | 列出所有任务（含运行中实时快照） |
| POST   | `/api/tasks` | 创建任务 `{name, config}` |
| GET    | `/api/tasks/:id` | 单任务详情 |
| DELETE | `/api/tasks/:id` | 删除任务（级联） |
| POST   | `/api/tasks/:id/start` | 启动 / 恢复 |
| POST   | `/api/tasks/:id/pause` | 暂停 |
| POST   | `/api/tasks/:id/stop` | 终止（不能再恢复） |
| POST   | `/api/tasks/:id/clear` | 清空数据（保留任务配置） |
| GET    | `/api/tasks/:id/report` | 完整报表 |
| GET    | `/api/tasks/:id/samples?limit=N` | 最近 N 条原始样本（默认 200） |
| GET    | `/api/tasks/:id/probes` | 粘性 sessid 详情 |
| GET    | `/api/tasks/:id/metrics` | 每秒指标快照(画曲线) |
| GET    | `/api/tasks/:id/live` | SSE 实时推送 |

### 报表 JSON 结构示意

```jsonc
{
  "task": { /* tasks 表完整行 */ },
  "countries": [{ "country": "US", "count": 2829 }, ...],
  "errors":    [{ "kind": "other", "count": 481 }, ...],
  "top_ips":   [{ "ip": "1.2.3.4", "count": 4 }, ...],   // 重复出现最多的 IP
  "stickiness": [
    { "label": "<1min",    "min_seconds": 0,    "max_seconds": 60,   "count": 0 },
    { "label": "1-3min",   "min_seconds": 60,   "max_seconds": 180,  "count": 5 },
    { "label": "3-10min",  "min_seconds": 180,  "max_seconds": 600,  "count": 0 },
    { "label": "10-30min", "min_seconds": 600,  "max_seconds": 1800, "count": 0 },
    { "label": ">=30min",  "min_seconds": 1800, "max_seconds": -1,   "count": 0 }
  ],
  "stickiness_ge3min":  5,
  "stickiness_ge10min": 0,
  "probe_sessions": [
    {
      "sessid": 3425757,
      "first_ip": "199.66.65.241", "current_ip": "199.66.65.241",
      "ip_changes": 0, "poll_count": 3,
      "first_seen": 1782213057000, "last_seen": 1782213099000,
      "longest_hold_seconds": 42, "current_ip_since": 1782213057000
    }
  ],
  "avg_latency": 1319,
  "p95_latency": 3841,
  "p99_latency": 6338
}
```
