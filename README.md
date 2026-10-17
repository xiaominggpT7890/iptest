# iptest — SOCKS5 IP 池探测工具

> 用 Go 实现的高并发 SOCKS5 出口 IP 质量评估工具。  
> 通过大量随机、递增循环或固定 `sessid` 调用 `cityinfo` 接口，回答三个问题：
> 1. **代理池有多大？** 独立 IP 总数 + 国家分布
> 2. **会话有多粘？** 同一 sessid 能保持同一 IP 多久（3min/10min 留存率）
> 3. **池子是否还在增长？** 滑动窗口命中率曲线 — 达阈值即可自动停

线上访问：

| 类型 | URL |
|---|---|
| **🌐 Web UI** | https://admin.dearka.cc/iptest/ |
| API 根 | https://admin.dearka.cc/iptest/api/ |
| 服务器 | `root@45.79.200.36` (CentOS 7) |
| 后端监听 | `127.0.0.1:8088` (systemd `iptest.service`) |
| 反代 | nginx `location ^~ /iptest/` |

---

## 一、目录结构

```
iptest/
├── cmd/iptest/main.go                  # 入口
├── internal/
│   ├── config/                         # 任务配置(JSON) + 校验
│   ├── proxy/                          # SOCKS5 client + cityinfo 解析
│   ├── store/                          # SQLite schema + 批量写入器 + 查询封装
│   ├── metrics/                        # 内存聚合器 + 滑动窗口命中率
│   ├── worker/                         # PoolSweeper + StickinessProbe
│   ├── task/                           # 任务生命周期(状态机/暂停/恢复/快照)
│   └── api/
│       ├── server.go                   # REST + SSE
│       └── static/                     # 前端 (单页 + Chart.js + Alpine.js)
├── deploy/
│   ├── iptest.service                  # systemd 单元
│   ├── deploy.sh                       # 一键编译+上传+重启
│   └── nginx-iptest.conf               # 反代示例(/iptest/ 子路径)
├── docs/
│   ├── DESIGN.md                       # 架构与并发模型详解
│   ├── DEPLOY.md                       # 部署/运维操作手册
│   └── TEST.md                         # 端到端测试与冒烟用例
├── go.mod / go.sum
└── README.md (本文件)
```

---

## 二、5 分钟上手

```bash
# 本地开发
go run ./cmd/iptest -listen :8088 -db ./iptest.db
# 浏览器打开 http://localhost:8088

# 部署到服务器
./deploy/deploy.sh deploy        # 编译 + 上传 + systemd 重启
./deploy/deploy.sh logs          # 跟日志
./deploy/deploy.sh restart       # 重启
```

详细请看：

- 📐 [docs/DESIGN.md](./docs/DESIGN.md) — 设计文档（架构、数据模型、并发策略）
- 🚀 [docs/DEPLOY.md](./docs/DEPLOY.md) — 部署文档（服务器、systemd、nginx）
- 🧪 [docs/TEST.md](./docs/TEST.md) — 测试文档（冒烟、压测、API 用例）
- 📜 [docs/CHANGELOG.md](./docs/CHANGELOG.md) — 变更日志（按需求变更追踪所有改动）

---

## 三、核心特性速览

| 类别 | 能力 |
|---|---|
| **任务管理** | 创建 / 启动 / 暂停 / 恢复 / 停止 / 清空数据 / 删除 |
| **两种 Worker** | PoolSweeper(高并发探池) + StickinessProbe(固定 sessid 跟踪) |
| **自动停止** | 时长到 / 滑动窗口命中率达阈值 |
| **数据持久化** | SQLite WAL + 批量事务写入 (单 writer goroutine) |
| **实时 UI** | SSE 推送，曲线/饼图/直方图 (Chart.js)，深色主题 |
| **报表** | 国家分布 / 命中率曲线 / 粘性直方图 / TOP 重复 IP / 错误分类 / 延迟 P50/P95/P99 |
| **生产部署** | 单二进制 (Go embed 前端) / systemd / 1M nofile / 跨平台编译 |

---

## 四、首条命令冒烟测试结果

实测（2 分钟，并发 50，5 个粘性探针）：

| 指标 | 数值 |
|---|---|
| 总请求 | 4664（约 39 QPS） |
| 独立 IP | **3666** |
| 国家分布 | US 3653, GB 5, CA 4, DE 3, NL 1 |
| 命中率窗口 | ~21% (池子远未探完) |
| 失败率 | 11.54% (主要是代理偶发 timeout) |
| 平均延迟 | 1307ms (P95 3646 / P99 5995) |
| 5 个粘性 sessid | 2 分钟内均未切换 ✅ (sesstime=600 一致) |

---

## 五、技术栈

- Go 1.21+ (使用 1.25.5 开发，1.21.5 部署)
- `github.com/gin-gonic/gin` — HTTP
- `modernc.org/sqlite` — 纯 Go SQLite (无需 CGO，单二进制)
- `golang.org/x/net/proxy` — SOCKS5 (socks5h 语义，DNS 由代理解析)
- 前端：原生 HTML + Alpine.js + Chart.js (CDN，零构建)
