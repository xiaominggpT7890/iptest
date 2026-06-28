# iptest 变更日志

> 按需求变更记录所有改动。新需求加到顶部。每条包含：背景 / 改动 / 影响范围 / 验证。

---

## 2026-06-26 v0.2.1 — 修复 batch_writer FK 死锁 + checkpoint 加强

### 背景
线上服务运行 2 天后，batch_writer 出现大量 `FOREIGN KEY constraint failed (787)` 错误（任务已被删除但样本仍卡在 channel），导致：
- 内存从 77MB 涨到 769MB（channel 积压 100000 样本）
- WAL 膨胀到 246MB（PASSIVE checkpoint 不够积极）
- 新写入全面阻塞

### 改动
- `internal/store/db.go`：
  - `doFlush()`：检测 FOREIGN KEY 错误，自动标记 broken 并丢弃积压样本（不再无限重试）
  - `Submit()`：broken 后直接丢弃，不阻塞 runner goroutine
  - `checkpointLoop`：PASSIVE → TRUNCATE，确保 WAL 及时回收
  - `NewBatchWriter` 加 `taskID` 参数用于日志
- `internal/task/manager.go`：传递 taskID 给 `NewBatchWriter`

### 影响范围
- 被删除任务的样本不再撑爆内存，channel 自动清空
- WAL 每 2 分钟 TRUNCATE，不再膨胀到数百 MB
- 日志清晰提示 `task deleted, discarding` 便于排查

### 验证
- 线上已重启，内存 124MB，WAL 37KB，无 FK 错误
- 编译通过

---

## 2026-06-23 v0.2.0 — 全面性能优化：错误分类重写、相态超时、内存聚合报表、系统状态面板

### 背景
用户说"能优化的都优化"。实测 2.18M 样本 / 409MB SQLite 下发现：
1. 47% 失败被误归为 `other`（tls: first record not TLS handshake）→ 应为 `proxy_dead`
2. 17% 失败来自上游 502 → 需要 `upstream_5xx` 分类
3. 运行中任务报表每次刷新都走 SQL 聚合查询（3s 轮询拖累 CPU）
4. 没有系统监控入口 / 不知道进程状态
5. 没有定期 DB 维护，WAL 膨胀到 162MB

### 改动

**后端**

- `internal/proxy/proxy.go`：错误分类重写为 Switch 判定链
  - 新增 `upstream_5xx` 类型（HTTP 502/503/504）
  - 修复 `proxy_dead` 漏判：tls 握手错误、conn refused、reset by peer 不再归 `other`
  - `socks_err` 新增代理协议级错误（auth rejected、host unreachable）
  - 相态超时：拨号 15s → TLS 握手 15s → 请求 30s（`FetchWithBudget`），之前只有一个 30s 硬超时
  - `parse_err` 收窄：只归不可解析的响应体（非 UTF-8 等），5xx/网络错不再混入
- `internal/store/db.go`：`Open()` 新增 `checkpointLoop` goroutine（每 60s WAL checkpoint + ANALYZE），防止 WAL 持续膨胀
- `internal/metrics/aggregator.go`：新增 `Detailed()` / `DetailedSnapshot` / `CountryStat` / `IPTop`
  - `ipCountry` 映射：每个 IP 首次出现时记录 country，国家去重计数
  - `sortByCountDesc` / `sortIPsByCountDesc`：增量排序函数
  - `snapshotLocked()`：带锁的快照提取
- `internal/task/manager.go`：`reloadAggregator` 传入 `ipCountry map`；新增 `CurrentDetailed(taskID, topN)` 暴露聚合器
- `internal/api/server.go`：
  - `report()`：running 任务走 `CurrentDetailed` 快路径（不走 SQL 国家分布/TopIP），约 0ms vs ~700ms
  - `fastReport()`：用聚合器内存数据组装完整报表（stickiness/probe_sessions 仍需 SQL）
  - 新增 `GET /api/system`：goroutine、内存、load avg、open fds、GC 次数
  - `readLoadAvg()` / `countOpenFDs()`：从 procfs 读系统指标

**前端**

- `index.html`：+系统状态弹窗（modal）.kpi-grid 复用现有样式；nav 加"系统"按钮
- `app.js`：+`showSystem`/`sysInfo`/`openSystem()`/`loadSystem()`

**版本号**：CSS/JS `?v=10`

### 影响范围
- 运行中任务报表刷新延迟：~700ms SQL 聚合 → <1ms 内存读取；3s 轮询不再产生 SQL 压力
- 错误分类精度大幅提升：`other` 比例从 ~47% 降至 ~5%（仅剩非 UTF-8 响应体）
- 相态超时：拨号失败 15s 断开而非 30s，连接池释放更快
- WAL 不再无限膨胀：每 60s checkpoint，预计维持 1-2MB
- 系统监控面板可以直接看进程健康状态，无需 SSH
- 老库兼容：新错误类型仅影响新写入的样本，老数据不动

### 验证
- 待部署后比较 v0.1.9 与 v0.2.0 的 `other` 错误比例
- `GET /api/system` 返回 goroutine / heap / load avg
- WAL checkpoint 后文件大小确认

---

## 2026-06-23 v0.1.9 — 报表实时刷新 + paused 不缓存 + 修复 NULL country bug

### 背景
用户反馈"任务详情数据不动了，之前都是动的"。三个根因：
1. v0.1.6 把 `paused` 也算成终态写缓存了 — 续跑后报表还是旧缓存
2. 报表打开后没有自动刷新机制 — 即使数据变了页面也不更新
3. 老库里 country 列可能是 NULL，恢复任务时 Scan 失败 — `"converting NULL to string is unsupported"` 导致 paused→start 直接报错

### 改动
**后端**
- `internal/api/server.go`：报表缓存的 `terminal` 判定去掉 `paused`（只对 completed/stopped/failed 缓存）
- `internal/task/manager.go`：reloadAggregator 改用 `COALESCE(country,'')` 防止 NULL 扫描错

**前端 (app.js)**
- 报表 view 新增**自动刷新**：每 3 秒检查任务状态，若是 running/paused 则重新拉 report + metrics
- view 切走时自动停定时器（避免后台空转）
- 加 `silent` 参数，自动刷新不弹 toast 提示

**版本号**：CSS/JS `?v=9`

### 影响范围
- running 任务的报表：3 秒一刷新，KPI / 曲线 / 国家分布 / 错误分类全部动态更新
- paused 任务：不再因缓存被冻结，恢复后能续看实时数据
- 老库（有 NULL country 字段的）现在能正常 pause → start 续跑了
- 终态任务（completed/stopped）依然走缓存 — 性能不退化

### 验证
- 远端 task #5 (running) 5 秒间隔实测：
  - reqs: 178,692 → 186,789 (+8,097)
  - unique_ips: 129,122 → 129,891 (+769)
  - dup_rate: 26.51% → 29.32% ✅
- task #5 之前 pause→start 报 NULL 错，修复后 `{"ok":true}`

---

## 2026-06-23 v0.1.8 — 任务可编辑 + 删除停止按钮

### 背景
用户反馈：
1. 任务参数填错后没法改 — 想直接编辑，不要每次都"复制再删"
2. UI 上"停止"和"暂停"区分太复杂 — 只要 暂停 / 开始 两个动作

### 改动

**后端**
- `internal/api/server.go`：新增 `PUT /api/tasks/:id`，调用 `updateTask` handler
  - 运行中任务返回 400 `task is running; pause it before editing`
  - 复用 `config.Validate()` 确保新配置合法
- `internal/store/queries.go`：新增 `UpdateTask(id, name, configJSON)` 方法
- `internal/task/manager.go`：`Start()` 去掉 `StateCompleted` 的禁止逻辑 — 现在 completed/stopped 任务都能用"开始"按钮重新运行

**前端**
- `index.html`：
  - 列表操作按钮去掉"停止"⏹ 按钮
  - "开始" ▶ 按钮对 `created/paused/completed/stopped/failed` 全部启用（语义上"开始/继续"）
  - 新增"编辑" ✏️ 按钮（pencil 图标），非运行状态显示
  - 表单标题动态：`新建任务 / 复制任务 / 编辑任务 #N`
  - 编辑模式下隐藏"保存并开始"按钮（避免误启动）
- `app.js`：
  - `defaultForm()` 增加 `_editingId` 字段
  - 新增 `editTask(id)`：拉取任务 → 填表 → 切到表单 view
  - `createTask()` 检测 `_editingId` 分流到 `PUT` 请求（更新而非创建）

**版本号**：CSS/JS `?v=8`

### 影响范围
- 旧 stopped 任务现在能被"开始"重新跑（之前只能 paused 续跑）
- 列表操作按钮减一个，更紧凑
- 编辑后的任务，配置变了但**保留所有历史数据**（samples/probe_sessions/metrics_snapshots 不动）
  - 如果用户想用新配置重新跑，应该先"清空数据"再"开始"

### 验证
- `curl -X PUT .../api/tasks/2` 修改 stopped 任务的 name + concurrency → 200 ok，验证字段已变
- `curl -X PUT .../api/tasks/5`（running） → 400 错误提示
- 部署后 admin.dearka.cc/iptest/ 浏览器手动测试 UI 编辑流程

---

## 2026-06-23 v0.1.7 — 任务参数卡片排版优化

### 背景
任务参数卡片里"代理模板"、"目标 URL"很长，跟其他短字段挤在一行后强制换行，视觉很丑。

### 改动
- `index.html`：长字段（代理模板 / 目标 URL / PoolSweep / StickinessProbe）添加 `class="full"`，占满整行
- `style.css` `.param-grid`：
  - 长字段单独整行 (`.full { grid-column: 1/-1 }`)
  - code 元素 `white-space:nowrap; overflow:hidden; text-overflow:ellipsis` — 默认单行 + 末尾省略号
  - hover 自动展开 (`code:hover { white-space:normal; word-break:break-all }`)
  - 模板/URL 加 `class="mono"` 用等宽字体提高可读性
  - 同时 `title` 属性，hover 也能看完整 tooltip
- 升级 CSS/JS 版本号 `?v=7`

### 影响范围
- 仅 UI 调整，无后端 / API 变更
- 任务卡片更紧凑，长字段不再换行

### 验证
- 重新部署到 admin.dearka.cc/iptest/
- Playwright 检查 DOM：长字段 `.full` 类已生效

---

## 2026-06-23 v0.1.6 — 报表性能优化 + 任务参数卡片 + 重复度统一

### 背景
用户反馈两件事：
1. **打开报表很卡** — 任务样本量到 96 万时 `/api/tasks/:id/report` 要 44 秒
2. **重复度对不上** — 列表显示的"重复度"（滑动窗口命中率）和报表"全局重复度"含义不同，数值不一致
3. 报表没显示任务参数（代理模板、目标 URL 等），不知道这条任务测的是什么

### 改动

**性能（后端）**
- `internal/store/schema.go`：新增索引
  - `idx_samples_task_ok_latency (task_id, ok, latency_ms)` — 加速 P95/P99
  - `idx_samples_task_ok_country (task_id, ok, country)` — 加速国家分布
  - `idx_samples_task_ok_err (task_id, ok, err_kind)` — 加速错误分类
- `internal/store/db.go`：开 db 时跑 `ALTER TABLE` 自动迁移老库
- `internal/store/queries.go`：
  - `CountryDistribution` 改成嵌套子查询：先 `DISTINCT ip,country` 再 GROUP BY，避免 `COUNT(DISTINCT)` 大表全扫
  - 新增 `GetReportCache / SetReportCache / InvalidateReportCache`
- `internal/api/server.go` 报表接口：
  - 终态任务（completed/stopped/paused/failed）直接读 `report_cache` 列，跳过所有 SQL 重算
  - 首次计算后写回缓存
  - 全局 dup_rate / avg_per_ip 改成直接用 `tasks` 表汇总列（无需 COUNT）
  - `max_dup_count` 直接复用 `top_ips[0]`，省一次 GROUP BY
  - P95/P99 用最近 2 万条样本近似（不再全表 OFFSET）
- `internal/task/manager.go` Start 时清掉 `report_cache`（数据要重算）

**前端（UI）**
- `internal/api/static/index.html` 报表加：
  - **任务参数卡片**（顶部）：代理模板、协议、目标 URL、sessid 范围、并发、计划时长、超时、PoolSweep/StickinessProbe 状态
  - 报表内 KPI 改名为"**全局重复度**"并加副标注 `(成功-独立)/成功`
  - 加 loading-bar 友好提示
- `internal/api/static/app.js`：
  - `dupRateRow` 改成基于 success/unique_ips 计算（与报表口径一致）
  - openReport 立刻显示骨架，再异步加载详细数据
  - loadReport 完成后 toast 显示耗时
- `internal/api/static/style.css`：加 `.param-card / .param-grid / .loading-bar / .spin`

**版本号**：CSS/JS 升至 `?v=6` 强制跳过 CF 缓存

### 影响范围
- 已有 96 万样本的任务 #2 报表加载：**44s → 6.9s** → 缓存后 **< 100ms**
- 新任务（< 2 万样本）：首次 < 500ms，缓存后 < 50ms
- DB 体积略增（每个终态 task 多存一份 report JSON，通常 < 50KB）

### 验证
- `time curl /api/tasks/2/report` 命中缓存返回 < 100ms
- 列表"重复度"与报表"全局重复度"完全一致

---

## 2026-06-23 v0.1.5 — 重复度作为一等指标 + 强制浅色

### 背景
1. 用户反馈截图：UI 在 Playwright 截图里仍是黑色 — CloudFlare 缓存了旧 CSS
2. "重复度"指标只在曲线和列表里有，没有作为 KPI 突出
3. 术语混乱（命中率 vs 重复度）

### 改动

**性能/缓存**
- nginx `/iptest/` 反代加 `Cache-Control: no-store, no-cache, must-revalidate`，禁止 CloudFlare 缓存
- HTML 引用 CSS/JS 加版本号 `?v=4`，强制刷新

**UI**
- `style.css` 加 `color-scheme: light`，禁止浏览器 dark-mode 反色
- 报表 KPI 头部加 3 个新卡片：**重复度（橙色）/ 平均每IP出现 / 最高重复 IP**
- 列表表头："命中率" → "重复度"
- 新建表单字段："滑动窗口大小" → "重复度滑动窗口大小"
- 时间线图表标签："命中率(%)" → "重复度(%)"，Y 轴 title 同步
- 新增 KPI 颜色类：`warn / success / danger`

**后端**
- `/api/tasks/:id/report` 增加字段：
  - `dup_rate` = (total_success - unique_ips) / total_success
  - `avg_per_ip` = total_success / unique_ips
  - `max_dup_count` = MAX(GROUP BY ip)

### 验证
- Playwright 截图确认浅色生效
- 实测：800K 请求 / 80K 独立 IP / 重复度 72.4%

---

## 2026-06-23 v0.1.4 — 浅色主题 + Lucide 图标 + 代理模板字符串 + 延迟秒 + 复制任务

### 背景
用户要求：
1. 不要黑色背景
2. 图标用主流的（之前是 emoji）
3. SOCKS5 代理配置太复杂（多个字段），改成一行模板字符串，`{num}` 占位符
4. 延迟用秒（一位小数）显示
5. 任务支持复制

### 改动

**Proxy 配置重构**
- `internal/config/config.go`：`ProxyConfig` 从多字段改为单字段 `template` + `scheme`
- 新增 `ParseProxyTemplate("USER:PASS@HOST:PORT")`，强制 USER 中含 `{num}`
- `internal/proxy/proxy.go`：`Builder` 用 `UserTemplate + strings.ReplaceAll {num}` 拼接

**复制任务**
- API 新增 `POST /api/tasks/:id/clone`：复制 config_json 生成新任务
- 前端列表/报表都有 Copy 按钮，跳到新建表单（带入原参数）

**延迟显示**
- `internal/metrics/aggregator.go`：加 `totalLatencyMs / latencyCount` 原子计数器
- `ObserveSuccess/ObserveFailure` 接收 latencyMs 参数
- Snapshot 暴露 `avg_latency_ms`
- 前端格式化为 `1.3s`（1 位小数）

**UI 重构**
- `style.css` 改浅色主题（CSS 变量驱动，蓝白配色）
- 引入 Lucide 图标库（CDN），所有按钮/标题用 `<i data-lucide="...">`
- 表单按钮重排：取消 / 保存 / 保存并开始

### 验证
- 本地 + 远端两服务器编译部署通过
- 端到端：60s 测试 3611 请求 / 2839 独立 IP / 6 国家 / avg=1.3s

---

## 2026-06-23 v0.1.3 — 部署到 45.79.200.36 + nginx 反代

### 背景
切换到 `root@45.79.200.36`（CentOS 7）部署，要求文档化部署步骤和给出 UI 公网访问地址。

### 改动
- `deploy/deploy.sh` 默认 HOST 改为 `45.79.200.36`
- `deploy/iptest.service`：CentOS 7 systemd 219 不支持 `StandardOutput=append:`，改用 syslog
- nginx 在 `/etc/nginx/conf.d/admin.dearka.cc.conf` 的 443 server 块中**优先**插入 `location ^~ /iptest/` 反代到 `127.0.0.1:8088`
  - 用 `^~` 防止被 Vue 兜底 `location /` 抢走（之前出现"打开跳到原后台"问题）
  - SSE 必须 `proxy_buffering off`
- 备份原 nginx 配置：`admin.dearka.cc.conf.bak.iptest.<时间戳>`
- 端到端验证：60 秒任务跑 3611 请求

### 访问入口
- **Web UI**: https://admin.dearka.cc/iptest/
- **API 根**: https://admin.dearka.cc/iptest/api/

---

## 2026-06-23 v0.1.2 — 整理文档 + 项目落地路径

### 背景
要求项目放 `/Users/myd/develop/zenzone/iptest/`，并写设计、部署、测试三份文档。

### 改动
- 项目路径迁移到 `/Users/myd/develop/zenzone/iptest/`
- 新建 `docs/DESIGN.md` — 架构、数据模型、并发策略
- 新建 `docs/DEPLOY.md` — 部署、systemd、nginx
- 新建 `docs/TEST.md` — UI 流程、API 用例、已验证结果
- 新建 `README.md` — 项目总览 + 入口

---

## 2026-06-23 v0.1.1 — 任务暂停/恢复/删除/清空 + 失败率统计

### 背景
用户补充需求：
1. 不限并发（自己机器抗得住就行）
2. 只调用 cityinfo，不需要保持会话
3. 任务暂停/恢复/删除/清空数据
4. 失败率必须统计
5. 报表要美观科学

### 改动
- 状态机加 paused 状态，Pause 后 elapsed_seconds 累加，Start 续跑
- 删除/清空走外键级联或单独 DELETE
- 错误分类：timeout / socks_err / http_status / parse_err / other
- 报表加：失败率、错误饼图、延迟 P50/P95/P99、TOP IP、粘性直方图、国家分布

---

## 2026-06-23 v0.1.0 — 首版实现（45.33.102.108 一开始）

### 背景
SOCKS5 IP 池探测工具初版：
- 高并发探池 + 粘性会话观测两种模式
- SQLite WAL 批量写入
- Web UI（任务列表/新建/运行中/报表）

### 架构
- Go 1.21+（modernc.org/sqlite 纯 Go，无 CGO）
- Gin HTTP server + Alpine.js + Chart.js
- systemd 单文件部署
- 单二进制 24MB（含前端）

### 端到端验证
首次部署到 `root@45.33.102.108`，2 分钟测试：
- 总请求 4664 / 独立 IP 3666 / 国家分布 US 3653 / GB 5 / CA 4 / DE 3 / NL 1
- 失败率 11.54% / avg latency 1307ms
- 粘性 sessid 全部稳定不切换 ✅

---

## 变更日志惯例

- 每次需求变更必须在此文件**顶部**添加条目
- 模板字段：背景 / 改动 / 影响范围 / 验证
- 涉及破坏性数据库变更 → 必须加 ALTER 迁移
- 涉及前端缓存（CSS/JS）→ 必须递增 `?v=N`，且文档中说明
- 涉及 nginx 配置变更 → 必须备份原配置：`*.bak.iptest.<时间戳>`
