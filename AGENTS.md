# iptest 项目惯例

> Agent 进入这个项目时必读。

## 文档惯例

### 每次需求变更必须更新 CHANGELOG

文件：`docs/CHANGELOG.md`

- 在**顶部**添加新条目（最新的在最上面）
- 必须包含四个字段：
  - **背景** — 用户为什么提这个需求 / 看到了什么问题
  - **改动** — 改了哪些文件 / 加了哪些函数 / 改了哪些 SQL
  - **影响范围** — 性能、兼容性、迁移成本
  - **验证** — 怎么证明改完是对的（实测数字最好）

### 涉及破坏性变更必须明确

- 数据库 schema 变更 → 必须在 `internal/store/db.go` 的 `Open()` 里加 `ALTER TABLE` 自动迁移（不破坏老库）
- 前端 CSS/JS 变更 → HTML 里 `?v=N` 递增（CloudFlare 缓存对策）
- nginx 配置变更 → 远端 `cp xxx.conf xxx.conf.bak.iptest.$(date +%s)` 备份

## 代码惯例

### 注释最少化
- 公共函数/类型可以有简短文档注释（解释意图，不解释代码）
- 内部实现细节**不**写注释 — 代码自解释
- 复杂算法 / 安全 / 性能优化 / 正则 / 数学公式 可以注释（说明 why 不说明 what）

### 数据库
- 所有"按 task_id 全表扫描"的查询必须有覆盖索引
- 终态任务（completed/stopped/paused/failed）的报表结果必须缓存到 `tasks.report_cache`
- 启动 / 清空数据 / 删除任务时必须 invalidate 缓存

### UI
- 浅色主题，遵循 `style.css` 顶部的 CSS 变量
- 图标统一用 Lucide（`<i data-lucide="...">`）
- 操作按钮统一 28×28 icon-only，title 悬浮提示

## 部署惯例

```bash
./deploy/deploy.sh deploy   # 默认目标 root@45.79.200.36
./deploy/deploy.sh logs
./deploy/deploy.sh status
```

如果任务正在运行，部署前先 `curl -X POST .../pause` 暂停（避免半路丢数据），部署后再 `curl -X POST .../start` 恢复。

## 公网入口（已生效）

- **UI**: https://admin.dearka.cc/iptest/
- **API 根**: https://admin.dearka.cc/iptest/api/
