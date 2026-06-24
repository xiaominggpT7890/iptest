# iptest — 部署文档

## 一、目标服务器

| 项 | 值 |
|---|---|
| 主机 | `root@45.79.200.36` |
| 系统 | CentOS 7.9 / kernel 3.10 / systemd 219 |
| 资源 | 6 核 / 15G 内存 |
| 入口域名 | `admin.dearka.cc`（已配 SSL，走 Cloudflare） |
| 子路径 | `/iptest/` → 反代到 `127.0.0.1:8088` |
| 部署路径 | `/call/iptest/`（与现有 server-zhy-api 同构） |
| 运行用户 | root |
| 进程托管 | systemd unit `iptest.service` |

> SSH 已配置免密登录（`ssh root@45.79.200.36` 直进）。

---

## 二、🌐 访问地址

| 用途 | URL |
|---|---|
| **Web UI** | `https://admin.dearka.cc/iptest/` |
| API 根   | `https://admin.dearka.cc/iptest/api/` |
| 任务列表 | `GET  https://admin.dearka.cc/iptest/api/tasks` |
| 创建任务 | `POST https://admin.dearka.cc/iptest/api/tasks` |
| 启动任务 | `POST https://admin.dearka.cc/iptest/api/tasks/:id/start` |
| 暂停 / 停止 / 清空 / 删除 | 见 [DESIGN.md#HTTP API](./DESIGN.md#七http-api) |
| 报表 | `GET  https://admin.dearka.cc/iptest/api/tasks/:id/report` |
| SSE 实时推送 | `GET  https://admin.dearka.cc/iptest/api/tasks/:id/live` |

UI 和 API 都从同一二进制内提供。前端用 `<base href="./">` + 相对路径，所以**直接挂任何子路径都不需要重新打包**。

---

## 三、首次部署（已完成）

```bash
# 在本地 mac:
cd /Users/myd/develop/zenzone/iptest

# 一键部署 (默认 HOST=root@45.79.200.36)
./deploy/deploy.sh deploy

# 自定义 HOST:
IPTEST_HOST=root@1.2.3.4 ./deploy/deploy.sh deploy
```

部署脚本会做：

1. `GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w"` → `/tmp/iptest-linux-amd64`
2. `scp` 上传二进制和 `iptest.service`
3. `systemctl daemon-reload && systemctl enable && restart`
4. 健康检查

> ✨ `modernc.org/sqlite` 是纯 Go 实现，**不需要 CGO**，单二进制（24MB）跨平台。

---

## 四、Nginx 反代（已生效）

文件：`/etc/nginx/conf.d/admin.dearka.cc.conf` 在 443 server 块的 `location /` 之前插入：

```nginx
# ============ iptest 反代 (子路径) ============
location ^~ /iptest/ {
    proxy_pass http://127.0.0.1:8088/;        # 注意末尾的 / — 剥掉 /iptest 前缀
    proxy_http_version 1.1;
    proxy_set_header Host $host;
    proxy_set_header X-Real-IP $remote_addr;
    proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
    proxy_set_header X-Forwarded-Proto $scheme;
    proxy_set_header X-Forwarded-Prefix /iptest;

    # SSE 不缓冲
    proxy_buffering off;
    proxy_cache off;
    proxy_read_timeout 24h;
    chunked_transfer_encoding on;

    client_max_body_size 100m;
}
location = /iptest {
    return 301 $scheme://$host/iptest/;
}
```

**关键点**：

- `^~` 前缀优先匹配，确保 `/iptest/` 不被通配 `location /` 兜底吃掉（Vue try_files 会把它 rewrite 成 `/index.html`，导致跳到原后台）
- `proxy_pass` 末尾的 `/` 让 nginx 剥掉 `/iptest` 前缀（`/iptest/api/x` → `/api/x`）
- SSE 必须 `proxy_buffering off`，否则实时推送看不到
- 备份原配置在 `/etc/nginx/conf.d/admin.dearka.cc.conf.bak.iptest.*`

修改完执行：

```bash
nginx -t && systemctl reload nginx
```

---

## 五、systemd unit

文件：`/etc/systemd/system/iptest.service`

```ini
[Unit]
Description=iptest - SOCKS5 IP pool probe
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=root
WorkingDirectory=/call/iptest
ExecStart=/call/iptest/iptest -listen 127.0.0.1:8088 -db /call/iptest/iptest.db
Restart=on-failure
RestartSec=3
LimitNOFILE=1048576
LimitNPROC=1048576
Environment=GOMAXPROCS=0

StandardOutput=syslog
StandardError=syslog
SyslogIdentifier=iptest

[Install]
WantedBy=multi-user.target
```

> CentOS 7 的 systemd (219) 不支持 `StandardOutput=append:...`，所以走 syslog。

常用命令：

```bash
systemctl status iptest
systemctl restart iptest
journalctl -u iptest -f               # 跟日志
journalctl -u iptest --since "5 min ago"
```

---

## 六、运维操作

```bash
./deploy/deploy.sh deploy     # 重新编译 + 上传 + 重启
./deploy/deploy.sh restart    # 只重启远端服务
./deploy/deploy.sh stop       # 停止远端服务
./deploy/deploy.sh status     # systemctl status
./deploy/deploy.sh logs       # tail -f 服务日志(journalctl)
./deploy/deploy.sh smoke      # 本地 SSH 隧道 + curl 烟雾测试
```

数据备份（SQLite 单文件）：

```bash
ssh root@45.79.200.36 'sqlite3 /call/iptest/iptest.db ".backup /tmp/iptest-$(date +%Y%m%d).db"'
scp root@45.79.200.36:/tmp/iptest-*.db ./
```

---

## 七、回滚

```bash
# 1. 停服务
ssh root@45.79.200.36 "systemctl stop iptest"

# 2. 还原 nginx
ssh root@45.79.200.36 "ls /etc/nginx/conf.d/admin.dearka.cc.conf.bak.iptest.*"
ssh root@45.79.200.36 "cp /etc/nginx/conf.d/admin.dearka.cc.conf.bak.iptest.<时间戳> /etc/nginx/conf.d/admin.dearka.cc.conf && nginx -t && systemctl reload nginx"

# 3. 彻底卸载
ssh root@45.79.200.36 "systemctl disable iptest && rm /etc/systemd/system/iptest.service && systemctl daemon-reload && rm -rf /call/iptest"
```

---

## 八、本地开发

```bash
cd /Users/myd/develop/zenzone/iptest
go run ./cmd/iptest -listen :8088 -db ./dev.db
# 浏览器: http://localhost:8088
```

热重启（每次改代码后）：

```bash
pkill -f "go run ./cmd/iptest" ; go run ./cmd/iptest &
```

---

## 九、扩到其它服务器

```bash
# 1. 部署到任意机器
IPTEST_HOST=root@<ip> ./deploy/deploy.sh deploy

# 2. 直接访问 (跳过 nginx)
ssh -L 18088:127.0.0.1:8088 root@<ip>
# 浏览器: http://localhost:18088
```

或开放端口（开发环境）：

修改 `deploy/iptest.service`，把 `-listen 127.0.0.1:8088` 改为 `-listen 0.0.0.0:8088`，然后：

```bash
ssh root@<ip> "firewall-cmd --add-port=8088/tcp --permanent && firewall-cmd --reload"
```

⚠️ 生产环境**不要**直接开 0.0.0.0，目前没有鉴权机制。务必走 nginx 反代 + 可选 basic auth：

```nginx
location ^~ /iptest/ {
    auth_basic "iptest";
    auth_basic_user_file /etc/nginx/.iptest_htpasswd;
    proxy_pass http://127.0.0.1:8088/;
    # ... 其余同上
}
```

```bash
htpasswd -c /etc/nginx/.iptest_htpasswd admin
```

---

## 十、故障排查

| 症状 | 排查 |
|---|---|
| `/iptest/` 404 | `nginx -t`、确认 `location ^~ /iptest/` 在 `location /` 之前；试 `curl -v http://127.0.0.1:8088/` 看后端是否活 |
| `/iptest/` 跳到原后台 | nginx 没 reload，或 `^~` 写漏了；`systemctl reload nginx` |
| API 跨域 | UI 和 API 同源同域，理论上不会有；如果走代理换域，加 nginx CORS header |
| SSE 实时数据不动 | nginx `proxy_buffering off;` 必须；CloudFlare 可能也会缓冲，关掉对应路径的缓存 |
| 失败率突然 > 50% | 代理网关挂了；`curl -x socks5h://... https://api.skyvora.cc/appapi/geo/cityinfo` 自测 |
| `too many open files` | `LimitNOFILE=1048576`；CentOS 7 默认 1024，systemd unit 已覆盖 |
| 数据库锁 | WAL 模式应不会出现；如出现重启服务 `systemctl restart iptest` |
