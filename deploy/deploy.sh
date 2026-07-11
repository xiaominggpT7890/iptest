#!/usr/bin/env bash
# ============================================================
# iptest 部署脚本 (Mac → 远端 root@47.84.225.130)
# 用法:
#   ./deploy/deploy.sh             # 编译 + 上传 + 重启
#   ./deploy/deploy.sh restart     # 只重启远端服务
#   ./deploy/deploy.sh logs        # 跟日志
#   ./deploy/deploy.sh status      # systemctl status
# ============================================================
set -euo pipefail

HOST="${IPTEST_HOST:-root@45.79.200.36}"
REMOTE_DIR="${IPTEST_REMOTE_DIR:-/call/iptest}"
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
REPO_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

action="${1:-deploy}"

case "$action" in
  deploy)
    echo "==[1/4] 交叉编译 linux/amd64..."
    cd "$REPO_DIR"
    GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -ldflags="-s -w" -o /tmp/iptest-linux-amd64 ./cmd/iptest
    ls -lh /tmp/iptest-linux-amd64

    echo "==[2/4] 远端准备目录..."
    ssh "$HOST" "mkdir -p $REMOTE_DIR && [ -f $REMOTE_DIR/iptest ] && (systemctl stop iptest 2>/dev/null || true) || true"

    echo "==[3/4] 上传二进制 + 服务文件..."
    scp /tmp/iptest-linux-amd64 "$HOST:$REMOTE_DIR/iptest.new"
    scp "$SCRIPT_DIR/iptest.service" "$HOST:/etc/systemd/system/iptest.service"

    echo "==[4/4] 原子替换 + 重启..."
    ssh "$HOST" "mv -f $REMOTE_DIR/iptest.new $REMOTE_DIR/iptest && chmod +x $REMOTE_DIR/iptest && systemctl daemon-reload && systemctl enable iptest && systemctl restart iptest && sleep 1 && systemctl status iptest --no-pager -l | head -20"
    echo
    echo "✅ 部署完成"
    echo "本地: ssh -L 18088:127.0.0.1:8088 $HOST    # 然后浏览器打开 http://localhost:18088"
    ;;

  restart)
    ssh "$HOST" "systemctl restart iptest && sleep 1 && systemctl status iptest --no-pager -l | head -10"
    ;;

  stop)
    ssh "$HOST" "systemctl stop iptest"
    ;;

  status)
    ssh "$HOST" "systemctl status iptest --no-pager -l"
    ;;

  logs)
    ssh "$HOST" "tail -f $REMOTE_DIR/iptest.log"
    ;;

  smoke)
    echo "== 本地 SSH 隧道 + curl 测试 =="
    ssh -fN -L 18088:127.0.0.1:8088 "$HOST"
    sleep 1
    echo "GET /api/tasks ->"
    curl -s http://127.0.0.1:18088/api/tasks
    echo
    echo "GET / (UI) ->"
    curl -s -o /dev/null -w "HTTP %{http_code}\n" http://127.0.0.1:18088/
    pkill -f "ssh -fN -L 18088" || true
    ;;

  *)
    echo "未知动作: $action"
    echo "可选: deploy | restart | stop | status | logs | smoke"
    exit 1
    ;;
esac
