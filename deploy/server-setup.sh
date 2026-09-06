#!/usr/bin/env bash
# ============================================================
# A股选股系统 · 云服务器一键部署脚本 (20260906)
# 适用: Ubuntu 22.04 / 24.04 (Debian 系), 以 root 运行
# 用法: 在服务器上执行
#   DATABASE_URL='postgresql://...' DOMAIN='airende.xyz' bash server-setup.sh
#   - DATABASE_URL: Supabase/Neon 等 Postgres 连接串 (持仓/标记持久化; 可留空走本地文件)
#   - DOMAIN:       绑定的域名 (Caddy 自动申请 HTTPS 证书; 留空则只用 8000 端口访问)
# 功能: 系统依赖 → 克隆仓库 → venv装依赖 → .env → systemd 常驻 → Caddy 反代+自动HTTPS
# ============================================================
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/Airende/a-stocks-screener.git}"
APP_DIR="/opt/a-stocks-screener"
DOMAIN="${DOMAIN:-}"
DB_URL="${DATABASE_URL:-}"

echo "[1/6] 系统依赖 (git python3-venv caddy)..."
export DEBIAN_FRONTEND=noninteractive
apt-get update -qq
apt-get install -y -qq git python3-venv python3-pip curl >/dev/null
if ! command -v caddy >/dev/null 2>&1; then
  apt-get install -y -qq debian-keyring debian-archive-keyring apt-transport-https >/dev/null
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/gpg.key' | gpg --batch --yes --dearmor -o /usr/share/keyrings/caddy-stable-archive-keyring.gpg 2>/dev/null
  curl -1sLf 'https://dl.cloudsmith.io/public/caddy/stable/debian.deb.txt' -o /etc/apt/sources.list.d/caddy-stable.list
  apt-get update -qq && apt-get install -y -qq caddy >/dev/null
fi

echo "[2/6] 拉取代码到 ${APP_DIR}..."
if [ -d "$APP_DIR/.git" ]; then
  git -C "$APP_DIR" fetch --all -q && git -C "$APP_DIR" reset --hard origin/main -q
else
  git clone -q "$REPO_URL" "$APP_DIR"
fi

echo "[3/6] Python 虚拟环境 + 依赖..."
cd "$APP_DIR"
python3 -m venv .venv
.venv/bin/pip install -q --upgrade pip
.venv/bin/pip install -q -r requirements.txt

echo "[4/6] 写入运行配置 .env..."
{
  echo "PORT=8000"
  [ -n "$DB_URL" ] && echo "DATABASE_URL=${DB_URL}"
} > "$APP_DIR/.env"
chmod 600 "$APP_DIR/.env"

echo "[5/6] systemd 服务 (开机自启 + 崩溃自动重启)..."
cat > /etc/systemd/system/stocks-screener.service <<EOF
[Unit]
Description=A股选股系统 (FastAPI/uvicorn)
After=network.target

[Service]
Type=simple
WorkingDirectory=${APP_DIR}
EnvironmentFile=${APP_DIR}/.env
ExecStart=${APP_DIR}/.venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000 --workers 1
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
EOF
systemctl daemon-reload
systemctl enable --now stocks-screener

echo "[6/6] Caddy 反向代理${DOMAIN:+ + 自动HTTPS(${DOMAIN})}..."
if [ -n "$DOMAIN" ]; then
  cat > /etc/caddy/Caddyfile <<EOF
${DOMAIN}, www.${DOMAIN} {
  reverse_proxy 127.0.0.1:8000
}
EOF
else
  cat > /etc/caddy/Caddyfile <<EOF
:80 {
  reverse_proxy 127.0.0.1:8000
}
EOF
fi
systemctl enable --now caddy 2>/dev/null || systemctl restart caddy

sleep 3
echo "===== 部署完成 ====="
systemctl is-active stocks-screener && echo "应用: 运行中"
systemctl is-active caddy && echo "Caddy: 运行中"
curl -s -o /dev/null -w "本地健康检查 HTTP %{http_code}\n" http://127.0.0.1:8000/api/health
[ -n "$DOMAIN" ] && echo "访问: https://${DOMAIN}  (DNS A记录指向本机IP后, 证书自动签发)"
[ -z "$DOMAIN" ] && echo "访问: http://<服务器IP> (80端口)"
echo "查看日志: journalctl -u stocks-screener -f"
echo "更新代码: cd ${APP_DIR} && git pull && systemctl restart stocks-screener"
