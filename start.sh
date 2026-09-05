#!/usr/bin/env bash
# ============================================================
# A股智能选股系统 - 本地部署启动脚本 (macOS / Linux)
# 用法: bash start.sh
# ============================================================
set -e

# 切换到脚本所在目录
cd "$(dirname "$0")"
ROOT="$(pwd)"
echo "[INFO] 项目目录: $ROOT"

# ---------- 1. 检查 Python ----------
PYTHON_BIN=""
for c in python3 python; do
    if command -v "$c" >/dev/null 2>&1; then
        ver=$("$c" -c 'import sys;print("%d.%d"%sys.version_info[:2])' 2>/dev/null || echo 0.0)
        major=$(echo "$ver" | cut -d. -f1)
        minor=$(echo "$ver" | cut -d. -f2)
        if [ "$major" -ge 3 ] 2>/dev/null && [ "$minor" -ge 8 ] 2>/dev/null; then
            PYTHON_BIN="$c"
            echo "[OK] 使用 Python: $c ($ver)"
            break
        fi
    fi
done
if [ -z "$PYTHON_BIN" ]; then
    echo "[ERROR] 未找到 Python 3.8+, 请先安装: https://www.python.org/downloads/"
    exit 1
fi

# ---------- 2. 创建/复用虚拟环境 ----------
VENV="$ROOT/.venv"
if [ ! -d "$VENV" ]; then
    echo "[INFO] 创建虚拟环境 .venv ..."
    "$PYTHON_BIN" -m venv "$VENV"
fi
# 激活虚拟环境
# shellcheck disable=SC1091
source "$VENV/bin/activate"

# ---------- 3. 升级 pip ----------
echo "[INFO] 升级 pip ..."
python -m pip install --upgrade pip -q

# ---------- 4. 安装依赖 ----------
if [ -f requirements.txt ]; then
    echo "[INFO] 安装依赖 (requirements.txt) ..."
    pip install -r requirements.txt -q
else
    echo "[WARN] 未找到 requirements.txt, 安装最小依赖 ..."
    pip install fastapi "uvicorn[standard]" requests pypinyin -q
fi

# ---------- 5. 启动服务 ----------
# 端口优先取环境变量 PORT, 否则默认 8000
PORT="${PORT:-8000}"
echo "[INFO] 启动服务, 端口: $PORT"
echo "[INFO] 访问地址: http://127.0.0.1:$PORT"
echo "[INFO] 局域网访问: http://$(hostname -I 2>/dev/null | awk '{print $1}' || echo '本机IP'):$PORT"
echo "[INFO] 按 Ctrl+C 停止服务"

# 后台打开浏览器 (仅当未禁用)
if [ -z "$NO_BROWSER" ]; then
    (sleep 2 && \
        if command -v open >/dev/null 2>&1; then
            open "http://127.0.0.1:$PORT"
        elif command -v xdg-open >/dev/null 2>&1; then
            xdg-open "http://127.0.0.1:$PORT"
        fi
    ) &
fi

# 启动 uvicorn (前台运行, Ctrl+C 退出)
exec python -m uvicorn app:app --host 0.0.0.0 --port "$PORT"
