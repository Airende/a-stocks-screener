#!/usr/bin/env bash
# ARD选股系统 - 看门狗保活脚本
# 用法: nohup bash keepalive.sh > /workspace/keepalive.log 2>&1 &
cd "$(dirname "$0")"
LOG="/workspace/keepalive.log"
PIDFILE="/workspace/.uvicorn.pid"

ensure_venv() {
    # 沙箱可能周期性清理 .venv, 缺失时自动重建并安装依赖
    if [ -x ".venv/bin/python" ]; then
        return 0
    fi
    echo "[$(date '+%F %T')] .venv missing, rebuilding..." >> "$LOG"
    PYSRC="$(command -v python3 || command -v python)"
    "$PYSRC" -m venv .venv || return 1
    .venv/bin/pip install -q --upgrade pip
    .venv/bin/pip install -q -r requirements.txt || return 1
    echo "[$(date '+%F %T')] venv rebuilt" >> "$LOG"
}

start_server() {
    ensure_venv
    echo "[$(date '+%F %T')] starting uvicorn..." >> "$LOG"
    PY=".venv/bin/python"; [ -x "$PY" ] || PY="$(command -v python3 || command -v python)"
    nohup "$PY" -m uvicorn app:app --host 127.0.0.1 --port 8000 > /workspace/uvicorn.log 2>&1 &
    echo $! > "$PIDFILE"
    sleep 2
    if kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        echo "[$(date '+%F %T')] uvicorn up, pid=$(cat "$PIDFILE")" >> "$LOG"
    else
        echo "[$(date '+%F %T')] uvicorn failed to start" >> "$LOG"
    fi
}

while true; do
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
        sleep 10
    else
        start_server
        sleep 5
    fi
done
