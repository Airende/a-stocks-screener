#!/usr/bin/env bash
# ARD选股系统 - 看门狗保活脚本
# 用法: nohup bash keepalive.sh > /workspace/keepalive.log 2>&1 &
cd "$(dirname "$0")"
LOG="/workspace/keepalive.log"
PIDFILE="/workspace/.uvicorn.pid"

start_server() {
    echo "[$(date '+%F %T')] starting uvicorn..." >> "$LOG"
    nohup .venv/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8000 > /workspace/uvicorn.log 2>&1 &
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
