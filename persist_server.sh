#!/usr/bin/env bash
# 持久化看门狗: 用已知可用的 pyenv Python 持续拉起 uvicorn
PY=/root/.pyenv/versions/3.14.7/bin/python3.14
cd /workspace
while true; do
  if pgrep -f "uvicorn app:app" >/dev/null 2>&1; then
    sleep 10
    continue
  fi
  echo "[$(date '+%F %T')] starting uvicorn..."
  nohup "$PY" -m uvicorn app:app --host 0.0.0.0 --port 8000 > /workspace/uvicorn.log 2>&1 &
  echo $! > /workspace/.uvicorn.pid
  sleep 10
done