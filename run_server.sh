#!/usr/bin/env bash
# ARD选股系统 - 启动服务 (使用 pyenv 真实二进制, 不依赖 .venv)
PY=/root/.pyenv/versions/3.14.7/bin/python3.14
cd "$(dirname "$0")"
pkill -f 'uvicorn app:app' 2>/dev/null
sleep 1
nohup "$PY" -m uvicorn app:app --host 0.0.0.0 --port 8000 > /workspace/uvicorn.log 2>&1 &
echo $! > /workspace/.uvicorn.pid
sleep 3
echo "started pid=$(cat /workspace/.uvicorn.pid)"
tail -n 8 /workspace/uvicorn.log