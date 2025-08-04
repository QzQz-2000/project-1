#!/bin/bash

set -e  # 出现错误立即退出

echo "🚀 Step 1: Starting Docker containers..."
docker-compose up -d

echo "✅ Docker containers are up."

# 启动 FastAPI 后端
echo "🚀 Step 2: Starting FastAPI backend..."
cd /Users/lbw1125/Desktop/project-1/backend/mongodb || { echo "❌ Backend folder not found"; exit 1; }
uvicorn main:app --reload &

BACKEND_PID=$!
echo "✅ FastAPI is running in background (PID: $BACKEND_PID)."

# 启动前端
echo "🚀 Step 3: Starting frontend..."
cd /Users/lbw1125/Desktop/project-1/digital-twin-platform || { echo "❌ Frontend (data) folder not found"; exit 1; }
npm run dev

# 注意：这一步不会退出，前端会一直运行
