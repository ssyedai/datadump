#!/bin/bash

# ==================================
# Simple Start Script - 3-File Architecture
# Auto-kill port conflicts and restart
# ==================================

echo "=================================="
echo "🚀 Starting Bus Alert System"
echo "=================================="

# ----------------------------
# Helper Functions
# ----------------------------

# Kill process using a port
kill_port() {
    local PORT=$1
    echo "   ⚠️  Port $PORT in use. Killing process..."
    lsof -t -i:$PORT | xargs -r kill -9
    sleep 1
}

# Start a service with health check
start_service() {
    local NAME=$1
    local PORT=$2
    local START_CMD=$3
    local LOG_FILE=$4

    echo -e "\n🚀 Starting $NAME (Port $PORT)..."

    # Try starting
    $START_CMD &> $LOG_FILE &
    local PID=$!
    sleep 2

    # Check if port is open
    if ! curl -s http://localhost:$PORT/health > /dev/null 2>&1; then
        echo "   ❌ $NAME failed to start, trying to free port $PORT..."
        kill_port $PORT
        sleep 1
        $START_CMD &> $LOG_FILE &
        PID=$!
        sleep 2
        if ! curl -s http://localhost:$PORT/health > /dev/null 2>&1; then
            echo "   ❌ $NAME still failed to start. Check log: $LOG_FILE"
            return 1
        fi
    fi
    echo "   ✅ $NAME running (PID: $PID)"
    echo $PID
}

# ----------------------------
# Check Docker permissions
# ----------------------------
if ! docker ps &> /dev/null; then
    echo ""
    echo "⚠️  Docker Permission Issue Detected!"
    echo ""
    read -p "Continue without MinIO Docker? (y/n) " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then exit 1; fi
    SKIP_MINIO=true
else
    SKIP_MINIO=false
fi

# ----------------------------
# MinIO
# ----------------------------
if [ "$SKIP_MINIO" = false ]; then
    echo -e "\n1️⃣ Checking MinIO..."
    if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
        echo "   ✅ MinIO is running"
    else
        kill_port 9000
        kill_port 9001
        docker run -d --name minio -p 9000:9000 -p 9001:9001 \
            -e "MINIO_ROOT_USER=minioadmin" \
            -e "MINIO_ROOT_PASSWORD=minioadmin" \
            minio/minio server /data --console-address ":9001" 2>&1
        sleep 5
        if curl -s http://localhost:9000/minio/health/live > /dev/null 2>&1; then
            echo "   ✅ MinIO started"
        else
            echo "   ❌ MinIO failed"
            exit 1
        fi
    fi
else
    echo -e "\n1️⃣ Skipping MinIO Docker..."
fi

# ----------------------------
# Upload API
# ----------------------------
UPLOAD_CMD="gunicorn upload_api:app -w 4 --worker-class uvicorn.workers.UvicornWorker -b 0.0.0.0:4000 --daemon --pid upload_api.pid --access-logfile upload_api.log --error-logfile upload_api_error.log"
start_service "Upload API" 4000 "$UPLOAD_CMD" "upload_api.log"

# ----------------------------
# Consumer
# ----------------------------
echo -e "\n3️⃣ Starting Consumer..."
pkill -f "consumer.py" 2>/dev/null || true
nohup python consumer.py > consumer.log 2>&1 &
echo $! > consumer.pid
sleep 2
if ps -p $(cat consumer.pid) > /dev/null 2>&1; then
    echo "   ✅ Consumer running (PID: $(cat consumer.pid))"
else
    echo "   ❌ Consumer failed - Check: consumer.log"
fi

# ----------------------------
# Results API
# ----------------------------
RESULTS_CMD="uvicorn results_api:app --host 0.0.0.0 --port 8001"
start_service "Results API" 8001 "$RESULTS_CMD" "results_api.log"

# ----------------------------
# Finished
# ----------------------------
echo -e "\n=================================="
echo "✅ Services Started"
echo "=================================="
echo ""
echo "🌐 URLs:"
echo "   Upload:  http://localhost:4000"
echo "   Results: http://localhost:8001"
if [ "$SKIP_MINIO" = false ]; then echo "   MinIO:   http://localhost:9001"; fi
echo ""
echo "📋 Logs:"
echo "   tail -f upload_api.log consumer.log results_api.log"
echo ""
echo "🛑 Stop all: pkill -f 'upload_api|consumer.py|results_api'"
echo "=================================="
