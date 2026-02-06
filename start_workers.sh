#!/bin/bash

# Start Multiple Consumers
# Usage: ./start_workers.sh 4

COUNT=${1:-2}
echo "🚀 Starting $COUNT worker(s)..."

# Ensure we clean up old locks and heartbeats for a fresh start?
# (Optional: might be dangerous if other real workers are running)
# rm -rf worker_logs/
# mkdir -p worker_logs

for i in $(seq 1 $COUNT)
do
    LOG_FILE="consumer_$i.log"
    echo "   🔹 Launching Worker $i (Log: $LOG_FILE)..."
    
    # Set unique log file for each worker
    nohup python consumer.py > "$LOG_FILE" 2>&1 &
done

echo ""
echo "✅ $COUNT Workers launched in background."
echo "📋 Monitor them with: python monitor_workers.py"
echo "🛑 Stop all with: pkill -f consumer.py"
