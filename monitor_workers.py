"""
Worker Monitor - Shows real-time status of all consumers
Run: python monitor_workers.py
"""

import os
import time
import json
from datetime import datetime
from minio import Minio

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bus-alerts"

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

def clear_screen():
    os.system('cls' if os.name == 'nt' else 'clear')

def get_workers():
    workers = []
    try:
        objects = minio_client.list_objects(MINIO_BUCKET, prefix="workers/", recursive=False)
        for obj in objects:
            try:
                response = minio_client.get_object(MINIO_BUCKET, obj.object_name)
                data = json.loads(response.read().decode('utf-8'))
                response.close()
                response.release_conn()
                workers.append(data)
            except:
                continue
    except Exception as e:
        print(f"Error fetching workers: {e}")
    return workers

def get_gpu_locks():
    locks = []
    try:
        objects = minio_client.list_objects(MINIO_BUCKET, prefix="gpu_locks/", recursive=False)
        for obj in objects:
            locks.append(obj.object_name.split('/')[-1].replace('.lock', ''))
    except:
        pass
    return locks

def main():
    print(f"Monitoring Bus Alert Workers on {MINIO_ENDPOINT}...")
    
    try:
        while True:
            workers = get_workers()
            locks = get_gpu_locks()
            
            clear_screen()
            print("="*80)
            print(f"👷 BUS ALERT WORKER MONITOR | {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("="*80)
            
            if not workers:
                print("\n   No active workers found.\n")
            else:
                print(f"{'WORKER ID':<40} | {'STATUS':<10} | {'JOB ID':<25} | {'LAST SEEN'}")
                print("-" * 80)
                
                for w in workers:
                    last_seen = datetime.fromisoformat(w['last_seen'])
                    delta = (datetime.now() - last_seen).total_seconds()
                    
                    # Highlight stale workers
                    status_prefix = "[OK]   " if delta < 30 else "[STALE]"
                    
                    status = w.get('status', 'IDLE')
                    # Check if worker holds a GPU lock
                    gpu_indicator = " * [GPU]" if w['worker_id'] in locks else ""
                    
                    print(f"{status_prefix} {w['worker_id'][:37]:<37} | {status:<10} | {str(w.get('job_id', 'N/A')):<25} | {int(delta)}s ago{gpu_indicator}")
            
            print("\n" + "="*80)
            print(f"GPU LOCKS: {len(locks)} active | {', '.join(locks) if locks else 'None'}")
            print("="*80)
            print("Press Ctrl+C to exit")
            
            time.sleep(2)
    except KeyboardInterrupt:
        print("\nExiting...")

if __name__ == "__main__":
    main()
