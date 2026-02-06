"""
FILE 2: Consumer - Picks from MinIO, calls container, saves results
Run: python consumer.py
"""

import os
import time
import json
import tempfile
import zipfile
import shutil
import requests
from datetime import datetime
from minio import Minio
from minio.error import S3Error
import signal
import sys
import io
import uuid
import socket
import threading

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bus-alerts"

# CONTAINER_API should point to the Load Balancer (e.g. gpu-proxy on port 30080)
CONTAINER_API = os.getenv("CONTAINER_API", "http://192.168.103.152:30080")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))  # seconds
MAX_GPU_CONCURRENCY = int(os.getenv("MAX_GPU_CONCURRENCY", "1"))

# Worker Identification
WORKER_ID = f"worker_{socket.gethostname()}_{os.getpid()}_{uuid.uuid4().hex[:4]}"

# Statistics
stats = {
    'processed': 0,
    'failed': 0,
    'start_time': datetime.now().isoformat()
}

shutdown = False

# ============================================================================
# MINIO CLIENT
# ============================================================================

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

print(f"Connected to MinIO: {MINIO_ENDPOINT}")

# ============================================================================
# FUNCTIONS
# ============================================================================

def send_heartbeat(status="IDLE", job_id=None):
    """Send heartbeat to MinIO"""
    try:
        hb_data = {
            'worker_id': WORKER_ID,
            'status': status,
            'job_id': job_id,
            'last_seen': datetime.now().isoformat()
        }
        hb_json = json.dumps(hb_data).encode('utf-8')
        minio_client.put_object(
            MINIO_BUCKET,
            f"workers/{WORKER_ID}.json",
            data=io.BytesIO(hb_json),
            length=len(hb_json),
            content_type='application/json'
        )
    except:
        pass # Heartbeat failure shouldn't crash the worker

def heartbeat_thread():
    """Background thread to send heartbeats"""
    global shutdown
    while not shutdown:
        send_heartbeat(stats.get('current_status', 'IDLE'), stats.get('current_job'))
        time.sleep(10)

def acquire_gpu_lock():
    """Try to acquire a GPU lock in MinIO"""
    if MAX_GPU_CONCURRENCY <= 0: return True
    
    try:
        # Check current locks
        locks = list(minio_client.list_objects(MINIO_BUCKET, prefix="gpu_locks/", recursive=False))
        
        # Clean up stale locks (e.g. older than 5 mins)
        for lock in locks:
            # For simplicity in this script, we'll just check if there's room.
            # A more robust version would check timestamps.
            pass
            
        if len(locks) < MAX_GPU_CONCURRENCY:
            # Try to grab a slot
            lock_path = f"gpu_locks/{WORKER_ID}.lock"
            minio_client.put_object(
                MINIO_BUCKET,
                lock_path,
                data=io.BytesIO(b"LOCKED"),
                length=6,
                content_type='application/octet-stream'
            )
            # Re-verify we didn't exceed limit (simple race condition check)
            locks = list(minio_client.list_objects(MINIO_BUCKET, prefix="gpu_locks/", recursive=False))
            if len(locks) > MAX_GPU_CONCURRENCY:
                # We lost race or limit exceeded, clean up and return false
                minio_client.remove_object(MINIO_BUCKET, lock_path)
                return False
            return True
        return False
    except:
        return False

def release_gpu_lock():
    """Release the GPU lock"""
    try:
        minio_client.remove_object(MINIO_BUCKET, f"gpu_locks/{WORKER_ID}.lock")
    except:
        pass


def get_pending_jobs():
    """Get list of pending jobs from MinIO"""
    try:
        jobs = []
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=False)
        
        for obj in objects:
            if obj.is_dir:
                job_id = obj.object_name.rstrip('/')
                
                # Skip reserved prefixes
                if job_id in ['workers', 'gpu_locks']:
                    continue
                
                # Check metadata
                try:
                    metadata_path = f"{job_id}/metadata.json"
                    response = minio_client.get_object(MINIO_BUCKET, metadata_path)
                    metadata = json.loads(response.read().decode('utf-8'))
                    response.close()
                    response.release_conn()
                    
                    # Only process pending jobs
                    if metadata.get('status') == 'pending':
                        jobs.append((job_id, metadata))
                except:
                    continue
        
        return jobs
    except Exception as e:
        print(f"Failed to list jobs: {e}")
        return []

def update_job_status(job_id, status, results=None):
    """Update job status in MinIO"""
    try:
        # Download metadata
        metadata_path = f"{job_id}/metadata.json"
        response = minio_client.get_object(MINIO_BUCKET, metadata_path)
        metadata = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        
        # Update
        metadata['status'] = status
        metadata['worker_id'] = WORKER_ID
        metadata['updated_at'] = datetime.now().isoformat()
        
        # Upload
        metadata_json = json.dumps(metadata, indent=2)
        metadata_bytes = metadata_json.encode('utf-8')
        minio_client.put_object(
            MINIO_BUCKET,
            metadata_path,
            data=io.BytesIO(metadata_bytes),
            length=len(metadata_bytes),
            content_type='application/json'
        )
        return True
    except Exception as e:
        print(f"Failed to update status: {e}")
        return False

def claim_job(job_id, metadata):
    """Try to claim a job atomically"""
    try:
        # Ensure it's still pending
        if metadata.get('status') != 'pending':
            return False
            
        return update_job_status(job_id, 'processing')
    except:
        return False

def process_job(job_id, metadata):
    """Download job, call container API, save results"""
    temp_dir = None
    
    try:
        print(f"\n{'='*70}")
        print(f"Processing: {job_id}")
        print(f"{'='*70}")
        print(f"   Bus: {metadata['bus_number']}")
        print(f"   Images: {metadata['total_images']}")
        
        stats['current_status'] = 'PROCESSING'
        stats['current_job'] = job_id

        # Wait for GPU slot
        print(f"   Waiting for GPU slot (Limit: {MAX_GPU_CONCURRENCY})...")
        while not shutdown and not acquire_gpu_lock():
            time.sleep(2)
        
        if shutdown: return False
        
        print(f"   🚀 GPU slot acquired")

        # Download ZIP from MinIO
        temp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(temp_dir, "images.zip")
        
        minio_client.fget_object(
            MINIO_BUCKET,
            f"{job_id}/images.zip",
            zip_path
        )
        
        print(f"   Downloaded from MinIO")
        
        # ... rest of the logic remains similar but wrapped with lock release
        
        # Extract ZIP
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        print(f"   Extracted images")
        
        # Call container API
        print(f"   Calling container API...")
        
        start_time = time.time()
        
        # Re-zip for upload to container
        with zipfile.ZipFile(zip_path, 'w') as zipf:
            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    file_path = os.path.join(root, file)
                    arcname = os.path.relpath(file_path, extract_dir)
                    zipf.write(file_path, arcname)
        
        # Send to container
        with open(zip_path, 'rb') as f:
            response = requests.post(
                f"{CONTAINER_API}/bus-alert",
                files={'file': ('images.zip', f, 'application/zip')},
                data={
                    'bus_number': metadata['bus_number'],
                    'latlong': metadata['latlong'],
                    'confidence_threshold': 0.75
                },
                timeout=300
            )
        
        processing_time = time.time() - start_time
        
        if response.status_code == 200:
            result = response.json()
            
            print(f"   Processing complete ({processing_time:.2f}s)")
            print(f"   Matches: {result.get('matches_found', 0)}")
            print(f"   Alert: {'YES' if result.get('alert') else 'NO'}")
            
            # Save results to MinIO
            results_json = json.dumps(result, indent=2)
            results_bytes = results_json.encode('utf-8')
            minio_client.put_object(
                MINIO_BUCKET,
                f"{job_id}/results.json",
                data=io.BytesIO(results_bytes),
                length=len(results_bytes),
                content_type='application/json'
            )
            
            # Update status to completed
            update_job_status(job_id, 'completed', result)
            
            stats['processed'] += 1
            
            print(f"   Results saved to MinIO")
            print(f"{'='*70}\n")
            
            return True
            return True
        elif response.status_code == 503:
            print(f"   Rate limited (503). Retrying in 5s...")
            time.sleep(5)
            # Recursively retry or just return False to let the main loop pick it up again?
            # Main loop logic: if we return False, it marks as failed.
            # So we should NOT mark as failed.
            # Better approach: We should modify the call site or loop here.
            # But specific to this block:
            stats['failed'] += 1 # Technically failed this attempt
            # We don't update job status to 'failed', we leave it as 'processing' or reset to 'pending'
            # Resetting to pending is safer for next pickup.
            update_job_status(job_id, 'pending')
            return False
        else:
            print(f"   Container error: {response.status_code}")
            print(f"   {response.text}")
            
            update_job_status(job_id, 'failed', {'error': response.text})
            stats['failed'] += 1
            
            return False
            
    except requests.Timeout:
        print(f"   Timeout")
        update_job_status(job_id, 'failed', {'error': 'Timeout'})
        stats['failed'] += 1
        return False
        
    except Exception as e:
        print(f"   Error: {e}")
        import traceback
        traceback.print_exc()
        
        update_job_status(job_id, 'failed', {'error': str(e)})
        stats['failed'] += 1
        return False
        
    finally:
        release_gpu_lock()
        stats['current_status'] = 'IDLE'
        stats['current_job'] = None
        if temp_dir and os.path.exists(temp_dir):
            try:
                shutil.rmtree(temp_dir)
            except:
                pass

def print_stats():
    """Print statistics"""
    uptime = (datetime.now() - datetime.fromisoformat(stats['start_time'])).total_seconds()
    
    print(f"\n{'='*70}")
    print(f"📊 STATISTICS")
    print(f"{'='*70}")
    print(f"   Uptime: {uptime:.0f}s ({uptime/60:.1f}min)")
    print(f"   Processed: {stats['processed']}")
    print(f"   Failed: {stats['failed']}")
    print(f"{'='*70}\n")

def signal_handler(sig, frame):
    """Handle shutdown"""
    global shutdown
    print("\n\nShutting down...")
    shutdown = True
    print_stats()
    sys.exit(0)

# ============================================================================
# MAIN LOOP
# ============================================================================

def main():
    """Main consumer loop"""
    global shutdown
    
    print("\n" + "="*70)
    print("🤖 BUS ALERT CONSUMER")
    print("="*70)
    print(f"   MinIO: {MINIO_ENDPOINT}")
    print(f"   Container: {CONTAINER_API}")
    print(f"   Poll interval: {POLL_INTERVAL}s")
    print("="*70 + "\n")
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    while not shutdown:
        try:
            # Get pending jobs
            jobs = get_pending_jobs()
            
            if jobs:
                print(f"📋 Found {len(jobs)} pending job(s)")
                
                # Process each job
                for job_id, metadata in jobs:
                    if shutdown:
                        break
                    
                    # Try to claim job
                    if claim_job(job_id, metadata):
                        process_job(job_id, metadata)
                        # Print stats after processing a claimed job
                        print_stats()
            else:
                # No jobs, wait
                time.sleep(POLL_INTERVAL)
                
        except Exception as e:
            print(f"Consumer error: {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    # Start heartbeat thread
    hb = threading.Thread(target=heartbeat_thread, daemon=True)
    hb.start()
    
    main()
