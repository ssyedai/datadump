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

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bus-alerts"

CONTAINER_API = os.getenv("CONTAINER_API", "http://192.168.103.152:8000")
POLL_INTERVAL = int(os.getenv("POLL_INTERVAL", "5"))  # seconds

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

print(f"✅ Connected to MinIO: {MINIO_ENDPOINT}")

# ============================================================================
# FUNCTIONS
# ============================================================================

def get_pending_jobs():
    """Get list of pending jobs from MinIO"""
    try:
        jobs = []
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=False)
        
        for obj in objects:
            if obj.is_dir:
                job_id = obj.object_name.rstrip('/')
                
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
        print(f"❌ Failed to list jobs: {e}")
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
        metadata['updated_at'] = datetime.now().isoformat()
        if results:
            pass # Do not add results to metadata as per user request
        
        # Upload
        metadata_json = json.dumps(metadata, indent=2)
        metadata_bytes = metadata_json.encode('utf-8')
        # import io
        minio_client.put_object(
            MINIO_BUCKET,
            metadata_path,
            data=io.BytesIO(metadata_bytes),
            length=len(metadata_bytes),
            content_type='application/json'
        )
        return True
    except Exception as e:
        print(f"❌ Failed to update status: {e}")
        return False

def process_job(job_id, metadata):
    """Download job, call container API, save results"""
    temp_dir = None
    
    try:
        print(f"\n{'='*70}")
        print(f"⚙️  Processing: {job_id}")
        print(f"{'='*70}")
        print(f"   Bus: {metadata['bus_number']}")
        print(f"   Images: {metadata['total_images']}")
        
        # Update status to processing
        update_job_status(job_id, 'processing')
        
        # Download ZIP from MinIO
        temp_dir = tempfile.mkdtemp()
        zip_path = os.path.join(temp_dir, "images.zip")
        
        minio_client.fget_object(
            MINIO_BUCKET,
            f"{job_id}/images.zip",
            zip_path
        )
        
        print(f"   📦 Downloaded from MinIO")
        
        # Extract ZIP
        extract_dir = os.path.join(temp_dir, "extracted")
        os.makedirs(extract_dir, exist_ok=True)
        
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_dir)
        
        print(f"   📂 Extracted images")
        
        # Call container API
        print(f"   🐳 Calling container API...")
        
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
            
            print(f"   ✅ Processing complete ({processing_time:.2f}s)")
            print(f"   Matches: {result.get('matches_found', 0)}")
            print(f"   Alert: {'🚨 YES' if result.get('alert') else '✅ NO'}")
            
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
            
            print(f"   💾 Results saved to MinIO")
            print(f"{'='*70}\n")
            
            return True
        else:
            print(f"   ❌ Container error: {response.status_code}")
            print(f"   {response.text}")
            
            update_job_status(job_id, 'failed', {'error': response.text})
            stats['failed'] += 1
            
            return False
            
    except requests.Timeout:
        print(f"   ❌ Timeout")
        update_job_status(job_id, 'failed', {'error': 'Timeout'})
        stats['failed'] += 1
        return False
        
    except Exception as e:
        print(f"   ❌ Error: {e}")
        import traceback
        traceback.print_exc()
        
        update_job_status(job_id, 'failed', {'error': str(e)})
        stats['failed'] += 1
        return False
        
    finally:
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
    print("\n\n⚠️  Shutting down...")
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
                    process_job(job_id, metadata)
                
                # Print stats after processing
                if stats['processed'] > 0 or stats['failed'] > 0:
                    print_stats()
            else:
                # No jobs, wait
                time.sleep(POLL_INTERVAL)
                
        except Exception as e:
            print(f"❌ Consumer error: {e}")
            time.sleep(POLL_INTERVAL)

if __name__ == "__main__":
    main()
