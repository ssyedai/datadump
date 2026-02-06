"""
Populate MinIO - Uploads test jobs to the Ingestion API
Run: python populate_minio.py --count 10
"""

import requests
import zipfile
import io
import time
import argparse
from concurrent.futures import ThreadPoolExecutor

UPLOAD_URL = "http://localhost:4000/upload"

def create_dummy_zip(num_images=3):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        for i in range(num_images):
            # Create a small dummy image content
            z.writestr(f"image_{i}.jpg", b"fake jpeg data " + str(i).encode())
    buf.seek(0)
    return buf

def upload_job(i):
    print(f"[Job {i:02d}] Uploading...")
    # Read real Fake2.zip instead of dummy data
    try:
        with open("Fake2.zip", "rb") as f:
            zip_contents = f.read()
    except FileNotFoundError:
        print("❌ Error: Fake2.zip not found in current directory!")
        return False

    files = {
        'file': (f'test_job_{i}.zip', zip_contents, 'application/zip')
    }
    data = {
        'bus_number': f'BUS_{100+i}',
        'latlong': '31.5204,74.3587', # Sample Lahore coords
        'device_id': f'TEST_DEV_{i}'
    }
    
    try:
        start_time = time.time()
        r = requests.post(UPLOAD_URL, files=files, data=data, timeout=30)
        elapsed = time.time() - start_time
        
        if r.status_code == 200:
            job_id = r.json().get('job_id', 'N/A')
            print(f"   [Job {i:02d}] Success in {elapsed:.2f}s | ID: {job_id}")
            return True
        else:
            print(f"   [Job {i:02d}] Failed: {r.status_code} - {r.text}")
            return False
    except Exception as e:
        print(f"   [Job {i:02d}] Error: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Upload test jobs to Bus Alert System")
    parser.add_argument("--count", type=int, default=10, help="Number of jobs to upload")
    parser.add_argument("--concurrency", type=int, default=2, help="Parallel uploads")
    args = parser.parse_args()

    print(f"Starting population of {args.count} jobs...")
    print(f"Target: {UPLOAD_URL}")
    print(f"Concurrency: {args.concurrency}")
    print("="*60)

    start_total = time.time()
    
    with ThreadPoolExecutor(max_workers=args.concurrency) as executor:
        results = list(executor.map(upload_job, range(1, args.count + 1)))

    total_time = time.time() - start_total
    success_count = sum(1 for r in results if r)
    
    print("="*60)
    print("Finished in {:.2f}s".format(total_time))
    print("Success: {}/{}".format(success_count, args.count))
    print("="*60)

if __name__ == "__main__":
    main()
