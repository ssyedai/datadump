
import requests
import time
import os
import zipfile
import io

UPLOAD_URL = "http://localhost:4000/upload"

def create_dummy_zip():
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, 'w') as z:
        z.writestr("test.jpg", b"dummy image data")
    buf.seek(0)
    return buf

def trigger_job(i):
    print(f"🚀 Triggering job {i}...")
    zip_data = create_dummy_zip()
    files = {'file': ('test.zip', zip_data, 'application/zip')}
    data = {
        'bus_number': f'TEST_{i}',
        'latlong': '0,0'
    }
    try:
        r = requests.post(UPLOAD_URL, files=files, data=data)
        print(f"   ✅ Job {i} status: {r.status_code}")
    except Exception as e:
        print(f"   ❌ Job {i} failed: {e}")

if __name__ == "__main__":
    print("=== Concurrency Verification Test ===")
    print("Spaming 5 jobs to trigger Nginx concurrency limit (Max 2)...")
    for i in range(1, 6):
        trigger_job(i)
        
    print("\nNext steps:")
    print("1. Watch consumer logs: Get-Content -Tail 20 -Wait consumer.log")
    print("2. Look for 'Rate limited (503)' messages.")
    print("3. Check Nginx logs in K8s: kubectl logs -l app=gpu-proxy --tail 20")
