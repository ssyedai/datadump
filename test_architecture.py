"""
TEST ARCHITECTURE SCRIPT
Run: python test_architecture.py

This script:
1. Creates a dummy ZIP file
2. Uploads it to the Upload API (Port 4000)
3. Polls the Results API (Port 8001) for completion
"""
import requests
import time
import zipfile
import io
import os
import json

UPLOAD_URL = "http://localhost:4000/upload"
RESULTS_API = "http://localhost:8001"

# def create_dummy_zip():
#     print("📦 Creating dummy ZIP file...")
#     buffer = io.BytesIO()
#     with zipfile.ZipFile(buffer, 'w') as zip_file:
#         zip_file.writestr('test.txt', 'This is a test file to simulate images')
#     buffer.seek(0)
#     return buffer

def read_zip_as_buffer(zip_path: str) -> io.BytesIO:
    print("📦 Reading ZIP file:", zip_path)

    buffer = io.BytesIO()
    with open(zip_path, "rb") as f:
        buffer.write(f.read())

    buffer.seek(0)
    return buffer

def test_flow():
    print(f"{'='*50}")
    print("🧪 TESTING BUS ALERT ARCHITECTURE")
    print(f"{'='*50}\n")

    # 1. Upload
    print("1️⃣  Uploading file...")
    try:
        dummy_zip = read_zip_as_buffer('Fake2.zip')
        files = {'file': ('Fake2.zip', dummy_zip, 'application/zip')}
        data = {
            'bus_number': 'TEST-BUS-001',
            'latlong': '0.0,0.0'
        }
        
        response = requests.post(UPLOAD_URL, files=files, data=data)
        
        if response.status_code != 200:
            print(f"❌ Upload Failed: {response.text}")
            return
            
        result = response.json()
        job_id = result['job_id']
        print(f"✅ Upload Successful! Job ID: {job_id}")
        
    except Exception as e:
        print(f"❌ Connection Error (Upload API): {e}")
        print("   Is the docker container running on port 4000?")
        return

    # 2. Poll Results
    print("\n2️⃣  Waiting for processing...")
    print(f"   Target AI Service: {os.getenv('AI_SERVICE_URL', 'Unknown')}")
    
    attempts = 0
    max_attempts = 100
    
    while attempts < max_attempts:
        try:
            start_time = time.perf_counter()
            r = requests.get(f"{RESULTS_API}/jobs/{job_id}")
            if r.status_code == 200:
                job = r.json().get('job', {})
                status = job.get('status')
                
                print(f"   [{attempts+1}/{max_attempts}] Status: {status}")
                
                if status == 'completed':
                    end_time = time.perf_counter()
                    print("\n✅ SUCCESS: Job processed successfully!")
                    print(f"   Results: {job.get('results')}")
                    print(f"   Processing time: {end_time - start_time:.2f} seconds")
                    return
                elif status == 'failed':
                    print("\n❌ FAILURE: Job failed processing.")
                    # Fetch logs hint
                    print("   Check consumer logs: docker-compose logs consumer")
                    return
                    
            time.sleep(200)
            attempts += 1
            
        except Exception as e:
            print(f"❌ Connection Error (Results API): {e}")
            return

    print("\n⚠️  TIMEOUT: Processing took too long.")
    print("   Check if your External AI Service is reachable from the container.")

if __name__ == "__main__":
    test_flow()
