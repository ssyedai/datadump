"""
FILE 1: Ingestion API - Receives uploads and stores to MinIO
Run: gunicorn upload_api:app -w 4 --worker-class uvicorn.workers.UvicornWorker -b 0.0.0.0:8000
"""

import os
import tempfile
import zipfile
from datetime import datetime
from typing import Optional
from fastapi import FastAPI, File, UploadFile, Form, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error
import json

# ============================================================================
# CONFIGURATION
# ============================================================================

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET = "bus-alerts"

# ============================================================================
# MINIO CLIENT
# ============================================================================

minio_client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

# Create bucket if not exists
try:
    if not minio_client.bucket_exists(MINIO_BUCKET):
        minio_client.make_bucket(MINIO_BUCKET)
        print(f"✅ Created bucket: {MINIO_BUCKET}")
except S3Error as e:
    print(f"⚠️ MinIO error: {e}")

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(title="Bus Alert Upload API", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/")
async def root():
    return {
        "status": "online",
        "service": "Bus Alert Upload API",
        "port": 8000,
        "minio_endpoint": MINIO_ENDPOINT,
        "bucket": MINIO_BUCKET
    }

@app.get("/health")
async def health():
    try:
        minio_client.bucket_exists(MINIO_BUCKET)
        return {"status": "healthy", "minio": "connected"}
    except:
        return {"status": "unhealthy", "minio": "disconnected"}

@app.post("/upload")
async def upload(
    file: UploadFile = File(...),
    bus_number: str = Form(...),
    latlong: str = Form(...),
    device_id: Optional[str] = Form(None),
    device_brand: Optional[str] = Form(None),
    device_model: Optional[str] = Form(None)
):
    """
    Upload bus images ZIP to MinIO
    
    Mobile devices upload here:
    - file: ZIP containing images
    - bus_number: Bus identifier
    - latlong: GPS coordinates
    """
    temp_zip = None
    
    try:
        # Validate ZIP file
        if not file.filename.lower().endswith('.zip'):
            raise HTTPException(400, "File must be ZIP")
        
        # Generate job ID
        timestamp = datetime.now()
        job_id = f"job_{bus_number}_{timestamp.strftime('%Y%m%d_%H%M%S')}"
        
        print(f"\n📥 Upload: {job_id}")
        
        # Read file
        contents = await file.read()
        if len(contents) == 0:
            raise HTTPException(400, "Empty ZIP file")
        
        zip_size_mb = len(contents) / (1024 * 1024)
        
        # Save to temp file and validate
        with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as tmp:
            tmp.write(contents)
            tmp.flush()
            temp_zip = tmp.name
        
        # Validate ZIP
        try:
            with zipfile.ZipFile(temp_zip, 'r') as zip_ref:
                file_count = len([f for f in zip_ref.namelist() 
                                 if f.lower().endswith(('.jpg', '.jpeg', '.png', '.bmp'))])
        except zipfile.BadZipFile:
            raise HTTPException(400, "Invalid ZIP file")
        
        # Create metadata
        metadata = {
            'job_id': job_id,
            'bus_number': bus_number,
            'latlong': latlong,
            'timestamp': timestamp.isoformat(),
            'total_images': file_count,
            'zip_size_mb': round(zip_size_mb, 2),
            'status': 'pending',
            'device_info': {
                'device_id': device_id,
                'device_brand': device_brand,
                'device_model': device_model
            } if device_id else None,
            'uploaded_at': timestamp.isoformat()
        }
        
        # Upload ZIP to MinIO
        zip_path = f"{job_id}/images.zip"
        with open(temp_zip, 'rb') as f:
            minio_client.put_object(
                MINIO_BUCKET,
                zip_path,
                f,
                length=len(contents),
                content_type='application/zip'
            )
        
        # Upload metadata
        metadata_json = json.dumps(metadata, indent=2)
        metadata_bytes = metadata_json.encode('utf-8')
        import io
        minio_client.put_object(
            MINIO_BUCKET,
            f"{job_id}/metadata.json",
            data=io.BytesIO(metadata_bytes),
            length=len(metadata_bytes),
            content_type='application/json'
        )
        
        # Save upload_log.json
        log_data = {
            "job_id": job_id,
            "timestamp": timestamp.isoformat(),
            "device_info": metadata['device_info'],
            "status": "success",
            "message": "Upload completed successfully"
        }
        log_json = json.dumps(log_data, indent=2)
        log_bytes = log_json.encode('utf-8')
        minio_client.put_object(
            MINIO_BUCKET,
            f"{job_id}/upload_log.json",
            data=io.BytesIO(log_bytes),
            length=len(log_bytes),
            content_type='application/json'
        )

        print(f"✅ Stored to MinIO: {job_id}")
        print(f"   Images: {file_count}")
        print(f"   Size: {zip_size_mb:.2f} MB")
        
        return {
            "success": True,
            "job_id": job_id,
            "bus_number": bus_number,
            "latlong": latlong,
            "total_images": file_count,
            "status": "queued",
            "device_info": metadata['device_info'],
            "message": f"✅ Upload successful! Job queued: {job_id}"
        }
        
    except HTTPException:
        raise
    except Exception as e:
        print(f"❌ Upload failed: {e}")
        raise HTTPException(500, str(e))
    finally:
        if temp_zip and os.path.exists(temp_zip):
            try:
                os.remove(temp_zip)
            except:
                pass

@app.get("/stats")
async def get_stats():
    """Get upload statistics"""
    try:
        # Count jobs in MinIO
        jobs = list(minio_client.list_objects(MINIO_BUCKET, recursive=False))
        pending = sum(1 for j in jobs if j.is_dir)
        
        return {
            "total_jobs": pending,
            "bucket": MINIO_BUCKET,
            "minio_endpoint": MINIO_ENDPOINT
        }
    except Exception as e:
        raise HTTPException(500, str(e))

if __name__ == "__main__":
    print("\n" + "="*70)
    print("🚀 BUS ALERT UPLOAD API")
    print("="*70)
    print(f"   MinIO: {MINIO_ENDPOINT}")
    print(f"   Bucket: {MINIO_BUCKET}")
    print("="*70 + "\n")
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=4000)
