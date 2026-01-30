"""
FILE 3: Results API - Get results from MinIO
Run: uvicorn results_api:app --host 0.0.0.0 --port 8001
"""

import os
import json
from typing import Optional, List
from datetime import datetime
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from minio import Minio
from minio.error import S3Error

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

# ============================================================================
# MODELS
# ============================================================================

class JobSummary(BaseModel):
    job_id: str
    bus_number: str
    latlong: str
    timestamp: str
    total_images: int
    status: str
    matches_found: Optional[int] = None
    alert: Optional[bool] = None

class JobDetail(BaseModel):
    job_id: str
    bus_number: str
    latlong: str
    timestamp: str
    total_images: int
    status: str
    results: Optional[dict] = None

# ============================================================================
# FASTAPI APP
# ============================================================================

app = FastAPI(title="Bus Alert Results API", version="1.0.0")

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
        "service": "Bus Alert Results API",
        "port": 8001,
        "minio_endpoint": MINIO_ENDPOINT,
        "bucket": MINIO_BUCKET,
        "endpoints": {
            "list_jobs": "GET /jobs",
            "get_job": "GET /jobs/{job_id}",
            "get_results": "GET /jobs/{job_id}/results",
            "stats": "GET /stats"
        }
    }

@app.get("/health")
async def health():
    try:
        minio_client.bucket_exists(MINIO_BUCKET)
        return {"status": "healthy", "minio": "connected"}
    except:
        return {"status": "unhealthy", "minio": "disconnected"}

@app.get("/jobs")
async def list_jobs(
    status: Optional[str] = None,
    alert_only: bool = False,
    limit: int = 50
):
    """
    List all jobs
    
    Query params:
    - status: Filter by status (pending/processing/completed/failed)
    - alert_only: Only show jobs with alerts
    - limit: Maximum number of jobs to return
    """
    try:
        jobs = []
        
        # List all job folders
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=False)
        
        for obj in objects:
            if obj.is_dir:
                job_id = obj.object_name.rstrip('/')
                
                try:
                    # Get metadata
                    metadata_path = f"{job_id}/metadata.json"
                    response = minio_client.get_object(MINIO_BUCKET, metadata_path)
                    metadata = json.loads(response.read().decode('utf-8'))
                    response.close()
                    response.release_conn()
                    
                    # Filter by status
                    if status and metadata.get('status') != status:
                        continue
                    
                    # Filter by alert
                    if alert_only:
                        results = metadata.get('results', {})
                        if not results.get('alert'):
                            continue
                    
                    # Create summary
                    results = metadata.get('results', {})
                    
                    jobs.append(JobSummary(
                        job_id=job_id,
                        bus_number=metadata['bus_number'],
                        latlong=metadata['latlong'],
                        timestamp=metadata['timestamp'],
                        total_images=metadata['total_images'],
                        status=metadata['status'],
                        matches_found=results.get('matches_found') if results else None,
                        alert=results.get('alert') if results else None
                    ))
                    
                except Exception as e:
                    print(f"⚠️ Failed to read {job_id}: {e}")
                    continue
        
        # Sort by timestamp (newest first)
        jobs.sort(key=lambda x: x.timestamp, reverse=True)
        
        # Apply limit
        jobs = jobs[:limit]
        
        return {
            "success": True,
            "total": len(jobs),
            "jobs": [j.dict() for j in jobs]
        }
        
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    """
    Get job details including metadata
    """
    try:
        # Get metadata
        metadata_path = f"{job_id}/metadata.json"
        response = minio_client.get_object(MINIO_BUCKET, metadata_path)
        metadata = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        
        return {
            "success": True,
            "job": JobDetail(
                job_id=job_id,
                bus_number=metadata['bus_number'],
                latlong=metadata['latlong'],
                timestamp=metadata['timestamp'],
                total_images=metadata['total_images'],
                status=metadata['status'],
                results=metadata.get('results')
            ).dict()
        }
        
    except S3Error as e:
        if e.code == 'NoSuchKey':
            raise HTTPException(404, f"Job '{job_id}' not found")
        raise HTTPException(500, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/jobs/{job_id}/results")
async def get_results(job_id: str):
    """
    Get processing results for a job
    
    Returns full results including:
    - All matched images
    - Person details
    - Confidence scores
    - Alert status
    """
    try:
        # Get results
        results_path = f"{job_id}/results.json"
        response = minio_client.get_object(MINIO_BUCKET, results_path)
        results = json.loads(response.read().decode('utf-8'))
        response.close()
        response.release_conn()
        
        return {
            "success": True,
            "job_id": job_id,
            "results": results
        }
        
    except S3Error as e:
        if e.code == 'NoSuchKey':
            raise HTTPException(404, f"Results not found for job '{job_id}'")
        raise HTTPException(500, str(e))
    except Exception as e:
        raise HTTPException(500, str(e))

@app.get("/stats")
async def get_stats():
    """
    Get system statistics
    """
    try:
        total = 0
        pending = 0
        processing = 0
        completed = 0
        failed = 0
        total_alerts = 0
        
        # Count jobs
        objects = minio_client.list_objects(MINIO_BUCKET, recursive=False)
        
        for obj in objects:
            if obj.is_dir:
                job_id = obj.object_name.rstrip('/')
                total += 1
                
                try:
                    metadata_path = f"{job_id}/metadata.json"
                    response = minio_client.get_object(MINIO_BUCKET, metadata_path)
                    metadata = json.loads(response.read().decode('utf-8'))
                    response.close()
                    response.release_conn()
                    
                    status = metadata.get('status', 'unknown')
                    
                    if status == 'pending':
                        pending += 1
                    elif status == 'processing':
                        processing += 1
                    elif status == 'completed':
                        completed += 1
                        
                        # Check for alert
                        results = metadata.get('results', {})
                        if results.get('alert'):
                            total_alerts += 1
                    elif status == 'failed':
                        failed += 1
                        
                except:
                    continue
        
        return {
            "success": True,
            "total_jobs": total,
            "pending": pending,
            "processing": processing,
            "completed": completed,
            "failed": failed,
            "total_alerts": total_alerts,
            "alert_rate": round((total_alerts / completed * 100), 2) if completed > 0 else 0
        }
        
    except Exception as e:
        raise HTTPException(500, str(e))

if __name__ == "__main__":
    print("\n" + "="*70)
    print("📊 BUS ALERT RESULTS API")
    print("="*70)
    print(f"   MinIO: {MINIO_ENDPOINT}")
    print(f"   Bucket: {MINIO_BUCKET}")
    print("="*70 + "\n")
    
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000)
