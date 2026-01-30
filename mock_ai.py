"""
FILE: Mock AI Service
To replace missing container on port 8000
"""

from fastapi import FastAPI, UploadFile, File, Form
import random

app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "healthy"}

@app.post("/bus-alert")
async def process(
    file: UploadFile = File(...),
    bus_number: str = Form(...),
    latlong: str = Form(...),
    confidence_threshold: float = Form(0.75)
):
    print(f"🤖 Mock AI processing for bus {bus_number}")
    
    # Simulate processing
    matches = random.randint(0, 5)
    alert = matches > 2
    
    return {
        "matches_found": matches,
        "alert": alert,
        "confidence": random.uniform(0.8, 0.99),
        "details": [
            {"id": i, "score": 0.9} for i in range(matches)
        ]
    }
