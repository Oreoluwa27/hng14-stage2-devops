from fastapi import FastAPI
import redis
import uuid
import os

app = FastAPI()

r = redis.Redis(host="localhost", port=6379)

@app.post("/jobs")
def create_job():
    job_id = str(uuid.uuid4())
    r.lpush("job", job_id)
    r.hset(f"job:{job_id}", "status", "queued")
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
def get_job(job_id: str):
    status = r.hget(f"job:{job_id}", "status")
    if not status:
        return {"error": "not found"}
    return {"job_id": job_id, "status": status.decode()}

@app.get("/health")
def health_check():
    try:
        r.ping()
        return {"status": "healthy"}
    except:
        return {"status": "unhealthy"}
