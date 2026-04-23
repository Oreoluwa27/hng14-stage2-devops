from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from redis.asyncio import Redis
import uuid, os, logging

logger = logging.getLogger(__name__)

r = Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=int(os.getenv("REDIS_PORT", 6379)),
    max_connections=20,
    decode_responses=True,
)

QUEUE_KEY = "myapp:jobs:queue"
JOB_TTL = 86400

@asynccontextmanager
async def lifespan(app: FastAPI):
    await r.ping()
    yield
    await r.aclose()

app = FastAPI(lifespan=lifespan)

@app.post("/jobs", status_code=202)
async def create_job():
    job_id = str(uuid.uuid4())
    async with r.pipeline() as pipe:
        await pipe.lpush(QUEUE_KEY, job_id)
        await pipe.hset(f"job:{job_id}", "status", "queued")
        await pipe.expire(f"job:{job_id}", JOB_TTL)
        await pipe.execute()
    return {"job_id": job_id}

@app.get("/jobs/{job_id}")
async def get_job(job_id: str):
    status = await r.hget(f"job:{job_id}", "status")
    if not status:
        raise HTTPException(status_code=404, detail="Job not found")
    return {"job_id": job_id, "status": status}

@app.get("/health")
async def health_check():
    try:
        await r.ping()
        return {"status": "healthy"}
    except Exception as e:
        logger.error("Redis ping failed: %s", e)
        return {"status": "unhealthy"}