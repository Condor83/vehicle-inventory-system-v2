from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class ScrapeJobIn(BaseModel):
    model: str
    region: str | None = None

@router.post("/jobs")
async def create_job(body: ScrapeJobIn):
    # TODO: enqueue orchestrator
    return {"job_id": "00000000-0000-0000-0000-000000000000", "status": "pending"}

@router.get("/jobs/{job_id}")
async def job_status(job_id: str):
    return {"job_id": job_id, "status": "pending", "target_count": 0, "success_count": 0, "fail_count": 0}
