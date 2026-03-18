# backend/app/api/routes_scripts.py
"""
API endpoints for running maintenance scripts as background tasks.
Now uses the shared backfill service, no dependency on 'scripts' folder.
"""
from fastapi import APIRouter, BackgroundTasks, Query, HTTPException
from pydantic import BaseModel
import uuid
from datetime import date
from typing import Optional

from app.services.backfill_service import backfill_league
from app.core.constants import LEAGUE_MAP

router = APIRouter(prefix="/scripts", tags=["Scripts"])

# In‑memory job store (for demo; in production consider a database)
_backfill_jobs = {}

class BackfillJobStatus(BaseModel):
    job_id: str
    status: str  # "queued", "running", "done", "error"
    progress: Optional[int] = None
    total: Optional[int] = None
    current_league: Optional[str] = None
    message: Optional[str] = None
    error: Optional[str] = None


def _run_backfill_in_background(
    job_id: str,
    league: Optional[str],
    start_date: Optional[date],
    end_date: Optional[date],
):
    """Background task using the shared backfill service."""
    from seleniumbase import Driver
    import time

    driver = None
    try:
        _backfill_jobs[job_id]["status"] = "running"
        _backfill_jobs[job_id]["message"] = "Starting backfill..."

        leagues_to_process = [league] if league else list(LEAGUE_MAP.keys())
        total = len(leagues_to_process)
        _backfill_jobs[job_id]["total"] = total

        # Create a single driver for all leagues (headless)
        driver = Driver(uc=True, headless2=True)

        for idx, lc in enumerate(leagues_to_process):
            _backfill_jobs[job_id]["progress"] = idx
            _backfill_jobs[job_id]["current_league"] = lc
            _backfill_jobs[job_id]["message"] = f"Processing {lc} ({idx+1}/{total})"

            # Call the service function
            result = backfill_league(lc, start_date, end_date, driver=driver)
            # You could store result details if needed

        _backfill_jobs[job_id]["status"] = "done"
        _backfill_jobs[job_id]["message"] = "Backfill completed successfully."

    except Exception as e:
        _backfill_jobs[job_id]["status"] = "error"
        _backfill_jobs[job_id]["error"] = str(e)
        import traceback
        _backfill_jobs[job_id]["traceback"] = traceback.format_exc()
    finally:
        if driver:
            driver.quit()


@router.post("/backfill-match-stats", response_model=BackfillJobStatus)
async def start_backfill_match_stats(
    background_tasks: BackgroundTasks,
    league: Optional[str] = Query(None, description="Single league code, omit for all"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """Start a background job to backfill player match statistics."""
    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None

    if league and league not in LEAGUE_MAP:
        raise HTTPException(status_code=400, detail=f"Unknown league: {league}")

    job_id = str(uuid.uuid4())[:8]
    _backfill_jobs[job_id] = {
        "job_id": job_id,
        "status": "queued",
        "progress": 0,
        "total": None,
        "current_league": None,
        "message": "Job queued, waiting to start...",
        "error": None,
    }

    background_tasks.add_task(
        _run_backfill_in_background,
        job_id,
        league,
        start,
        end,
    )

    return BackfillJobStatus(**_backfill_jobs[job_id])


@router.get("/backfill-match-stats/status/{job_id}", response_model=BackfillJobStatus)
async def get_backfill_status(job_id: str):
    """Get the status of a backfill job."""
    job = _backfill_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return BackfillJobStatus(**job)
