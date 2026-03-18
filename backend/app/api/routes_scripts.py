# backend/app/api/routes_scripts.py
"""
API endpoints for running maintenance scripts as background tasks.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path

# Add project root to sys.path so that 'scripts' can be imported
project_root = Path(__file__).resolve().parent.parent.parent  # goes to /app
sys.path.insert(0, str(project_root))

import asyncio
import uuid
from datetime import date, datetime
from typing import Optional
from fastapi import APIRouter, BackgroundTasks, Query, HTTPException
from pydantic import BaseModel

from app.database.db import SessionLocal
from app.core.constants import LEAGUE_MAP

router = APIRouter(prefix="/scripts", tags=["Scripts"])

# Simple in‑memory job store (for demo; in production use a proper task queue)
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
    """
    Background task that actually runs the backfill.
    """
    # Now that project root is in sys.path, we can import from scripts
    from scripts.backfill_match_stats import backfill_league
    from seleniumbase import Driver
    import time

    db = SessionLocal()
    driver = None
    try:
        # Update job status
        _backfill_jobs[job_id]["status"] = "running"
        _backfill_jobs[job_id]["message"] = "Starting backfill..."

        # Determine which leagues to process
        if league:
            leagues_to_process = [league]
        else:
            leagues_to_process = list(LEAGUE_MAP.keys())

        # Create a single driver (headless) for all leagues
        driver = Driver(uc=True, headless2=True)

        total_leagues = len(leagues_to_process)
        _backfill_jobs[job_id]["total"] = total_leagues

        for idx, lc in enumerate(leagues_to_process):
            _backfill_jobs[job_id]["progress"] = idx
            _backfill_jobs[job_id]["current_league"] = lc
            _backfill_jobs[job_id]["message"] = f"Processing {lc} ({idx+1}/{total_leagues})"

            # Call the original backfill function (it will use its own driver and db session)
            # We pass the existing driver to avoid creating a new one for each league.
            backfill_league(lc, start_date, end_date, driver=driver)

        _backfill_jobs[job_id]["status"] = "done"
        _backfill_jobs[job_id]["message"] = "Backfill completed successfully."

    except Exception as e:
        _backfill_jobs[job_id]["status"] = "error"
        _backfill_jobs[job_id]["error"] = str(e)
        import traceback
        _backfill_jobs[job_id]["traceback"] = traceback.format_exc()
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass
        db.close()


@router.post("/backfill-match-stats", response_model=BackfillJobStatus)
async def start_backfill_match_stats(
    background_tasks: BackgroundTasks,
    league: Optional[str] = Query(None, description="Single league code, omit for all"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
):
    """
    Start a background job to backfill player match statistics.
    This will scrape all completed matches from FBref and store player‑level stats.
    The job runs asynchronously – you will receive a job_id to poll status.
    """
    # Parse dates if provided
    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None

    # Validate league if provided
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
