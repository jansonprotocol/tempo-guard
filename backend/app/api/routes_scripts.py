# backend/app/api/routes_scripts.py
"""
API endpoints for running maintenance scripts as background tasks.
"""
from __future__ import annotations

import sys
import os
from pathlib import Path
from fastapi import APIRouter, BackgroundTasks, Query, HTTPException
from pydantic import BaseModel
import uuid
from datetime import date
from typing import Optional

router = APIRouter(prefix="/scripts", tags=["Scripts"])

# ----------------------------------------------------------------------
# DEBUG ENDPOINT – helps locate the scripts folder
# ----------------------------------------------------------------------
@router.get("/debug-paths")
async def debug_paths():
    """Return diagnostic information about file paths to help locate the scripts folder."""
    import os
    from pathlib import Path
    
    current_file = Path(__file__).resolve()
    cwd = Path.cwd().resolve()
    
    # Common possible locations for the scripts folder
    possible_paths = [
        current_file.parent.parent.parent / "scripts",               # /app/scripts
        current_file.parent.parent.parent / "backend" / "scripts",   # /app/backend/scripts
        current_file.parent.parent / "scripts",                      # /app/app/scripts
        current_file.parent / "scripts",                              # /app/api/scripts (unlikely)
        cwd / "scripts",                                              # from current working directory
        cwd / "backend" / "scripts",
        Path("/app/scripts"),
        Path("/app/backend/scripts"),
        Path("/opt/render/project/src/scripts"),                     # sometimes Render uses this
        Path("/opt/render/project/src/backend/scripts"),
    ]
    
    results = {
        "current_file": str(current_file),
        "cwd": str(cwd),
        "possible_locations": []
    }
    
    for p in possible_paths:
        exists = p.exists()
        results["possible_locations"].append({
            "path": str(p),
            "exists": exists,
            "is_dir": p.is_dir() if exists else False,
        })
        if exists and p.is_dir():
            # List first few items to confirm
            try:
                contents = [str(f.name) for f in p.iterdir()][:10]
                results["possible_locations"][-1]["contents_sample"] = contents
            except:
                pass
    return results


# ----------------------------------------------------------------------
# Dynamic scripts folder finder
# ----------------------------------------------------------------------
def find_scripts_folder() -> Path:
    """Locate the scripts folder by checking common locations."""
    current_file = Path(__file__).resolve()
    cwd = Path.cwd().resolve()
    
    candidates = [
        current_file.parent.parent.parent / "scripts",
        current_file.parent.parent.parent / "backend" / "scripts",
        current_file.parent.parent / "scripts",
        cwd / "scripts",
        cwd / "backend" / "scripts",
        Path("/app/scripts"),
        Path("/app/backend/scripts"),
        Path("/opt/render/project/src/scripts"),
        Path("/opt/render/project/src/backend/scripts"),
    ]
    
    for path in candidates:
        if path.exists() and path.is_dir():
            return path
    
    raise RuntimeError(
        f"Could not locate 'scripts' folder. Tried:\n" +
        "\n".join(f"  {p}" for p in candidates)
    )

# Add the found scripts folder to sys.path
scripts_root = find_scripts_folder()
sys.path.insert(0, str(scripts_root.parent))  # add the parent so that 'import scripts' works

# Now we can import from scripts
from scripts.backfill_match_stats import backfill_league

# ----------------------------------------------------------------------
# Background job infrastructure
# ----------------------------------------------------------------------
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
    from seleniumbase import Driver
    import time
    from app.database.db import SessionLocal
    from app.core.constants import LEAGUE_MAP

    db = SessionLocal()
    driver = None
    try:
        _backfill_jobs[job_id]["status"] = "running"
        _backfill_jobs[job_id]["message"] = "Starting backfill..."

        if league:
            leagues_to_process = [league]
        else:
            leagues_to_process = list(LEAGUE_MAP.keys())

        driver = Driver(uc=True, headless2=True)
        total_leagues = len(leagues_to_process)
        _backfill_jobs[job_id]["total"] = total_leagues

        for idx, lc in enumerate(leagues_to_process):
            _backfill_jobs[job_id]["progress"] = idx
            _backfill_jobs[job_id]["current_league"] = lc
            _backfill_jobs[job_id]["message"] = f"Processing {lc} ({idx+1}/{total_leagues})"

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
    start = date.fromisoformat(start_date) if start_date else None
    end = date.fromisoformat(end_date) if end_date else None

    from app.core.constants import LEAGUE_MAP
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
    job = _backfill_jobs.get(job_id)
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")
    return BackfillJobStatus(**job)
