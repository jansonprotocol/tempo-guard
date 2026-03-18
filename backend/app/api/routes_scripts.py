# backend/app/api/routes_scripts.py
"""
API endpoints for running maintenance scripts as background tasks.
"""
from __future__ import annotations

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

            # Call the core backfill function (adapted from the script)
            # We need to import the function here to avoid circular imports
            from scripts.backfill_match_stats import backfill_league as original_backfill
            # But original_backfill expects its own driver and db session.
            # We'll need to adapt it. For simplicity, we'll reimplement a simplified version here.
            # Alternatively, we can modify the original script to expose a function that accepts driver and db.
            # Given time, we'll integrate the core logic here.

            # Simplified backfill logic (copy‑pasted and adapted from the script)
            from app.database.models_fbref import FBrefSnapshot
            from app.services.data_providers.fbref_base import _parse_score_column, _resolve_columns
            from app.services.scrapers.match_stats_scraper import scrape_match_player_stats, _store_match_stats
            import pandas as pd
            import io

            snap = db.query(FBrefSnapshot).filter_by(league_code=lc).first()
            if not snap:
                _backfill_jobs[job_id]["message"] = f"No snapshot for {lc}, skipping"
                continue

            df = pd.read_parquet(io.BytesIO(snap.data))
            score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
            if score_col and "hg" not in df.columns:
                df = _parse_score_column(df, score_col)
            c = _resolve_columns(df)

            # Filter by date if provided
            if start_date:
                df = df[df[c["date"]] >= pd.Timestamp(start_date)]
            if end_date:
                df = df[df[c["date"]] <= pd.Timestamp(end_date)]

            # Only completed matches
            df = df[df[c["score"]].notna()]

            total_matches = len(df)
            for i, (_, row) in enumerate(df.iterrows()):
                match_date = row[c["date"]].date()
                home_raw = str(row[c["ht"]]).strip()
                away_raw = str(row[c["at"]]).strip()

                from app.services.resolve_team import resolve_team_name
                home = resolve_team_name(db, home_raw, lc)
                away = resolve_team_name(db, away_raw, lc)

                match_url = row.get("Match Report", "") if "Match Report" in row else None
                if not match_url or not match_url.startswith("http"):
                    continue

                player_stats = scrape_match_player_stats(
                    match_url, lc, match_date, home, away, driver
                )
                if player_stats:
                    _store_match_stats(db, player_stats, match_date, lc)

                _backfill_jobs[job_id]["message"] = f"{lc}: {i+1}/{total_matches} matches"

                # Be nice to FBref
                time.sleep(2)

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
