# backend/app/services/backfill_service.py
"""
Core logic for backfilling player match statistics.
Shared between the CLI script and the API endpoint.
"""

from datetime import date
from typing import Optional
import time
import pandas as pd
import io
from seleniumbase import Driver

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.services.data_providers.fbref_base import _parse_score_column, _resolve_columns
from app.services.scrapers.match_stats_scraper import scrape_match_player_stats, _store_match_stats
from app.services.resolve_team import resolve_team_name

def backfill_league(
    league_code: str,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    driver: Optional[Driver] = None
) -> dict:
    """
    Backfill player match statistics for a single league.
    Returns a summary dict with counts.
    """
    db = SessionLocal()
    close_driver = False
    if driver is None:
        driver = Driver(uc=True, headless2=True)
        close_driver = True

    summary = {
        "league_code": league_code,
        "matches_processed": 0,
        "player_stats_stored": 0,
        "errors": 0,
    }

    try:
        snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if not snap:
            summary["error"] = f"No snapshot for {league_code}"
            return summary

        df = pd.read_parquet(io.BytesIO(snap.data))
        score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
        if score_col and "hg" not in df.columns:
            df = _parse_score_column(df, score_col)
        c = _resolve_columns(df)

        if start_date:
            df = df[df[c["date"]] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df[c["date"]] <= pd.Timestamp(end_date)]

        df = df[df[c["score"]].notna()]  # only completed matches

        total = len(df)
        for i, (_, row) in enumerate(df.iterrows()):
            match_date = row[c["date"]].date()
            home_raw = str(row[c["ht"]]).strip()
            away_raw = str(row[c["at"]]).strip()

            home = resolve_team_name(db, home_raw, league_code)
            away = resolve_team_name(db, away_raw, league_code)

            match_url = row.get("Match Report", "") if "Match Report" in row else None
            if not match_url or not match_url.startswith("http"):
                summary["errors"] += 1
                continue

            try:
                player_stats = scrape_match_player_stats(
                    match_url, league_code, match_date, home, away, driver
                )
                if player_stats:
                    _store_match_stats(db, player_stats, match_date, league_code)
                    summary["player_stats_stored"] += len(player_stats)
                summary["matches_processed"] += 1
                # Be nice to FBref
                time.sleep(2)
            except Exception as e:
                summary["errors"] += 1
                print(f"Error processing match {match_url}: {e}")

        return summary

    finally:
        if close_driver and driver:
            driver.quit()
        db.close()
