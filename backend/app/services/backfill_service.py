# backend/app/services/backfill_service.py
"""
Core logic for backfilling player match statistics.
Shared between the CLI script and the API endpoint.
"""

import os
import sys
import io
import time  # <-- ADDED
import json
from pathlib import Path
from typing import Optional, List, Dict, Any
from datetime import datetime, date, timedelta

from dotenv import load_dotenv
from seleniumbase import Driver

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import func, and_

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog
from app.models.team_config import TeamConfig
from app.models.league_config import LeagueConfig
from app.models.models_players import Player, PlayerSeasonStats  # <-- ADDED
from app.services.data_providers.fbref_base import asof_features, _parse_score_column, _resolve_columns
from app.services.predict import predict_match
from app.services.resolve_team import resolve_team_name
from app.engine.types import MatchRequest
from app.util.asian_lines import evaluate_market, hit_weight

# The following function needs to be imported or defined
# Assuming scrape_match_player_stats exists elsewhere, if not we need to implement it
try:
    from app.services.scrapers.match_player_scraper import scrape_match_player_stats
except ImportError:
    # Fallback implementation if the module doesn't exist
    def scrape_match_player_stats(url, league_code, match_date, home, away, driver):
        """
        Placeholder - replace with actual implementation.
        """
        print(f"WARNING: Using placeholder scrape_match_player_stats for {url}")
        return []

def _store_match_stats(db, player_stats, match_date, league_code):
    """
    Store player statistics from a single match into PlayerSeasonStats.
    """
    season = _get_season_for_date(match_date, league_code)
    for stat in player_stats:
        player = db.query(Player).filter_by(fbref_id=stat["fbref_id"]).first()
        if not player:
            continue

        existing = db.query(PlayerSeasonStats).filter_by(
            player_id=player.id,
            season=season,
            league_code=league_code
        ).first()

        if existing:
            # Update existing record
            existing.matches_played += 1
            existing.minutes += stat.get("minutes", 0)
            # Update other stats as needed
        else:
            # Create new record
            new_stats = PlayerSeasonStats(
                player_id=player.id,
                season=season,
                league_code=league_code,
                matches_played=1,
                minutes=stat.get("minutes", 0),
                goals_per90=stat.get("goals_per90", 0),
                assists_per90=stat.get("assists_per90", 0),
                xg_per90=stat.get("xg_per90", 0),
                xa_per90=stat.get("xa_per90", 0),
                progressive_passes_per90=stat.get("progressive_passes_per90", 0),
                progressive_carries_per90=stat.get("progressive_carries_per90", 0),
                pass_completion_pct=stat.get("pass_completion_pct", 0),
                tackles_won_per90=stat.get("tackles_won_per90", 0),
                interceptions_per90=stat.get("interceptions_per90", 0),
                blocks_per90=stat.get("blocks_per90", 0),
                clearances_per90=stat.get("clearances_per90", 0),
                aerials_won_pct=stat.get("aerials_won_pct", 0),
                save_pct=stat.get("save_pct", 0),
                cs_pct=stat.get("cs_pct", 0),
                psxg_minus_ga=stat.get("psxg_minus_ga", 0),
                last_updated=datetime.utcnow()
            )
            db.add(new_stats)

def _get_season_for_date(match_date: date, league_code: str) -> str:
    """
    Determine which season label to use for a given date.
    """
    from app.core.constants import SEASON_MAP
    cutoff = _get_season_cutoff(league_code)
    if match_date >= cutoff:
        return SEASON_MAP.get(league_code, "2025-2026")
    else:
        # Previous season
        season = SEASON_MAP.get(league_code, "2025-2026")
        if "-" in season:
            start, end = season.split("-")
            return f"{int(start)-1}-{int(end)-1}"
        else:
            return str(int(season) - 1)

def _get_season_cutoff(league_code: str) -> date:
    """
    Return the cutoff date between seasons for a league.
    """
    _AUG_MAY_CUTOFF = date(2025, 7, 1)
    _CALENDAR_CUTOFF = date(2025, 12, 15)
    _CALENDAR_YEAR_PREFIXES = {"BRA", "MLS", "NOR", "SWE", "CHN", "JPN", "COL"}

    prefix = league_code.split("-")[0] if "-" in league_code else league_code
    if prefix in _CALENDAR_YEAR_PREFIXES:
        return _CALENDAR_CUTOFF
    return _AUG_MAY_CUTOFF

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
        print(f"Processing {total} matches for {league_code}")

        for i, (_, row) in enumerate(df.iterrows()):
            match_date = row[c["date"]].date()
            home_raw = str(row[c["ht"]]).strip()
            away_raw = str(row[c["at"]]).strip()

            home = resolve_team_name(db, home_raw, league_code)
            away = resolve_team_name(db, away_raw, league_code)

            match_url = row.get("Match Report", "") if "Match Report" in row else None
            if not match_url or not match_url.startswith("http"):
                print(f"  Skipping match {i+1}/{total}: no Match Report URL")
                summary["errors"] += 1
                continue

            print(f"  Processing match {i+1}/{total}: {home_raw} vs {away_raw}")

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
                print(f"    Error: {e}")
                import traceback
                traceback.print_exc()

        return summary

    finally:
        if close_driver and driver:
            driver.quit()
        db.close()
