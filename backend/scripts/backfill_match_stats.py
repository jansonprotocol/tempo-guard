"""
backend/scripts/backfill_match_stats.py

Backfill player match statistics for historical matches.
Run this once to populate the player_match_stats table.
"""

import sys
import time
from datetime import datetime, date
from pathlib import Path
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import FBrefFixture
from app.services.data_providers.fbref_base import _parse_score_column, _resolve_columns
from app.services.scrapers.match_stats_scraper import scrape_match_player_stats
from app.services.resolve_team import resolve_team_name
from seleniumbase import Driver

SLEEP_BETWEEN_REQUESTS = 3

def backfill_league(league_code: str, start_date: Optional[date] = None, end_date: Optional[date] = None):
    """Backfill match stats for a league."""
    print(f"\n{'='*60}")
    print(f"Backfilling {league_code}")
    
    db = SessionLocal()
    driver = Driver(uc=True, headless2=True)
    
    try:
        # Get completed matches from FBrefSnapshot
        snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if not snap:
            print(f"  No snapshot for {league_code}")
            return
        
        df = pd.read_parquet(io.BytesIO(snap.data))
        
        # Parse scores and columns
        score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
        if score_col and "hg" not in df.columns:
            df = _parse_score_column(df, score_col)
        
        c = _resolve_columns(df)
        
        # Filter by date if specified
        if start_date:
            df = df[df[c["date"]] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df[c["date"]] <= pd.Timestamp(end_date)]
        
        # Only completed matches
        df = df[df[c["score"]].notna()]
        
        total = len(df)
        processed = 0
        
        for _, row in df.iterrows():
            match_date = row[c["date"]].date()
            home_raw = str(row[c["ht"]]).strip()
            away_raw = str(row[c["at"]]).strip()
            
            # Resolve team names
            home = resolve_team_name(db, home_raw, league_code)
            away = resolve_team_name(db, away_raw, league_code)
            
            # Get match URL (if available in the data)
            match_url = row.get("Match Report", "") if "Match Report" in row else None
            
            if not match_url or not match_url.startswith("http"):
                print(f"  No match URL for {home} vs {away} on {match_date}, skipping")
                processed += 1
                continue
            
            try:
                # Scrape match stats
                player_stats = scrape_match_player_stats(
                    match_url, league_code, match_date, home, away, driver
                )
                
                # Store in database
                _store_match_stats(db, player_stats, match_date, league_code)
                
                processed += 1
                print(f"  Processed {processed}/{total}: {home} vs {away}")
                
                # Be nice to FBref
                time.sleep(SLEEP_BETWEEN_REQUESTS)
                
            except Exception as e:
                print(f"  Error processing {home} vs {away}: {e}")
                continue
        
        print(f"  Completed {processed}/{total} matches for {league_code}")
        
    finally:
        db.close()
        driver.quit()

def _store_match_stats(db, player_stats, match_date, league_code):
    """Store player match stats in database."""
    from app.models.models_players import Player, PlayerMatchStats
    
    for stat in player_stats:
        # Find player by name
        player = db.query(Player).filter(
            Player.name.ilike(f"%{stat['player_name']}%"),
            Player.league_code == league_code
        ).first()
        
        if not player:
            continue
        
        # Check if already exists
        existing = db.query(PlayerMatchStats).filter_by(
            player_id=player.id,
            match_date=match_date,
            league_code=league_code
        ).first()
        
        if existing:
            # Update
            for key, value in stat.items():
                if hasattr(existing, key) and key not in ["player_name"]:
                    setattr(existing, key, value)
        else:
            # Create new
            match_stats = PlayerMatchStats(
                player_id=player.id,
                match_date=match_date,
                league_code=league_code,
                opponent=stat.get("opponent"),
                is_home=stat.get("is_home", True),
                minutes=stat.get("minutes", 0),
                goals=stat.get("goals", 0),
                assists=stat.get("assists", 0),
                shots=stat.get("shots", 0),
                shots_on_target=stat.get("shots_on_target", 0),
                passes_completed=stat.get("passes_completed", 0),
                passes_attempted=stat.get("passes_attempted", 0),
                tackles=stat.get("tackles", 0),
                interceptions=stat.get("interceptions", 0),
                blocks=stat.get("blocks", 0),
                saves=stat.get("saves", 0),
                xg=stat.get("xg", 0.0),
                xa=stat.get("xa", 0.0),
            )
            db.add(match_stats)
    
    db.commit()

def main():
    """Main entry point."""
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, help="Single league to backfill")
    parser.add_argument("--all", action="store_true", help="Backfill all leagues")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()
    
    from app.core.constants import LEAGUE_MAP
    
    start_date = date.fromisoformat(args.start_date) if args.start_date else None
    end_date = date.fromisoformat(args.end_date) if args.end_date else None
    
    if args.league:
        backfill_league(args.league, start_date, end_date)
    elif args.all:
        for league in LEAGUE_MAP.keys():
            backfill_league(league, start_date, end_date)
            time.sleep(5)  # Wait between leagues
    else:
        print("Please specify --league or --all")

if __name__ == "__main__":
    main()
