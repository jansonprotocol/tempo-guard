"""
backend/scripts/recompute_power.py

Recompute player power indices and form delta for one or all leagues.
Usage:
    python -m scripts.recompute_power --league ENG-PL
    python -m scripts.recompute_power --all
"""

import sys
import os
from pathlib import Path
from dotenv import load_dotenv

load_dotenv()  # Load environment variables from .env

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.services.player_index import compute_league_power
from app.services.form_delta import compute_form_delta
from scripts.scrape_players import SEASON_MAP, SCHEDULE_URLS

def recompute_league(league_code: str):
    """Recompute both player power and form delta for a league."""
    print(f"\n📊 Recomputing for {league_code}")

    db = SessionLocal()
    try:
        # 1. Player power
        season = SEASON_MAP.get(league_code, "2025-2026")
        print(f"  ⚡ Player power (season {season})...")
        power_result = compute_league_power(db, league_code, season)
        print(f"     → {power_result.get('players_indexed', 0)} players")
        print(f"     → {power_result.get('teams_updated', 0)} teams")

        # 2. Form delta
        print(f"  📈 Form delta...")
        delta_result = compute_form_delta(db, league_code)
        teams = delta_result.get('teams', [])
        over = sum(1 for t in teams if t['form_delta'] >= 3)
        under = sum(1 for t in teams if t['form_delta'] <= -3)
        print(f"     → {len(teams)} teams")
        print(f"     → {over} overperforming, {under} underperforming")

    finally:
        db.close()

def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, help="Single league code")
    parser.add_argument("--all", action="store_true", help="All leagues")
    args = parser.parse_args()

    if args.all:
        leagues = list(SCHEDULE_URLS.keys())
        print(f"🔄 Recomputing {len(leagues)} leagues...")
        for league in leagues:
            recompute_league(league)
    elif args.league:
        recompute_league(args.league)
    else:
        print("Please specify --league or --all")

if __name__ == "__main__":
    main()
