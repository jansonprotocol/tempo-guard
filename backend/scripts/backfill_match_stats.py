# backend/scripts/backfill_match_stats.py
"""
CLI wrapper for backfilling player match statistics.
"""
import argparse
import sys
from datetime import date
from pathlib import Path

# Add project root to path so that app can be imported
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.services.backfill_service import backfill_league
from app.core.constants import LEAGUE_MAP

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--league", type=str, help="Single league code")
    parser.add_argument("--all", action="store_true", help="Backfill all leagues")
    parser.add_argument("--start-date", type=str, help="Start date (YYYY-MM-DD)")
    parser.add_argument("--end-date", type=str, help="End date (YYYY-MM-DD)")
    args = parser.parse_args()

    start = date.fromisoformat(args.start_date) if args.start_date else None
    end = date.fromisoformat(args.end_date) if args.end_date else None

    if args.league:
        leagues = [args.league]
    elif args.all:
        leagues = list(LEAGUE_MAP.keys())
    else:
        print("Please specify --league or --all")
        return

    for lc in leagues:
        print(f"\nStarting backfill for {lc}")
        result = backfill_league(lc, start, end)
        print(f"  Matches processed: {result['matches_processed']}")
        print(f"  Player stats stored: {result['player_stats_stored']}")
        print(f"  Errors: {result['errors']}")

if __name__ == "__main__":
    main()
