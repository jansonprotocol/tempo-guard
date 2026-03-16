"""
backend/scripts/scrape_daily_update.py

ATHENA Daily Lightweight Updater.

Run this EVERY DAY (via cron/Render scheduler) to:
- Fetch new match results from last 7 days
- Update player stats for teams that played
- Update upcoming fixtures for next 14 days
- Recompute power indices for affected leagues

Usage:
    python -m scripts.scrape_daily_update
    python -m scripts.scrape_daily_update --days-back 14
"""

import sys
from pathlib import Path
from datetime import date, timedelta, datetime  # <-- added datetime
from typing import Set

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from dotenv import load_dotenv
load_dotenv()

from app.database.db import SessionLocal
from app.database.models_predictions import FBrefFixture
from app.services.daily_updater import update_league_daily

# Constants
DAYS_BACK_DEFAULT = 7          # Look back this many days for new results
DAYS_AHEAD_DEFAULT = 14        # Look ahead this many days for fixtures
SLEEP_BETWEEN_LEAGUES = 5      # seconds

def get_active_leagues(days_back: int) -> list[str]:
    """Get leagues that have had matches in the last N days."""
    db = SessionLocal()
    try:
        cutoff = date.today() - timedelta(days=days_back)
        fixtures = db.query(FBrefFixture.league_code).distinct().filter(
            FBrefFixture.match_date >= cutoff
        ).all()
        return [f[0] for f in fixtures]
    finally:
        db.close()

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Daily lightweight updater")
    parser.add_argument("--days-back", type=int, default=DAYS_BACK_DEFAULT,
                       help=f"Days to look back (default: {DAYS_BACK_DEFAULT})")
    parser.add_argument("--league", type=str, help="Single league to update")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--api", type=str, help="ScraperAPI key")
    args = parser.parse_args()

    if args.api:
        os.environ["SCRAPER_API_KEY"] = args.api

    # Determine which leagues to update
    if args.league:
        leagues = [args.league]
    else:
        leagues = get_active_leagues(args.days_back)

    if not leagues:
        print("✅ No active leagues found. Nothing to update.")
        return

    print(f"\n🔄 Daily update starting for {len(leagues)} leagues")
    print(f"   Looking back {args.days_back} days\n")

    for i, league in enumerate(leagues):
        print(f"\n{'='*60}")
        print(f"📅 {league} ({i+1}/{len(leagues)})")
        print(f"{'='*60}")

        try:
            update_league_daily(
                league,
                days_back=args.days_back,
                days_ahead=DAYS_AHEAD_DEFAULT,
                headless=args.headless
            )
        except Exception as e:
            print(f"❌ Error updating {league}: {e}")

        if i < len(leagues) - 1:
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n✅ Daily update complete!")

if __name__ == "__main__":
    main()
