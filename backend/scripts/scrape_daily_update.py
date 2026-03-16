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
    python -m scripts.scrape_daily_update --force
    python -m scripts.scrape_daily_update --db-url postgresql://user:pass@host/db
"""

import sys
import os
import time
from datetime import datetime, date, timedelta
from pathlib import Path

# ----------------------------------------------------------------------
# 1. Load environment variables EARLY – before any app imports
# ----------------------------------------------------------------------
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"   # backend/.env
print(f"Looking for .env at: {env_path}")
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print("✅ .env file loaded.")
else:
    print("⚠️  No .env file found, relying on system environment variables.")

# ----------------------------------------------------------------------
# 2. Handle command-line overrides for DATABASE_URL
#    (must be done before importing app modules)
# ----------------------------------------------------------------------
import argparse
parser = argparse.ArgumentParser(description="Daily lightweight updater")
parser.add_argument("--days-back", type=int, default=7,
                   help="Days to look back (default: 7)")
parser.add_argument("--league", type=str, help="Single league to update")
parser.add_argument("--headless", action="store_true", help="Headless Chrome")
parser.add_argument("--api", type=str, help="ScraperAPI key")
parser.add_argument("--force", action="store_true",
                   help="Ignore cache and force full refresh")
parser.add_argument("--db-url", type=str,
                   help="Override DATABASE_URL for this run")
args, unknown = parser.parse_known_args()   # parse known args, ignore others

# Apply command-line overrides
if args.api:
    os.environ["SCRAPER_API_KEY"] = args.api
if args.db_url:
    os.environ["DATABASE_URL"] = args.db_url
    print("✅ Using database URL from --db-url argument.")

# ----------------------------------------------------------------------
# 3. Verify DATABASE_URL is set
# ----------------------------------------------------------------------
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("\n❌ ERROR: DATABASE_URL environment variable is not set.")
    print("   Please create a .env file in the backend folder with:")
    print('   DATABASE_URL=postgresql://user:pass@host:port/dbname?sslmode=require')
    print("   Or set it manually before running the script.")
    print("   Or use --db-url argument.")
    sys.exit(1)

# Print masked version for debugging
if "@" in db_url:
    userpass, rest = db_url.split("@", 1)
    if ":" in userpass:
        user, _ = userpass.split(":", 1)
        masked = f"{user}:****@{rest}"
    else:
        masked = db_url
else:
    masked = db_url
print(f"✅ Using database: {masked}")

# ----------------------------------------------------------------------
# 4. Now it's safe to import app modules
# ----------------------------------------------------------------------
path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.database.models_predictions import FBrefFixture
from app.services.daily_updater import update_league_daily

# Constants
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
    # Determine which leagues to update
    if args.league:
        leagues = [args.league]
    else:
        leagues = get_active_leagues(args.days_back)

    if not leagues:
        print("✅ No active leagues found. Nothing to update.")
        return

    print(f"\n🔄 Daily update starting for {len(leagues)} leagues")
    print(f"   Looking back {args.days_back} days")
    if args.force:
        print("   ⚡ Force mode enabled – will ignore stats fetch cache")

    for i, league in enumerate(leagues):
        print(f"\n{'='*60}")
        print(f"📅 {league} ({i+1}/{len(leagues)})")
        print(f"{'='*60}")

        try:
            update_league_daily(
                league,
                days_back=args.days_back,
                days_ahead=DAYS_AHEAD_DEFAULT,
                headless=args.headless,
                force=args.force
            )
        except Exception as e:
            print(f"❌ Error updating {league}: {e}")

        if i < len(leagues) - 1:
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n✅ Daily update complete!")

if __name__ == "__main__":
    main()
