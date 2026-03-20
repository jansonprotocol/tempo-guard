"""
backend/scripts/scrape_full_history.py

ATHENA Complete Historical Data Loader.

Run this ONCE per league when:
- Adding a new league to the system
- Doing a complete data reset
- Backfilling after schema changes

Fetches:
- All fixtures (current + previous seasons)
- All player stats for all teams in the league
- Computes league standings (teams.current_position)
- Builds squad power indices

Usage:
    python -m scripts.scrape_full_history --league ENG-PL
    python -m scripts.scrape_full_history --all              # all leagues
    python -m scripts.scrape_full_history --league ENG-PL --force   # force full refresh
    python -m scripts.scrape_full_history --league ENG-PL --skip-standings
"""

import sys
import os
import time
from datetime import datetime
from pathlib import Path

# ----------------------------------------------------------------------
# 1. Load environment variables EARLY – before any app imports
# ----------------------------------------------------------------------
from dotenv import load_dotenv

env_path = Path(__file__).parent.parent / ".env"
print(f"Looking for .env at: {env_path}")
if env_path.exists():
    load_dotenv(dotenv_path=env_path, override=True)
    print("✅ .env file loaded.")
else:
    print("⚠️  No .env file found, relying on system environment variables.")

# ----------------------------------------------------------------------
# 2. Verify DATABASE_URL is set
# ----------------------------------------------------------------------
db_url = os.getenv("DATABASE_URL")
if not db_url:
    print("\n❌ ERROR: DATABASE_URL environment variable is not set.")
    print("   Please create a .env file in the backend folder with:")
    print('   DATABASE_URL=postgresql://user:pass@host:port/dbname?sslmode=require')
    print("   Or set it manually before running the script.")
    sys.exit(1)

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
# 3. Now safe to import app modules
# ----------------------------------------------------------------------
path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.services.full_history_loader import load_league_full_history
from app.core.constants import LEAGUE_MAP
from scripts.scrape_fixtures import scrape_league_standings

SLEEP_BETWEEN_LEAGUES = 30
SLEEP_BEFORE_STANDINGS = 5

ALL_LEAGUES = [
    "ENG-PL", "ENG-CH", "ESP-LL", "ESP-LL2", "FRA-L1", "FRA-L2",
    "GER-BUN", "GER-B2", "ITA-SA", "ITA-SB", "NED-ERE", "TUR-SL",
    "BRA-SA", "BRA-SB", "MLS", "SAU-SPL", "DEN-SL", "BEL-PL",
    "NOR-EL", "SWE-AL", "MEX-LMX", "CHN-CSL", "JPN-J1", "COL-PA",
    "AUT-BL", "SUI-SL", "CHI-LP", "PER-L1", "POR-LP",
    "UCL", "UEL", "UECL",
]


def _schedule_url_for(league_code: str) -> str | None:
    entry = LEAGUE_MAP.get(league_code)
    if not entry:
        return None
    return entry[0] if isinstance(entry, tuple) else entry


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Complete historical data loader")
    parser.add_argument("--league", type=str, help="Single league code")
    parser.add_argument("--all", action="store_true", help="Load all leagues")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--api", type=str, help="ScraperAPI key")
    parser.add_argument("--force", action="store_true", help="Force full refresh (ignore cache)")
    parser.add_argument("--skip-standings", action="store_true",
                        help="Skip the standings scrape step")
    args = parser.parse_args()

    if args.api:
        os.environ["SCRAPER_API_KEY"] = args.api

    leagues_to_process = []
    if args.all:
        leagues_to_process = ALL_LEAGUES
    elif args.league:
        leagues_to_process = [args.league]
    else:
        print("Please specify --league or --all")
        return

    if args.skip_standings:
        print("⏭  Standings refresh skipped (--skip-standings)")

    standings_results: dict = {}

    for i, league in enumerate(leagues_to_process):
        print(f"\n{'='*70}")
        print(f"📊 Processing {league} ({i+1}/{len(leagues_to_process)})")
        print(f"{'='*70}")

        # ── Full history (fixtures, scores, player stats) ────────────
        try:
            load_league_full_history(
                league,
                headless=args.headless,
                force=args.force,
            )
        except Exception as e:
            print(f"❌ Error processing {league}: {e}")
            import traceback
            traceback.print_exc()

        # ── Standings ────────────────────────────────────────────────
        if not args.skip_standings:
            time.sleep(SLEEP_BEFORE_STANDINGS)
            print(f"\n📋 Fetching current standings for {league}...")
            try:
                schedule_url = _schedule_url_for(league)
                updated = scrape_league_standings(league, schedule_url=schedule_url)
                standings_results[league] = updated
                if updated:
                    print(f"   ✅ {league}: {updated} team position(s) updated")
                else:
                    print(f"   ⚠️  {league}: standings not updated (international comp or no data)")
            except Exception as e:
                standings_results[league] = 0
                print(f"   ❌ Standings error for {league}: {e}")

        if i < len(leagues_to_process) - 1:
            print(f"\n😴 Waiting {SLEEP_BETWEEN_LEAGUES}s before next league...")
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    # ── Summary ──────────────────────────────────────────────────────
    print("\n✅ Full history load complete!")
    if standings_results:
        total_positions = sum(standings_results.values())
        leagues_updated = sum(1 for v in standings_results.values() if v > 0)
        print(f"   Standings: {total_positions} team positions set across {leagues_updated}/{len(standings_results)} leagues")


if __name__ == "__main__":
    main()
