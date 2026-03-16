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
- Computes league standings
- Builds squad power indices

Usage:
    python -m scripts.scrape_full_history --league ENG-PL
    python -m scripts.scrape_full_history --all              # all leagues
    python -m scripts.scrape_full_history --league ENG-PL --force   # force full refresh (ignore cache)
"""

import sys
import os
import time
from datetime import datetime
from pathlib import Path

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from dotenv import load_dotenv
load_dotenv()

from app.services.full_history_loader import load_league_full_history

# Constants
SLEEP_BETWEEN_LEAGUES = 30  # seconds to be kind to FBref

# All leagues we support
ALL_LEAGUES = [
    "ENG-PL", "ENG-CH", "ESP-LL", "ESP-LL2", "FRA-L1", "FRA-L2",
    "GER-BUN", "GER-B2", "ITA-SA", "ITA-SB", "NED-ERE", "TUR-SL",
    "BRA-SA", "BRA-SB", "MLS", "SAU-SPL", "DEN-SL", "BEL-PL",
    "NOR-EL", "SWE-AL", "MEX-LMX", "CHN-CSL", "JPN-J1", "COL-PA",
    "AUT-BL", "SUI-SL", "CHI-LP", "PER-L1", "POR-LP",
    "UCL", "UEL", "UECL"
]

def main():
    import argparse
    parser = argparse.ArgumentParser(description="Complete historical data loader")
    parser.add_argument("--league", type=str, help="Single league code")
    parser.add_argument("--all", action="store_true", help="Load all leagues")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--api", type=str, help="ScraperAPI key")
    parser.add_argument("--force", action="store_true", help="Force full refresh (ignore cache)")
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

    for i, league in enumerate(leagues_to_process):
        print(f"\n{'='*70}")
        print(f"📊 Processing {league} ({i+1}/{len(leagues_to_process)})")
        print(f"{'='*70}")

        try:
            load_league_full_history(
                league,
                headless=args.headless,
                force=args.force          # pass force down
            )
        except Exception as e:
            print(f"❌ Error processing {league}: {e}")
            import traceback
            traceback.print_exc()

        if i < len(leagues_to_process) - 1:
            print(f"\n😴 Waiting {SLEEP_BETWEEN_LEAGUES}s before next league...")
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n✅ Full history load complete!")

if __name__ == "__main__":
    main()
