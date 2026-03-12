"""
backend/scripts/update_players.py

ATHENA v2.0 — Lightweight post-matchday player stats updater.

Run this AFTER scrape_fixtures.py to refresh only the players whose teams
played since the last update. Much faster than a full scrape_players.py run.

Workflow:
  1. Query FBrefFixtures for matches in the last N days
  2. Collect unique teams that played
  3. For each team's league, re-fetch ONLY that league's stats pages
     (if not already fetched today)
  4. Update PlayerSeasonStats for affected players
  5. Recompute power indices for affected leagues

This is designed to slot into your daily workflow:
  1. python -m scripts.scrape_fixtures     (daily — updates scores + fixtures)
  2. python -m scripts.update_players      (daily — refreshes affected players)
  3. POST /api/batch-predict               (predictions for upcoming matches)

Usage:
    cd backend
    venv312\\Scripts\\activate
    python -m scripts.update_players              # last 3 days
    python -m scripts.update_players --days 7     # last 7 days
    python -m scripts.update_players --reindex    # also recompute power indices

NOTE: Uses same Selenium/ScraperAPI infrastructure as scrape_players.py.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database.db import SessionLocal
from app.database.models_predictions import FBrefFixture


def get_affected_leagues(days: int = 3) -> dict[str, set[str]]:
    """
    Query FBrefFixtures for recent matches and return
    { league_code: {team1, team2, ...} } for leagues that had activity.
    """
    db = SessionLocal()
    try:
        cutoff = date.today() - timedelta(days=days)
        fixtures = (
            db.query(FBrefFixture)
            .filter(FBrefFixture.match_date >= cutoff)
            .all()
        )

        leagues: dict[str, set[str]] = {}
        for fix in fixtures:
            leagues.setdefault(fix.league_code, set())
            leagues[fix.league_code].add(fix.home_team)
            leagues[fix.league_code].add(fix.away_team)

        return leagues
    finally:
        db.close()


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Update player stats for recent matches")
    parser.add_argument("--days", type=int, default=3, help="Look back N days for matches (default: 3)")
    parser.add_argument("--reindex", action="store_true", help="Also recompute power indices")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--api", type=str, default=None, metavar="KEY", help="ScraperAPI key")
    parser.add_argument("--force", action="store_true", help="Ignore 10-match interval")
    args = parser.parse_args()

    # Step 1: Find affected leagues
    affected = get_affected_leagues(args.days)

    if not affected:
        print(f"[update] No matches found in the last {args.days} days. Nothing to update.")
        sys.exit(0)

    total_teams = sum(len(teams) for teams in affected.values())
    print(f"[update] Found {total_teams} teams across {len(affected)} leagues in last {args.days} days:")
    for lc, teams in sorted(affected.items()):
        print(f"  {lc}: {len(teams)} teams")

    # Step 2: Import and run scrape_players for each affected league
    # We import here to avoid loading Selenium unless needed
    from scripts.scrape_players import (
        scrape_league_players, SCHEDULE_URLS,
        SLEEP_BETWEEN_LEAGUES,
    )
    import scripts.scrape_players as sp
    import time

    if args.headless:
        sp.HEADLESS = True
    if args.api:
        sp.SCRAPER_API_KEY = args.api

    scraped_leagues = []
    for i, (league_code, teams) in enumerate(sorted(affected.items())):
        if league_code not in SCHEDULE_URLS:
            print(f"\n[update] {league_code} not in SCHEDULE_URLS — skipping")
            continue

        print(f"\n[update] Scraping {league_code} ({len(teams)} teams played)...")
        scrape_league_players(league_code, SCHEDULE_URLS[league_code], args.force)
        scraped_leagues.append(league_code)

        if i < len(affected) - 1:
            print(f"  Waiting {SLEEP_BETWEEN_LEAGUES}s...")
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    # Step 3: Optionally recompute power indices
    if args.reindex and scraped_leagues:
        print(f"\n[update] Recomputing power indices for {len(scraped_leagues)} leagues...")

        from app.services.player_index import compute_league_power
        from scripts.scrape_players import SEASON_MAP

        db = SessionLocal()
        try:
            for league_code in scraped_leagues:
                season = SEASON_MAP.get(league_code, "2025-2026")
                try:
                    result = compute_league_power(db, league_code, season)
                    print(f"  {league_code}: {result.get('players_indexed', 0)} players, "
                          f"{result.get('teams_updated', 0)} teams")
                except Exception as e:
                    print(f"  {league_code}: error — {e}")
        finally:
            db.close()

    print(f"\n[update] Done. Updated {len(scraped_leagues)} leagues.")
