"""
backend/app/services/full_history_loader.py

Core logic for loading complete historical data for a league.
"""

import time
from datetime import datetime
from typing import Optional

from app.database.db import SessionLocal
from app.services.data_providers.fbref_urls import extract_comp_info
from app.services.scrapers.fixture_scraper import scrape_fixtures_for_league
from app.services.scrapers.player_scraper import scrape_player_stats_for_league
from app.services.player_index import compute_league_power
from app.services.form_delta import compute_form_delta
from app.util.team_resolver import ensure_team_exists

# Reuse your existing scraper functions, but orchestrate them

def load_league_full_history(league_code: str, headless: bool = False) -> dict:
    """
    Load complete historical data for a league:
    1. Fixtures (current + previous seasons)
    2. Player stats for all teams
    3. Compute power indices
    4. Compute form delta/standings
    """
    print(f"\n🔨 Full history load for {league_code}")

    # Step 1: Scrape all fixtures (current + previous seasons)
    print("\n📋 Step 1/4: Scraping fixtures...")
    from scripts.scrape_fbref import scrape_league as scrape_fbref_league
    # (You'll need to adapt this to call your existing scraper functions)

    # Step 2: Scrape all player stats
    print("\n👤 Step 2/4: Scraping player stats...")
    from scripts.scrape_players import scrape_league_players

    # Step 3: Compute squad power indices
    print("\n⚡ Step 3/4: Computing player power indices...")
    db = SessionLocal()
    try:
        from scripts.scrape_players import SEASON_MAP
        season = SEASON_MAP.get(league_code, "2025-2026")
        result = compute_league_power(db, league_code, season)
        print(f"   → {result.get('players_indexed', 0)} players indexed")
        print(f"   → {result.get('teams_updated', 0)} teams updated")
    finally:
        db.close()

    # Step 4: Compute standings/form delta
    print("\n📊 Step 4/4: Computing league standings...")
    db = SessionLocal()
    try:
        form_delta = compute_form_delta(db, league_code)
        print(f"   → {len(form_delta.get('teams', []))} teams in standings")
    finally:
        db.close()

    return {
        "league_code": league_code,
        "status": "success",
        "timestamp": datetime.utcnow().isoformat()
    }
