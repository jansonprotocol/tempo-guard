# backend/app/services/full_history_loader.py
"""
Complete historical data loader – orchestrates fixture + player scraping,
then recomputes power and form delta.
"""
import sys
from pathlib import Path
from typing import Optional

path_root = Path(__file__).resolve().parents[2]  # to backend/
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.services.scrapers.fixture_scraper import update_fixtures_for_league  # <-- CORRECT
from app.services.scrapers.player_scraper import update_player_stats_for_teams
from app.services.player_index import compute_league_power
from app.services.form_delta import compute_form_delta
from scripts.scrape_players import SEASON_MAP

def load_league_full_history(league_code: str, headless: bool = False, force: bool = False) -> dict:
    """
    Step 1: Scrape all fixtures (current + previous) – uses update_fixtures_for_league
    Step 2: Scrape all player stats – uses update_player_stats_for_teams (with all teams)
    Step 3: Compute player power indices
    Step 4: Compute form delta / standings
    """
    print(f"\n🔨 Full history load for {league_code}")

    # 1. Fixtures
    print("  📋 Step 1/4: Scraping fixtures...")
    try:
        update_fixtures_for_league(league_code, headless=headless)
    except Exception as e:
        return {"league_code": league_code, "error": f"Fixture scraping failed: {e}"}

    # 2. Player stats – we need to scrape for ALL teams in the league.
    #    Pass an empty set to indicate "all teams" (the scraper will fetch the whole league page)
    print("  👤 Step 2/4: Scraping player stats...")
    try:
        update_player_stats_for_teams(
            league_code,
            set(),  # empty set = all teams
            force=force,
            headless=headless
        )
    except Exception as e:
        return {"league_code": league_code, "error": f"Player scraping failed: {e}"}

    # 3. Compute power indices
    print("  ⚡ Step 3/4: Computing player power indices...")
    db = SessionLocal()
    try:
        season = SEASON_MAP.get(league_code, "2025-2026")
        power_result = compute_league_power(db, league_code, season)
        print(f"     → {power_result.get('players_indexed', 0)} players indexed")
        print(f"     → {power_result.get('teams_updated', 0)} teams updated")
    except Exception as e:
        db.close()
        return {"league_code": league_code, "error": f"Power computation failed: {e}"}
    db.close()

    # 4. Compute form delta
    print("  📊 Step 4/4: Computing league standings...")
    db = SessionLocal()
    try:
        delta_result = compute_form_delta(db, league_code)
        teams = delta_result.get('teams', [])
        print(f"     → {len(teams)} teams in standings")
    except Exception as e:
        db.close()
        return {"league_code": league_code, "error": f"Form delta failed: {e}"}
    db.close()

    return {
        "league_code": league_code,
        "status": "success",
        "players_indexed": power_result.get('players_indexed', 0),
        "teams_updated": power_result.get('teams_updated', 0)
    }
