# backend/app/services/daily_updater.py
from datetime import datetime, date, timedelta
from typing import Set

from app.database.db import SessionLocal
from app.database.models_predictions import FBrefFixture
from app.services.scrapers.fixture_scraper import update_fixtures_for_league
from app.services.scrapers.player_scraper import update_player_stats_for_teams

def update_league_daily(
    league_code: str,
    days_back: int = 7,
    days_ahead: int = 14,
    headless: bool = False,
    force_player_update: bool = False
) -> dict:
    """
    Daily update for a single league:
    1. Fetch new fixtures/results
    2. Update player stats for teams that played
    3. Recompute power indices if needed
    """
    print(f"\n🔄 Daily update for {league_code}")

    # Step 1: Get teams that played in last N days
    db = SessionLocal()
    try:
        cutoff = date.today() - timedelta(days=days_back)
        recent_fixtures = db.query(FBrefFixture).filter(
            FBrefFixture.league_code == league_code,
            FBrefFixture.match_date >= cutoff
        ).all()

        affected_teams: Set[str] = set()
        for fix in recent_fixtures:
            affected_teams.add(fix.home_team)
            affected_teams.add(fix.away_team)

        print(f"   → {len(recent_fixtures)} recent matches")
        print(f"   → {len(affected_teams)} teams played")
    finally:
        db.close()

    # Step 2: Update fixtures (get new results + upcoming)
    print("\n📋 Updating fixtures...")
    update_fixtures_for_league(league_code, headless=headless)

    # Step 3: Update player stats for affected teams
    if affected_teams:
        print("\n👤 Updating player stats...")
        update_player_stats_for_teams(
            league_code,
            affected_teams,
            force=force_player_update,
            headless=headless
        )

        # Step 4: Recompute power indices
        print("\n⚡ Recomputing power indices...")
        from app.services.player_index import compute_league_power
        from scripts.scrape_players import SEASON_MAP
        db = SessionLocal()
        try:
            season = SEASON_MAP.get(league_code, "2025-2026")
            result = compute_league_power(db, league_code, season)
            print(f"   → Power indices updated for {league_code}")
        finally:
            db.close()
    else:
        print("\n👤 No teams played – skipping player update.")

    return {
        "league_code": league_code,
        "teams_updated": len(affected_teams),
        "timestamp": datetime.utcnow().isoformat()
    }
