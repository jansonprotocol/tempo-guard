# backend/app/services/scrapers/player_scraper.py
"""
Temporary wrapper for the old player scraper.
"""
from scripts.scrape_players import scrape_league_players, SCHEDULE_URLS

def update_player_stats_for_teams(league_code: str, teams: set, force: bool = False, headless: bool = False) -> None:
    """
    Update player stats for a league. If teams is provided, it still scrapes
    the entire league page (FBref doesn't have per-team stats pages), but
    we'll only update stats for the given teams (handled by interval logic).
    """
    url = SCHEDULE_URLS.get(league_code)
    if not url:
        raise ValueError(f"Unknown league: {league_code}")
    # Patch HEADLESS in the module
    import scripts.scrape_players as sp
    original_headless = sp.HEADLESS
    sp.HEADLESS = headless
    try:
        scrape_league_players(league_code, url, force=force)
    finally:
        sp.HEADLESS = original_headless
