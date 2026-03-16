# backend/app/services/scrapers/fixture_scraper.py
"""
Temporary wrapper for the old fixture scraper.
Will be replaced by refactored code later.
"""
from scripts.scrape_fixtures import scrape_league
from app.database.db import SessionLocal

def update_fixtures_for_league(league_code: str, headless: bool = False) -> None:
    """
    Update fixtures for a league by calling the old scraper.
    """
    from scripts.scrape_fixtures import LEAGUE_MAP
    url = LEAGUE_MAP.get(league_code)
    if not url:
        raise ValueError(f"Unknown league: {league_code}")
    # Temporarily patch HEADLESS
    import scripts.scrape_fixtures as sf
    original_headless = sf.HEADLESS
    sf.HEADLESS = headless
    try:
        scrape_league(league_code, url)
    finally:
        sf.HEADLESS = original_headless
