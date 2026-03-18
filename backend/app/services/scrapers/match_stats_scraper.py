# backend/app/services/scrapers/match_stats_scraper.py

from datetime import date  # <-- ADD THIS
import re
import time
from typing import Dict, List, Optional
import pandas as pd
from seleniumbase import Driver

from app.database.db import SessionLocal
from app.models.models_players import Player, PlayerMatchStats

def scrape_match_player_stats(
    match_url: str,
    league_code: str,
    match_date: date,
    home_team: str,
    away_team: str,
    driver: Optional[Driver] = None
) -> List[Dict]:
    """
    Scrape player statistics for a single match from FBref.
    Returns list of player stat dictionaries.
    """
    print(f"  [match_stats] Scraping {home_team} vs {away_team}")
    
    # Use existing driver or create new one
    close_driver = False
    if not driver:
        driver = Driver(uc=True, headless2=True)
        close_driver = True
    
    try:
        driver.get(match_url)
        time.sleep(3)
        
        # Find the match stats tables - FBref has separate tables for home and away
        tables = pd.read_html(driver.page_source)
        
        player_stats = []
        
        # Process home team stats table
        home_stats = _extract_team_match_stats(tables, home_team, is_home=True)
        for stat in home_stats:
            stat["league_code"] = league_code
            stat["match_date"] = match_date
            stat["opponent"] = away_team
            player_stats.append(stat)
        
        # Process away team stats table
        away_stats = _extract_team_match_stats(tables, away_team, is_home=False)
        for stat in away_stats:
            stat["league_code"] = league_code
            stat["match_date"] = match_date
            stat["opponent"] = home_team
            player_stats.append(stat)
        
        return player_stats
        
    finally:
        if close_driver and driver:
            driver.quit()

def _extract_team_match_stats(tables: List[pd.DataFrame], team_name: str, is_home: bool) -> List[Dict]:
    """
    Extract player stats for a single team from the match tables.
    """
    # This is simplified - actual implementation needs to identify the correct table
    # and map FBref's column names to your schema
    player_stats = []
    
    for df in tables:
        # Look for a table containing player names and the team name
        if team_name in df.to_string():
            # Found the team's stats table
            # Map columns based on your needs
            for _, row in df.iterrows():
                if pd.isna(row.get("Player")):  # Skip summary rows
                    continue
                    
                stat = {
                    "player_name": str(row.get("Player", "")),
                    "minutes": _safe_int(row.get("Min", 0)),
                    "goals": _safe_int(row.get("Gls", 0)),
                    "assists": _safe_int(row.get("Ast", 0)),
                    "shots": _safe_int(row.get("Sh", 0)),
                    "shots_on_target": _safe_int(row.get("SoT", 0)),
                    "passes_completed": _safe_int(row.get("Cmp", 0)),
                    "passes_attempted": _safe_int(row.get("Att", 0)),
                    "tackles": _safe_int(row.get("Tkl", 0)),
                    "interceptions": _safe_int(row.get("Int", 0)),
                    "blocks": _safe_int(row.get("Blocks", 0)),
                    "xg": _safe_float(row.get("xG", 0.0)),
                    "xa": _safe_float(row.get("xA", 0.0)),
                }
                player_stats.append(stat)
            break
    
    return player_stats

def _safe_int(val) -> int:
    try:
        return int(float(val)) if pd.notna(val) else 0
    except (ValueError, TypeError):
        return 0

def _safe_float(val) -> float:
    try:
        return float(val) if pd.notna(val) else 0.0
    except (ValueError, TypeError):
        return 0.0
