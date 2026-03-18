# backend/app/services/scrapers/match_stats_scraper.py

from datetime import date
import re
import time
from typing import Dict, List, Optional
import pandas as pd
from seleniumbase import Driver

from app.database.db import SessionLocal
from app.models.models_players import Player, PlayerMatchStats
from app.services.resolve_team import resolve_team_name

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
    print(f"    [match_stats] Fetching {home_team} vs {away_team}")
    
    close_driver = False
    if not driver:
        driver = Driver(uc=True, headless2=True)
        close_driver = True
    
    try:
        driver.get(match_url)
        time.sleep(3)
        
        tables = pd.read_html(driver.page_source)
        
        player_stats = []
        
        for df in tables:
            if "Player" in df.columns or " player" in str(df.columns).lower():
                table_text = df.to_string().lower()
                is_home = home_team.lower() in table_text
                is_away = away_team.lower() in table_text
                
                if not (is_home or is_away):
                    continue
                
                for _, row in df.iterrows():
                    if pd.isna(row.get("Player")):
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
                        "saves": _safe_int(row.get("Saves", 0)),
                        "xg": _safe_float(row.get("xG", 0.0)),
                        "xa": _safe_float(row.get("xA", 0.0)),
                        "is_home": is_home,
                        "opponent": away_team if is_home else home_team,
                    }
                    player_stats.append(stat)
        
        print(f"    [match_stats] Found {len(player_stats)} player entries")
        return player_stats
        
    except Exception as e:
        print(f"    [match_stats] Error: {e}")
        return []
    finally:
        if close_driver and driver:
            try:
                driver.quit()
            except Exception:
                pass


def _store_match_stats(db, player_stats: list[dict], match_date: date, league_code: str):
    """Store player match statistics in the database."""
    stored = 0
    for stat in player_stats:
        player_name = stat.pop("player_name")
        
        # Find player by name (case-insensitive match)
        player = db.query(Player).filter(
            Player.name.ilike(f"%{player_name}%"),
            Player.league_code == league_code
        ).first()
        
        if not player:
            # Try without league restriction as fallback
            player = db.query(Player).filter(
                Player.name.ilike(f"%{player_name}%")
            ).first()
        
        if not player:
            continue
        
        # Check if this match stat already exists
        existing = db.query(PlayerMatchStats).filter_by(
            player_id=player.id,
            match_date=match_date,
            league_code=league_code
        ).first()
        
        if existing:
            # Update existing record
            for key, value in stat.items():
                if hasattr(existing, key):
                    setattr(existing, key, value)
        else:
            # Create new record
            match_stats = PlayerMatchStats(
                player_id=player.id,
                match_date=match_date,
                league_code=league_code,
                **stat
            )
            db.add(match_stats)
        
        stored += 1
    
    if stored:
        db.commit()
        print(f"    [match_stats] Stored/updated {stored} player records")
    return stored


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
