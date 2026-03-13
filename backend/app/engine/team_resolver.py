from typing import List, Optional
from sqlalchemy.orm import Session
from rapidfuzz import process, fuzz
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

# Threshold: 90/100 confidence is safe for autopilot
AUTO_MAP_THRESHOLD = 90 

def resolve_and_learn(db: Session, raw_name: str, league_code: str) -> str:
    """
    Resolves a raw name to a master team_key.
    Autopilot: If it finds a 90%+ match, it creates a permanent Alias in the DB.
    """
    if not raw_name:
        return raw_name
        
    normalized_key = normalize_team(raw_name)

    # 1. Check if we already know this name (Direct or Alias)
    # Check Team table
    t = db.query(Team).filter(Team.team_key == normalized_key, Team.league_code == league_code).first()
    if t: return t.team_key
    
    # Check Alias table
    alias = db.query(TeamAlias).join(Team).filter(
        TeamAlias.alias_key == normalized_key, 
        Team.league_code == league_code
    ).first()
    if alias: return alias.team.team_key

    # 2. Autopilot: Fuzzy Match against known teams in this league
    known_teams = db.query(Team).filter(Team.league_code == league_code).all()
    if not known_teams:
        return normalized_key

    choices = {t.team_key: t for t in known_teams}
    # find best match among keys
    best_match, score, _ = process.extractOne(normalized_key, list(choices.keys()), scorer=fuzz.WRatio)

    if score >= AUTO_MAP_THRESHOLD:
        master_team = choices[best_match]
        # Create the alias record so we 'learn' this for the future
        try:
            new_alias = TeamAlias(team_id=master_team.id, alias_key=normalized_key)
            db.add(new_alias)
            db.commit()
            print(f"  [Autopilot] Linked variant '{raw_name}' to master '{master_team.team_key}' (Score: {score:.1f})")
            return master_team.team_key
        except Exception:
            db.rollback()
            return master_team.team_key

    return normalized_key
