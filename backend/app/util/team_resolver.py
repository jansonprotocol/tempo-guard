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
    
def resolve_league_for_match(db: Session, home_team: str, away_team: str) -> dict:
    """
    Determines the correct league_code for a matchup between two teams.
    Used by the prediction and validation routes.
    """
    from app.util.text_norm import normalize_team
    
    h_key = normalize_team(home_team)
    a_key = normalize_team(away_team)

    h_teams = _find_team_ids_by_key_or_alias(db, h_key)
    a_teams = _find_team_ids_by_key_or_alias(db, a_key)

    suggestions = {
        "team_a": [] if h_teams else _fuzzy_candidates(db, h_key),
        "team_b": [] if a_teams else _fuzzy_candidates(db, a_key),
    }

    if h_teams and a_teams:
        leagues_h = set(t.league_code for t in h_teams)
        leagues_a = set(t.league_code for t in a_teams)

        # 1. Check for standard league match (both in same league)
        intersection = leagues_h.intersection(leagues_a)
        if intersection:
            # Pick the first common league found
            return {
                "resolved": True, 
                "league_code": list(intersection)[0], 
                "suggestions": suggestions
            }

        # 2. If different leagues, treat as International Club (UCL/UEL)
        return {
            "resolved": True, 
            "league_code": "INTERNATIONAL_CLUB", 
            "suggestions": suggestions
        }

    # 3. Unresolved
    return {
        "resolved": False, 
        "league_code": None, 
        "suggestions": suggestions
    }
