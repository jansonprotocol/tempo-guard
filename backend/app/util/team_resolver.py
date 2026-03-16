from typing import List, Optional, Dict, Set
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


def resolve_team_name(db: Session, raw_name: str) -> str:
    """
    Resolve a single raw team name to its canonical team_key using the alias system.
    Uses request-level cache to avoid repeated database queries.
    """
    if not raw_name:
        return raw_name
        
    normalized = normalize_team(raw_name)
    
    # Use a request-level cache dictionary attached to the session
    if not hasattr(db, "_team_resolver_cache"):
        db._team_resolver_cache = {}
    
    cache = db._team_resolver_cache
    if normalized in cache:
        return cache[normalized]
    
    # Check if this normalized key is an alias
    alias = db.query(TeamAlias).filter(
        TeamAlias.alias_key == normalized
    ).first()
    
    if alias and alias.team:
        result = alias.team.team_key
        cache[normalized] = result
        return result
    
    # Check if it's a direct team key
    team = db.query(Team).filter(
        Team.team_key == normalized
    ).first()
    
    if team:
        result = team.team_key
        cache[normalized] = result
        return result
    
    # Not found in alias system, return normalized raw name
    cache[normalized] = normalized
    return normalized


def batch_resolve_team_names(db: Session, raw_names: List[str]) -> Dict[str, str]:
    """
    Resolve multiple team names in a single batch query.
    Returns dict of {raw_name: canonical_name}
    This is MUCH faster than resolving names one by one.
    """
    if not raw_names:
        return {}
    
    # Use request-level cache
    if not hasattr(db, "_team_resolver_cache"):
        db._team_resolver_cache = {}
    cache = db._team_resolver_cache
    
    # Separate names that are already cached vs need resolution
    to_resolve = []
    result = {}
    
    for raw_name in raw_names:
        if not raw_name:
            result[raw_name] = raw_name
            continue
            
        normalized = normalize_team(raw_name)
        if normalized in cache:
            result[raw_name] = cache[normalized]
        else:
            to_resolve.append((raw_name, normalized))
    
    if not to_resolve:
        return result
    
    # Get all normalized names that need resolution
    norm_names = [norm for _, norm in to_resolve]
    
    # Batch query 1: Find all matching aliases
    aliases = db.query(TeamAlias).filter(
        TeamAlias.alias_key.in_(norm_names)
    ).all()
    
    # Build alias lookup
    alias_to_team = {}
    for alias in aliases:
        if alias.team:
            alias_to_team[alias.alias_key] = alias.team.team_key
    
    # Batch query 2: Find direct team matches for remaining names
    remaining_norms = [n for n in norm_names if n not in alias_to_team]
    teams = {}
    if remaining_norms:
        team_results = db.query(Team).filter(
            Team.team_key.in_(remaining_norms)
        ).all()
        teams = {t.team_key: t.team_key for t in team_results}
    
    # Resolve all names and update cache
    for raw_name, norm in to_resolve:
        if norm in alias_to_team:
            canonical = alias_to_team[norm]
        elif norm in teams:
            canonical = teams[norm]
        else:
            canonical = norm
        
        cache[norm] = canonical
        result[raw_name] = canonical
    
    return result


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


# Helper functions needed for resolve_league_for_match
def _find_team_ids_by_key_or_alias(db: Session, team_key: str) -> List[Team]:
    """Find teams matching a key either directly or via alias."""
    # Direct match
    teams = db.query(Team).filter(Team.team_key == team_key).all()
    if teams:
        return teams
    
    # Via alias
    alias = db.query(TeamAlias).filter(TeamAlias.alias_key == team_key).first()
    if alias:
        return [alias.team]
    
    return []


def _fuzzy_candidates(db: Session, team_key: str, limit: int = 5) -> List[dict]:
    """Return fuzzy matching suggestions for an unresolved team."""
    all_teams = db.query(Team).all()
    choices = {t.team_key: t for t in all_teams}
    
    matches = process.extract(
        team_key, 
        list(choices.keys()), 
        scorer=fuzz.WRatio,
        limit=limit
    )
    
    return [
        {
            "team_key": m[0],
            "score": m[1],
            "league_code": choices[m[0]].league_code
        }
        for m in matches if m[1] >= 70
    ]
