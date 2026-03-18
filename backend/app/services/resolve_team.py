# backend/app/services/resolve_team.py
from sqlalchemy.orm import Session
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

# Simple in-memory cache for team resolutions
_resolve_cache = {}

def resolve_team_name(db: Session, raw_name: str, league_code: str = None) -> str:
    """
    Resolve a raw team name to its canonical team_key using the alias system.
    If league_code is provided, use it to disambiguate when multiple teams match.
    Returns the canonical key, or the normalized raw name if not found.
    Results are cached to avoid repeated database queries.
    """
    if not raw_name:
        return raw_name

    normalized = normalize_team(raw_name)
    cache_key = f"{league_code or 'ALL'}:{normalized}"

    # Check cache first
    if cache_key in _resolve_cache:
        return _resolve_cache[cache_key]

    # First, try to find an alias in any league (or restricted if league_code provided)
    query = db.query(TeamAlias).join(Team)
    if league_code:
        query = query.filter(Team.league_code == league_code)
    alias = query.filter(TeamAlias.alias_key == normalized).first()

    if alias and alias.team:
        result = alias.team.team_key
        _resolve_cache[cache_key] = result
        return result

    # Direct team match
    query = db.query(Team)
    if league_code:
        query = query.filter(Team.league_code == league_code)
    team = query.filter(Team.team_key == normalized).first()

    if team:
        result = team.team_key
        _resolve_cache[cache_key] = result
        return result

    # Not found, cache the normalized name
    _resolve_cache[cache_key] = normalized
    return normalized

def clear_resolve_cache() -> None:
    """Clear the in-memory resolution cache."""
    global _resolve_cache
    _resolve_cache = {}
    print("[resolve_team] Cache cleared")
