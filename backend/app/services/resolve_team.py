# backend/app/services/resolve_team.py
from sqlalchemy.orm import Session
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

# Simple in-memory cache for team resolutions
_resolve_cache = {}

def resolve_team_name(db: Session, raw_name: str, league_code: str) -> str:
    """
    Resolve a raw team name to its canonical team_key using the alias system.
    Returns the canonical key, or the normalized raw name if not found.
    Results are cached to avoid repeated database queries.
    """
    if not raw_name:
        return raw_name

    normalized = normalize_team(raw_name)
    cache_key = f"{league_code}:{normalized}"

    # Check cache first
    if cache_key in _resolve_cache:
        return _resolve_cache[cache_key]

    # Check if this normalized key is an alias
    alias = db.query(TeamAlias).join(Team).filter(
        TeamAlias.alias_key == normalized,
        Team.league_code == league_code
    ).first()

    if alias and alias.team:
        result = alias.team.team_key
        _resolve_cache[cache_key] = result
        return result

    # Check if it's a direct team key
    team = db.query(Team).filter(
        Team.team_key == normalized,
        Team.league_code == league_code
    ).first()

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
