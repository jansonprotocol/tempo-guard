# backend/app/services/resolve_team.py
from sqlalchemy.orm import Session
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

def resolve_team_name(db: Session, raw_name: str, league_code: str) -> str:
    """
    Resolve a raw team name to its canonical team_key using the alias system.
    Returns the canonical key, or the normalized raw name if not found.
    """
    if not raw_name:
        return raw_name

    normalized = normalize_team(raw_name)

    # Check if this normalized key is an alias
    alias = db.query(TeamAlias).join(Team).filter(
        TeamAlias.alias_key == normalized,
        Team.league_code == league_code
    ).first()

    if alias and alias.team:
        return alias.team.team_key

    # Check if it's a direct team key
    team = db.query(Team).filter(
        Team.team_key == normalized,
        Team.league_code == league_code
    ).first()

    if team:
        return team.team_key

    return normalized
