from typing import List, Optional
from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database.db import get_db
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team
from app.engine.team_resolver import resolve_league_for_match

router = APIRouter()

# ---------- Admin Upsert Team ----------
class TeamUpsertPayload(BaseModel):
    display_name: str
    league_code: str
    country: Optional[str] = ""
    aliases: Optional[List[str]] = []

@router.post("/team-upsert")
def team_upsert(payload: TeamUpsertPayload, db: Session = Depends(get_db)):
    """
    Create or update a team record, with optional aliases.
    """
    team_key = normalize_team(payload.display_name)
    team = db.query(Team).filter(Team.team_key == team_key).first()
    if team is None:
        team = Team(team_key=team_key, display_name=payload.display_name, league_code=payload.league_code, country=payload.country or "")
    else:
        team.display_name = payload.display_name
        team.league_code = payload.league_code
        team.country = payload.country or team.country

    # Replace aliases
    team.aliases.clear()
    for al in (payload.aliases or []):
        ak = normalize_team(al)
        if ak and ak != team_key:
            team.aliases.append(TeamAlias(alias_key=ak))

    db.add(team)
    db.commit()
    db.refresh(team)
    return {"message": "team_upsert_ok", "team_key": team.team_key, "league_code": team.league_code}

# ---------- Resolve League for Match ----------
@router.get("/resolve-league")
def resolve_league(team_a: str, team_b: str, db: Session = Depends(get_db)):
    """
    Given two team names (any order), try to resolve unique league_code.
    Returns {resolved, league_code?, leagues?, suggestions{team_a[], team_b[]}}
    """
    return resolve_league_for_match(db, team_a, team_b)
