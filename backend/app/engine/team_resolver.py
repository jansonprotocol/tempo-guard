from typing import List, Dict
from sqlalchemy.orm import Session
from difflib import get_close_matches
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team


INTERNATIONAL_CLUB = "INTERNATIONAL_CLUB"
INTERNATIONAL_NATIONS = "INTERNATIONAL_NATIONS"


def _find_team_ids_by_key_or_alias(db: Session, key: str) -> List[Team]:
    # Exact match by team_key
    t = db.query(Team).filter(Team.team_key == key).all()
    if t:
        return t
    # Or alias match
    alias_rows = db.query(TeamAlias).filter(TeamAlias.alias_key == key).all()
    if not alias_rows:
        return []
    team_ids = [a.team_id for a in alias_rows]
    return db.query(Team).filter(Team.id.in_(team_ids)).all()


def _fuzzy_candidates(db: Session, key: str, n: int = 5) -> List[Dict]:
    keys = [t.team_key for t in db.query(Team).all()]
    alias_keys = [a.alias_key for a in db.query(TeamAlias).all()]
    universe = list(set(keys + alias_keys))
    close = get_close_matches(key, universe, n=n, cutoff=0.8)

    out = []
    for ck in close:
        teams = _find_team_ids_by_key_or_alias(db, ck)
        for team in teams:
            out.append({
                "team_key": team.team_key,
                "display_name": team.display_name,
                "league_code": team.league_code
            })
    return out


def resolve_league_for_match(db: Session, team_a: str, team_b: str) -> Dict:
    a_key = normalize_team(team_a)
    b_key = normalize_team(team_b)

    # Fetch teams
    a_teams = _find_team_ids_by_key_or_alias(db, a_key)
    b_teams = _find_team_ids_by_key_or_alias(db, b_key)

    # Suggestions
    suggestions = {
        "team_a": [] if a_teams else _fuzzy_candidates(db, a_key),
        "team_b": [] if b_teams else _fuzzy_candidates(db, b_key),
    }

    # If both sides found exact teams:
    if a_teams and b_teams:
        leagues_a = set(t.league_code for t in a_teams)
        leagues_b = set(t.league_code for t in b_teams)

        # CASE: National teams → INTERNATIONAL_NATIONS
        if all(lc == "NATIONAL" for lc in leagues_a.union(leagues_b)):
            return {
                "resolved": True,
                "league_code": INTERNATIONAL_NATIONS,
                "suggestions": suggestions
            }

        # CASE: Same club league
        intersection = leagues_a.intersection(leagues_b)
        if len(intersection) == 1:
            return {
                "resolved": True,
                "league_code": list(intersection)[0],
                "suggestions": suggestions
            }

        # CASE: Teams exist but different leagues → INTERNATIONAL CLUB
        return {
            "resolved": True,
            "league_code": INTERNATIONAL_CLUB,
            "suggestions": suggestions
        }

    # Only one team found → unresolved
    if a_teams or b_teams:
        detected = list(set([t.league_code for t in (a_teams + b_teams)]))
        return {
            "resolved": False,
            "leagues": detected,
            "suggestions": suggestions
        }

    # Nothing found
    return {
        "resolved": False,
        "leagues": [],
        "suggestions": suggestions
    }
