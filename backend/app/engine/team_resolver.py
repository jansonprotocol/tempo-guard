from typing import List, Dict, Tuple
from sqlalchemy.orm import Session
from difflib import get_close_matches
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

def _find_team_ids_by_key_or_alias(db: Session, key: str) -> List[Team]:
    # Exact match by team_key
    t = db.query(Team).filter(Team.team_key == key).all()
    if t:
        return t
    # Exact match by alias
    alias_rows = db.query(TeamAlias).filter(TeamAlias.alias_key == key).all()
    if not alias_rows:
        return []
    team_ids = [a.team_id for a in alias_rows]
    if not team_ids:
        return []
    teams = db.query(Team).filter(Team.id.in_(team_ids)).all()
    return teams

def _fuzzy_candidates(db: Session, key: str, n: int = 5) -> List[Dict]:
    # Collect all keys and aliases (small MVP-scale acceptable)
    keys = [t.team_key for t in db.query(Team).all()]
    alias_keys = [a.alias_key for a in db.query(TeamAlias).all()]
    universe = list(set(keys + alias_keys))
    close = get_close_matches(key, universe, n=n, cutoff=0.8)
    results = []
    for ck in close:
        # Resolve ck back to Team
        teams = _find_team_ids_by_key_or_alias(db, ck)
        for team in teams:
            results.append({
                "team_key": team.team_key,
                "display_name": team.display_name,
                "league_code": team.league_code
            })
    # Deduplicate by (team_key, league_code)
    seen = set()
    unique = []
    for r in results:
        sig = (r["team_key"], r["league_code"])
        if sig not in seen:
            seen.add(sig)
            unique.append(r)
    return unique

def resolve_league_for_match(db: Session, team_a: str, team_b: str) -> Dict:
    a_key = normalize_team(team_a)
    b_key = normalize_team(team_b)

    a_teams = _find_team_ids_by_key_or_alias(db, a_key)
    b_teams = _find_team_ids_by_key_or_alias(db, b_key)

    # If exact not found, offer fuzzy suggestions
    suggestions = {"team_a": [], "team_b": []}
    if not a_teams:
        suggestions["team_a"] = _fuzzy_candidates(db, a_key)
    if not b_teams:
        suggestions["team_b"] = _fuzzy_candidates(db, b_key)

    leagues_a = set(t.league_code for t in a_teams)
    leagues_b = set(t.league_code for t in b_teams)

    # If both resolved and intersection is one league → success
    if leagues_a and leagues_b:
        inter = leagues_a.intersection(leagues_b)
        if len(inter) == 1:
            return {"resolved": True, "league_code": list(inter)[0], "suggestions": suggestions}
        elif len(inter) > 1:
            return {"resolved": False, "leagues": sorted(list(inter)), "suggestions": suggestions}
        else:
            # Both found but in different leagues; ambiguous
            return {
                "resolved": False,
                "leagues": sorted(list(leagues_a.union(leagues_b))),
                "suggestions": suggestions
            }

    # If only one side resolves → still helpful
    if leagues_a and not leagues_b:
        return {"resolved": False, "leagues": sorted(list(leagues_a)), "suggestions": suggestions}
    if leagues_b and not leagues_a:
        return {"resolved": False, "leagues": sorted(list(leagues_b)), "suggestions": suggestions}

    # None resolved
    return {"resolved": False, "leagues": [], "suggestions": suggestions}
