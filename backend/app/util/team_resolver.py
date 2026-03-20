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
    Already league-scoped — only fuzzy-matches against teams in the given league.
    """
    if not raw_name:
        return raw_name

    normalized_key = normalize_team(raw_name)

    # 1. Check if we already know this name (Direct or Alias)
    t = db.query(Team).filter(Team.team_key == normalized_key, Team.league_code == league_code).first()
    if t:
        return t.team_key

    alias = db.query(TeamAlias).join(Team).filter(
        TeamAlias.alias_key == normalized_key,
        Team.league_code == league_code
    ).first()
    if alias:
        return alias.team.team_key

    # 2. Autopilot: Fuzzy Match against known teams in this league only
    known_teams = db.query(Team).filter(Team.league_code == league_code).all()
    if not known_teams:
        return normalized_key

    choices = {t.team_key: t for t in known_teams}
    best_match, score, _ = process.extractOne(normalized_key, list(choices.keys()), scorer=fuzz.WRatio)

    if score >= AUTO_MAP_THRESHOLD:
        master_team = choices[best_match]
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

    Note: this is the league-blind internal version used by team_resolver helpers.
    For prediction/calibration code use app.services.resolve_team.resolve_team_name
    which accepts a league_code parameter and is properly league-scoped.
    """
    if not raw_name:
        return raw_name

    normalized = normalize_team(raw_name)

    if not hasattr(db, "_team_resolver_cache"):
        db._team_resolver_cache = {}
    cache = db._team_resolver_cache

    if normalized in cache:
        return cache[normalized]

    alias = db.query(TeamAlias).filter(TeamAlias.alias_key == normalized).first()
    if alias and alias.team:
        result = alias.team.team_key
        cache[normalized] = result
        return result

    team = db.query(Team).filter(Team.team_key == normalized).first()
    if team:
        result = team.team_key
        cache[normalized] = result
        return result

    cache[normalized] = normalized
    return normalized


def batch_resolve_team_names(
    db: Session,
    raw_names: List[str],
    league_code: Optional[str] = None,
) -> Dict[str, str]:
    """
    Resolve multiple team names in a single batch query.
    Returns dict of {raw_name: canonical_name}.
    This is MUCH faster than resolving names one by one.

    league_code: when provided, alias and team lookups are scoped to that league.
    This prevents cross-league contamination — e.g. "Paris" being resolved to
    PSG (FRA-L1) when the fixture is actually Paris FC (FRA-L2).

    Cache keys include the league prefix so FRA-L1 and FRA-L2 resolutions are
    stored separately and never bleed into each other.
    """
    if not raw_names:
        return {}

    cache_prefix = league_code or "ALL"
    if not hasattr(db, "_team_resolver_cache"):
        db._team_resolver_cache = {}
    cache = db._team_resolver_cache

    to_resolve = []
    result = {}

    for raw_name in raw_names:
        if not raw_name:
            result[raw_name] = raw_name
            continue

        normalized = normalize_team(raw_name)
        cache_key = f"{cache_prefix}:{normalized}"
        if cache_key in cache:
            result[raw_name] = cache[cache_key]
        else:
            to_resolve.append((raw_name, normalized))

    if not to_resolve:
        return result

    norm_names = [norm for _, norm in to_resolve]

    # Batch query 1: aliases — scoped by league when provided.
    # Without this, "paris" → PSG (FRA-L1) would bleed into FRA-L2 lookups.
    alias_query = db.query(TeamAlias).join(Team).filter(
        TeamAlias.alias_key.in_(norm_names)
    )
    if league_code:
        alias_query = alias_query.filter(Team.league_code == league_code)
    aliases = alias_query.all()

    alias_to_team: Dict[str, str] = {}
    for alias in aliases:
        if alias.team:
            alias_to_team[alias.alias_key] = alias.team.team_key

    # Batch query 2: direct team key matches — scoped by league when provided
    remaining_norms = [n for n in norm_names if n not in alias_to_team]
    teams: Dict[str, str] = {}
    if remaining_norms:
        team_query = db.query(Team).filter(Team.team_key.in_(remaining_norms))
        if league_code:
            team_query = team_query.filter(Team.league_code == league_code)
        teams = {t.team_key: t.team_key for t in team_query.all()}

    for raw_name, norm in to_resolve:
        if norm in alias_to_team:
            canonical = alias_to_team[norm]
        elif norm in teams:
            canonical = teams[norm]
        else:
            canonical = norm

        cache_key = f"{cache_prefix}:{norm}"
        cache[cache_key] = canonical
        result[raw_name] = canonical

    return result


def resolve_league_for_match(db: Session, home_team: str, away_team: str) -> dict:
    """
    Determines the correct league_code for a matchup between two teams.
    """
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

        intersection = leagues_h.intersection(leagues_a)
        if intersection:
            return {
                "resolved": True,
                "league_code": list(intersection)[0],
                "suggestions": suggestions,
            }

        return {
            "resolved": True,
            "league_code": "INTERNATIONAL_CLUB",
            "suggestions": suggestions,
        }

    return {
        "resolved": False,
        "league_code": None,
        "suggestions": suggestions,
    }


def _find_team_ids_by_key_or_alias(db: Session, team_key: str) -> List[Team]:
    teams = db.query(Team).filter(Team.team_key == team_key).all()
    if teams:
        return teams
    alias = db.query(TeamAlias).filter(TeamAlias.alias_key == team_key).first()
    if alias:
        return [alias.team]
    return []


def _fuzzy_candidates(db: Session, team_key: str, limit: int = 5) -> List[dict]:
    all_teams = db.query(Team).all()
    choices = {t.team_key: t for t in all_teams}
    matches = process.extract(
        team_key,
        list(choices.keys()),
        scorer=fuzz.WRatio,
        limit=limit,
    )
    return [
        {
            "team_key": m[0],
            "score": m[1],
            "league_code": choices[m[0]].league_code,
        }
        for m in matches if m[1] >= 70
    ]
