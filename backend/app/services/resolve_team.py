# backend/app/services/resolve_team.py
"""
ATHENA v2.0 — Central Team Name Resolver.

Every part of the prediction pipeline calls resolve_team_name() before
doing any DB lookups. This ensures that "FC Fredericia", "fc fredericia",
and "Fredericia" all map to the same canonical team name stored in the
Team table.

Resolution order:
  1. Exact match on Team.team_key (normalised)
  2. Exact match on TeamAlias.alias_key (normalised)
  3. Fuzzy match on Team.team_key (using difflib, cutoff 0.85)
  4. Fuzzy match on TeamAlias.alias_key
  5. Return original name unchanged (no match found)

When a fuzzy match is found with score ≥ 0.90, a new TeamAlias is
automatically created so future lookups are instant.

Called from:
  - routes_batch.py (batch-predict, batch-validate)
  - routes_predict.py (single match prediction)
  - routes_futurematch.py / routes_retrosim.py (frontend predictions)
  - performance_tags.py, form_delta.py (anywhere team names are used)
"""
from __future__ import annotations

import unicodedata
from difflib import get_close_matches
from functools import lru_cache
from typing import Optional

from sqlalchemy.orm import Session


def _norm(s: str) -> str:
    """Normalise: lowercase, strip whitespace, strip accents."""
    s = s.strip().lower()
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


# In-process cache: cleared on each new request cycle (not persistent)
_resolve_cache: dict[str, str] = {}
_CACHE_MAX = 2000


def clear_resolve_cache():
    """Call at the start of batch operations to prevent stale mappings."""
    _resolve_cache.clear()


def resolve_team_name(
    db: Session,
    raw_name: str,
    league_code: str,
    auto_learn: bool = True,
) -> str:
    """
    Resolve a raw team name to the canonical display_name from the Team table.

    Args:
        db: database session
        raw_name: the name as it appears in fixtures/snapshots
        league_code: league context (aliases are league-scoped via Team)
        auto_learn: if True and a fuzzy match ≥ 0.90 is found, create an alias

    Returns:
        The canonical display_name if found, otherwise raw_name unchanged.
    """
    if not raw_name or not raw_name.strip():
        return raw_name

    # Cache check
    cache_key = f"{league_code}|{_norm(raw_name)}"
    if cache_key in _resolve_cache:
        return _resolve_cache[cache_key]

    from app.models.team import Team, TeamAlias

    norm_name = _norm(raw_name)

    # ── 1. Exact match on Team.team_key ──────────────────────────────
    team = (
        db.query(Team)
        .filter(Team.team_key == norm_name, Team.league_code == league_code)
        .first()
    )
    if team:
        _cache_put(cache_key, team.display_name)
        return team.display_name

    # ── 2. Exact match on TeamAlias.alias_key ────────────────────────
    alias = (
        db.query(TeamAlias)
        .join(Team)
        .filter(TeamAlias.alias_key == norm_name, Team.league_code == league_code)
        .first()
    )
    if alias:
        canonical = alias.team.display_name
        _cache_put(cache_key, canonical)
        return canonical

    # ── 3. Fuzzy match on team_key ───────────────────────────────────
    all_teams = (
        db.query(Team)
        .filter(Team.league_code == league_code)
        .all()
    )

    if not all_teams:
        return raw_name

    # Build candidate maps
    key_map = {_norm(t.team_key): t for t in all_teams}
    alias_map = {}
    for t in all_teams:
        for a in t.aliases:
            alias_map[_norm(a.alias_key)] = t

    # Fuzzy on team keys
    all_keys = list(key_map.keys())
    close = get_close_matches(norm_name, all_keys, n=1, cutoff=0.85)
    if close:
        matched_team = key_map[close[0]]
        _maybe_learn(db, matched_team, norm_name, close[0], auto_learn)
        _cache_put(cache_key, matched_team.display_name)
        return matched_team.display_name

    # ── 4. Fuzzy on alias keys ───────────────────────────────────────
    all_alias_keys = list(alias_map.keys())
    close = get_close_matches(norm_name, all_alias_keys, n=1, cutoff=0.85)
    if close:
        matched_team = alias_map[close[0]]
        _maybe_learn(db, matched_team, norm_name, close[0], auto_learn)
        _cache_put(cache_key, matched_team.display_name)
        return matched_team.display_name

    # ── 5. No match — return unchanged ───────────────────────────────
    _cache_put(cache_key, raw_name)
    return raw_name


def _cache_put(key: str, value: str):
    if len(_resolve_cache) > _CACHE_MAX:
        _resolve_cache.clear()
    _resolve_cache[key] = value


def _maybe_learn(db: Session, team, norm_name: str, matched_key: str, auto_learn: bool):
    """
    If the fuzzy match is strong (≥ 0.90 via difflib) and the normalised name
    isn't already the team_key, auto-create an alias so future lookups are instant.
    """
    if not auto_learn:
        return
    if norm_name == _norm(team.team_key):
        return  # exact match, no alias needed

    from difflib import SequenceMatcher
    score = SequenceMatcher(None, norm_name, matched_key).ratio()
    if score < 0.90:
        return  # not confident enough to auto-learn

    from app.models.team import TeamAlias

    # Check alias doesn't already exist
    existing = (
        db.query(TeamAlias)
        .filter(TeamAlias.alias_key == norm_name, TeamAlias.team_id == team.id)
        .first()
    )
    if existing:
        return

    try:
        db.add(TeamAlias(team_id=team.id, alias_key=norm_name))
        db.flush()
        print(f"  [resolve] Auto-learned alias '{norm_name}' → '{team.display_name}'")
    except Exception:
        db.rollback()
