"""
Fixture-feed team names mapped to the names the results store actually uses.

The pipeline reads its two halves from different providers. Results come from
football-data.co.uk, which files clubs under terse trading names — `Man United`,
`QPR`, `Nott'm Forest`, `M'gladbach`. Fixtures come from a feed that uses full
legal names — `Manchester United FC`, `Queens Park Rangers FC`. The resolver in
`features._match_team` bridges most of that gap on its own by stripping club
suffixes and accents, but it cannot bridge an abbreviation: `qpr` and
`queens park rangers` share no text for a fuzzy scorer to work with, and
lowering the cutoff far enough to join them would start joining genuinely
different clubs instead. (That failure mode is already on record here: a loose
match once resolved Yokohama F. Marinos onto Yokohama FC.)

So the mapping is data rather than an algorithm, and every entry was read off
the store's own name list.

An alias is consulted ONLY when the raw name resolves to nothing — see
`features._aliased`. That ordering is the safety property: an alias can add a
fixture the engine previously withheld, and can never change a fixture it
already prices, so the file can be extended without re-validating past tips.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = Path(os.environ.get("ATHENA_CONFIG_DIR", _REPO_ROOT / "config"))
ALIASES_FILE = CONFIG_DIR / "team_aliases.json"

_CACHE: Optional[dict[str, dict[str, str]]] = None


def _norm(s: str) -> str:
    return (s or "").strip().lower()


def load_all(refresh: bool = False) -> dict[str, dict[str, str]]:
    """league_code -> {normalised feed name: store name}."""
    global _CACHE
    if _CACHE is not None and not refresh:
        return _CACHE

    if not ALIASES_FILE.exists():
        _CACHE = {}
        return _CACHE

    raw = json.loads(ALIASES_FILE.read_text(encoding="utf-8"))
    _CACHE = {
        code: {_norm(k): v for k, v in table.items()}
        for code, table in raw.items()
        # Keys beginning with an underscore carry the file's own documentation.
        if not code.startswith("_") and isinstance(table, dict)
    }
    return _CACHE


def get(league_code: str, team: str) -> Optional[str]:
    """The store name this league files `team` under, or None if unmapped."""
    return load_all().get(league_code, {}).get(_norm(team))


def leagues() -> list[str]:
    return sorted(load_all())


def count() -> int:
    return sum(len(t) for t in load_all().values())
