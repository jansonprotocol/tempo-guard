"""
File-backed match store — the git-native replacement for the Postgres
`fbref_snapshots` table.

Snapshots live as parquet under the repo's `data/` directory:

    data/ENG-PL/2025-26.parquet
    data/ENG-PL/2026-27.parquet
    data/UCL/2025-26.parquet

Parquet keeps a full season under ~50 KB, so seasons commit comfortably into
git and the whole dataset travels with the repository. No database, no server,
no DATABASE_URL — `load()` is a file read.

The in-process cache means a calibration run that replays hundreds of matches
reads each season exactly once.
"""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Optional

import pandas as pd

from app.data import names

# Repo root = .../tempo-guard (this file is backend/app/data/store.py)
_REPO_ROOT = Path(__file__).resolve().parents[3]

DATA_DIR = Path(os.environ.get("ATHENA_DATA_DIR", _REPO_ROOT / "data"))

# league_code -> season -> DataFrame
_CACHE: dict[str, dict[str, pd.DataFrame]] = {}


def season_path(league_code: str, season: str) -> Path:
    return DATA_DIR / league_code / f"{season}.parquet"


def available_seasons(league_code: str) -> list[str]:
    """Seasons stored for a league, oldest first."""
    d = DATA_DIR / league_code
    if not d.is_dir():
        return []
    return sorted(p.stem for p in d.glob("*.parquet"))


def available_leagues() -> list[str]:
    if not DATA_DIR.is_dir():
        return []
    return sorted(p.name for p in DATA_DIR.iterdir()
                  if p.is_dir() and any(p.glob("*.parquet")))


def save(league_code: str, season: str, df: pd.DataFrame) -> Path:
    """
    Write a season snapshot, creating directories as needed.

    Duplicate fixtures are dropped on the way in. Two providers now feed the
    same league, and a match counted twice would silently distort every rolling
    feature computed from it — cheap insurance against an expensive bug.
    """
    if not df.empty and {"date", "home", "away"} <= set(df.columns):
        df = df.drop_duplicates(subset=["date", "home", "away"], keep="first")
    path = season_path(league_code, season)
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(path, index=False)
    _CACHE.setdefault(league_code, {})[season] = df
    _VIEW_CACHE.clear()   # stored data changed; cached views are stale
    return path


def load(league_code: str, season: Optional[str] = None) -> pd.DataFrame:
    """
    Load one season, or all stored seasons concatenated when `season` is None.

    Returns an empty DataFrame when nothing is stored — callers treat that the
    same way they treat a missing snapshot.
    """
    seasons = [season] if season else available_seasons(league_code)
    if not seasons:
        return pd.DataFrame()

    frames = []
    league_cache = _CACHE.setdefault(league_code, {})
    for s in seasons:
        if s in league_cache:
            frames.append(league_cache[s])
            continue
        path = season_path(league_code, s)
        if not path.exists():
            continue
        df = pd.read_parquet(path)
        league_cache[s] = df
        frames.append(df)

    if not frames:
        return pd.DataFrame()
    out = pd.concat(frames, ignore_index=True)
    if "date" in out.columns:
        out["date"] = pd.to_datetime(out["date"])
        out = out.sort_values("date").reset_index(drop=True)
    return out


# Concatenated per-league views, cached so repeated calls return the *same*
# object. A replay asks for a league's results once per match; rebuilding and
# re-concatenating that frame each time is pure waste, and downstream feature
# code keys its own index cache on frame identity.
_VIEW_CACHE: dict[tuple[str, Optional[str], str], pd.DataFrame] = {}


# ── One club, one name ────────────────────────────────────────────────────────
# A club filed under two spellings is two clubs to everything downstream. The
# results provider changed convention on 2024-08-10 — short trading names became
# full legal ones — and the store kept both, so `Chelsea` holds 912 matches
# ending in May 2024 while `Chelsea FC` holds the two seasons since. Measured
# before this was written: 22.2% of every row stored since that date was
# invisible to the spelling a hand-typed fixture resolves to, over 60% in ESP-L2
# and POR-PL. Ajax, Porto, Roma, Juventus, Valencia and Chelsea each lost their
# entire modern record.
#
# Folding happens HERE, on the loaded frame, rather than at lookup time, so team
# rows, venue rows and the per-team nudges all see one name. Renaming adds and
# removes no rows, so league means and base rates are untouched.
#
# TWO LAYERS, in order:
#
#   1. the canonical key — names.canonical, the same reduction the resolver
#      uses, so there is exactly one definition of what a club is called. This
#      is decoration only: legal forms, accents, punctuation. `Chelsea FC` folds
#      onto `Chelsea`; `Malaga B` does NOT fold onto `Malaga`, because `B` is a
#      different team and survives the reduction.
#
#   2. config/team_merges.json — typed pairs the key cannot reach, because one
#      spelling abbreviates a word the other spells out: `Man United` against
#      `Manchester United`, `Legia` against `Legia Warszawa`. Those share no key
#      and no algorithm should join them; the file states it and is checkable.
#
# THE SAFETY TEST, applied to both layers: a club plays at most one match a day.
# So if two spellings ever appear on the same date, or face each other, they are
# different clubs and the fold is REFUSED however well their names match. That
# is a fact about football rather than a similarity threshold, and it is what
# separates `Club Nacional` (Paraguay) from `Nacional` (Uruguay), and Finland's
# `FC Inter` from `Inter`, in the continental cups where both play.
#
# The test only means anything on rows that HAPPENED. The store also holds
# unplayed fixture rows, which the provider files under its long names while
# ingest_board files the played result under the short one — so on results plus
# fixtures every current club looks like two clubs playing on the same day, and
# every fold is refused. Hence the fold is applied by load_results, on the
# result rows alone, and never by load().
#
# UNGATED, unlike the version this replaces. That one folded only when the
# primary held fewer than five rows, which made it safe to ship mid-season but
# also structurally unable to see the defect above: the gate needs the primary
# under five rows, and Chelsea has 912. Folding unconditionally does move tips
# the engine already prices. That is the point — they were priced on the wrong
# club's history.
_MERGES: Optional[dict[str, list[tuple[str, str]]]] = None


def _merge_map(league_code: str) -> list[tuple[str, str]]:
    """The typed (variant, primary) PAIRS for one league.

    A list rather than a variant -> primary dict, because one spelling can sit
    in two typed pairs: FRA-L1 states both `Brest` = `Stade Brestois` and
    `Stade Brestois` = `Stade Brestois 29`, three names for one club that no
    single key reaches. Keyed by variant, the second line silently replaced
    the first and `Brest` was left out of its own club, still reading a window
    that stopped in May 2024. Pairs compose; a mapping does not.
    """
    global _MERGES
    if _MERGES is None:
        path = Path(os.environ.get("ATHENA_CONFIG_DIR",
                                   Path(__file__).resolve().parents[3] / "config"))
        f = path / "team_merges.json"
        raw = json.loads(f.read_text(encoding="utf-8")) if f.exists() else {}
        _MERGES = {}
        for code, groups in raw.items():
            if code.startswith("_") or not isinstance(groups, dict):
                continue
            _MERGES[code] = [(v.strip(), primary)
                             for primary, variants in groups.items()
                             for v in variants]
    return _MERGES.get(league_code, [])


def _day_sets(df: pd.DataFrame) -> dict[str, set]:
    """name -> the dates it played on. The safety test's raw material."""
    out: dict[str, set] = {}
    for col in ("home", "away"):
        for name, grp in df.groupby(df[col].astype(str))["date"]:
            out.setdefault(name, set()).update(grp)
    return out


def name_groups(league_code: str, df: pd.DataFrame) -> tuple[dict, list]:
    """(variant -> primary, refusals) for one frame. Exposed so an
    instrument can print exactly what the store will do — see
    scripts/name_unify.py — instead of the fold being invisible."""
    if df.empty or "home" not in df.columns:
        return {}, []

    names_seen = pd.concat([df["home"].astype(str),
                            df["away"].astype(str)])
    counts = names_seen.value_counts().to_dict()
    days = _day_sets(df)

    # Candidate groups: the canonical key, then the typed pairs folded into
    # whichever group already holds their primary, so a club reached both ways
    # ends up in ONE group rather than two half-merged ones.
    groups: dict[str, set] = {}
    for n in counts:
        groups.setdefault(names.canonical(n), set()).add(n)
    lower = {n.lower(): n for n in counts}
    key_of = {}                       # name -> the group key it now lives in
    for k, members in groups.items():
        for m in members:
            key_of[m] = k
    for variant, primary in _merge_map(league_code):
        v, p = lower.get(variant.lower()), lower.get(primary.lower())
        if v is None or p is None:
            continue
        kv, kp = key_of.get(v), key_of.get(p)
        if kv is None or kp is None or kv == kp:
            continue
        moved = groups.pop(kv)
        groups[kp] |= moved
        for m in moved:
            key_of[m] = kp

    mapping, refused = {}, []
    last = {n: max(d) for n, d in days.items() if d}
    for members in groups.values():
        if len(members) < 2:
            continue
        # The CURRENT spelling wins — the one the league most recently played
        # under — with row count as the tie-break. Which name survives is
        # cosmetic for the data, since the fold moves every row onto it either
        # way, but it decides whether the board can still resolve the club.
        # Most-rows was tried first and picked the dead spelling: `Atlético de
        # Madrid` folded onto `Ath Madrid`, whose 456 rows all predate the
        # provider change, and a board typing `Atlético Madrid` then resolved
        # to nothing — an abstention created by the fix. Latest-played picks
        # the name the world is using, which is the name a fixture is typed
        # from.
        order = sorted(members, key=lambda n: (last.get(n), counts[n], n),
                       reverse=True)
        primary = order[0]
        # The day test runs against everything ALREADY in the group, not just
        # against the primary. A group of three can otherwise join two clubs
        # indirectly: if A and B played each other but neither ever met C,
        # testing only against C accepts both and quietly merges A with B.
        taken = set(days.get(primary, set()))
        for other in order[1:]:
            if taken & days.get(other, set()):
                refused.append((primary, other))
                continue
            mapping[other] = primary
            taken |= days.get(other, set())
    return mapping, refused


# league_code -> {folded-away spelling (lowercased): the name that survived}.
# Filled by _unify_names, read by features._aliased.
#
# Without this a fold can REMOVE a name the board types. `Grasshoppers` folds
# onto `Grasshopper Club Zürich`, the deeper history — and then the resolver,
# asked for `Grasshoppers`, finds nothing, because the two share no canonical
# key and that is precisely why the pair had to be typed. The fixture that used
# to price now abstains, which is a worse outcome than the split it fixed.
#
# So every spelling the fold consumes stays reachable. This is the same
# statement the alias table makes — "this name IS that name" — derived rather
# than typed, and it cannot go stale, because it is produced by the fold it
# describes.
_FOLDED: dict[str, dict[str, str]] = {}
_VARIANTS: dict[str, dict[str, str]] = {}


def folded_name(league_code: str, team: str) -> Optional[str]:
    """The surviving name for a spelling the fold consumed, or None."""
    return _FOLDED.get(league_code, {}).get((team or "").strip().lower())


def folded_variants(league_code: str) -> dict[str, str]:
    """Every spelling the fold consumed -> the name that survived, in the
    store's own casing, so the resolver can be offered them as candidates."""
    return _VARIANTS.get(league_code, {})


def _unify_names(league_code: str, df: pd.DataFrame) -> pd.DataFrame:
    """Fold every spelling of a club onto one name."""
    mapping, _refused = name_groups(league_code, df)
    _FOLDED[league_code] = {k.strip().lower(): v for k, v in mapping.items()}
    _VARIANTS[league_code] = dict(mapping)
    if not mapping:
        return df
    out = df.copy()
    out["home"] = df["home"].astype(str).map(lambda n: mapping.get(n, n))
    out["away"] = df["away"].astype(str).map(lambda n: mapping.get(n, n))
    return out


def load_results(league_code: str, season: Optional[str] = None) -> pd.DataFrame:
    """
    Load only genuine played results.

    Excludes upcoming fixtures and administrative outcomes (cancelled, awarded,
    abandoned, postponed) — forfeit scorelines are not football results and
    would distort goal distributions used for calibration.

    The returned frame is cached and shared; treat it as read-only.
    """
    key = (league_code, season, "result")
    hit = _VIEW_CACHE.get(key)
    if hit is not None:
        return hit

    df = load(league_code, season)
    if df.empty or "status" not in df.columns:
        out = df
    else:
        out = df[df["status"] == "result"].reset_index(drop=True)
    out = _unify_names(league_code, out)
    _VIEW_CACHE[key] = out
    return out


def load_fixtures(league_code: str, season: Optional[str] = None) -> pd.DataFrame:
    """Load only scheduled (unplayed) fixtures."""
    df = load(league_code, season)
    if df.empty or "status" not in df.columns:
        return df
    return df[df["status"] == "fixture"].reset_index(drop=True)


def clear_cache() -> None:
    _CACHE.clear()
    _VIEW_CACHE.clear()


def stats() -> dict:
    """Summary of what is stored — used by the CLI's `data` command."""
    out = {}
    for code in available_leagues():
        df = load(code)
        if df.empty:
            continue
        counts = df["status"].value_counts().to_dict() if "status" in df else {}
        out[code] = {
            "seasons": available_seasons(code),
            "matches": len(df),
            "results": int(counts.get("result", 0)),
            "fixtures": int(counts.get("fixture", 0)),
            "date_range": (
                f"{df['date'].min():%Y-%m-%d} → {df['date'].max():%Y-%m-%d}"
                if "date" in df and not df["date"].isna().all() else "?"
            ),
        }
    return out
