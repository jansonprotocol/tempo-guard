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

import os
from pathlib import Path
from typing import Optional

import pandas as pd

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
