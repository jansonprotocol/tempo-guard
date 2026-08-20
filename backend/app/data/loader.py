"""
Data loader — fetches openfootball repos and materialises ATHENA snapshots.

Flow:
    sync_repos()   git clone/pull the upstream openfootball repos into a cache
    load_league()  parse one league-season .txt -> parquet under data/
    load_all()     do that for every registered league and season

The upstream cache lives outside the repo (default ~/.cache/athena/openfootball)
so only the parsed parquet snapshots are committed. Clones are shallow: history
is irrelevant here, only the current files matter.
"""
from __future__ import annotations

import os
import subprocess
from pathlib import Path
from typing import Iterable, Optional

import pandas as pd

from app.data import sources, store
from app.data.openfootball import parse_file

CACHE_DIR = Path(
    os.environ.get("ATHENA_SOURCE_CACHE", Path.home() / ".cache" / "athena" / "openfootball")
)

# Seasons ATHENA tracks by default: the completed season used for calibration,
# and the upcoming one whose fixtures we predict.
DEFAULT_SEASONS = ["2025-26", "2026-27"]


def _run(cmd: list[str], cwd: Optional[Path] = None, timeout: int = 600) -> tuple[int, str]:
    proc = subprocess.run(
        cmd, cwd=str(cwd) if cwd else None,
        capture_output=True, text=True, timeout=timeout,
        env={**os.environ, "GIT_LFS_SKIP_SMUDGE": "1", "GIT_TERMINAL_PROMPT": "0"},
    )
    return proc.returncode, (proc.stdout + proc.stderr).strip()


def sync_repos(repos: Optional[Iterable[str]] = None, quiet: bool = False) -> dict[str, str]:
    """
    Clone (or fast-forward) the openfootball repos backing the registry.

    Returns {repo: status} where status is "cloned", "updated" or an error string.
    Network access is required only here; everything downstream is offline.
    """
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    todo = list(repos) if repos else sorted(sources.repos())
    results: dict[str, str] = {}

    for repo in todo:
        dest = CACHE_DIR / repo
        url = f"{sources.OPENFOOTBALL_ORG}/{repo}"
        if (dest / ".git").is_dir():
            code, out = _run(["git", "pull", "--ff-only", "--depth", "1"], cwd=dest)
            results[repo] = "updated" if code == 0 else f"error: {out.splitlines()[-1] if out else code}"
        else:
            code, out = _run(["git", "clone", "--depth", "1", "-q", url, str(dest)])
            results[repo] = "cloned" if code == 0 else f"error: {out.splitlines()[-1] if out else code}"
        if not quiet:
            print(f"  [sync] {repo:20s} {results[repo]}")

    return results


def source_file(league_code: str, season: str) -> Optional[Path]:
    """Resolve the on-disk .txt for a league-season, or None if absent."""
    src = sources.get(league_code)
    path = CACHE_DIR / src.repo / src.season_path(season)
    return path if path.exists() else None


def load_league(
    league_code: str,
    season: str,
    quiet: bool = False,
    skip_quiet: bool = False,
) -> Optional[pd.DataFrame]:
    """
    Parse one league-season into the store. Returns the DataFrame, or None when
    the upstream file does not exist (common for seasons a repo has not published).
    """
    path = source_file(league_code, season)
    if path is None:
        if not quiet and not skip_quiet:
            print(f"  [load] {league_code} {season}: not published upstream — skipped")
        return None

    df = parse_file(path)
    if df.empty:
        if not quiet:
            print(f"  [load] {league_code} {season}: parsed 0 matches — skipped")
        return None

    df = df.assign(league_code=league_code, season=season)
    store.save(league_code, season, df)

    if not quiet:
        counts = df["status"].value_counts().to_dict()
        print(
            f"  [load] {league_code:7s} {season}  "
            f"{len(df):4d} matches  "
            f"results={counts.get('result', 0):4d} fixtures={counts.get('fixture', 0):4d}"
        )
    return df


def load_all(
    league_codes: Optional[Iterable[str]] = None,
    seasons: Optional[Iterable[str]] = None,
    quiet: bool = False,
    history: bool = False,
) -> dict[str, list[str]]:
    """
    Parse every requested league-season into the store.

    history=True loads every season the upstream repo publishes rather than the
    recent default. That is the difference between ~300 matches per league and
    several thousand — and only the latter can resolve a small edge.
    """
    codes = list(league_codes) if league_codes else sources.codes()
    explicit = list(seasons) if seasons else None
    loaded: dict[str, list[str]] = {}

    for code in codes:
        # Season keys differ by competition: European winter leagues use
        # "2025-26", calendar-year leagues (Brazil, MLS, Japan, the Nordics)
        # use "2025". Each league declares its own convention.
        src = sources.get(code)
        if explicit is not None:
            szns = explicit
        elif history:
            szns = src.all_seasons()
        else:
            szns = src.default_seasons()
        for season in szns:
            df = load_league(code, season, quiet=quiet, skip_quiet=history)
            if df is not None:
                loaded.setdefault(code, []).append(season)

    return loaded
