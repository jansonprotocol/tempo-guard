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


def _load_espn(src: "sources.LeagueSource", season: str):
    """
    Fetch one season from ESPN's scoreboard.

    Like the football-data path this needs network at load time rather than
    reading a local clone. ESPN seasons are calendar years, so the season label
    is the year; a league whose season straddles two years would need mapping
    here before being pointed at this provider.
    """
    from app.data import espn

    if not src.espn_code:
        return None
    try:
        year = int(str(season)[:4])
    except ValueError:
        return None
    df = espn.fetch_season(src.espn_code, year, src.code,
                           calendar_year=src.calendar_year)
    if df is None or df.empty:
        return None
    # The store expects a status vocabulary; ESPN only yields finished matches.
    return df.assign(status="result")


def _load_footballdata(src: "sources.LeagueSource", season: str):
    """
    Fetch one season from football-data.co.uk.

    Requires network access at load time — unlike the openfootball path, which
    reads a local clone. The odds guard runs before anything is returned, so a
    bookmaker column can never reach the store.
    """
    from app.data import footballdata as fd

    if src.fd_country:
        df = fd.fetch_extra(src.fd_country, season=season, league=src.fd_league)
    elif src.fd_div:
        # Main-league URLs key seasons as "2526" for 2025-26.
        start = int(season.split("-")[0])
        df = fd.fetch_main(src.fd_div, fd.season_key(start))
    else:
        return None

    if df is not None and not df.empty:
        fd.assert_no_odds(df)
        df = df.drop(columns=[c for c in ("season_raw", "league_raw") if c in df.columns])
    return df


def _merge_live(base: pd.DataFrame, live: pd.DataFrame) -> pd.DataFrame:
    """
    Combine a git-sourced season with one fetched from football-data.

    The two providers are complementary and neither alone is sufficient for a
    season in progress: openfootball publishes the full fixture list months
    ahead but lags weeks behind on results, while football-data carries results
    within hours but lists only matches already played.

    The merge deliberately does NOT try to pair rows by team name. The two
    sources abbreviate differently — "Man United" against "Manchester United
    FC", "Wolves" against "Wolverhampton Wanderers FC" — and the strict matcher
    correctly refuses those, while loosening it enough to catch them would
    reintroduce the wrong-club matching it exists to prevent. An earlier
    name-keyed version silently doubled ENG-PL from 9,880 rows to 17,616.

    Instead the split is by date, which needs no matching at all:

      * every played match comes from live, which is authoritative for results
        and carries the richer statistics
      * base contributes only fixtures dated after live's last result — matches
        that demonstrably have not been played yet

    A consequence worth knowing: within a season, team naming then comes from a
    single source, so the feature layer never has to reconcile two spellings.
    """
    if base is None or base.empty:
        return live
    if live is None or live.empty:
        return base

    live_results = live[live["status"] == "result"] if "status" in live else live
    if live_results.empty:
        # Nothing played yet upstream — the git schedule is all we have.
        return base

    cutoff = live_results["date"].max()
    future = base[(base["status"] == "fixture") & (base["date"] > cutoff)] \
        if "status" in base else base[base["date"] > cutoff]

    merged = pd.concat([live, future], ignore_index=True)
    return merged.sort_values("date").reset_index(drop=True)


def refresh_live(
    league_codes: Optional[Iterable[str]] = None,
    season: Optional[str] = None,
    quiet: bool = False,
) -> dict[str, int]:
    """
    Top up the current season from football-data.co.uk for every league that
    has an identifier there, merging over whatever the git source provided.

    This is the online half of the hybrid: git carries deep history and the
    fixture schedule offline, this brings results up to the minute on request.
    """
    codes = list(league_codes) if league_codes else [
        c for c, s in sources.LEAGUES.items() if s.fd_div or s.fd_country
    ]
    out: dict[str, int] = {}

    for code in codes:
        src = sources.get(code)
        target = season or (src.default_seasons()[-1])
        live = _load_footballdata(src, target)
        if live is None or live.empty:
            if not quiet:
                print(f"  [live] {code:8s} {target}: nothing published yet")
            continue

        base = store.load(code, target)
        merged = _merge_live(base, live)
        merged = merged.assign(league_code=code, season=target)
        store.save(code, target, merged)

        played = int((merged["status"] == "result").sum())
        out[code] = played
        if not quiet:
            fixtures = int((merged["status"] == "fixture").sum())
            print(f"  [live] {code:8s} {target}  results={played:4d} fixtures={fixtures:4d}"
                  f"  latest={merged['date'].max():%Y-%m-%d}")

    return out


def enrich_from_footballdata(
    league_codes: Optional[Iterable[str]] = None,
    quiet: bool = False,
) -> dict[str, int]:
    """
    Re-source stored seasons from football-data.co.uk where it covers them.

    Same matches, richer record: openfootball publishes goals only, while
    football-data carries measured shots, shots on target, corners, cards and
    the referee for the same fixtures back to 2000. Wiring real shot counts in
    replaces an estimate (sot_proj_total, currently derived from goals via a
    fixed ratio) with a measurement.

    Merged rather than overwritten, for the same reason `refresh_live` merges:
    football-data lists only matches already played, so a straight overwrite
    would delete the fixture schedule openfootball publishes months ahead.

    Returns {league_code: matches now carrying shot data}.
    """
    from app.data import sources as _s  # noqa: F401  (kept explicit for clarity)
    codes = list(league_codes) if league_codes else [
        c for c, s in sources.LEAGUES.items()
        if s.fd_div and s.provider == "openfootball"
    ]
    out: dict[str, int] = {}

    for code in codes:
        src = sources.get(code)
        enriched = 0
        # Union of what is stored and what football-data publishes. Iterating
        # only stored seasons would enrich in place but never backfill, and
        # several leagues turned out to be nearly empty because their git path
        # was wrong — Scotland's top flight held 12 matches, Belgium 18.
        seasons = sorted(set(store.available_seasons(code)) | set(src.all_seasons(since=2000)))
        for season in seasons:
            live = _load_footballdata(src, season)
            if live is None or live.empty:
                continue
            base = store.load(code, season)
            merged = _merge_live(base, live).assign(league_code=code, season=season)
            store.save(code, season, merged)
            if "hst" in merged.columns:
                enriched += int(merged["hst"].notna().sum())
        out[code] = enriched
        if not quiet:
            total = len(store.load_results(code))
            print(f"  [enrich] {code:8s} {enriched:6d} of {total:6d} results now carry shots")

    return out


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
    src = sources.get(league_code)

    if src.provider == "espn":
        df = _load_espn(src, season)
        if df is None or df.empty:
            if not quiet and not skip_quiet:
                print(f"  [load] {league_code} {season}: not available upstream — skipped")
            return None
    elif src.provider == "footballdata":
        df = _load_footballdata(src, season)
        if df is None or df.empty:
            if not quiet and not skip_quiet:
                print(f"  [load] {league_code} {season}: not available upstream — skipped")
            return None
    else:
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
