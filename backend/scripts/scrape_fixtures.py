"""
backend/scripts/scrape_fixtures.py

Scrapes FBref schedule pages for completed scores and upcoming fixtures.
Also fetches current league standings from the FBref stats page and updates
teams.current_position.

Standings are fetched from a SEPARATE page:
  Schedule:  /en/comps/{id}/schedule/{Name}-Scores-and-Fixtures
  Standings: /en/comps/{id}/{Name}-Stats

  The schedule page rarely embeds a full standings table — the stats page
  is the reliable source. Both are fetched per league.

Stores completed matches in FBrefSnapshot, upcoming matches in FBrefFixtures.

Usage:
    cd backend
    python -m scripts.scrape_fixtures [--league LEAGUE] [--headless] [--api KEY]

Public callable (used by daily updater and full-history loader):
    from scripts.scrape_fixtures import scrape_league_standings
    scrape_league_standings("ENG-PL")
"""

import sys
import os
import re
import time
import io
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional, Tuple

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from dotenv import load_dotenv
load_dotenv(override=True)

import pandas as pd
import requests
from seleniumbase import Driver
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import FBrefFixture
from app.models.team import Team
from app.util.team_resolver import resolve_and_learn
from app.core.constants import LEAGUE_MAP, SEASON_MAP
try:
    from app.seed.teams_sync import sync_league_teams as _sync_league_teams
except ImportError:
    _sync_league_teams = None
from app.services.resolve_team import resolve_team_name

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SLEEP_BETWEEN_FETCHES = 4
SLEEP_BETWEEN_LEAGUES = 6
FIXTURE_DAYS = 5

HEADLESS = False
SCRAPER_API_KEY: Optional[str] = os.environ.get("SCRAPER_API_KEY")

# International competitions — no simple league table, standings skipped.
INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}

# Explicit stats page URL overrides for leagues where the auto-derived URL
# returns wrong data (e.g. FBref redirects "Serie-A-Stats" to Italian Serie A
# instead of Brazilian Serie A, or similar slug collisions).
# Format: league_code → full stats page URL
_STANDINGS_URL_OVERRIDES: dict[str, str] = {
    "BRA-SA": "https://fbref.com/en/comps/24/2026/2026-Serie-A-Stats",
    "BRA-SB": "https://fbref.com/en/comps/38/2026/2026-Serie-B-Stats",
    "MLS":    "https://fbref.com/en/comps/22/2026/2026-Major-League-Soccer-Stats",
    "NOR-EL": "https://fbref.com/en/comps/28/2026/2026-Eliteserien-Stats",
    "SWE-AL": "https://fbref.com/en/comps/29/2026/2026-Allsvenskan-Stats",
    "JPN-J1": "https://fbref.com/en/comps/25/2026/2026-J1-League-Stats",
    "CHN-CSL":"https://fbref.com/en/comps/62/2026/2026-Chinese-Super-League-Stats",
    "COL-PA": "https://fbref.com/en/comps/41/2026/2026-Primera-A-Stats",
}


# ---------------------------------------------------------------------------
# URL helpers
# ---------------------------------------------------------------------------
def _standings_url_from_schedule(schedule_url: str, league_code: str = "") -> Optional[str]:
    """
    Derive the FBref stats/standings page URL from the schedule URL.
    Builds a year-specific URL using SEASON_MAP to prevent FBref redirecting
    smaller leagues (and comp/13 Ligue 1) to the Premier League stats page.
    """
    m = re.match(
        r"(https://fbref\.com/en/comps/\d+)/schedule/(.+?)-Scores-and-Fixtures",
        schedule_url,
    )
    if not m:
        return None
    base = m.group(1)
    slug = m.group(2)
    season = SEASON_MAP.get(league_code) if league_code else None
    if season:
        return f"{base}/{season}/{season}-{slug}-Stats"
    return f"{base}/{slug}-Stats"


# ---------------------------------------------------------------------------
# Fetch helpers (ScraperAPI or Selenium)
# ---------------------------------------------------------------------------
def _fetch_page(url: str, label: str, bust_cache: bool = False) -> Optional[str]:
    if SCRAPER_API_KEY:
        return _fetch_via_scraperapi(url, label, bust_cache=bust_cache)
    return _fetch_via_selenium(url, label)


def _fetch_via_scraperapi(url: str, label: str, bust_cache: bool = False) -> Optional[str]:
    print(f"  [ScraperAPI] {label}")
    try:
        params = {
            "api_key": SCRAPER_API_KEY,
            "url": url,
            "render": "true",
            "premium": "true",
        }
        # ScraperAPI caches responses — standings pages for different leagues
        # can return cached PL content if fetched in quick succession.
        # bust_cache=True forces a fresh fetch by disabling the cache.
        if bust_cache:
            params["cache"] = "false"
            params["keep_headers"] = "true"  # prevents ScraperAPI from reusing cached headers
            # Rotate session to force a fresh proxy/browser instance
            import time as _time, random as _random
            params["session_number"] = str(_random.randint(1, 99999))
            # Also timestamp the URL to bust any CDN cache
            sep = "&" if "?" in params["url"] else "?"
            params["url"] = f'{params["url"]}{sep}_cb={int(_time.time())}'
        resp = requests.get(
            "http://api.scraperapi.com",
            params=params,
            timeout=90,
        )
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}")
            return None
        print(f"    {len(resp.text)} bytes")
        return resp.text
    except Exception as e:
        print(f"    Error: {e}")
        return None


def _fetch_via_selenium(url: str, label: str) -> Optional[str]:
    driver = None
    try:
        driver = Driver(uc=True, headless2=HEADLESS)
        driver.uc_open_with_reconnect(url, 4)
        if not HEADLESS:
            driver.uc_gui_click_captcha()
        time.sleep(3)
        html = driver.get_page_source()
        print(f"    {len(html)} bytes")
        return html
    except Exception as e:
        print(f"    Browser error ({label}): {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _fetch_with_driver(driver, url: str, label: str) -> Optional[str]:
    """Use an existing Selenium driver to fetch a page (reuse browser)."""
    try:
        driver.get(url)
        time.sleep(3)
        html = driver.get_page_source()
        print(f"    {len(html)} bytes")
        return html
    except Exception as e:
        print(f"    Browser error ({label}): {e}")
        return None


# ---------------------------------------------------------------------------
# Page parsing
# ---------------------------------------------------------------------------
def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns to single strings."""
    if isinstance(df.columns, pd.MultiIndex):
        df = df.copy()
        df.columns = [
            " ".join(str(v) for v in col if str(v) != "nan").strip()
            for col in df.columns
        ]
    return df


def _is_schedule_table(df: pd.DataFrame) -> bool:
    df = _flatten_columns(df)
    cols_lower = [str(c).lower() for c in df.columns]
    return (
        any("date" in c for c in cols_lower)
        and any("home" in c for c in cols_lower)
        and any("away" in c for c in cols_lower)
    )


def _is_standings_table(df: pd.DataFrame) -> bool:
    """
    Robust check for a league standings table.

    Looks for any combination of:
      - A rank/position column: 'rk', 'rank', 'pos', '#'
      - A team name column:     'squad', 'team', 'club'
      - A points column:        'pts', 'points'
      - Reasonable row count:   8–30 rows (covers large leagues)
    """
    df = _flatten_columns(df)
    cols_lower = [str(c).lower().strip() for c in df.columns]
    has_rank  = any(c in ("rk", "rank", "pos", "#") for c in cols_lower)
    has_team  = any(c in ("squad", "team", "club") for c in cols_lower)
    has_pts   = any("pts" in c or c == "points" for c in cols_lower)
    has_rows  = 8 <= len(df.dropna(how="all")) <= 30
    return has_rank and has_team and has_pts and has_rows


def _verify_page_league(html: str, league_code: str) -> bool:
    """
    Verify the fetched HTML is actually for the right league by checking the
    page <title>. Guards against ScraperAPI returning a cached PL page.
    Returns True if page looks correct or no check is defined.
    """
    import re as _re
    LEAGUE_KEYWORDS = {
        "ENG-PL":  ["premier league"],
        "ENG-CH":  ["championship"],
        "ESP-LL":  ["la liga"],
        "ESP-LL2": ["segunda"],
        "FRA-L1":  ["ligue 1"],
        "FRA-L2":  ["ligue 2"],
        "GER-BUN": ["bundesliga"],
        "GER-B2":  ["2. bundesliga", "2-bundesliga"],
        "ITA-SA":  ["serie a"],
        "ITA-SB":  ["serie b"],
        "NED-ERE": ["eredivisie"],
        "TUR-SL":  ["super lig"],
        "SAU-SPL": ["saudi", "pro league"],
        "DEN-SL":  ["superliga", "danish"],
        "BEL-PL":  ["jupiler", "belgian"],
        "POR-LP":  ["primeira liga", "liga portugal"],
        "NOR-EL":  ["eliteserien"],
        "SWE-AL":  ["allsvenskan"],
        "MLS":     ["major league soccer"],
        "BRA-SA":  ["série a", "brasileirao"],
        "BRA-SB":  ["série b"],
        "JPN-J1":  ["j1 league"],
        "CHN-CSL": ["chinese super league"],
        "POL-EK":  ["ekstraklasa"],
        "SUI-SL":  ["swiss super league"],
        "MEX-LMX": ["liga mx"],
    }
    keywords = LEAGUE_KEYWORDS.get(league_code)
    if not keywords:
        return True
    title_match = _re.search(r"<title[^>]*>(.*?)</title>", html, _re.IGNORECASE | _re.DOTALL)
    if not title_match:
        return True
    title = title_match.group(1).lower()
    if any(kw in title for kw in keywords):
        return True
    print(f"  [standings] Page title mismatch for {league_code}: '{title_match.group(1)[:60]}'")
    return False


def _contains_dates(df: pd.DataFrame, sample_rows: int = 5) -> bool:
    df = _flatten_columns(df)
    sample = df.head(sample_rows).astype(str)
    for _, row in sample.iterrows():
        for val in row:
            if re.search(r"\d{4}-\d{2}-\d{2}", val) or re.search(r"\d{2}/\d{2}/\d{4}", val):
                return True
    return False


def _parse_page(html: str, league_code: str = "") -> Tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Parse FBref schedule page HTML.
    Returns (schedule_df, standings_df).  standings_df may be None — that's
    normal for a schedule-only page; standings are fetched separately.
    """
    if "Just a moment" in html or len(html) < 5000:
        print("  Cloudflare blocked.")
        return None, None

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as e:
        print(f"  Parse error: {e}")
        return None, None

    if not tables:
        return None, None

    is_intl = league_code in INTL_LEAGUE_CODES
    schedule_df = None
    standings_df = None

    for df in tables:
        df = _flatten_columns(df)
        if _is_standings_table(df) and not is_intl:
            standings_df = df.dropna(how="all")
            print(f"  Found standings table in schedule page ({len(standings_df)} rows)")
        elif _is_schedule_table(df) and schedule_df is None:
            schedule_df = df.dropna(how="all")
            print(f"  Found schedule table ({len(schedule_df)} rows)")

    # Date-based fallback for schedule
    if schedule_df is None and not is_intl:
        candidates = [
            df for df in tables
            if df.shape[1] >= 3
            and _contains_dates(df)
            and not _is_standings_table(df)
        ]
        if candidates:
            schedule_df = max(candidates, key=len).dropna(how="all")
            print(f"  Fallback schedule table ({len(schedule_df)} rows)")

    # International: merge multiple schedule tables (group stage etc.)
    if is_intl and schedule_df is None:
        parts = []
        for t in tables:
            if _is_schedule_table(t):
                t = _flatten_columns(t).dropna(how="all").copy()
                cols_lower_map = {str(c).lower(): c for c in t.columns}
                round_col = cols_lower_map.get("round") or cols_lower_map.get("wk")
                if round_col:
                    vals = t[round_col].dropna().astype(str)
                    vals = vals[~vals.str.lower().isin(["nan", "", "round", "wk"])]
                    label = vals.mode()[0] if not vals.empty else None
                else:
                    label = None
                t["_round_raw"] = label
                parts.append(t)
        if parts:
            schedule_df = pd.concat(parts, ignore_index=True)
            print(f"  Merged {len(parts)} schedule tables → {len(schedule_df)} rows")

    # Last resort fallback
    if schedule_df is None and not is_intl:
        non_standings = [df for df in tables if not _is_standings_table(df)]
        if non_standings:
            schedule_df = max(non_standings, key=len).dropna(how="all")
            print(f"  Last-resort schedule table ({len(schedule_df)} rows)")

    return schedule_df, standings_df


def _parse_standings_page(html: str, league_code: str) -> Optional[pd.DataFrame]:
    """
    Parse a dedicated FBref stats page (/en/comps/{id}/{Name}-Stats) for
    the standings table.  More permissive than the schedule parser since
    we know we're on a stats page.
    """
    if "Just a moment" in html or len(html) < 5000:
        print("  [standings] Cloudflare blocked stats page.")
        return None

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as e:
        print(f"  [standings] Parse error on stats page: {e}")
        return None

    # Priority: prefer tables that look like overall standings
    for df in tables:
        df = _flatten_columns(df)
        if _is_standings_table(df):
            print(f"  [standings] Found standings table on stats page ({len(df.dropna(how='all'))} rows)")
            return df.dropna(how="all")

    print("  [standings] No standings table found on stats page.")
    return None


# ---------------------------------------------------------------------------
# Column resolution
# ---------------------------------------------------------------------------
def _get_columns(df: pd.DataFrame) -> dict:
    df = _flatten_columns(df)
    cols = {str(c).lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    return {
        "date":      col("date"),
        "home":      col("home"),
        "away":      col("away"),
        "score":     col("score", "scores"),
        "time":      col("time"),
        "round_raw": col("_round_raw"),
    }


def _safe_to_parquet(df: pd.DataFrame) -> bytes:
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df.to_parquet(index=True)


def _classify_round_type(raw: Optional[str], match_date: date, league_code: str) -> Optional[str]:
    if league_code not in ("UCL", "UEL", "UECL"):
        return None
    label = (raw or "").lower().strip()
    if any(k in label for k in ("league phase", "group", "matchday")):
        return "league_phase"
    if "playoff" in label or "play-off" in label:
        return "playoff"
    if any(k in label for k in ("round of 16", "r16", "last 16", "ro16")):
        return "round_of_16"
    if any(k in label for k in ("quarter", "qf")):
        return "quarter_final"
    if any(k in label for k in ("semi", "sf")):
        return "semi_final"
    if "final" in label and "semi" not in label and "quarter" not in label:
        return "final"
    month = match_date.month
    if month in (9, 10, 11, 12, 1):
        return "league_phase"
    if month == 2:
        return "playoff"
    if month == 3:
        return "round_of_16"
    if month == 4:
        return "quarter_final"
    if month == 5:
        return "semi_final" if match_date.day < 25 else "final"
    return None


# ---------------------------------------------------------------------------
# Standings processing
# ---------------------------------------------------------------------------
# League tier map — used to distinguish promotions from relegations
# Higher number = higher tier (1st division = highest)
_LEAGUE_TIERS: dict[str, int] = {
    "ENG-PL": 1, "ENG-CH": 2,
    "ESP-LL": 1, "ESP-LL2": 2,
    "FRA-L1": 1, "FRA-L2": 2,
    "GER-BUN": 1, "GER-B2": 2,
    "ITA-SA": 1, "ITA-SB": 2,
    "NED-ERE": 1, "BEL-PL": 1, "TUR-SL": 1,
    "POR-LP": 1, "NOR-EL": 1, "SWE-AL": 1,
    "DEN-SL": 1, "SAU-SPL": 1, "MLS": 1,
    "BRA-SA": 1, "BRA-SB": 2,
    "JPN-J1": 1, "CHN-CSL": 1,
    "AUT-BL": 1, "SUI-SL": 1,
    "POL-EK": 1, "MEX-LMX": 1,
}


def _process_standings(db: Session, league_code: str, standings_df: pd.DataFrame) -> int:
    """
    Parse standings table and update teams.current_position.
    Auto-detects promotions and relegations:
    - Team on table but in wrong league in DB → update league_code
    - Team in DB for this league but not on table → clear position (relegated/promoted out)
    Returns the number of teams updated.
    """
    if standings_df is None or standings_df.empty:
        return 0

    standings_df = _flatten_columns(standings_df)
    cols_lower = {str(c).lower().strip(): c for c in standings_df.columns}

    # Find position column
    pos_col = None
    for key in ("rk", "rank", "pos", "#"):
        if key in cols_lower:
            pos_col = cols_lower[key]
            break

    # Find team name column
    team_col = None
    for key in ("squad", "team", "club"):
        if key in cols_lower:
            team_col = cols_lower[key]
            break

    if not pos_col or not team_col:
        print(f"  [standings] Cannot find position/team columns. Available: {list(cols_lower.keys())}")
        return 0

    print(f"  [standings] Using pos_col='{pos_col}', team_col='{team_col}'")
    updated = 0
    teams_on_table: set = set()  # team_keys confirmed on this standings page
    this_tier = _LEAGUE_TIERS.get(league_code, 0)

    for _, row in standings_df.iterrows():
        team_name_raw = str(row[team_col]).strip()
        if not team_name_raw or team_name_raw.lower() in ("nan", "", "squad", "team"):
            continue

        try:
            position = int(float(str(row[pos_col]).strip()))
        except (ValueError, TypeError):
            continue

        if position < 1 or position > 30:
            continue

        team_key = resolve_and_learn(db, team_name_raw, league_code)
        team = db.query(Team).filter_by(team_key=team_key, league_code=league_code).first()

        if team:
            # Team already in correct league — normal update
            team.current_position = position
            teams_on_table.add(team_key)
            updated += 1
        else:
            # Not found in this league — check if it exists in another league
            # Search by team_key first, then by display_name as fallback
            team_elsewhere = db.query(Team).filter_by(team_key=team_key).first()
            if not team_elsewhere:
                # Try display_name match (case-insensitive)
                from sqlalchemy import func as _func
                team_elsewhere = db.query(Team).filter(
                    _func.lower(Team.display_name) == team_name_raw.lower()
                ).first()
            if not team_elsewhere:
                # Try partial normalised key match
                team_elsewhere = db.query(Team).filter(
                    Team.team_key.contains(team_key[:6])
                ).first() if len(team_key) >= 6 else None
            if team_elsewhere:
                old_lc = team_elsewhere.league_code
                old_tier = _LEAGUE_TIERS.get(old_lc, 0)
                new_tier = _LEAGUE_TIERS.get(league_code, 0)
                old_country = (old_lc or "").split("-")[0]
                new_country = league_code.split("-")[0]
                same_country = old_country == new_country
                adjacent_tier = (
                    old_tier > 0 and new_tier > 0
                    and abs(old_tier - new_tier) <= 1
                )
                if same_country and adjacent_tier:
                    team_elsewhere.league_code = league_code
                    team_elsewhere.current_position = position
                    teams_on_table.add(team_key)
                    updated += 1
                    print(
                        f"  [standings] UP Moved {team_elsewhere.display_name}: "
                        f"{old_lc} -> {league_code} (pos {position})"
                    )
                    # Also migrate player stats to new league so power index
                    # picks them up correctly in their new competition context
                    try:
                        from app.models.models_players import PlayerSeasonStats
                        player_rows = (
                            db.query(PlayerSeasonStats)
                            .filter_by(league_code=old_lc, team=team_key)
                            .all()
                        )
                        for pr in player_rows:
                            pr.league_code = league_code
                        if player_rows:
                            print(
                                f"  [standings] Migrated {len(player_rows)} player "
                                f"stat rows for {team_elsewhere.display_name} "
                                f"to {league_code}"
                            )
                    except Exception as _pe:
                        print(f"  [standings] Player migration error: {_pe}")
                    try:
                        from app.seed.teams_sync import update_team_league
                        update_team_league(team_elsewhere.display_name, league_code)
                    except Exception:
                        pass
                else:
                    reason = "cross-country" if not same_country else "non-adjacent tier"
                    print(
                        f"  [standings] REJECTED move: {team_elsewhere.display_name} "
                        f"({old_lc} -> {league_code}) blocked: {reason}"
                    )
            else:
                print(f"  [standings] Team not found: '{team_name_raw}' -> '{team_key}'")

    # ── Relegation/promotion-out detection ───────────────────────────
    # Find teams currently registered in this league that weren't on the table.
    # If they appear in a higher-tier league, they were promoted — leave alone.
    # Otherwise clear their position (relegated to lower tier or data gap).
    if teams_on_table and updated > 0:
        all_in_league = db.query(Team).filter_by(league_code=league_code).all()
        for t in all_in_league:
            if t.team_key in teams_on_table:
                continue  # on table — fine
            if t.current_position is None:
                continue  # already cleared
            # Check if this team now appears in a higher-tier league
            promoted = False
            if this_tier > 0:
                higher_leagues = [
                    lc for lc, tier in _LEAGUE_TIERS.items()
                    if tier < this_tier  # lower number = higher tier
                ]
                for hlc in higher_leagues:
                    if db.query(Team).filter_by(team_key=t.team_key, league_code=hlc).first():
                        promoted = True
                        break
            if not promoted:
                print(
                    f"  [standings] ↓ {t.display_name} not on {league_code} table — "
                    f"clearing position (was {t.current_position})"
                )
                t.current_position = None

    if updated:
        db.commit()
        print(f"  [standings] Updated {updated} team positions for {league_code}")
    else:
        print(f"  [standings] No teams updated for {league_code} — check team name resolution")

    return updated


# ---------------------------------------------------------------------------
# Public: standalone standings scraper
# ---------------------------------------------------------------------------
def scrape_league_standings(
    league_code: str,
    schedule_url: Optional[str] = None,
    db: Optional[Session] = None,
) -> int:
    """
    Fetch and persist current standings for a single league.

    Tries two sources in order:
      1. The FBref stats page (/en/comps/{id}/{Name}-Stats) — primary source
      2. The schedule page (passed as schedule_url) — rarely includes standings
         but worth trying as a free fallback when already fetched

    Args:
        league_code:   e.g. "ENG-PL"
        schedule_url:  The schedule URL for this league (from LEAGUE_MAP).
                       If None, looks up LEAGUE_MAP automatically.
        db:            SQLAlchemy session. If None, creates and closes one.

    Returns:
        Number of team positions updated (0 if nothing found/updated).
    """
    if league_code in INTL_LEAGUE_CODES:
        print(f"  [standings] Skipping {league_code} — international competition, no league table.")
        return 0

    # Resolve schedule URL
    if schedule_url is None:
        entry = LEAGUE_MAP.get(league_code)
        if not entry:
            print(f"  [standings] Unknown league: {league_code}")
            return 0
        schedule_url = entry[0] if isinstance(entry, tuple) else entry

    # Check explicit override first — some leagues have URL slug collisions
    # (e.g. BRA-SA "Serie-A-Stats" redirects to Italian Serie A on FBref)
    if league_code in _STANDINGS_URL_OVERRIDES:
        stats_url = _STANDINGS_URL_OVERRIDES[league_code]
        print(f"  [standings] Using URL override for {league_code}")
    else:
        stats_url = _standings_url_from_schedule(schedule_url, league_code)

    if not stats_url:
        print(f"  [standings] Could not derive stats URL from: {schedule_url}")
        return 0

    print(f"  [standings] Fetching stats page: {stats_url}")
    html = _fetch_page(stats_url, f"{league_code} standings", bust_cache=True)
    if not html:
        print(f"  [standings] Failed to fetch stats page for {league_code}")
        return 0

    standings_df = _parse_standings_page(html, league_code)
    if standings_df is None or standings_df.empty:
        print(f"  [standings] No standings found on stats page for {league_code}")
        return 0

    close_db = db is None
    if db is None:
        db = SessionLocal()

    try:
        # Sanity check: verify at least half the sampled teams belong to this league.
        # Catches ScraperAPI returning a cached PL page even with bust_cache.
        standings_flat = _flatten_columns(standings_df)
        cols_lower = {str(c).lower().strip(): c for c in standings_flat.columns}
        team_col_chk = cols_lower.get("squad") or cols_lower.get("team") or cols_lower.get("club")
        if team_col_chk:
            sample_names = [
                str(r).strip() for r in standings_flat[team_col_chk].dropna().head(6)
                if str(r).strip().lower() not in ("nan", "", "squad", "team")
            ]
            matched = sum(
                1 for raw in sample_names
                if db.query(Team).filter_by(
                    team_key=resolve_and_learn(db, raw, league_code),
                    league_code=league_code
                ).first()
            )
            min_match = max(3, (len(sample_names) * 2) // 3)  # at least 2/3 must match
            if matched < min_match and len(sample_names) >= 3:
                print(
                    f"  [standings] REJECTED stats page: {matched}/{len(sample_names)} "
                    f"sampled teams match {league_code} — likely wrong page. "
                    f"Sample: {sample_names[:3]}"
                )
                return 0
        return _process_standings(db, league_code, standings_df)
    finally:
        if close_db:
            db.close()


# ---------------------------------------------------------------------------
# Team name helpers
# ---------------------------------------------------------------------------

# Generic football suffixes that on their own are not valid team names.
# If name resolution produces one of these it means the original name was
# mangled by over-aggressive stripping.
_GENERIC_SUFFIXES = {
    "fc", "sc", "ac", "cf", "rc", "fk", "bk", "sk", "if", "gk",
    "afc", "bfc", "cfc", "dfc", "efc", "rfc", "sfc", "ufc",
    "utd", "united", "city", "town", "rovers", "wanderers",
}

def _strip_country_code(name: str) -> str:
    """
    Strip leading/trailing 2-3 letter country codes added by FBref for
    international competitions (e.g. "ENG Arsenal" → "Arsenal").
    Only applied when the result would leave a meaningful name behind.
    """
    # Try stripping leading code
    stripped = re.sub(r"(?i)^[a-z]{2,3}\s+", "", name).strip()
    # Only accept the stripped version if it leaves more than a bare suffix
    if stripped and not _is_generic_suffix(stripped):
        name = stripped
    # Try stripping trailing code
    stripped = re.sub(r"(?i)\s+[a-z]{2,3}$", "", name).strip()
    if stripped and not _is_generic_suffix(stripped):
        name = stripped
    return name


def _is_generic_suffix(name: str) -> bool:
    """Return True if name is nothing but a generic football suffix."""
    return name.strip().lower() in _GENERIC_SUFFIXES


# ---------------------------------------------------------------------------
# Database operations
# ---------------------------------------------------------------------------
def _update_snapshot(league_code: str, completed_df: pd.DataFrame) -> None:
    db = SessionLocal()
    try:
        snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if snap:
            existing = pd.read_parquet(io.BytesIO(snap.data))
            col_map = {str(c).lower(): c for c in existing.columns}
            date_col = col_map.get("date")
            home_col = col_map.get("home") or col_map.get("home_team")
            away_col = col_map.get("away") or col_map.get("away_team")
            if date_col and home_col and away_col:
                combined = pd.concat([existing, completed_df], ignore_index=True)
                combined = combined.drop_duplicates(subset=[date_col, home_col, away_col])
                combined[date_col] = pd.to_datetime(combined[date_col], errors="coerce")
                combined = combined.sort_values(date_col).reset_index(drop=True)
                snap.data = _safe_to_parquet(combined)
            else:
                snap.data = _safe_to_parquet(completed_df)
            snap.fetched_at = datetime.utcnow()
            action = "Updated"
        else:
            new_snap = FBrefSnapshot(
                league_code=league_code,
                data=_safe_to_parquet(completed_df),
                fetched_at=datetime.utcnow(),
            )
            db.add(new_snap)
            action = "Created"
        db.commit()
        print(f"  Snapshot {action} for {league_code}")
    except Exception as e:
        print(f"  Snapshot error: {e}")
    finally:
        db.close()


def _upsert_fixtures(league_code: str, upcoming_df: pd.DataFrame, c: dict) -> None:
    db = SessionLocal()
    try:
        today = date.today()
        db.query(FBrefFixture).filter(
            FBrefFixture.league_code == league_code,
            FBrefFixture.match_date >= today,
        ).delete()

        upcoming_df = upcoming_df.drop_duplicates(subset=[c["date"], c["home"], c["away"]])
        added = 0

        for _, row in upcoming_df.iterrows():
            try:
                match_date = row[c["date"]].date()
                home_raw = str(row[c["home"]]).strip()
                away_raw = str(row[c["away"]]).strip()

                # Strip country codes added by FBref — but ONLY for international
                # competitions (UCL, UEL, UECL etc). Applying this to domestic
                # leagues destroys short team names like "Pau FC" → "FC".
                if league_code in INTL_LEAGUE_CODES:
                    home_raw = _strip_country_code(home_raw)
                    away_raw = _strip_country_code(away_raw)

                if not home_raw or not away_raw or home_raw.lower() == "nan" or away_raw.lower() == "nan":
                    continue

                home = resolve_and_learn(db, home_raw, league_code)
                away = resolve_and_learn(db, away_raw, league_code)

                # Guard: if resolution returned a bare generic suffix (e.g. "fc",
                # "sc", "ac") it means the name was mangled. Skip this fixture.
                if _is_generic_suffix(home) or _is_generic_suffix(away):
                    print(f"  [fixtures] Skipped mangled name: '{home_raw}'→'{home}' / '{away_raw}'→'{away}'")
                    continue

                mtime_raw = row[c["time"]] if c["time"] else None
                mtime = (
                    str(mtime_raw).strip()
                    if mtime_raw is not None
                    and pd.notnull(mtime_raw)
                    and str(mtime_raw).strip().lower() not in ("nan", "nat", "")
                    else None
                )
                raw_round = str(row[c["round_raw"]]).strip() if c["round_raw"] and pd.notnull(row.get(c["round_raw"])) else None
                round_type = _classify_round_type(raw_round, match_date, league_code)

                fixture = FBrefFixture(
                    league_code=league_code,
                    home_team=home,
                    away_team=away,
                    match_date=match_date,
                    match_time=mtime,
                    round_type=round_type,
                    scraped_at=datetime.utcnow(),
                )
                db.add(fixture)
                added += 1
            except Exception as e:
                print(f"  Row error: {e}")

        db.commit()
        print(f"  Added {added} upcoming fixtures for {league_code}")
    except Exception as e:
        print(f"  Upsert fixtures error: {e}")
    finally:
        db.close()


# ---------------------------------------------------------------------------
# Main per‑league scraping function
# ---------------------------------------------------------------------------
def scrape_league(league_code: str, url: str) -> None:
    """
    Scrape one league: fetch schedule page, process scores + fixtures,
    then fetch the dedicated stats page for standings.
    """
    print(f"\n{'='*60}")
    print(f"[fixtures] {league_code}")

    # ── 1. Schedule page (scores + upcoming fixtures) ────────────────
    html = _fetch_page(url, league_code, bust_cache=True)
    if not html:
        return

    # Quick title check — if ScraperAPI returns a cached page for this
    # league URL, skip entirely rather than writing wrong fixtures/standings.
    if not _verify_page_league(html, league_code):
        print(f"  [fixtures] Schedule page content does not match {league_code} "
              f"— skipping (likely cached proxy response).")
        return

    schedule_df, schedule_standings_df = _parse_page(html, league_code)

    # ── 2. Standings — prefer dedicated stats page, fall back to any
    #       standings found in the schedule page HTML ─────────────────
    if league_code not in INTL_LEAGUE_CODES:
        standings_updated = scrape_league_standings(league_code, schedule_url=url)
        if standings_updated == 0 and schedule_standings_df is not None:
            # Rare fallback: standings happened to be embedded in schedule page.
            # Run the same sanity check as scrape_league_standings to reject
            # misdirected PL tables before writing them.
            print(f"  [standings] Falling back to embedded standings from schedule page")
            db = SessionLocal()
            try:
                standings_flat = _flatten_columns(schedule_standings_df)
                cols_lower = {str(c).lower().strip(): c for c in standings_flat.columns}
                team_col_check = cols_lower.get("squad") or cols_lower.get("team") or cols_lower.get("club")
                sane = False
                if team_col_check:
                    sample_names = [
                        str(r).strip() for r in standings_flat[team_col_check].dropna().head(5)
                        if str(r).strip().lower() not in ("nan", "", "squad", "team")
                    ]
                    matched = sum(
                        1 for raw in sample_names
                        if db.query(Team).filter_by(
                            team_key=resolve_and_learn(db, raw, league_code),
                            league_code=league_code
                        ).first()
                    )
                    min_match = max(3, (len(sample_names) * 2) // 3)  # at least 2/3 must match
                    if matched >= min_match or len(sample_names) < 3:
                        sane = True
                    else:
                        print(f"  [standings] REJECTED embedded: {matched}/{len(sample_names)} "
                              f"sampled teams match {league_code}. Sample: {sample_names[:3]}")
                if sane:
                    _process_standings(db, league_code, schedule_standings_df)
            finally:
                db.close()
    else:
        print(f"  [standings] Skipping standings for international competition {league_code}")

    # ── 3. Process schedule ───────────────────────────────────────────
    if schedule_df is None or schedule_df.empty:
        print("  No schedule data parsed.")
        return

    c = _get_columns(schedule_df)
    if not all([c["date"], c["home"], c["away"]]):
        print(f"  Missing required columns. Found: {list(schedule_df.columns[:10])}")
        return

    schedule_df[c["date"]] = pd.to_datetime(schedule_df[c["date"]], errors="coerce")
    schedule_df = schedule_df.dropna(subset=[c["date"]])

    today = date.today()
    cutoff = today + timedelta(days=FIXTURE_DAYS)
    score_col = c["score"]

    if score_col:
        has_score = schedule_df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)
        completed_df = schedule_df[has_score].copy()
        upcoming_df = schedule_df[
            ~has_score
            & (schedule_df[c["date"]].dt.date >= today)
            & (schedule_df[c["date"]].dt.date <= cutoff)
        ].copy()
    else:
        completed_df = pd.DataFrame()
        upcoming_df = schedule_df[
            (schedule_df[c["date"]].dt.date >= today)
            & (schedule_df[c["date"]].dt.date <= cutoff)
        ].copy()

    print(f"  Completed rows: {len(completed_df)}")
    print(f"  Upcoming rows (next {FIXTURE_DAYS} days): {len(upcoming_df)}")

    if not completed_df.empty:
        _update_snapshot(league_code, completed_df)

    if not upcoming_df.empty:
        _upsert_fixtures(league_code, upcoming_df, c)

    # Sync any newly resolved teams back to teams.json
    if _sync_league_teams:
        _db = SessionLocal()
        try:
            _sync_league_teams(_db, league_code)
        except Exception as _e:
            print(f"  [teams_sync] Warning: {_e}")
        finally:
            _db.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape FBref fixtures, scores, and standings")
    parser.add_argument("--league", type=str, default=None, help="Single league code")
    parser.add_argument("--standings-only", action="store_true",
                        help="Only refresh standings, skip schedule/fixtures scrape")
    parser.add_argument("--headless", action="store_true", help="Run Chrome headless")
    parser.add_argument("--api", type=str, default=None, metavar="KEY", help="ScraperAPI key")
    args = parser.parse_args()

    if args.headless:
        HEADLESS = True
        print("[fixtures] Running in headless mode")
    if args.api:
        SCRAPER_API_KEY = args.api
        print("[fixtures] Using ScraperAPI")

    if args.league:
        if args.league not in LEAGUE_MAP:
            print(f"Unknown league: {args.league}")
            print(f"Available: {list(LEAGUE_MAP.keys())}")
        else:
            entry = LEAGUE_MAP[args.league]
            url = entry[0] if isinstance(entry, tuple) else entry
            if args.standings_only:
                scrape_league_standings(args.league, schedule_url=url)
            else:
                scrape_league(args.league, url)
    else:
        print(f"[fixtures] Starting scrape for {len(LEAGUE_MAP)} leagues")
        codes = list(LEAGUE_MAP.keys())
        for i, (code, entry) in enumerate(LEAGUE_MAP.items()):
            url = entry[0] if isinstance(entry, tuple) else entry
            if args.standings_only:
                print(f"\n{'='*60}")
                print(f"[standings] {code}")
                scrape_league_standings(code, schedule_url=url)
            else:
                scrape_league(code, url)
            if i < len(codes) - 1:
                print(f"\n  Waiting {SLEEP_BETWEEN_LEAGUES}s...")
                time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[fixtures] Done.")

    # ── Auto-commit teams.json if changed ────────────────────────────
    # teams_sync writes new teams and aliases discovered during the scrape.
    # Commit them so they survive the next deploy without re-learning.
    try:
        import subprocess
        teams_file = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            "..", "app", "seed", "teams.json"
        )
        # Check if teams.json has uncommitted changes
        status = subprocess.run(
            ["git", "status", "--porcelain", teams_file],
            capture_output=True, text=True
        )
        if status.stdout.strip():
            subprocess.run(["git", "add", teams_file], check=True)
            subprocess.run(
                ["git", "commit", "-m",
                 f"[auto] teams.json updated by scrape_fixtures ({date.today()})"],
                check=True
            )
            print("[fixtures] teams.json committed to git.")
        else:
            print("[fixtures] teams.json unchanged — no commit needed.")
    except Exception as _git_err:
        print(f"[fixtures] Warning: could not auto-commit teams.json: {_git_err}")
