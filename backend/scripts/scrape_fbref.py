"""
backend/scripts/scrape_fbref.py

Scrapes FBref using SeleniumBase UC mode to bypass Cloudflare.
Fetches CURRENT + PREVIOUS season for each league, merges and deduplicates.
Runs locally only — never on Render.

Usage:
    cd backend
    venv312\\Scripts\\activate
    python -m scripts.scrape_fbref

NOTE: Chrome will open and close for each fetch.
      Do not click anything while scraping is in progress.
"""

import io
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import requests
from seleniumbase import Driver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

SLEEP_BETWEEN_FETCHES = 4   # seconds between each browser fetch
SLEEP_BETWEEN_LEAGUES = 6   # seconds between leagues

# Set to True via --headless flag (used in CI / GitHub Actions)
HEADLESS = False

# Set via --api flag or SCRAPER_API_KEY env var (used in CI to bypass Cloudflare)
SCRAPER_API_KEY: str | None = os.environ.get("SCRAPER_API_KEY")

# ── League map ─────────────────────────────────────────────────────────────────
# Each entry: (current_url, prev_url)
# European leagues run Aug–May  → 2025-2026 current, 2024-2025 previous
# Calendar-year leagues         → 2026 current, 2025 previous
# ──────────────────────────────────────────────────────────────────────────────
LEAGUE_MAP = {
    "ENG-PL": (
        "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures",
    ),
    "ENG-CH": (
        "https://fbref.com/en/comps/10/schedule/Championship-Scores-and-Fixtures",
        "https://fbref.com/en/comps/10/2024-2025/schedule/2024-2025-Championship-Scores-and-Fixtures",
    ),
    "ESP-LL": (
        "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/12/2024-2025/schedule/2024-2025-La-Liga-Scores-and-Fixtures",
    ),
    "FRA-L1": (
        "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
        "https://fbref.com/en/comps/13/2024-2025/schedule/2024-2025-Ligue-1-Scores-and-Fixtures",
    ),
    "GER-BUN": (
        "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/20/2024-2025/schedule/2024-2025-Bundesliga-Scores-and-Fixtures",
    ),
    "ITA-SA": (
        "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/11/2024-2025/schedule/2024-2025-Serie-A-Scores-and-Fixtures",
    ),
    "NED-ERE": (
        "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
        "https://fbref.com/en/comps/23/2024-2025/schedule/2024-2025-Eredivisie-Scores-and-Fixtures",
    ),
    "TUR-SL": (
        "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
        "https://fbref.com/en/comps/26/2024-2025/schedule/2024-2025-Super-Lig-Scores-and-Fixtures",
    ),
    "BRA-SA": (
        "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/24/2025/schedule/2025-Serie-A-Scores-and-Fixtures",
    ),
    "MLS": (
        "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
        "https://fbref.com/en/comps/22/2025/schedule/2025-Major-League-Soccer-Scores-and-Fixtures",
    ),
    "SAU-SPL": (
        "https://fbref.com/en/comps/70/schedule/Saudi-Pro-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/70/2024-2025/schedule/2024-2025-Saudi-Pro-League-Scores-and-Fixtures",
    ),
    "DEN-SL": (
        "https://fbref.com/en/comps/50/schedule/Danish-Superliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/50/2024-2025/schedule/2024-2025-Danish-Superliga-Scores-and-Fixtures",
    ),
    "ESP-LL2": (
        "https://fbref.com/en/comps/17/schedule/Segunda-Division-Scores-and-Fixtures",
        "https://fbref.com/en/comps/17/2024-2025/schedule/2024-2025-Segunda-Division-Scores-and-Fixtures",
    ),
    "BEL-PL": (
        "https://fbref.com/en/comps/37/schedule/Belgian-Pro-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/37/2024-2025/schedule/2024-2025-Belgian-Pro-League-Scores-and-Fixtures",
    ),
    "NOR-EL": (
        "https://fbref.com/en/comps/28/schedule/Eliteserien-Scores-and-Fixtures",
        "https://fbref.com/en/comps/28/2025/schedule/2025-Eliteserien-Scores-and-Fixtures",
    ),
    "SWE-AL": (
        "https://fbref.com/en/comps/29/schedule/Allsvenskan-Scores-and-Fixtures",
        "https://fbref.com/en/comps/29/2025/schedule/2025-Allsvenskan-Scores-and-Fixtures",
    ),
    "MEX-LMX": (
        "https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures",
        "https://fbref.com/en/comps/31/2024-2025/schedule/2024-2025-Liga-MX-Scores-and-Fixtures",
    ),
    # ── New leagues ───────────────────────────────────────────────────────────
    # Calendar-year leagues (current = 2026, prev = 2025)
    "CHN-CSL": (
        "https://fbref.com/en/comps/62/schedule/Chinese-Super-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/62/2025/schedule/2025-Chinese-Super-League-Scores-and-Fixtures",
    ),
    "JPN-J1": (
        "https://fbref.com/en/comps/25/schedule/J1-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/25/2025/schedule/2025-J1-League-Scores-and-Fixtures",
    ),
    "COL-PA": (
        "https://fbref.com/en/comps/41/schedule/Primera-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/41/2025/schedule/2025-Primera-A-Scores-and-Fixtures",
    ),
    "BRA-SB": (
        "https://fbref.com/en/comps/38/schedule/Serie-B-Scores-and-Fixtures",
        "https://fbref.com/en/comps/38/2025/schedule/2025-Serie-B-Scores-and-Fixtures",
    ),
    # Aug–May season leagues (current = 2025-2026, prev = 2024-2025)
    "ITA-SB": (
        "https://fbref.com/en/comps/18/schedule/Serie-B-Scores-and-Fixtures",
        "https://fbref.com/en/comps/18/2024-2025/schedule/2024-2025-Serie-B-Scores-and-Fixtures",
    ),
    "FRA-L2": (
        "https://fbref.com/en/comps/60/schedule/Ligue-2-Scores-and-Fixtures",
        "https://fbref.com/en/comps/60/2024-2025/schedule/2024-2025-Ligue-2-Scores-and-Fixtures",
    ),
    "GER-B2": (
        "https://fbref.com/en/comps/33/schedule/2-Bundesliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/33/2024-2025/schedule/2024-2025-2-Bundesliga-Scores-and-Fixtures",
    ),
    "POL-EK": (
        "https://fbref.com/en/comps/36/schedule/Ekstraklasa-Scores-and-Fixtures",
        "https://fbref.com/en/comps/36/2024-2025/schedule/2024-2025-Ekstraklasa-Scores-and-Fixtures",
    ),
    "AUT-BL": (
        "https://fbref.com/en/comps/56/schedule/Austrian-Football-Bundesliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/56/2024-2025/schedule/2024-2025-Austrian-Football-Bundesliga-Scores-and-Fixtures",
    ),
    "SUI-SL": (
        "https://fbref.com/en/comps/57/schedule/Swiss-Super-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/57/2024-2025/schedule/2024-2025-Swiss-Super-League-Scores-and-Fixtures",
    ),
    "CHI-LP": (
        "https://fbref.com/en/comps/35/schedule/Primera-Division-Scores-and-Fixtures",
        "https://fbref.com/en/comps/35/2024-2025/schedule/2024-2025-Primera-Division-Scores-and-Fixtures",
    ),
    "PER-L1": (
        "https://fbref.com/en/comps/44/schedule/Liga-1-Scores-and-Fixtures",
        "https://fbref.com/en/comps/44/2024-2025/schedule/2024-2025-Liga-1-Scores-and-Fixtures",
    ),
    "POR-LP": (
        "https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/32/2024-2025/schedule/2024-2025-Primeira-Liga-Scores-and-Fixtures",
    ),
    # Cuba (CUB-PD) is NOT on FBref — scraping skipped, add manually if needed
    # ── UEFA club competitions ────────────────────────────────────────────────
    "UCL": (
        "https://fbref.com/en/comps/8/schedule/Champions-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/8/2024-2025/schedule/2024-2025-Champions-League-Scores-and-Fixtures",
    ),
    "UEL": (
        "https://fbref.com/en/comps/19/schedule/Europa-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/19/2024-2025/schedule/2024-2025-Europa-League-Scores-and-Fixtures",
    ),
    "UECL": (
        "https://fbref.com/en/comps/882/schedule/Conference-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/882/2024-2025/schedule/2024-2025-Conference-League-Scores-and-Fixtures",
    ),
}


# ── HTML fetch helpers ────────────────────────────────────────────────────────
def _get_html(url: str, label: str) -> str | None:
    """Route to ScraperAPI (CI) or Selenium (local)."""
    if SCRAPER_API_KEY:
        return _fetch_via_scraperapi(url, label)
    return _fetch_via_selenium(url, label)


def _fetch_via_scraperapi(url: str, label: str) -> str | None:
    """Fetch using ScraperAPI — bypasses Cloudflare in CI."""
    print(f"  [ScraperAPI] {label}")
    try:
        resp = requests.get(
            "http://api.scraperapi.com",
            params={
                "api_key": SCRAPER_API_KEY,
                "url": url,
                "render": "true",
                "premium": "true",
            },
            timeout=60,
        )
        if resp.status_code != 200:
            print(f"  ScraperAPI error: HTTP {resp.status_code}")
            return None
        html = resp.text
        print(f"  Page loaded ({len(html)} bytes)")
        return html
    except Exception as e:
        print(f"  ScraperAPI error: {e}")
        return None


def _fetch_via_selenium(url: str, label: str) -> str | None:
    """Fetch using local Selenium + Chrome."""
    driver = None
    try:
        driver = Driver(uc=True, headless2=HEADLESS)
        driver.uc_open_with_reconnect(url, 4)
        if not HEADLESS:
            driver.uc_gui_click_captcha()
        time.sleep(3)
        html = driver.get_page_source()
        print(f"  Page loaded ({len(html)} bytes)")
        return html
    except Exception as e:
        print(f"  Browser error ({label}): {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ── Single URL fetch ──────────────────────────────────────────────────────────
def _fetch_url(url: str, label: str) -> pd.DataFrame | None:
    """Fetch one FBref fixtures page, return DataFrame or None."""
    print(f"  Fetching [{label}]: {url}")
    html = _get_html(url, label)
    if not html:
        return None

    if "Just a moment" in html or len(html) < 5000:
        print(f"  Still Cloudflare blocked [{label}].")
        return None

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as e:
        print(f"  Could not parse tables [{label}]: {e}")
        return None

    if not tables:
        print(f"  No tables found [{label}].")
        return None

    # ── International comps (UCL/UEL/UECL): merge all schedule tables ────────
    # FBref splits these into one table per round — "largest table" misses most.
    schedule_tables = []
    for t in tables:
        if isinstance(t.columns, pd.MultiIndex):
            t.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in t.columns
            ]
        cols_lower = [str(c).lower() for c in t.columns]
        if "home" in cols_lower and "away" in cols_lower:
            schedule_tables.append(t)

    if len(schedule_tables) > 1:
        df = pd.concat(schedule_tables, ignore_index=True)
        df = df.dropna(how="all")
        print(f"  [{label}] merged {len(schedule_tables)} schedule tables → {len(df)} rows")
    else:
        df = max(tables, key=len)
        df = df.dropna(how="all")

    # Flatten any remaining MultiIndex columns
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(v) for v in col if str(v) != "nan").strip()
            for col in df.columns
        ]

    # Keep only completed matches (score column contains digits with dash/en-dash)
    score_col = next(
        (c for c in df.columns if str(c).lower() in ("score", "scores")), None
    )
    if score_col:
        df = df[df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)]

    print(f"  [{label}] completed rows: {len(df)}")

    # Strip leading/trailing 2-3 letter country codes from team names on
    # international competition pages (e.g. "eng Liverpool", "es Barcelona")
    for col_name in df.columns:
        if str(col_name).lower() in ("home", "away"):
            df[col_name] = (
                df[col_name].astype(str)
                .str.strip()
                .str.replace(r'(?i)^[a-z]{2,3}\s+', '', regex=True)
                .str.replace(r'(?i)\s+[a-z]{2,3}$', '', regex=True)
                .str.strip()
            )

    return df if not df.empty else None


# ── Date normalizer ───────────────────────────────────────────────────────────
def _normalize_dates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalize the date column to a clean, timezone-naive datetime64[ns].

    FBref can return dates in multiple formats depending on the league/season:
      - "2025-09-14"  (ISO)
      - "14/09/2025"  (DD/MM/YYYY)
      - "September 14, 2025"
      - "2025-09-14 00:00:00"  (with time component)
      - NaT / empty strings

    Strategy:
      1. Locate the date column (case-insensitive "date")
      2. Parse with pd.to_datetime(dayfirst=True) which handles most formats
      3. Strip any timezone info (tz_localize → tz-naive)
      4. Normalize to midnight (date only — no time component)
      5. Replace unparseable values with NaT (handled downstream by dropna)

    Result: every date stored as YYYY-MM-DD 00:00:00 (pandas datetime64[ns]).
    Display as DD/MM/YYYY is handled at output time, not at storage time.
    """
    col_map  = {c.lower(): c for c in df.columns}
    date_col = col_map.get("date")
    if not date_col:
        return df  # no date column found — pass through unchanged

    raw = df[date_col].astype(str).str.strip()

    parsed = pd.to_datetime(raw, format="%Y-%m-%d", errors="coerce")

    # For leagues where FBref returns non-ISO formats (DD/MM/YYYY etc.),
    # re-parse any NaT values with dayfirst=True as fallback
    mask = parsed.isna() & raw.notna() & (raw != "") & (raw != "nan")
    if mask.any():
        fallback = pd.to_datetime(raw[mask], dayfirst=True, errors="coerce")
        parsed = parsed.copy()
        parsed[mask] = fallback

    # Strip timezone if present
    if hasattr(parsed.dt, "tz") and parsed.dt.tz is not None:
        parsed = parsed.dt.tz_localize(None)

    # Normalize to midnight — removes any sub-day precision that causes
    # duplicate rows to appear distinct (e.g. 00:00:00 vs 12:00:00)
    parsed = parsed.dt.normalize()

    df = df.copy()
    df[date_col] = parsed

    unparseable = parsed.isna().sum()
    if unparseable:
        print(f"  [date_normalize] {unparseable} unparseable date(s) set to NaT — will be dropped.")

    return df


# ── Merge two season DataFrames ───────────────────────────────────────────────
def _merge_seasons(current: pd.DataFrame | None, previous: pd.DataFrame | None) -> pd.DataFrame | None:
    """
    Concatenate current + previous season data.
    Deduplicates on (Date, Home, Away) to avoid double-counting.
    Sorts by Date ascending.
    """
    frames = [f for f in [current, previous] if f is not None and not f.empty]
    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)

    # Find date/home/away columns for dedup
    col_map = {c.lower(): c for c in combined.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home")
    away_col = col_map.get("away")

    if date_col and home_col and away_col:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[date_col, home_col, away_col])
        dupes = before - len(combined)
        if dupes:
            print(f"  Removed {dupes} duplicate rows")

        # Sort chronologically (oldest first — good for asof_features)
        combined[date_col] = pd.to_datetime(combined[date_col], errors="coerce")
        combined = combined.sort_values(date_col, ascending=True).reset_index(drop=True)

        # Strip any matches dated in the future — these are scheduled fixtures
        # that slipped through the score filter (e.g. Liga MX full-season pages
        # include upcoming matches). Only keep matches up to and including today.
        today = pd.Timestamp.now().normalize()
        before_cutoff = len(combined)
        combined = combined[combined[date_col] <= today]
        future_removed = before_cutoff - len(combined)
        if future_removed:
            print(f"  Removed {future_removed} future-dated row(s) (after {today.date()})")

    print(f"  Combined total: {len(combined)} rows")
    return combined


# ── Save to DB ────────────────────────────────────────────────────────────────
def _save_snapshot(league_code: str, df: pd.DataFrame) -> None:
    try:
        parquet_bytes = df.to_parquet(index=True)
        db = SessionLocal()
        try:
            snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
            if snap:
                snap.data       = parquet_bytes
                snap.fetched_at = datetime.utcnow()
                action = "Updated"
            else:
                db.add(FBrefSnapshot(
                    league_code=league_code,
                    data=parquet_bytes,
                    fetched_at=datetime.utcnow(),
                ))
                action = "Created"
            db.commit()
            print(f"  DB {action} — {len(df)} rows saved")
        finally:
            db.close()
    except Exception as e:
        print(f"  Database error: {e}")


# ── Main per-league function ──────────────────────────────────────────────────
def scrape_league(league_code: str, current_url: str, prev_url: str) -> None:
    print(f"\n{'='*60}")
    print(f"[scraper] {league_code}")

    # Current season
    current_df = _fetch_url(current_url, "current")
    if current_df is not None:
        current_df = _normalize_dates(current_df)
    time.sleep(SLEEP_BETWEEN_FETCHES)

    # Previous season
    prev_df = _fetch_url(prev_url, "previous")
    if prev_df is not None:
        prev_df = _normalize_dates(prev_df)

    # Merge
    merged = _merge_seasons(current_df, prev_df)
    if merged is None:
        print(f"  No data to save for {league_code}.")
        return

    _save_snapshot(league_code, merged)


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--league", type=str, default=None,
        help="Scrape a single league only (e.g. --league MEX-LMX)"
    )
    parser.add_argument(
        "--headless", action="store_true",
        help="Run Chrome in headless mode (for CI / GitHub Actions)"
    )
    parser.add_argument(
        "--api", type=str, default=None, metavar="KEY",
        help="Use ScraperAPI with this key instead of Selenium"
    )
    args = parser.parse_args()

    if args.headless:
        HEADLESS = True
        print("[scraper] Running in headless mode (no browser window)")

    if args.api:
        SCRAPER_API_KEY = args.api
        print("[scraper] Using ScraperAPI for fetching")

    if args.league:
        if args.league not in LEAGUE_MAP:
            print(f"[scraper] Unknown league: {args.league}. Available: {list(LEAGUE_MAP.keys())}")
        else:
            cur_url, prev_url = LEAGUE_MAP[args.league]
            scrape_league(args.league, cur_url, prev_url)
    else:
        print("[scraper] Starting two-season FBref scrape")
        print("[scraper] Chrome will open and close for each fetch.")
        print(f"[scraper] Leagues: {list(LEAGUE_MAP.keys())}")
        print("[scraper] Each league = 2 fetches (current + previous season)\n")

        codes = list(LEAGUE_MAP.keys())
        for i, (code, (cur_url, prev_url)) in enumerate(LEAGUE_MAP.items()):
            scrape_league(code, cur_url, prev_url)
            if i < len(codes) - 1:
                print(f"\n  Waiting {SLEEP_BETWEEN_LEAGUES}s before next league...")
                time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[scraper] Done.")
