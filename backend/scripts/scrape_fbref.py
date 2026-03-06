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
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from seleniumbase import Driver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

SLEEP_BETWEEN_FETCHES = 4   # seconds between each browser fetch
SLEEP_BETWEEN_LEAGUES = 6   # seconds between leagues

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
}


# ── Single URL fetch ──────────────────────────────────────────────────────────
def _fetch_url(url: str, label: str) -> pd.DataFrame | None:
    """Fetch one FBref fixtures page, return DataFrame or None."""
    print(f"  Fetching [{label}]: {url}")
    driver = None
    try:
        driver = Driver(uc=True, headless=False)
        driver.uc_open_with_reconnect(url, 4)
        driver.uc_gui_click_captcha()
        time.sleep(3)
        html = driver.get_page_source()
    except Exception as e:
        print(f"  Browser error ({label}): {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

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

    df = max(tables, key=len)
    df = df.dropna(how="all")

    # Keep only completed matches (score column contains digits with dash/en-dash)
    score_col = next(
        (c for c in df.columns if str(c).lower() in ("score", "scores")), None
    )
    if score_col:
        df = df[df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)]

    print(f"  [{label}] completed rows: {len(df)}")
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
