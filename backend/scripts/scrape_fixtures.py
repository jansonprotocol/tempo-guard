"""
backend/scripts/scrape_fixtures.py

Scrapes FBref schedule pages for completed scores and upcoming fixtures.
Stores completed matches in FBrefSnapshot, upcoming matches in FBrefFixtures.
"""

import sys
import os
import re
import time
import io
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import Optional

# Ensure we can import from app
path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from dotenv import load_dotenv
load_dotenv(override=True)

import pandas as pd
import requests
from seleniumbase import Driver

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import FBrefFixture
from app.util.team_resolver import resolve_and_learn
from app.core.constants import LEAGUE_MAP  # <-- IMPORT FROM CONSTANTS

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
SLEEP_BETWEEN_FETCHES = 4      # seconds between each browser fetch
SLEEP_BETWEEN_LEAGUES = 6       # seconds between leagues
FIXTURE_DAYS = 5                 # how many days ahead to store fixtures

HEADLESS = False                 # set True via --headless
SCRAPER_API_KEY: Optional[str] = os.environ.get("SCRAPER_API_KEY")

# ---------------------------------------------------------------------------
# Fetch helpers (ScraperAPI or Selenium)
# ---------------------------------------------------------------------------
def _fetch_page(url: str, label: str) -> Optional[str]:
    """Route to ScraperAPI (if key present) or Selenium."""
    if SCRAPER_API_KEY:
        return _fetch_via_scraperapi(url, label)
    return _fetch_via_selenium(url, label)

def _fetch_via_scraperapi(url: str, label: str) -> Optional[str]:
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

def _parse_page(html: str, league_code: str = "") -> Optional[pd.DataFrame]:
    if "Just a moment" in html or len(html) < 5000:
        print("  Cloudflare blocked.")
        return None

    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as e:
        print(f"  Parse error: {e}")
        return None

    if not tables:
        return None

    is_intl = league_code in ("UCL", "UEL", "UECL", "EC", "WC")

    # Helper to detect schedule by column names
    def has_schedule_columns(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        cols_lower = [str(c).lower() for c in df.columns]
        return any('date' in c for c in cols_lower) and any('home' in c for c in cols_lower) and any('away' in c for c in cols_lower)

    # Helper to detect schedule by looking at row values (date pattern)
    def contains_dates(df, sample_rows=5):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        sample = df.head(sample_rows).astype(str)
        for _, row in sample.iterrows():
            for val in row:
                if re.search(r'\d{4}-\d{2}-\d{2}', val) or re.search(r'\d{2}/\d{2}/\d{4}', val):
                    return True
        return False

    if not is_intl:
        # First pass: look for table with date/home/away columns
        for df in tables:
            if has_schedule_columns(df):
                df = df.dropna(how="all")
                print(f"  Found schedule table by column names with {len(df)} rows")
                return df

        # Second pass: look for tables containing date strings
        candidates = []
        for idx, df in enumerate(tables):
            if df.shape[1] >= 3 and contains_dates(df):
                print(f"  Table {idx+1} contains dates – candidate schedule table")
                candidates.append(df)

        if candidates:
            best = max(candidates, key=len)
            best = best.dropna(how="all")
            print(f"  Selected candidate schedule table with {len(best)} rows")
            return best

        print("  No schedule table found – using largest table")
        df = max(tables, key=len)
        df = df.dropna(how="all")
        if not contains_dates(df):
            print(f"  ⚠️ Largest table does not appear to contain dates. Columns: {list(df.columns[:10])}")
        return df

    # International competitions: merge all schedule tables and tag with round info
    schedule_tables = []
    for t in tables:
        if has_schedule_columns(t) or contains_dates(t):
            if isinstance(t.columns, pd.MultiIndex):
                t.columns = [
                    " ".join(str(v) for v in col if str(v) != "nan").strip()
                    for col in t.columns
                ]
            schedule_tables.append(t)

    if not schedule_tables:
        print("  No schedule tables found – falling back to largest table.")
        df = max(tables, key=len)
        df = df.dropna(how="all")
        return df

    merged_parts = []
    for t in schedule_tables:
        t = t.dropna(how="all").copy()
        if isinstance(t.columns, pd.MultiIndex):
            t.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in t.columns
            ]
        cols_lower_map = {str(c).lower(): c for c in t.columns}

        round_col = cols_lower_map.get("round") or cols_lower_map.get("wk")
        if round_col:
            vals = t[round_col].dropna().astype(str)
            vals = vals[~vals.str.lower().isin(["nan", "", "round", "wk"])]
            label = vals.mode()[0] if not vals.empty else None
        else:
            label = None

        t["_round_raw"] = label
        merged_parts.append(t)

    df = pd.concat(merged_parts, ignore_index=True)
    print(f"  Merged {len(schedule_tables)} schedule tables → {len(df)} rows")
    return df

def _get_columns(df: pd.DataFrame) -> dict:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(v) for v in col if str(v) != "nan").strip()
            for col in df.columns
        ]
    cols = {str(c).lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    return {
        "date":       col("date"),
        "home":       col("home"),
        "away":       col("away"),
        "score":      col("score", "scores"),
        "time":       col("time"),
        "round_raw":  col("_round_raw"),
    }

def _safe_to_parquet(df: pd.DataFrame) -> bytes:
    """Coerce object columns to string before serialising to parquet."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df.to_parquet(index=True)

def _classify_round_type(raw: Optional[str], match_date: date, league_code: str) -> Optional[str]:
    """Normalise round label for UEFA competitions."""
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
# Database operations
# ---------------------------------------------------------------------------
def _update_snapshot(league_code: str, completed_df: pd.DataFrame) -> None:
    """Merge new completed rows into the FBrefSnapshot for this league."""
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
    """Write upcoming fixtures to FBrefFixtures, replacing old future fixtures."""
    db = SessionLocal()
    try:
        today = date.today()
        # Delete stale upcoming fixtures for this league
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

                # Strip country codes that FBref adds for international comps
                home_raw = re.sub(r'(?i)^[a-z]{2,3}\s+', '', home_raw).strip()
                home_raw = re.sub(r'(?i)\s+[a-z]{2,3}$', '', home_raw).strip()
                away_raw = re.sub(r'(?i)^[a-z]{2,3}\s+', '', away_raw).strip()
                away_raw = re.sub(r'(?i)\s+[a-z]{2,3}$', '', away_raw).strip()

                if not home_raw or not away_raw or home_raw == "nan" or away_raw == "nan":
                    continue

                # Resolve team names using autopilot (creates aliases if needed)
                home = resolve_and_learn(db, home_raw, league_code)
                away = resolve_and_learn(db, away_raw, league_code)

                mtime = str(row[c["time"]]).strip() if c["time"] and pd.notnull(row.get(c["time"])) else None

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
    print(f"\n{'='*60}")
    print(f"[fixtures] {league_code}")

    html = _fetch_page(url, league_code)
    if not html:
        return

    df = _parse_page(html, league_code)
    if df is None or df.empty:
        print("  No data parsed.")
        return

    c = _get_columns(df)
    if not all([c["date"], c["home"], c["away"]]):
        print(f"  Missing required columns. Found: {list(df.columns[:10])}")
        return

    df[c["date"]] = pd.to_datetime(df[c["date"]], errors="coerce")
    df = df.dropna(subset=[c["date"]])

    today = date.today()
    cutoff = today + timedelta(days=FIXTURE_DAYS)

    # Split into completed and upcoming
    score_col = c["score"]
    if score_col:
        has_score = df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)
        completed_df = df[has_score].copy()
        upcoming_df = df[
            ~has_score &
            (df[c["date"]].dt.date >= today) &
            (df[c["date"]].dt.date <= cutoff)
        ].copy()
    else:
        completed_df = pd.DataFrame()
        upcoming_df = df[
            (df[c["date"]].dt.date >= today) &
            (df[c["date"]].dt.date <= cutoff)
        ].copy()

    print(f"  Completed rows: {len(completed_df)}")
    print(f"  Upcoming rows (next {FIXTURE_DAYS} days): {len(upcoming_df)}")

    if not completed_df.empty:
        _update_snapshot(league_code, completed_df)

    if not upcoming_df.empty:
        _upsert_fixtures(league_code, upcoming_df, c)

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape FBref fixtures and scores")
    parser.add_argument("--league", type=str, default=None, help="Single league code")
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
            scrape_league(args.league, LEAGUE_MAP[args.league])
    else:
        print(f"[fixtures] Starting daily scrape for {len(LEAGUE_MAP)} leagues")
        codes = list(LEAGUE_MAP.keys())
        for i, (code, url) in enumerate(LEAGUE_MAP.items()):
            scrape_league(code, url)
            if i < len(codes) - 1:
                print(f"\n  Waiting {SLEEP_BETWEEN_LEAGUES}s...")
                time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[fixtures] Done.")
