"""
backend/scripts/scrape_fixtures.py

Scrapes FBref schedule pages for completed scores and upcoming fixtures.
Also extracts current league standings and updates teams table.
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
from sqlalchemy.orm import Session 

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import FBrefFixture
from app.models.team import Team
from app.util.team_resolver import resolve_and_learn
from app.core.constants import LEAGUE_MAP

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

def _parse_page(html: str, league_code: str = "") -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame]]:
    """
    Parse FBref page, returns (schedule_df, standings_df).
    Both may be None if not found.
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

    is_intl = league_code in ("UCL", "UEL", "UECL", "EC", "WC")

    # Helper to check if a table is a schedule (has date, home, away columns)
    def is_schedule_table(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        cols_lower = [str(c).lower() for c in df.columns]
        has_date = any('date' in c for c in cols_lower)
        has_home = any('home' in c for c in cols_lower)
        has_away = any('away' in c for c in cols_lower)
        return has_date and has_home and has_away

    # Helper to check if a table is a standings table
    def is_standings_table(df):
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in df.columns
            ]
        cols_lower = [str(c).lower() for c in df.columns]
        # Standings tables typically have 'rk', 'squad', 'pts', etc.
        has_rk = any(c in ['rk', 'rank'] for c in cols_lower)
        has_squad = any(c in ['squad', 'team'] for c in cols_lower)
        has_pts = any(c in ['pts', 'points'] for c in cols_lower)
        return has_rk and has_squad and has_pts

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

    schedule_df = None
    standings_df = None

    if not is_intl:
        # Find schedule table
        for df in tables:
            if is_schedule_table(df):
                schedule_df = df.dropna(how="all")
                print(f"  Found schedule table by column names with {len(schedule_df)} rows")
                break

        if schedule_df is None:
            # Second pass: look for tables containing date strings
            candidates = []
            for idx, df in enumerate(tables):
                if is_standings_table(df):
                    # This is standings, save it
                    standings_df = df.dropna(how="all")
                    print(f"  Found standings table with {len(standings_df)} rows")
                    continue
                if df.shape[1] >= 3 and contains_dates(df):
                    print(f"  Table {idx+1} contains dates – candidate schedule table")
                    candidates.append(df)

            if candidates:
                schedule_df = max(candidates, key=len)
                schedule_df = schedule_df.dropna(how="all")
                print(f"  Selected candidate schedule table with {len(schedule_df)} rows")

        if schedule_df is None:
            print("  No schedule table found – using largest non-standings table")
            non_standings = [df for df in tables if not is_standings_table(df)]
            if non_standings:
                schedule_df = max(non_standings, key=len)
                schedule_df = schedule_df.dropna(how="all")
                print(f"  Selected largest non-standings table with {len(schedule_df)} rows")
            else:
                schedule_df = max(tables, key=len)
                schedule_df = schedule_df.dropna(how="all")
                print(f"  Falling back to largest table with {len(schedule_df)} rows")

    else:
        # International competitions: merge all schedule tables
        schedule_tables = []
        for t in tables:
            if is_schedule_table(t):
                if isinstance(t.columns, pd.MultiIndex):
                    t.columns = [
                        " ".join(str(v) for v in col if str(v) != "nan").strip()
                        for col in t.columns
                    ]
                schedule_tables.append(t)

        if schedule_tables:
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

            schedule_df = pd.concat(merged_parts, ignore_index=True)
            print(f"  Merged {len(schedule_tables)} schedule tables → {len(schedule_df)} rows")
        else:
            schedule_df = max(tables, key=len)
            schedule_df = schedule_df.dropna(how="all")
            print(f"  No schedule tables – using largest table with {len(schedule_df)} rows")

    return schedule_df, standings_df

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
# Standings processing
# ---------------------------------------------------------------------------
def _process_standings(db: Session, league_code: str, standings_df: pd.DataFrame) -> None:
    """
    Parse standings table and update teams.current_position.
    """
    if standings_df is None or standings_df.empty:
        return

    # Flatten columns if MultiIndex
    if isinstance(standings_df.columns, pd.MultiIndex):
        standings_df.columns = [
            " ".join(str(v) for v in col if str(v) != "nan").strip()
            for col in standings_df.columns
        ]

    # Find the relevant columns
    cols_lower = {str(c).lower(): c for c in standings_df.columns}
    
    # Find position column (usually 'rk' or 'rank')
    pos_col = None
    for key in ['rk', 'rank', 'pos', 'position']:
        if key in cols_lower:
            pos_col = cols_lower[key]
            break
    
    # Find team name column
    team_col = None
    for key in ['squad', 'team', 'club']:
        if key in cols_lower:
            team_col = cols_lower[key]
            break
    
    if not pos_col or not team_col:
        print(f"  Could not find position/team columns in standings")
        return

    # Process each row
    updated = 0
    for _, row in standings_df.iterrows():
        try:
            position = int(row[pos_col])
            team_name_raw = str(row[team_col]).strip()
            
            if pd.isna(team_name_raw) or team_name_raw in ['', 'nan']:
                continue
            
            # Resolve team name to canonical key
            from app.services.resolve_team import resolve_team_name
            team_key = resolve_team_name(db, team_name_raw, league_code)
            
            # Find and update team
            team = db.query(Team).filter_by(team_key=team_key, league_code=league_code).first()
            if team:
                team.current_position = position
                updated += 1
            else:
                print(f"  Team not found: {team_name_raw} -> {team_key}")
        except Exception as e:
            print(f"  Error processing standings row: {e}")
            continue
    
    if updated:
        db.commit()
        print(f"  Updated positions for {updated} teams in {league_code}")

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

    schedule_df, standings_df = _parse_page(html, league_code)

    # Process standings if found
    if standings_df is not None and not standings_df.empty:
        db = SessionLocal()
        try:
            _process_standings(db, league_code, standings_df)
        finally:
            db.close()

    # Process schedule
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

    # Split into completed and upcoming
    score_col = c["score"]
    if score_col:
        has_score = schedule_df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)
        completed_df = schedule_df[has_score].copy()
        upcoming_df = schedule_df[
            ~has_score &
            (schedule_df[c["date"]].dt.date >= today) &
            (schedule_df[c["date"]].dt.date <= cutoff)
        ].copy()
    else:
        completed_df = pd.DataFrame()
        upcoming_df = schedule_df[
            (schedule_df[c["date"]].dt.date >= today) &
            (schedule_df[c["date"]].dt.date <= cutoff)
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
