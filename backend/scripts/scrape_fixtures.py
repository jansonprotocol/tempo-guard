"""
backend/scripts/scrape_fixtures.py

Lightweight DAILY scraper — much faster than the weekly full scrape.

What it does per league (single browser fetch):
  1. Fetches current season fixtures page
  2. Completed rows   → updates FBrefSnapshot with latest scores
  3. Upcoming rows    → writes to FBrefFixtures table (next FIXTURE_DAYS days)

Run time: ~6-8 minutes for all 16 leagues (vs 15-20 for full scraper)

Usage:
    cd backend
    venv312\\Scripts\\activate
    python -m scripts.scrape_fixtures

NOTE: Chrome opens once per league. Do not click anything.
"""
from __future__ import annotations

import io
import os
import re
import sys
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import requests
from seleniumbase import Driver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import FBrefFixture, Base
from app.database.db import engine

# Auto-create new tables if they don't exist yet
Base.metadata.create_all(bind=engine)

FIXTURE_DAYS        = 5     # how many days ahead to store fixtures
SLEEP_BETWEEN       = 4     # seconds between leagues

# Set to True via --headless flag (used in CI / GitHub Actions)
HEADLESS = False

# Set via --api flag or SCRAPER_API_KEY env var (used in CI to bypass Cloudflare)
SCRAPER_API_KEY: str | None = os.environ.get("SCRAPER_API_KEY")

# ── Current season URLs only ───────────────────────────────────────────────────
LEAGUE_MAP = {
    "ENG-PL":  "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ESP-LL":  "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "FRA-L1":  "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
    "GER-BUN": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "ITA-SA":  "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "NED-ERE": "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
    "TUR-SL":  "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
    "BRA-SA":  "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
    "MLS":     "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
    "SAU-SPL": "https://fbref.com/en/comps/70/schedule/Saudi-Pro-League-Scores-and-Fixtures",
    "DEN-SL":  "https://fbref.com/en/comps/50/schedule/Danish-Superliga-Scores-and-Fixtures",
    "ESP-LL2": "https://fbref.com/en/comps/17/schedule/Segunda-Division-Scores-and-Fixtures",
    "BEL-PL":  "https://fbref.com/en/comps/37/schedule/Belgian-Pro-League-Scores-and-Fixtures",
    "NOR-EL":  "https://fbref.com/en/comps/28/schedule/Eliteserien-Scores-and-Fixtures",
    "SWE-AL":  "https://fbref.com/en/comps/29/schedule/Allsvenskan-Scores-and-Fixtures",
    "MEX-LMX": "https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures",
    # ── Expanded leagues ─────────────────────────────────────────────────────
    "CHN-CSL": "https://fbref.com/en/comps/62/schedule/Chinese-Super-League-Scores-and-Fixtures",
    "JPN-J1":  "https://fbref.com/en/comps/25/schedule/J1-League-Scores-and-Fixtures",
    "COL-PA":  "https://fbref.com/en/comps/41/schedule/Primera-A-Scores-and-Fixtures",
    "BRA-SB":  "https://fbref.com/en/comps/38/schedule/Serie-B-Scores-and-Fixtures",
    "ITA-SB":  "https://fbref.com/en/comps/18/schedule/Serie-B-Scores-and-Fixtures",
    "FRA-L2":  "https://fbref.com/en/comps/60/schedule/Ligue-2-Scores-and-Fixtures",
    "GER-B2":  "https://fbref.com/en/comps/33/schedule/2-Bundesliga-Scores-and-Fixtures",
    "POL-EK":  "https://fbref.com/en/comps/36/schedule/Ekstraklasa-Scores-and-Fixtures",
    "AUT-BL":  "https://fbref.com/en/comps/56/schedule/Austrian-Football-Bundesliga-Scores-and-Fixtures",
    "SUI-SL":  "https://fbref.com/en/comps/57/schedule/Swiss-Super-League-Scores-and-Fixtures",
    "CHI-LP":  "https://fbref.com/en/comps/35/schedule/Primera-Division-Scores-and-Fixtures",
    "PER-L1":  "https://fbref.com/en/comps/44/schedule/Liga-1-Scores-and-Fixtures",
    "POR-LP":  "https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures",
    # Cuba (CUB-PD) not on FBref — scraping skipped
    # ── UEFA club competitions ────────────────────────────────────────────────
    # NOTE: FBref stores completed match scores AND upcoming fixtures on these
    # pages. Team names here are short international forms (e.g. "Man City",
    # "Atlético Madrid") — fbref_base.py's fuzzy matcher handles the mapping
    # to domestic snapshot names automatically.
    "UCL":  "https://fbref.com/en/comps/8/schedule/Champions-League-Scores-and-Fixtures",
    "UEL":  "https://fbref.com/en/comps/19/schedule/Europa-League-Scores-and-Fixtures",
    "UECL": "https://fbref.com/en/comps/882/schedule/Conference-League-Scores-and-Fixtures",
}


def _fetch_page(url: str, league_code: str) -> str | None:
    """Fetch page via ScraperAPI (CI) or Selenium (local)."""
    if SCRAPER_API_KEY:
        return _fetch_via_scraperapi(url, league_code)
    return _fetch_via_selenium(url, league_code)


def _fetch_via_scraperapi(url: str, league_code: str) -> str | None:
    """Fetch using ScraperAPI — bypasses Cloudflare in CI."""
    print(f"  Fetching via ScraperAPI [{league_code}]...")
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
            print(f"  Response: {resp.text[:300]}")
            return None
        html = resp.text
        print(f"  Page loaded ({len(html)} bytes)")
        return html
    except Exception as e:
        print(f"  ScraperAPI error: {e}")
        return None


def _fetch_via_selenium(url: str, league_code: str) -> str | None:
    """Fetch using local Selenium + Chrome."""
    print(f"  Opening Chrome for {league_code}...")
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
        print(f"  Browser error: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _parse_page(html: str, league_code: str = "") -> pd.DataFrame | None:
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

    if not is_intl:
        # Domestic leagues: single schedule table, take the largest
        df = max(tables, key=len)
        df = df.dropna(how="all")
        return df

    # ── International competition pages ──────────────────────────────
    # FBref structures these as multiple tables per round/phase.
    # We merge them all and tag each row with its round label so
    # batch-predict and calibration can filter by phase later.
    schedule_tables = []
    for t in tables:
        # Flatten MultiIndex if present
        if isinstance(t.columns, pd.MultiIndex):
            t.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in t.columns
            ]
        cols_lower = [str(c).lower() for c in t.columns]
        if "home" in cols_lower and "away" in cols_lower:
            schedule_tables.append(t)

    if not schedule_tables:
        print("  No schedule tables found — falling back to largest table.")
        df = max(tables, key=len)
        df = df.dropna(how="all")
        return df

    merged_parts = []
    for t in schedule_tables:
        t = t.dropna(how="all").copy()
        # Flatten MultiIndex again in case it wasn't caught above
        if isinstance(t.columns, pd.MultiIndex):
            t.columns = [
                " ".join(str(v) for v in col if str(v) != "nan").strip()
                for col in t.columns
            ]
        cols_lower_map = {str(c).lower(): c for c in t.columns}

        # Try to derive a round label from the table's own Round/Wk column,
        # falling back to a date-based phase classifier.
        round_col = cols_lower_map.get("round") or cols_lower_map.get("wk")
        if round_col:
            # Use the most common non-null value in the column as the label
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
    # FBref sometimes returns MultiIndex columns — flatten to string
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
        "round_raw":  col("_round_raw"),  # injected by _parse_page for intl comps
    }


def scrape_league(league_code: str, url: str) -> None:
    print(f"\n{'='*55}")
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

    # Parse dates
    df[c["date"]] = pd.to_datetime(df[c["date"]], errors="coerce")
    df = df.dropna(subset=[c["date"]])

    today     = date.today()
    cutoff    = today + timedelta(days=FIXTURE_DAYS)

    # ── Split into completed vs upcoming ─────────────────────────────
    score_col = c["score"]

    if score_col:
        has_score = df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)
        completed_df = df[has_score].copy()
        upcoming_df  = df[
            ~has_score &
            (df[c["date"]].dt.date >= today) &
            (df[c["date"]].dt.date <= cutoff)
        ].copy()
    else:
        completed_df = pd.DataFrame()
        upcoming_df  = df[
            (df[c["date"]].dt.date >= today) &
            (df[c["date"]].dt.date <= cutoff)
        ].copy()

    print(f"  Completed rows: {len(completed_df)}")
    print(f"  Upcoming rows (next {FIXTURE_DAYS} days): {len(upcoming_df)}")

    # ── Update FBrefSnapshot with latest completed scores ─────────────
    if not completed_df.empty:
        _update_snapshot(league_code, completed_df)

    # ── Write upcoming fixtures to FBrefFixtures ──────────────────────
    if not upcoming_df.empty:
        _upsert_fixtures(league_code, upcoming_df, c)


def _safe_to_parquet(df: pd.DataFrame) -> bytes:
    """Serialize DataFrame to parquet, coercing mixed-type object columns to string."""
    df = df.copy()
    for col in df.columns:
        if df[col].dtype == object:
            df[col] = df[col].astype(str)
    return df.to_parquet(index=True)


def _update_snapshot(league_code: str, completed_df: pd.DataFrame) -> None:
    """Merge new completed rows into existing snapshot."""
    db = SessionLocal()
    try:
        snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()

        if snap:
            existing = pd.read_parquet(io.BytesIO(snap.data))
            col_map  = {str(c).lower(): c for c in existing.columns}
            date_col = col_map.get("date")
            home_col = col_map.get("home")
            away_col = col_map.get("away")

            if date_col and home_col and away_col:
                combined = pd.concat([existing, completed_df], ignore_index=True)
                combined = combined.drop_duplicates(
                    subset=[date_col, home_col, away_col]
                )
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


def _classify_round_type(raw: str | None, match_date: date, league_code: str) -> str | None:
    """
    Map a raw round label (from FBref) or match date to a normalised round_type.

    Normalised values:
        "league_phase"   — UCL/UEL/UECL group/league phase (Sep–Jan)
        "playoff"        — two-legged playoff rounds (Feb)
        "round_of_16"
        "quarter_final"
        "semi_final"
        "final"
        None             — domestic leagues or unknown

    The raw label from FBref varies ("Round of 16", "QF", "Group Stage", etc.)
    so we normalise via keyword matching, then fall back to date heuristics.
    """
    if league_code not in ("UCL", "UEL", "UECL"):
        return None  # only classify UEFA club comps for now

    label = (raw or "").lower().strip()

    # Keyword normalisation
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

    # Date-based fallback for UCL (2024-25 schedule)
    month = match_date.month
    if month in (9, 10, 11, 12, 1):   # Sep–Jan
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


def _upsert_fixtures(
    league_code: str, upcoming_df: pd.DataFrame, c: dict
) -> None:
    """
    Write upcoming fixtures to FBrefFixtures.
    Clears existing future fixtures for this league first to avoid dupes.
    """
    db = SessionLocal()
    try:
        today = date.today()
        # Delete stale upcoming fixtures for this league
        db.query(FBrefFixture).filter(
            FBrefFixture.league_code == league_code,
            FBrefFixture.match_date  >= today,
        ).delete()

        # Deduplicate by date+home+away (multi-table merge can produce dupes)
        upcoming_df = upcoming_df.drop_duplicates(
            subset=[c["date"], c["home"], c["away"]]
        ).copy()

        added = 0
        for _, row in upcoming_df.iterrows():
            try:
                match_date = row[c["date"]].date()
                home = str(row[c["home"]]).strip()
                away = str(row[c["away"]]).strip()

                # Strip leading/trailing 2-3 letter country codes injected by
                # FBref on international competition pages (e.g. "eng Liverpool",
                # "Newcastle United eng", "es Barcelona")
                home = re.sub(r'(?i)^[a-z]{2,3}\s+', '', home).strip()
                home = re.sub(r'(?i)\s+[a-z]{2,3}$', '', home).strip()
                away = re.sub(r'(?i)^[a-z]{2,3}\s+', '', away).strip()
                away = re.sub(r'(?i)\s+[a-z]{2,3}$', '', away).strip()

                mtime = str(row[c["time"]]).strip() if c["time"] and pd.notnull(row.get(c["time"])) else None

                if not home or not away or home == "nan" or away == "nan":
                    continue

                # Derive round_type for international competitions
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
                continue

        db.commit()
        print(f"  Fixtures: {added} upcoming matches saved for {league_code}")
    except Exception as e:
        print(f"  Fixture DB error: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--league", type=str, default=None,
        help="Scrape a single league only (e.g. --league UCL)"
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
        print("[fixtures] Running in headless mode (no browser window)")

    if args.api:
        SCRAPER_API_KEY = args.api
        print("[fixtures] Using ScraperAPI for fetching")

    if args.league:
        if args.league not in LEAGUE_MAP:
            print(f"[fixtures] Unknown league: {args.league}. Available: {list(LEAGUE_MAP.keys())}")
        else:
            scrape_league(args.league, LEAGUE_MAP[args.league])
    else:
        print("[fixtures] Starting daily fixture scrape")
        print(f"[fixtures] Storing upcoming matches for next {FIXTURE_DAYS} days")
        print(f"[fixtures] Leagues: {list(LEAGUE_MAP.keys())}\n")

        codes = list(LEAGUE_MAP.keys())
        for i, (code, url) in enumerate(LEAGUE_MAP.items()):
            scrape_league(code, url)
            if i < len(codes) - 1:
                print(f"\n  Waiting {SLEEP_BETWEEN}s...")
                time.sleep(SLEEP_BETWEEN)

    print("\n[fixtures] Done.")