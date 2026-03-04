"""
backend/scripts/scrape_fbref.py

Scrapes FBref directly using requests + pandas.read_html().
No soccerdata dependency — works on any Python version.

Run locally:
    cd backend
    python -m scripts.scrape_fbref
"""

import io
import sys
import time
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
from curl_cffi import requests

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Browser headers ───────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://fbref.com/en/",
}

SLEEP_BETWEEN_LEAGUES = 5  # seconds — be polite to FBref

# ── League map: internal code → FBref fixtures page URL ──────────────────────
LEAGUE_MAP = {
    "ENG-PL":  "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ESP-LL":  "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "FRA-L1":  "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
    "GER-BUN": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "ITA-SA":  "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "NED-ERE": "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
    "TUR-SL":  "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
    "BRA-SA":  "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
}


def fetch_league(league_code: str, url: str) -> None:
    print(f"\n[scraper] {league_code}")
    print(f"  URL: {url}")

    session = requests.Session()
    session.headers.update(HEADERS)

    # Warm-up visit to get cookies
    try:
        session.get("https://fbref.com/en/", timeout=20)
        print("  Warm-up OK")
    except Exception as e:
        print(f"  Warm-up failed (continuing): {e}")

    time.sleep(2)

    try:
        resp = session.get(url, timeout=30)
        print(f"  Status: {resp.status_code}")

        if resp.status_code == 403:
            print("  BLOCKED (403) — FBref is rate limiting. Wait a few minutes and try again.")
            return
        if resp.status_code != 200:
            print(f"  ERROR: Unexpected status {resp.status_code}")
            return

    except Exception as e:
        print(f"  Request failed: {e}")
        return

    try:
        tables = pd.read_html(io.StringIO(resp.text))
        print(f"  Found {len(tables)} tables on page")
    except Exception as e:
        print(f"  Could not parse tables: {e}")
        return

    if not tables:
        print("  No tables found on page.")
        return

    # Biggest table = fixtures/results table
    df = max(tables, key=len)
    df = df.dropna(how="all")

    # Keep only completed matches (rows that have a score like 2-1 or 2–1)
    score_col = next(
        (c for c in df.columns if str(c).lower() in ("score", "scores")), None
    )
    if score_col:
        df = df[df[score_col].astype(str).str.contains(r"\d[–-]\d", na=False)]

    print(f"  Completed match rows: {len(df)}")

    if df.empty:
        print("  No completed match rows found.")
        return

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
            print(f"  OK — {action} snapshot ({len(df)} rows)")
        finally:
            db.close()
    except Exception as e:
        print(f"  Database error: {e}")


if __name__ == "__main__":
    print("[scraper] Starting FBref scrape")
    print(f"[scraper] Leagues: {list(LEAGUE_MAP.keys())}\n")

    codes = list(LEAGUE_MAP.keys())
    for i, (code, url) in enumerate(LEAGUE_MAP.items()):
        fetch_league(code, url)
        if i < len(codes) - 1:
            print(f"\n  Waiting {SLEEP_BETWEEN_LEAGUES}s before next league...")
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[scraper] Done.")
