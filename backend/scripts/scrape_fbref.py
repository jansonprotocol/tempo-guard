"""
backend/scripts/scrape_fbref.py

Run this script locally (on your laptop) to scrape FBref and push
the results directly into your database.

Usage:
    cd backend
    python -m scripts.scrape_fbref

Render never calls FBref. It only reads from the database.

NOTE: European/international competitions (UCL, UEL, UECL, EC, WC) are
excluded — soccerdata handles these inconsistently and ATHENA's core
logic is league-based. Those league codes will simply return {} from
fbref_base.py, which is handled gracefully.
"""

import io
import math
import os
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Optional, Tuple

# Load .env so DATABASE_URL is available locally
from dotenv import load_dotenv
load_dotenv()

import pandas as pd

# ── Patch requests BEFORE importing soccerdata ──────────────────────────────
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BROWSER_HEADERS = {
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

_ORIG_INIT = requests.sessions.Session.__init__

def _patched_init(self, *args, **kwargs):
    _ORIG_INIT(self, *args, **kwargs)
    self.headers.update(BROWSER_HEADERS)
    retries = Retry(
        total=3,
        backoff_factor=2.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET", "HEAD"],
        raise_on_status=False,
    )
    adapter = HTTPAdapter(max_retries=retries)
    self.mount("http://", adapter)
    self.mount("https://", adapter)

requests.sessions.Session.__init__ = _patched_init

import soccerdata as sd  # noqa: E402  (must be after patch)

# ── Database (reuse your existing setup) ────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Config ───────────────────────────────────────────────────────────────────
CACHE_DIR = Path("/tmp/fbref_scraper_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)

ROLLING_MATCHES = 10
MIN_MATCHES = 5
SLEEP_BETWEEN_LEAGUES = 5  # seconds — be polite to FBref

# Only domestic leagues — European/international cups excluded (see note above)
LEAGUE_MAP: Dict[str, str] = {
    "ENG-PL":  "ENG-Premier League",
    "ESP-LL":  "ESP-La Liga",
    "FRA-L1":  "FRA-Ligue 1",
    "GER-BUN": "GER-Bundesliga",
    "ITA-SA":  "ITA-Serie A",
    "NED-ERE": "NED-Eredivisie",
    "TUR-SL":  "TUR-Super Lig",
    "BRA-SA":  "BRA-Serie A",
}

_SCRAPE_LOCK = threading.Lock()


# ── Helpers ──────────────────────────────────────────────────────────────────
def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


# ── Core scrape + store ───────────────────────────────────────────────────────
def scrape_and_store(league_code: str, fbref_comp: str, seasons: list):
    print(f"\n[scraper] {league_code} ({fbref_comp})  seasons={seasons}")
    try:
        with _SCRAPE_LOCK:
            fb = sd.FBref(
                leagues=[fbref_comp],
                seasons=seasons,
                data_dir=CACHE_DIR,
                proxy=None,
            )

            matches = None
            for fn in ("read_matches", "read_schedule", "read_team_match_stats"):
                if hasattr(fb, fn):
                    df_try = getattr(fb, fn)()
                    if isinstance(df_try, pd.DataFrame) and not df_try.empty:
                        matches = df_try
                        print(f"  [ok] Got {len(matches)} rows via fb.{fn}()")
                        break

        if matches is None or matches.empty:
            print(f"  [warn] No data returned for {league_code} — skipping.")
            return

        # Persist as parquet bytes in the database
        parquet_bytes = matches.to_parquet(index=True)

        db = SessionLocal()
        try:
            snapshot = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
            if snapshot:
                snapshot.data         = parquet_bytes
                snapshot.fetched_at   = datetime.utcnow()
                snapshot.seasons_json = str(seasons)
                print(f"  [ok] Updated existing snapshot for {league_code}.")
            else:
                db.add(FBrefSnapshot(
                    league_code=league_code,
                    data=parquet_bytes,
                    fetched_at=datetime.utcnow(),
                    seasons_json=str(seasons),
                ))
                print(f"  [ok] Created new snapshot for {league_code}.")
            db.commit()
        finally:
            db.close()

    except Exception as e:
        print(f"  [error] {league_code}: {e}")


# ── Entry point ───────────────────────────────────────────────────────────────
if __name__ == "__main__":
    today  = date.today()
    year   = today.year
    start  = year - 1 if today.month < 7 else year
    seasons = [f"{start - 1}-{start}", f"{start}-{start + 1}"]

    print(f"[scraper] Starting. Seasons to fetch: {seasons}")
    print(f"[scraper] Leagues to scrape: {list(LEAGUE_MAP.keys())}\n")

    for code, comp in LEAGUE_MAP.items():
        scrape_and_store(code, comp, seasons)
        time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[scraper] All done.")
