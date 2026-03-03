"""
Run this script locally (on your laptop) to scrape FBref and push
the results directly into your database.

Usage:
    cd backend
    python -m scripts.scrape_fbref

Render never calls FBref. It only reads from the database.
"""

import os
import sys
import time
import math
import threading
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
from app.database.models import FBrefSnapshot  # we create this model below

# ── Config ───────────────────────────────────────────────────────────────────
CACHE_DIR = Path("/tmp/fbref_scraper_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
ROLLING_MATCHES = 10
MIN_MATCHES = 5
SLEEP_BETWEEN_LEAGUES = 4  # seconds — be polite to FBref

# Map your internal league codes to FBref competition names
# Edit this to match your fbref_league_map.json
LEAGUE_MAP: Dict[str, str] = {
    "ENG-PL":   "ENG-Premier League",
    "ESP-LL":   "ESP-La Liga",
    "GER-BL":   "GER-Bundesliga",
    "ITA-SA":   "ITA-Serie A",
    "FRA-L1":   "FRA-Ligue 1",
    "NED-ED":   "NED-Eredivisie",
    "BRA-SA":   "BRA-Série A",
    # add more as needed
}

_SCRAPE_LOCK = threading.Lock()


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def compute_features(
    df: pd.DataFrame,
    home_team: str,
    away_team: str,
    match_date: date,
    fbref_comp: str,
) -> Optional[Dict[str, float]]:
    """Given a full league dataframe, compute ATHENA features for one fixture."""
    cols = {c.lower(): c for c in df.columns}

    def col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    c_date = col("date")
    c_ht   = col("home", "home_team")
    c_at   = col("away", "away_team")
    c_hg   = col("home_goals", "hg", "score_home", "goals_home")
    c_ag   = col("away_goals", "ag", "score_away", "goals_away")
    c_comp = col("comp", "competition")
    c_soth = col("home_shots_on_target", "shots_on_target_home", "sot_home")
    c_sota = col("away_shots_on_target", "shots_on_target_away", "sot_away")

    if not all([c_date, c_ht, c_at, c_hg, c_ag, c_comp]):
        print("  [warn] Missing essential columns.")
        return None

    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)
    work = df.copy()
    work[c_date] = pd.to_datetime(work[c_date], errors="coerce")
    work = work[work[c_comp].astype(str).str.lower() == fbref_comp.lower()]
    work = work[work[c_date] < cutoff]

    if work.empty:
        return None

    hname = _norm(home_team)
    aname = _norm(away_team)

    def last_n(team_lc: str) -> pd.DataFrame:
        d1 = work[work[c_ht].astype(str).str.lower() == team_lc]
        d2 = work[work[c_at].astype(str).str.lower() == team_lc]
        return (
            pd.concat([d1, d2])
            .sort_values(c_date, ascending=False)
            .head(ROLLING_MATCHES)
        )

    H, A = last_n(hname), last_n(aname)
    if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
        return None

    def goals_fa(frame: pd.DataFrame, team_lc: str) -> Tuple[float, float]:
        gf = ga = 0
        for _, r in frame.iterrows():
            is_home = str(r[c_ht]).lower() == team_lc
            hg = int(r[c_hg] or 0) if pd.notnull(r[c_hg]) else 0
            ag = int(r[c_ag] or 0) if pd.notnull(r[c_ag]) else 0
            gf += hg if is_home else ag
            ga += ag if is_home else hg
        n = len(frame)
        return (gf / n, ga / n) if n else (0.0, 0.0)

    gfh, gah = goals_fa(H, hname)
    gfa, gaa = goals_fa(A, aname)

    ratio_sot = 3.2
    if c_soth and c_sota:
        tmp = work[[c_hg, c_ag, c_soth, c_sota]].copy()
        tmp["goals"] = tmp[c_hg].fillna(0) + tmp[c_ag].fillna(0)
        tmp["sot"]   = tmp[c_soth].fillna(0) + tmp[c_sota].fillna(0)
        agg = tmp[tmp["goals"] > 0]
        if not agg.empty and agg["goals"].sum() > 0:
            ratio_sot = _clip(agg["sot"].sum() / agg["goals"].sum(), 2.2, 4.5)

    mu_total = max(0.2, gfh + gfa)
    p0 = math.exp(-mu_total)
    p1 = mu_total * p0
    p_two_plus = 1.0 - (p0 + p1)

    league_mu = float(
        (work[c_hg].fillna(0) + work[c_ag].fillna(0)).mean() or 2.5
    )

    return {
        "p_two_plus":              round(float(p_two_plus), 3),
        "p_home_tt05":             round(float(1.0 - _poisson_p0(gfh)), 3),
        "p_away_tt05":             round(float(1.0 - _poisson_p0(gfa)), 3),
        "tempo_index":             round(_clip(mu_total / 3.0, 0.2, 0.9), 3),
        "sot_proj_total":          round(_clip(mu_total * ratio_sot, 6.0, 16.0), 2),
        "support_idx_over_delta":  round(_clip((mu_total - league_mu) * 0.12, -0.15, 0.15), 3),
    }


def scrape_and_store(league_code: str, fbref_comp: str, seasons: list[str]):
    print(f"\n[scraper] {league_code} ({fbref_comp}) seasons={seasons}")
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
                        break

        if matches is None or matches.empty:
            print(f"  [warn] No data returned for {league_code}")
            return

        # Store raw snapshot as parquet bytes in the DB
        parquet_bytes = matches.to_parquet(index=True)
        db = SessionLocal()
        try:
            snapshot = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
            if snapshot:
                snapshot.data = parquet_bytes
                snapshot.fetched_at = datetime.utcnow()
                snapshot.seasons_json = str(seasons)
            else:
                snapshot = FBrefSnapshot(
                    league_code=league_code,
                    data=parquet_bytes,
                    fetched_at=datetime.utcnow(),
                    seasons_json=str(seasons),
                )
                db.add(snapshot)
            db.commit()
            print(f"  [ok] Stored {len(matches)} rows for {league_code}")
        finally:
            db.close()

    except Exception as e:
        print(f"  [error] {league_code}: {e}")


if __name__ == "__main__":
    today = date.today()
    year = today.year
    start = year - 1 if today.month < 7 else year
    seasons = [f"{start-1}-{start}", f"{start}-{start+1}"]

    for code, comp in LEAGUE_MAP.items():
        scrape_and_store(code, comp, seasons)
        time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[scraper] Done.")
