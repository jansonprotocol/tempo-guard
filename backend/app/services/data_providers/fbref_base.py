# backend/app/services/data_providers/fbref_base.py
from __future__ import annotations

import os
import math
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple, Optional

import pandas as pd

# ------------------------------------------------------------------------------
# Hardening for FBref (works with soccerdata==1.3.0 which uses `requests`)
# We patch requests.Session so any session created (including inside soccerdata)
# looks like a normal browser, uses gentle retries, and does a one-time warm-up.
# ------------------------------------------------------------------------------

# Never use proxies in this process
for _k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(_k, None)
os.environ["NO_PROXY"] = "*"

# Try to install a small cache so repeat calls don't refetch (optional)
try:
    import requests_cache  # type: ignore
    requests_cache.install_cache("/tmp/http_cache", expire_after=86400)  # 1 day
except Exception as _e:
    print("[fbref_base] INFO: requests_cache not available:", repr(_e))

# Patch requests to add browser-like headers and a warm-up call to FBref
try:
    import requests  # type: ignore
    from requests.adapters import HTTPAdapter  # type: ignore
    from urllib3.util.retry import Retry  # type: ignore

    _ORIG_SESSION_INIT = requests.sessions.Session.__init__

    def _patched_session_init(self, *args, **kwargs):
        _ORIG_SESSION_INIT(self, *args, **kwargs)
        # Browser-like headers
        self.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://fbref.com/en/",
        })

        # Gentle retries for transient 429/5xx
        retries = Retry(
            total=2,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

        # One-time warm-up to establish cookies before /en/comps/
        try:
            self.get("https://fbref.com/en/", timeout=20)
        except Exception as _warm_e:
            print("[fbref_base] WARN: FBref warm-up failed (continuing):", repr(_warm_e))

    # Apply the patch once per process
    if requests.sessions.Session.__init__ is not _patched_session_init:
        requests.sessions.Session.__init__ = _patched_session_init
        print("[fbref_base] INFO: Patched requests.Session (headers, retries, warm-up).")

except Exception as _patch_e:
    print("[fbref_base] WARN: Could not patch requests.Session:", repr(_patch_e))

# ------------------------------------------------------------------------------
# Import SOCCERDATA after patching requests (important for 1.3.0)
# ------------------------------------------------------------------------------
try:
    import soccerdata as sd  # type: ignore
except Exception as e:
    sd = None
    print("[fbref_base] WARN: soccerdata not importable:", e)

# ------------------------------------------------------------------------------
# Configuration
# ------------------------------------------------------------------------------
FBREF_CACHE_DIR = Path(os.getenv("FBREF_CACHE_DIR", "/tmp/fbref_cache"))
FBREF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

ROLLING_MATCHES = int(os.getenv("FBREF_ROLLING_MATCHES", "10"))
MIN_MATCHES = int(os.getenv("FBREF_MIN_MATCHES", "5"))

# league_code -> FBref competition name map
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FBREF_MAP_PATH = os.path.join(_THIS_DIR, "fbref_league_map.json")
_FBREF_MAP: Dict[str, str] = (
    pd.read_json(_FBREF_MAP_PATH, typ="series").to_dict()
    if os.path.exists(_FBREF_MAP_PATH)
    else {}
)

# Serialize scrapes to avoid bursty concurrent requests
_SCRAPE_LOCK = threading.Lock()

# ------------------------------------------------------------------------------
# Utilities
# ------------------------------------------------------------------------------
def _asof_cutoff(d: date) -> datetime:
    """Exclude any data on the target day (pre‑kickoff perspective)."""
    return datetime.combine(d, datetime.min.time()) - timedelta(seconds=1)

def _poisson_p0(mu: float) -> float:
    mu = max(0.001, float(mu))
    return math.exp(-mu)

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()

# ------------------------------------------------------------------------------
# Core feature builder
# ------------------------------------------------------------------------------
def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date
) -> Dict[str, float]:
    """
    Compute ATHENA features from FBref using only matches STRICTLY BEFORE `match_date`.
    Returns dict with keys:
      p_two_plus, p_home_tt05, p_away_tt05, tempo_index, sot_proj_total, support_idx_over_delta
    If data insufficient or a scrape issue occurs, returns {} (so caller can fallback).
    """
    if sd is None:
        print("[fbref_base] soccerdata unavailable; returning {}")
        return {}

    fbref_comp = _FBREF_MAP.get(league_code)
    if not fbref_comp:
        print(f"[fbref_base] No FBref mapping for league_code={league_code}")
        return {}

    # Derive two seasons around the match date to ensure enough recent history
    year = match_date.year
    start_season = year - 1 if match_date.month < 7 else year
    seasons = [f"{start_season-1}-{start_season}", f"{start_season}-{start_season+1}"]

    try:
        # Ensure we don't spike FBref with concurrent requests
        with _SCRAPE_LOCK:
            # Gentle spacing so multiple API calls don't all hit at once
            time.sleep(1.0)

            # IMPORTANT: proxy=None (no Tor/SOCKS). Data cached in FBREF_CACHE_DIR.
            fb = sd.FBref(
                leagues=[fbref_comp],
                seasons=seasons,
                data_dir=FBREF_CACHE_DIR,
                proxy=None
            )

            # soccerdata 1.3.0 provides multiple readers; try a cascade
            matches = None
            for fn in ("read_matches", "read_schedule", "read_team_match_stats"):
                if hasattr(fb, fn):
                    df_try = getattr(fb, fn)()
                    if isinstance(df_try, pd.DataFrame) and not df_try.empty:
                        matches = df_try
                        break

        if matches is None or matches.empty:
            print("[fbref_base] No matches dataframe returned.")
            return {}

        # Normalize column names (case-insensitive lookup)
        cols = {c.lower(): c for c in matches.columns}

        def col(*names: str) -> Optional[str]:
            for n in names:
                if n in cols:
                    return cols[n]
            return None

        c_date  = col("date")
        c_ht    = col("home", "home_team")
        c_at    = col("away", "away_team")
        c_hg    = col("home_goals", "hg", "score_home", "goals_home")
        c_ag    = col("away_goals", "ag", "score_away", "goals_away")
        c_comp  = col("comp", "competition")
        c_soth  = col("home_shots_on_target", "shots_on_target_home", "sot_home")
        c_sota  = col("away_shots_on_target", "shots_on_target_away", "sot_away")

        # Essential columns required
        if not all([c_date, c_ht, c_at, c_hg, c_ag, c_comp]):
            print("[fbref_base] Missing essential columns in FBref dataframe.")
            return {}

        # Filter to selected competition and strictly before the cutoff datetime
        cutoff = _asof_cutoff(match_date)
        df = matches.copy()
        df[c_date] = pd.to_datetime(df[c_date], errors="coerce", utc=False)
        df = df[df[c_comp].astype(str).str.lower() == fbref_comp.lower()]
        df = df[df[c_date] < cutoff]
        if df.empty:
            return {}

        # Team name normalization
        hname = _norm(home_team)
        aname = _norm(away_team)

        # Build last-N matches per team (either home or away)
        def last_n(team_lc: str) -> pd.DataFrame:
            d1 = df[df[c_ht].astype(str).str.lower() == team_lc]
            d2 = df[df[c_at].astype(str).str.lower() == team_lc]
            return (
                pd.concat([d1, d2], axis=0)
                .sort_values(by=c_date, ascending=False)
                .head(ROLLING_MATCHES)
            )

        H = last_n(hname)
        A = last_n(aname)

        # Require minimum sample size for stability
        if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
            print("[fbref_base] Not enough matches for one or both teams.")
            return {}

        # Compute goals for/against in team perspective
        def goals_for_against(frame: pd.DataFrame, team_lc: str) -> Tuple[float, float]:
            gf = ga = 0
            n = len(frame)
            for _, r in frame.iterrows():
                is_home = str(r[c_ht]).lower() == team_lc
                hg = int((r[c_hg] if pd.notnull(r[c_hg]) else 0) or 0)
                ag = int((r[c_ag] if pd.notnull(r[c_ag]) else 0) or 0)
                if is_home:
                    gf += hg
                    ga += ag
                else:
                    gf += ag
                    ga += hg
            return (gf / n, ga / n) if n > 0 else (0.0, 0.0)

        gfh, gah = goals_for_against(H, hname)
        gfa, gaa = goals_for_against(A, aname)

        # League-level shots-per-goal ratio (approximation for SoT projection)
        ratio_sot_per_goal = 3.2
        if c_soth and c_sota:
            tmp = df[[c_hg, c_ag, c_soth, c_sota]].copy()
            tmp["goals"] = tmp[c_hg].fillna(0).astype(int) + tmp[c_ag].fillna(0).astype(int)
            tmp["sot"]   = tmp[c_soth].fillna(0).astype(int) + tmp[c_sota].fillna(0).astype(int)
            agg = tmp[tmp["goals"] > 0]
            if not agg.empty and agg["goals"].sum() > 0:
                # Keep within reasonable bounds
                ratio_sot_per_goal = max(2.2, min(4.5, agg["sot"].sum() / agg["goals"].sum()))

        # Poisson-based probabilities
        g_home = max(0.05, gfh)
        g_away = max(0.05, gfa)
        mu_total = max(0.2, g_home + g_away)

        p0 = math.exp(-mu_total)
        p1 = mu_total * p0
        p_two_plus = 1.0 - (p0 + p1)

        p_home_tt05 = 1.0 - _poisson_p0(g_home)
        p_away_tt05 = 1.0 - _poisson_p0(g_away)

        tempo_index = _clip(mu_total / 3.0, 0.2, 0.9)
        sot_proj_total = _clip(mu_total * ratio_sot_per_goal, 6.0, 16.0)

        # Over-support delta vs league mean goals per game
        league_mu = (
            df[c_hg].fillna(0).astype(int) + df[c_ag].fillna(0).astype(int)
        ).mean()
        league_mu = float(league_mu) if pd.notnull(league_mu) else 2.5
        support_idx_over_delta = _clip((mu_total - league_mu) * 0.12, -0.15, 0.15)

        return {
            "p_two_plus": round(float(p_two_plus), 3),
            "p_home_tt05": round(float(p_home_tt05), 3),
            "p_away_tt05": round(float(p_away_tt05), 3),
            "tempo_index": round(float(tempo_index), 3),
            "sot_proj_total": round(float(sot_proj_total), 2),
            "support_idx_over_delta": round(float(support_idx_over_delta), 3),
        }

    except Exception as e:
        # We do not crash the app; caller can fallback when {} is returned.
        print(f"[fbref_base] error: {e}")
        return {}
