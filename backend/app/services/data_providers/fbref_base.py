# backend/app/services/data_providers/fbref_base.py
from __future__ import annotations

import os
import math
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, Tuple

import pandas as pd

# ------------------------------------------------------------------------------
# IMPORT SOCCERDATA + CLEAN NO-PROXY SESSION
# ------------------------------------------------------------------------------
try:
    import soccerdata as sd
    import tls_requests as tls
except Exception as e:
    sd = None
    print("[fbref_base] WARN: soccerdata not importable:", e)

# Create a clean HTTPS session with **no proxy**
session = None
try:
    session = tls.Session()
    session.trust_env = False    # do not read HTTP_PROXY / HTTPS_PROXY
    session.proxies = {}         # explicit: NO proxy
except Exception as e:
    print("[fbref_base] WARN: Could not create no-proxy session:", e)


# ------------------------------------------------------------------------------
# CONFIGURATION
# ------------------------------------------------------------------------------
FBREF_CACHE_DIR = Path(os.getenv("FBREF_CACHE_DIR", "/tmp/fbref_cache"))
FBREF_CACHE_DIR.mkdir(parents=True, exist_ok=True)

ROLLING_MATCHES = int(os.getenv("FBREF_ROLLING_MATCHES", "10"))
MIN_MATCHES = int(os.getenv("FBREF_MIN_MATCHES", "5"))

# Load league-code -> FBref-name map
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FBREF_MAP_PATH = os.path.join(_THIS_DIR, "fbref_league_map.json")
_FBREF_MAP = (
    pd.read_json(_FBREF_MAP_PATH, typ="series").to_dict()
    if os.path.exists(_FBREF_MAP_PATH)
    else {}
)

# ------------------------------------------------------------------------------
# UTILITIES
# ------------------------------------------------------------------------------

def _asof_cutoff(d: date) -> datetime:
    return datetime.combine(d, datetime.min.time()) - timedelta(seconds=1)

def _poisson_p0(mu: float) -> float:
    mu = max(0.001, float(mu))
    return math.exp(-mu)

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _norm(s: str) -> str:
    return (s or "").strip().lower()


# ------------------------------------------------------------------------------
# MAIN FEATURE FUNCTION
# ------------------------------------------------------------------------------

def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date
) -> Dict[str, float]:

    """
    Computes TempoGuard feature bundle based on FBref match data strictly BEFORE match_date.
    Returns dict or {} if insufficient data.
    """

    if sd is None:
        print("[fbref_base] soccerdata unavailable; returning {}")
        return {}

    fbref_comp = _FBREF_MAP.get(league_code)
    if not fbref_comp:
        print(f"[fbref_base] No FBref mapping found for league_code={league_code}")
        return {}

    # Determine FBref seasons around the match date
    year = match_date.year
    start_season = year - 1 if match_date.month < 7 else year
    seasons = [
        f"{start_season-1}-{start_season}",
        f"{start_season}-{start_season+1}"
    ]

    # --------------------------------------------------------------------------
    # DOWNLOAD FROM FBREF WITHOUT ANY PROXY
    # --------------------------------------------------------------------------
    try:
        fb = sd.FBref(
            leagues=[fbref_comp],
            seasons=seasons,
            data_dir=FBREF_CACHE_DIR,
            session=session,      # <--- NO PROXY SESSION
            proxy=None            # <--- ensure no proxy
        )

        # Soccerdata exposes different readers depending on version
        matches = None
        for fn in ("read_matches", "read_schedule", "read_team_match_stats"):
            if hasattr(fb, fn):
                df_try = getattr(fb, fn)()
                if isinstance(df_try, pd.DataFrame) and not df_try.empty:
                    matches = df_try
                    break

        if matches is None or matches.empty:
            print("[fbref_base] No matches dataframe returned")
            return {}

        # ----------------------------------------------------------------------
        # COLUMN NORMALIZATION
        # ----------------------------------------------------------------------
        cols = {c.lower(): c for c in matches.columns}

        def col(*names):
            for n in names:
                if n in cols:
                    return cols[n]
            return None

        c_date = col("date")
        c_ht = col("home", "home_team")
        c_at = col("away", "away_team")
        c_hg = col("home_goals", "hg", "score_home", "goals_home")
        c_ag = col("away_goals", "ag", "score_away", "goals_away")
        c_comp = col("comp", "competition")
        c_soth = col("home_shots_on_target", "shots_on_target_home", "sot_home")
        c_sota = col("away_shots_on_target", "shots_on_target_away", "sot_away")

        if not all([c_date, c_ht, c_at, c_hg, c_ag, c_comp]):
            print("[fbref_base] Missing essential match columns.")
            return {}

        # ----------------------------------------------------------------------
        # FILTER BY COMPETITION + CUTOFF DATE
        # ----------------------------------------------------------------------
        cutoff = _asof_cutoff(match_date)
        df = matches.copy()
        df[c_date] = pd.to_datetime(df[c_date], errors="coerce", utc=False)

        df = df[df[c_comp].astype(str).str.lower() == fbref_comp.lower()]
        df = df[df[c_date] < cutoff]

        if df.empty:
            return {}

        # Lowercase team names
        hname = _norm(home_team)
        aname = _norm(away_team)

        # ----------------------------------------------------------------------
        # LAST N MATCHES PER TEAM
        # ----------------------------------------------------------------------
        def last_n(team_lc: str):
            d1 = df[df[c_ht].astype(str).str.lower() == team_lc]
            d2 = df[df[c_at].astype(str).str.lower() == team_lc]
            return (
                pd.concat([d1, d2], axis=0)
                .sort_values(by=c_date, ascending=False)
                .head(ROLLING_MATCHES)
            )

        H = last_n(hname)
        A = last_n(aname)

        if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
            print("[fbref_base] Not enough history for one or both teams.")
            return {}

        # ----------------------------------------------------------------------
        # GOALS FOR / AGAINST
        # ----------------------------------------------------------------------
        def gf_ga(frame: pd.DataFrame, team_lc: str):
            gf = ga = 0
            n = len(frame)
            for _, r in frame.iterrows():
                is_home = str(r[c_ht]).lower() == team_lc
                hg, ag = int(r[c_hg] or 0), int(r[c_ag] or 0)
                if is_home:
                    gf += hg; ga += ag
                else:
                    gf += ag; ga += hg
            return gf / n, ga / n

        gfh, gah = gf_ga(H, hname)
        gfa, gaa = gf_ga(A, aname)

        # ----------------------------------------------------------------------
        # SHOTS-ON-TARGET RATIO
        # ----------------------------------------------------------------------
        ratio_sot = 3.2
        if c_soth and c_sota:
            tmp = df[[c_hg, c_ag, c_soth, c_sota]].copy()
            tmp["goals"] = tmp[c_hg].fillna(0).astype(int) + tmp[c_ag].fillna(0).astype(int)
            tmp["sot"]   = tmp[c_soth].fillna(0).astype(int) + tmp[c_sota].fillna(0).astype(int)
            valid = tmp[tmp["goals"] > 0]
            if not valid.empty:
                ratio_sot = max(2.2, min(4.5, valid["sot"].sum() / valid["goals"].sum()))

        # ----------------------------------------------------------------------
        # FEATURE CALCULATIONS
        # ----------------------------------------------------------------------
        g_home = max(0.05, gfh)
        g_away = max(0.05, gfa)
        mu_total = max(0.2, g_home + g_away)

        p0 = math.exp(-mu_total)
        p1 = mu_total * p0
        p_two_plus = 1.0 - (p0 + p1)

        p_home_tt05 = 1.0 - _poisson_p0(g_home)
        p_away_tt05 = 1.0 - _poisson_p0(g_away)

        tempo_index = _clip(mu_total / 3.0, 0.2, 0.9)
        sot_proj_total = _clip(mu_total * ratio_sot, 6.0, 16.0)

        league_mu = (
            df[c_hg].fillna(0).astype(int) + df[c_ag].fillna(0).astype(int)
        ).mean()
        league_mu = float(league_mu) if pd.notnull(league_mu) else 2.5

        support_delta = (mu_total - league_mu) * 0.12
        support_delta = _clip(support_delta, -0.15, 0.15)

        return {
            "p_two_plus": round(float(p_two_plus), 3),
            "p_home_tt05": round(float(p_home_tt05), 3),
            "p_away_tt05": round(float(p_away_tt05), 3),
            "tempo_index": round(float(tempo_index), 3),
            "sot_proj_total": round(float(sot_proj_total), 2),
            "support_idx_over_delta": round(float(support_delta), 3),
        }

    except Exception as e:
        print(f"[fbref_base] error:", e)
        return {}
