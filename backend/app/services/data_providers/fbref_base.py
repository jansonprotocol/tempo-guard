# backend/app/services/data_providers/fbref_base.py
from __future__ import annotations
import os
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple, List

import math
import pandas as pd

# You’ll need these packages in your requirements:
# soccerdata>=1.7.0  pandas>=2.1.0  lxml  pyarrow  requests-cache
try:
    import soccerdata as sd
except Exception as e:
    sd = None
    print("[fbref_base] WARN: soccerdata not importable:", e)

# --- Configuration ---
FBREF_CACHE_DIR = os.getenv("FBREF_CACHE_DIR", "/tmp/fbref_cache")
ROLLING_MATCHES = int(os.getenv("FBREF_ROLLING_MATCHES", "10"))  # N last matches per team
MIN_MATCHES = int(os.getenv("FBREF_MIN_MATCHES", "5"))           # require at least 5 for stability

# Map canonical league code -> FBref competition name
_THIS_DIR = os.path.dirname(os.path.abspath(__file__))
_FBREF_MAP_PATH = os.path.join(_THIS_DIR, "fbref_league_map.json")
_FBREF_MAP = pd.read_json(_FBREF_MAP_PATH, typ="series").to_dict() if os.path.exists(_FBREF_MAP_PATH) else {}

# --- Utilities ---

def _asof_cutoff(d: date) -> datetime:
    # as-of rule: exclude any data on the target day (pre-kickoff perspective)
    return datetime.combine(d, datetime.min.time()) - timedelta(seconds=1)

def _poisson_p0(mu: float) -> float:
    mu = max(0.001, float(mu))
    return math.exp(-mu)

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))

def _norm(s: str) -> str:
    return (s or "").strip().lower()

# --- Core feature builder ---

def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date
) -> Dict[str, float]:
    """
    Compute ATHENA features from FBref using only matches BEFORE match_date.
    Works for both Futurematch and Retrosim (equal inputs).
    Returns dict with keys:
      p_two_plus, p_home_tt05, p_away_tt05, tempo_index, sot_proj_total, support_idx_over_delta
    If data insufficient, returns {} so caller may fallback (but try to avoid).
    """
    if sd is None:
        print("[fbref_base] soccerdata is unavailable; returning {}")
        return {}

    fbref_comp = _FBREF_MAP.get(league_code)
    if not fbref_comp:
        print(f"[fbref_base] No FBref mapping for league_code={league_code}")
        return {}

    # seasons needed: derive from date; include previous season to ensure enough history around new seasons
    # FBref uses season like “2024-2025”, soccerdata expects string season boundaries.
    year = match_date.year
    # heuristic: European season roll in July
    start_season = year - 1 if match_date.month < 7 else year
    seasons = [f"{start_season-1}-{start_season}", f"{start_season}-{start_season+1}"]

    try:
        fb = sd.FBref(leagues=[fbref_comp], seasons=seasons, data_dir=FBREF_CACHE_DIR)
        # Read match results with shots; soccerdata exposes unified match-level frames from FBref.
        # API name may differ by version; common are:
        #   read_schedule() / read_matches() / read_team_match_stats()
        # We’ll try a robust cascade:
        matches = None
        for fn in ("read_matches", "read_schedule", "read_team_match_stats"):
            if hasattr(fb, fn):
                matches = getattr(fb, fn)()
                if isinstance(matches, pd.DataFrame) and not matches.empty:
                    break
        if matches is None or matches.empty:
            print("[fbref_base] No matches frame returned.")
            return {}

        # Expected columns (names can vary by soccerdata version, so normalize)
        # Typical FBref columns:
        # date, home_team, away_team, home_goals, away_goals, comp, season, home_shots_on_target, away_shots_on_target, etc.
        # Normalize likely names:
        cols = {c.lower(): c for c in matches.columns}
        def col(*cands):  # pick first present
            for c in cands:
                if c in cols:
                    return cols[c]
            return None

        c_date  = col("date")
        c_ht    = col("home", "home_team")
        c_at    = col("away", "away_team")
        c_hg    = col("home_goals", "hg", "score_home", "goals_home")
        c_ag    = col("away_goals", "ag", "score_away", "goals_away")
        c_comp  = col("comp", "competition")
        c_soth  = col("home_shots_on_target", "shots_on_target_home", "sot_home")
        c_sota  = col("away_shots_on_target", "shots_on_target_away", "sot_away")

        if not all([c_date, c_ht, c_at, c_hg, c_ag, c_comp]):
            print("[fbref_base] Missing essential columns in FBref dataframe.")
            return {}

        # Filter to competition and to strictly “as-of” cutoff
        cutoff = _asof_cutoff(match_date)
        df = matches.copy()
        # Coerce date
        df[c_date] = pd.to_datetime(df[c_date], errors="coerce", utc=False)
        df = df[df[c_comp].astype(str).str.lower() == fbref_comp.lower()]
        df = df[df[c_date] < cutoff]
        if df.empty:
            return {}

        # Team name normalization (lowercase)
        hname = _norm(home_team)
        aname = _norm(away_team)

        # Build each team’s last-N matches (any venue) strictly before date
        def last_n(team_lc: str) -> pd.DataFrame:
            d1 = df[df[c_ht].astype(str).str.lower() == team_lc]
            d2 = df[df[c_at].astype(str).str.lower() == team_lc]
            out = pd.concat([d1, d2], axis=0).sort_values(by=c_date, ascending=False).head(ROLLING_MATCHES)
            return out

        H = last_n(hname)
        A = last_n(aname)

        # Require minimum sample size for stability
        if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
            # If early season, try previous season more aggressively is already handled by 'seasons' span.
            print("[fbref_base] Not enough matches for one or both teams.")
            return {}

        # Compute goals for team in its perspective
        def goals_for_against(frame: pd.DataFrame, team_lc: str) -> Tuple[float, float]:
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

        gfh, gah = goals_for_against(H, hname)
        gfa, gaa = goals_for_against(A, aname)

        # league-level shots-per-goal ratio (approximation for SoT projection)
        ratio_sot_per_goal = 3.2
        if c_soth and c_sota:
            tmp = df[[c_hg, c_ag, c_soth, c_sota]].copy()
            tmp["goals"] = tmp[c_hg].fillna(0).astype(int) + tmp[c_ag].fillna(0).astype(int)
            tmp["sot"]   = tmp[c_soth].fillna(0).astype(int) + tmp[c_sota].fillna(0).astype(int)
            agg = tmp[tmp["goals"] > 0]
            if not agg.empty and agg["goals"].sum() > 0:
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

        # Over support delta: relative to league mean goals per game
        league_mu = (df[c_hg].fillna(0).astype(int) + df[c_ag].fillna(0).astype(int)).mean()
        league_mu = float(league_mu) if pd.notnull(league_mu) else 2.5
        support_idx_over_delta = _clip((mu_total - league_mu) * 0.12, -0.15, 0.15)

        return {
            "p_two_plus": round(float(p_two_plus), 3),
            "p_home_tt05": round(float(p_home_tt05), 3),
            "p_away_tt05": round(float(p_away_tt05), 3),
            "tempo_index": round(float(tempo_index), 3),
            "sot_proj_total": round(float(sot_proj_total), 2),
            "support_idx_over_delta": round(float(support_idx_over_delta), 3)
        }

    except Exception as e:
        print(f"[fbref_base] error: {e}")
        return {}
