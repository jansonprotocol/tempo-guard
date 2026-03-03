"""
fbref_base.py — RENDER-SAFE version.

Render NEVER scrapes FBref. Instead it reads the parquet snapshot that was
stored by running:   python -m scripts.scrape_fbref   on your local machine.

If no snapshot exists yet, all feature calls return {} gracefully.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd

# DB access
from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Config ───────────────────────────────────────────────────────────────────
ROLLING_MATCHES = 10
MIN_MATCHES = 5


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def _load_snapshot(league_code: str) -> Optional[pd.DataFrame]:
    """Load the parquet snapshot from the database. Returns None if not found."""
    db = SessionLocal()
    try:
        row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if row is None:
            print(f"[fbref_base] No snapshot in DB for league_code={league_code}. "
                  "Run scripts/scrape_fbref.py locally first.")
            return None
        import io
        df = pd.read_parquet(io.BytesIO(row.data))
        print(f"[fbref_base] Loaded snapshot for {league_code} "
              f"({len(df)} rows, fetched {row.fetched_at})")
        return df
    except Exception as e:
        print(f"[fbref_base] Error loading snapshot for {league_code}: {e}")
        return None
    finally:
        db.close()


def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
) -> Dict[str, float]:
    """
    Compute ATHENA features using the stored DB snapshot.
    Returns {} if no data is available (caller should handle gracefully).
    """
    df = _load_snapshot(league_code)
    if df is None or df.empty:
        return {}

    # ── Column resolution ─────────────────────────────────────────────────
    cols = {c.lower(): c for c in df.columns}

    def col(*names: str) -> Optional[str]:
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

    if not all([c_date, c_ht, c_at, c_hg, c_ag]):
        print("[fbref_base] Snapshot is missing essential columns.")
        return {}

    # ── Filter to before match_date ───────────────────────────────────────
    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)
    work = df.copy()
    work[c_date] = pd.to_datetime(work[c_date], errors="coerce")
    if c_comp:
        # Try to filter by competition if the column exists
        pass  # snapshot is already league-specific; skip comp filter if needed
    work = work[work[c_date] < cutoff]

    if work.empty:
        return {}

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
        print(f"[fbref_base] Not enough matches: "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

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

    gfh, _ = goals_fa(H, hname)
    gfa, _ = goals_fa(A, aname)

    # ── SoT ratio ─────────────────────────────────────────────────────────
    ratio_sot = 3.2
    if c_soth and c_sota:
        tmp = work[[c_hg, c_ag, c_soth, c_sota]].copy()
        tmp["goals"] = tmp[c_hg].fillna(0) + tmp[c_ag].fillna(0)
        tmp["sot"]   = tmp[c_soth].fillna(0) + tmp[c_sota].fillna(0)
        agg = tmp[tmp["goals"] > 0]
        if not agg.empty and agg["goals"].sum() > 0:
            ratio_sot = _clip(agg["sot"].sum() / agg["goals"].sum(), 2.2, 4.5)

    # ── Probabilities ─────────────────────────────────────────────────────
    mu_total = max(0.2, gfh + gfa)
    p0 = math.exp(-mu_total)
    p1 = mu_total * p0
    p_two_plus = 1.0 - (p0 + p1)

    league_mu = float(
        (work[c_hg].fillna(0) + work[c_ag].fillna(0)).mean() or 2.5
    )

    return {
        "p_two_plus":             round(float(p_two_plus), 3),
        "p_home_tt05":            round(float(1.0 - _poisson_p0(gfh)), 3),
        "p_away_tt05":            round(float(1.0 - _poisson_p0(gfa)), 3),
        "tempo_index":            round(_clip(mu_total / 3.0, 0.2, 0.9), 3),
        "sot_proj_total":         round(_clip(mu_total * ratio_sot, 6.0, 16.0), 2),
        "support_idx_over_delta": round(_clip((mu_total - league_mu) * 0.12, -0.15, 0.15), 3),
    }
