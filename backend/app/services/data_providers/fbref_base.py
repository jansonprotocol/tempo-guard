"""
fbref_base.py — RENDER-SAFE version.

Render NEVER scrapes FBref. It reads parquet snapshots stored by running:
    python -m scripts.scrape_fbref   (on your local machine)

International competitions (UCL, UEL, UECL, EC, WC):
    No dedicated snapshot is scraped for these. Instead, ATHENA looks up
    each team across ALL available domestic league snapshots and uses their
    recent domestic form as the feature foundation. This gives a solid base
    without needing to scrape separate international datasets.
"""
from __future__ import annotations

import io
import math
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Constants ─────────────────────────────────────────────────────────────────
ROLLING_MATCHES = 10
MIN_MATCHES = 5

# These league codes have no dedicated snapshot — handled via domestic fallback
INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def _load_snapshot(league_code: str) -> Optional[pd.DataFrame]:
    """Load a single league's parquet snapshot from the database."""
    db = SessionLocal()
    try:
        row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if row is None:
            return None
        df = pd.read_parquet(io.BytesIO(row.data))
        print(f"[fbref_base] Loaded snapshot: {league_code} ({len(df)} rows, "
              f"fetched {row.fetched_at})")
        return df
    except Exception as e:
        print(f"[fbref_base] Error loading snapshot for {league_code}: {e}")
        return None
    finally:
        db.close()


def _load_all_snapshots() -> List[pd.DataFrame]:
    """Load every available domestic league snapshot from the database."""
    db = SessionLocal()
    try:
        rows = db.query(FBrefSnapshot).all()
        result = []
        for row in rows:
            try:
                df = pd.read_parquet(io.BytesIO(row.data))
                result.append(df)
            except Exception as e:
                print(f"[fbref_base] Could not parse snapshot {row.league_code}: {e}")
        print(f"[fbref_base] Loaded {len(result)} domestic snapshots for intl lookup.")
        return result
    finally:
        db.close()


def _resolve_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """Map logical column names to actual dataframe column names (case-insensitive)."""
    cols = {c.lower(): c for c in df.columns}

    def col(*names: str) -> Optional[str]:
        for n in names:
            if n in cols:
                return cols[n]
        return None

    return {
        "date":  col("date"),
        "ht":    col("home", "home_team"),
        "at":    col("away", "away_team"),
        "hg":    col("home_goals", "hg", "score_home", "goals_home"),
        "ag":    col("away_goals", "ag", "score_away", "goals_away"),
        "comp":  col("comp", "competition"),
        "soth":  col("home_shots_on_target", "shots_on_target_home", "sot_home"),
        "sota":  col("away_shots_on_target", "shots_on_target_away", "sot_away"),
    }


def _find_team_rows(df: pd.DataFrame, team_lc: str, cutoff: datetime,
                    c: Dict) -> pd.DataFrame:
    """Return the last N rows for a team in a dataframe, strictly before cutoff."""
    if not all([c["date"], c["ht"], c["at"]]):
        return pd.DataFrame()

    work = df.copy()
    work[c["date"]] = pd.to_datetime(work[c["date"]], errors="coerce")
    work = work[work[c["date"]] < cutoff]

    d1 = work[work[c["ht"]].astype(str).str.lower() == team_lc]
    d2 = work[work[c["at"]].astype(str).str.lower() == team_lc]
    return (
        pd.concat([d1, d2])
        .sort_values(c["date"], ascending=False)
        .head(ROLLING_MATCHES)
    )


def _compute_features_from_frames(
    H: pd.DataFrame,
    A: pd.DataFrame,
    hname: str,
    aname: str,
    full_df: pd.DataFrame,
    c: Dict,
) -> Dict[str, float]:
    """
    Core feature computation given last-N match frames for home and away teams.
    Shared by both the domestic and international code paths.
    """

    def goals_fa(frame: pd.DataFrame, team_lc: str) -> Tuple[float, float]:
        gf = ga = 0
        for _, r in frame.iterrows():
            is_home = str(r[c["ht"]]).lower() == team_lc
            hg = int(r[c["hg"]] or 0) if pd.notnull(r[c["hg"]]) else 0
            ag = int(r[c["ag"]] or 0) if pd.notnull(r[c["ag"]]) else 0
            gf += hg if is_home else ag
            ga += ag if is_home else hg
        n = len(frame)
        return (gf / n, ga / n) if n else (0.0, 0.0)

    gfh, _ = goals_fa(H, hname)
    gfa, _ = goals_fa(A, aname)

    # SoT ratio from the reference dataframe (whichever snapshot we're using)
    ratio_sot = 3.2
    if c["soth"] and c["sota"] and c["hg"] and c["ag"]:
        tmp = full_df[[c["hg"], c["ag"], c["soth"], c["sota"]]].copy()
        tmp["goals"] = tmp[c["hg"]].fillna(0) + tmp[c["ag"]].fillna(0)
        tmp["sot"]   = tmp[c["soth"]].fillna(0) + tmp[c["sota"]].fillna(0)
        agg = tmp[tmp["goals"] > 0]
        if not agg.empty and agg["goals"].sum() > 0:
            ratio_sot = _clip(agg["sot"].sum() / agg["goals"].sum(), 2.2, 4.5)

    mu_total = max(0.2, gfh + gfa)
    p0 = math.exp(-mu_total)
    p1 = mu_total * p0
    p_two_plus = 1.0 - (p0 + p1)

    league_mu = float(
        (full_df[c["hg"]].fillna(0) + full_df[c["ag"]].fillna(0)).mean() or 2.5
    ) if c["hg"] and c["ag"] else 2.5

    return {
        "p_two_plus":             round(float(p_two_plus), 3),
        "p_home_tt05":            round(float(1.0 - _poisson_p0(gfh)), 3),
        "p_away_tt05":            round(float(1.0 - _poisson_p0(gfa)), 3),
        "tempo_index":            round(_clip(mu_total / 3.0, 0.2, 0.9), 3),
        "sot_proj_total":         round(_clip(mu_total * ratio_sot, 6.0, 16.0), 2),
        "support_idx_over_delta": round(_clip((mu_total - league_mu) * 0.12, -0.15, 0.15), 3),
    }


# ── International fallback ────────────────────────────────────────────────────
def _asof_features_intl(
    home_team: str,
    away_team: str,
    match_date: date,
) -> Dict[str, float]:
    """
    For international competitions (UCL, UEL, UECL, EC, WC).

    Searches ALL available domestic league snapshots to find recent form
    for each team, then computes features from that domestic data.
    Each team may come from a different snapshot (e.g. Man City from ENG-PL,
    Real Madrid from ESP-LL) — we find whichever snapshot has the most data.
    """
    snapshots = _load_all_snapshots()
    if not snapshots:
        print("[fbref_base] No domestic snapshots available for intl fallback.")
        return {}

    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)
    hname  = _norm(home_team)
    aname  = _norm(away_team)

    # Find the best (most rows) frame for each team across all snapshots
    def best_frame_for_team(team_lc: str) -> Tuple[Optional[pd.DataFrame],
                                                     Optional[pd.DataFrame],
                                                     Optional[Dict]]:
        best_rows = pd.DataFrame()
        best_full = None
        best_cols = None
        for snap in snapshots:
            c = _resolve_columns(snap)
            if not all([c["date"], c["ht"], c["at"], c["hg"], c["ag"]]):
                continue
            rows = _find_team_rows(snap, team_lc, cutoff, c)
            if len(rows) > len(best_rows):
                best_rows = rows
                best_full = snap
                best_cols = c
        return best_rows, best_full, best_cols

    H, full_H, cols_H = best_frame_for_team(hname)
    A, full_A, cols_A = best_frame_for_team(aname)

    if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
        print(f"[fbref_base] Intl fallback: not enough domestic data — "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

    # Use the home team's snapshot as the reference for league-level stats
    # (arbitrary but consistent; both teams' goal rates are computed independently)
    ref_df   = full_H
    ref_cols = cols_H

    # Merge H and A into one frame using common columns for _compute_features_from_frames
    # Since they may come from different snapshots we build a unified view
    combined = pd.concat([H, A]).drop_duplicates()

    print(f"[fbref_base] Intl fallback OK — "
          f"{home_team} ({len(H)} rows), {away_team} ({len(A)} rows)")

    return _compute_features_from_frames(H, A, hname, aname, ref_df, ref_cols)


# ── Public API ────────────────────────────────────────────────────────────────
def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
) -> Dict[str, float]:
    """
    Main entry point. Called by routes/services for every match prediction.

    - Domestic leagues (ENG-PL, ESP-LL, etc.): uses the league's own snapshot.
    - International (UCL, UEL, UECL, EC, WC): falls back to each team's
      domestic league snapshot automatically.
    - Returns {} if data is unavailable — callers should handle gracefully.
    """

    # ── International path ────────────────────────────────────────────────
    if league_code in INTL_LEAGUE_CODES:
        print(f"[fbref_base] Intl competition ({league_code}) — "
              "using domestic fallback for features.")
        return _asof_features_intl(home_team, away_team, match_date)

    # ── Domestic path ─────────────────────────────────────────────────────
    df = _load_snapshot(league_code)
    if df is None or df.empty:
        print(f"[fbref_base] No snapshot for {league_code}. "
              "Run scripts/scrape_fbref.py locally.")
        return {}

    c = _resolve_columns(df)
    if not all([c["date"], c["ht"], c["at"], c["hg"], c["ag"]]):
        print("[fbref_base] Snapshot missing essential columns.")
        return {}

    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)
    work   = df.copy()
    work[c["date"]] = pd.to_datetime(work[c["date"]], errors="coerce")
    work = work[work[c["date"]] < cutoff]

    if work.empty:
        return {}

    hname = _norm(home_team)
    aname = _norm(away_team)

    H = _find_team_rows(df, hname, cutoff, c)
    A = _find_team_rows(df, aname, cutoff, c)

    if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
        print(f"[fbref_base] Not enough matches — "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

    return _compute_features_from_frames(H, A, hname, aname, work, c)
