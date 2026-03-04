"""
fbref_base.py — RENDER-SAFE version.

Render NEVER scrapes FBref. It reads parquet snapshots stored by running:
    python -m scripts.admin_server   (on your local machine)

International competitions (UCL, UEL, UECL, EC, WC):
    No dedicated snapshot is scraped for these. Instead, ATHENA looks up
    each team across ALL available domestic league snapshots and uses their
    recent domestic form as the feature foundation.
"""
from __future__ import annotations

import io
import math
from datetime import date, datetime, timedelta
from typing import Dict, Optional, Tuple

import pandas as pd

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Constants ─────────────────────────────────────────────────────────────────
ROLLING_MATCHES = 10
MIN_MATCHES = 5

INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}


# ── Helpers ───────────────────────────────────────────────────────────────────
def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def _load_snapshot(league_code: str) -> Optional[pd.DataFrame]:
    db = SessionLocal()
    try:
        row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if row is None:
            print(f"[fbref_base] No snapshot in DB for {league_code}.")
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


def _load_all_snapshots() -> list[pd.DataFrame]:
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
        print(f"[fbref_base] Loaded {len(result)} snapshots for intl lookup.")
        return result
    finally:
        db.close()


def _parse_score_column(df: pd.DataFrame, score_col: str) -> pd.DataFrame:
    """
    FBref stores scores as '2–1' or '2-1' in a single column.
    Split this into hg (home goals) and ag (away goals) integer columns.
    Rows without a valid score are dropped.
    """
    df = df.copy()

    # Normalise dash variants
    df["_score_clean"] = df[score_col].astype(str).str.replace("–", "-", regex=False)

    # Only keep rows with a valid score like '2-1'
    mask = df["_score_clean"].str.match(r"^\d+\s*-\s*\d+$", na=False)
    df = df[mask].copy()

    if df.empty:
        return df

    split = df["_score_clean"].str.split("-", expand=True)
    df["hg"] = pd.to_numeric(split[0].str.strip(), errors="coerce").fillna(0).astype(int)
    df["ag"] = pd.to_numeric(split[1].str.strip(), errors="coerce").fillna(0).astype(int)
    df = df.drop(columns=["_score_clean"])

    return df


def _resolve_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
    """
    Map logical names to actual dataframe column names.
    Handles both FBref's raw format (Home/Away/Score)
    and pre-processed formats (home_goals, away_goals etc.)
    """
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
        "hg":    col("hg", "home_goals", "score_home", "goals_home"),
        "ag":    col("ag", "away_goals", "score_away", "goals_away"),
        "score": col("score", "scores"),
        "comp":  col("comp", "competition"),
        "soth":  col("home_shots_on_target", "shots_on_target_home", "sot_home"),
        "sota":  col("away_shots_on_target", "shots_on_target_away", "sot_away"),
    }


def _prepare_df(df: pd.DataFrame, c: Dict) -> Optional[pd.DataFrame]:
    """
    Ensure the dataframe has hg/ag columns.
    If only a Score column exists, parse it into hg/ag.
    Returns None if we can't get goal data.
    """
    # Already has goal columns
    if c["hg"] and c["ag"]:
        return df

    # Has a Score column — parse it
    if c["score"]:
        print(f"[fbref_base] Parsing Score column into hg/ag...")
        df = _parse_score_column(df, c["score"])
        if df.empty:
            print("[fbref_base] No valid score rows after parsing.")
            return None
        # Refresh column map after adding hg/ag
        return df

    print("[fbref_base] No goal or score columns found.")
    return None


def _find_team_rows(df: pd.DataFrame, team_lc: str, cutoff: datetime,
                    c: Dict) -> pd.DataFrame:
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
) -> Dict[str, float]:
    """Core feature computation given last-N match frames."""

    # Resolve columns on the prepared df (which now has hg/ag)
    cols = {c.lower(): c for c in full_df.columns}

    def col(*names):
        for n in names:
            if n in cols:
                return cols[n]
        return None

    c_ht   = col("home", "home_team")
    c_at   = col("away", "away_team")
    c_hg   = col("hg", "home_goals", "score_home", "goals_home")
    c_ag   = col("ag", "away_goals", "score_away", "goals_away")
    c_soth = col("home_shots_on_target", "shots_on_target_home", "sot_home")
    c_sota = col("away_shots_on_target", "shots_on_target_away", "sot_away")

    def goals_fa(frame: pd.DataFrame, team_lc: str) -> Tuple[float, float]:
        gf = ga = 0
        for _, r in frame.iterrows():
            is_home = str(r[c_ht]).lower() == team_lc
            hg = int(r[c_hg]) if pd.notnull(r[c_hg]) else 0
            ag = int(r[c_ag]) if pd.notnull(r[c_ag]) else 0
            gf += hg if is_home else ag
            ga += ag if is_home else hg
        n = len(frame)
        return (gf / n, ga / n) if n else (0.0, 0.0)

    gfh, _ = goals_fa(H, hname)
    gfa, _ = goals_fa(A, aname)

    # SoT ratio
    ratio_sot = 3.2
    if c_soth and c_sota and c_hg and c_ag:
        tmp = full_df[[c_hg, c_ag, c_soth, c_sota]].copy()
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
        (full_df[c_hg].fillna(0) + full_df[c_ag].fillna(0)).mean() or 2.5
    ) if c_hg and c_ag else 2.5

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
    snapshots = _load_all_snapshots()
    if not snapshots:
        return {}

    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)
    hname  = _norm(home_team)
    aname  = _norm(away_team)

    def best_frame_for_team(team_lc: str):
        best_rows = pd.DataFrame()
        best_full = None
        best_c    = None
        for snap in snapshots:
            c = _resolve_columns(snap)
            prepared = _prepare_df(snap, c)
            if prepared is None:
                continue
            c2 = _resolve_columns(prepared)
            if not all([c2["date"], c2["ht"], c2["at"], c2["hg"], c2["ag"]]):
                continue
            rows = _find_team_rows(prepared, team_lc, cutoff, c2)
            if len(rows) > len(best_rows):
                best_rows = rows
                best_full = prepared
                best_c    = c2
        return best_rows, best_full, best_c

    H, full_H, _ = best_frame_for_team(hname)
    A, full_A, _ = best_frame_for_team(aname)

    if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
        print(f"[fbref_base] Intl fallback: not enough data — "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

    print(f"[fbref_base] Intl fallback OK — "
          f"{home_team} ({len(H)} rows), {away_team} ({len(A)} rows)")

    return _compute_features_from_frames(H, A, hname, aname, full_H)


# ── Public API ────────────────────────────────────────────────────────────────
def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
) -> Dict[str, float]:
    """
    Main entry point called for every match prediction.
    Returns {} if data unavailable — callers handle gracefully.
    """

    # International path
    if league_code in INTL_LEAGUE_CODES:
        print(f"[fbref_base] Intl competition ({league_code}) — using domestic fallback.")
        return _asof_features_intl(home_team, away_team, match_date)

    # Domestic path
    df = _load_snapshot(league_code)
    if df is None or df.empty:
        return {}

    c = _resolve_columns(df)

    # Ensure we have goal data (parse Score column if needed)
    df = _prepare_df(df, c)
    if df is None:
        return {}

    # Refresh column map after potential score parsing
    c = _resolve_columns(df)

    if not all([c["date"], c["ht"], c["at"], c["hg"], c["ag"]]):
        print("[fbref_base] Still missing essential columns after prepare.")
        return {}

    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)
    work   = df.copy()
    work[c["date"]] = pd.to_datetime(work[c["date"]], errors="coerce")
    work = work[work[c["date"]] < cutoff]

    if work.empty:
        print("[fbref_base] No matches before cutoff date.")
        return {}

    hname = _norm(home_team)
    aname = _norm(away_team)

    H = _find_team_rows(df, hname, cutoff, c)
    A = _find_team_rows(df, aname, cutoff, c)

    if len(H) < MIN_MATCHES or len(A) < MIN_MATCHES:
        print(f"[fbref_base] Not enough matches — "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

    print(f"[fbref_base] Computing features: "
          f"{home_team} ({len(H)} rows), {away_team} ({len(A)} rows)")

    return _compute_features_from_frames(H, A, hname, aname, work)
