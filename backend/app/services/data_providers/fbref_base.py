"""
fbref_base.py — RENDER-SAFE version.

Render NEVER scrapes FBref. It reads parquet snapshots stored by running:
    python -m scripts.admin_server   (on your local machine)

Team name matching:
  1. Exact normalised match (lowercase, stripped)
  2. Accent-stripped match  (Atlético → atletico)
  3. Fuzzy match via difflib (cutoff 0.82) — handles typos, suffixes like (RJ)

International competitions (UCL, UEL, UECL, EC, WC):
    No dedicated snapshot is scraped. ATHENA searches all domestic snapshots
    for each team's recent club form.

v2.2 additions:
  - _SNAPSHOT_OVERRIDE: dict injected by feature_cache.py to eliminate
    repeated parquet reads during calibration loops (Level 1 speedup).
  - Home/away venue splits: gfh blends home team's home-game scoring rate;
    gfa blends away team's away-game scoring rate. More accurate than using
    all games equally.
  - DEG/DET/EPS now computed from rolling data instead of returning None:
    deg_pressure  — recent defensive deterioration trend (last 3 vs last 10)
    home_det      — home team volatility (std dev of total goals in their matches)
    away_det      — away team volatility
    det_boost     — combined match volatility
    eps_stability — combined consistency (inverse of coefficient of variation)
"""
from __future__ import annotations

import io
import math
import unicodedata
from datetime import date, datetime, timedelta
from difflib import get_close_matches
from typing import Dict, List, Optional, Tuple

import pandas as pd

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Constants ─────────────────────────────────────────────────────────────────
ROLLING_MATCHES  = 10
MIN_MATCHES      = 5   # default; calibration can lower this
FUZZY_CUTOFF     = 0.82

INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}

# Historical average goals/game per competition — used as baseline for
# support_idx_over_delta when the league code is international.
INTL_GOAL_AVERAGES: Dict[str, float] = {
    "UCL":  2.70,
    "UEL":  2.50,
    "UECL": 2.40,
    "EC":   2.25,
    "WC":   2.30,
}

# ── Feature cache injection ───────────────────────────────────────────────────
# Set by feature_cache.warm_snapshot_cache() before a calibration loop.
# Maps league_code → pre-loaded pd.DataFrame, skipping the DB read inside
# asof_features for every call in that loop (Level 1 speedup).
# Do not modify this directly — use feature_cache.py's public API.
_SNAPSHOT_OVERRIDE: dict = {}


# ── Name normalisation ────────────────────────────────────────────────────────
def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _norm_accent(s: Optional[str]) -> str:
    return _strip_accents(_norm(s or ""))


def _match_team(target: str, candidates: List[str]) -> Optional[str]:
    t_norm   = _norm(target)
    t_accent = _norm_accent(target)
    norm_map   = {_norm(c): c for c in candidates}
    accent_map = {_norm_accent(c): c for c in candidates}

    if t_norm in norm_map:
        return norm_map[t_norm]
    if t_accent in accent_map:
        return accent_map[t_accent]

    accent_keys = list(accent_map.keys())
    close = get_close_matches(t_accent, accent_keys, n=1, cutoff=FUZZY_CUTOFF)
    if close:
        return accent_map[close[0]]

    return None


# ── Helpers ───────────────────────────────────────────────────────────────────
def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def _load_snapshot(league_code: str) -> Optional[pd.DataFrame]:
    # Check in-memory override first (injected by feature_cache)
    if league_code in _SNAPSHOT_OVERRIDE:
        return _SNAPSHOT_OVERRIDE[league_code].copy()

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
    df = df.copy()
    df["_score_clean"] = df[score_col].astype(str).str.replace("–", "-", regex=False)
    mask = df["_score_clean"].str.match(r"^\d+\s*-\s*\d+$", na=False)
    df   = df[mask].copy()
    if df.empty:
        return df
    split = df["_score_clean"].str.split("-", expand=True)
    df["hg"] = pd.to_numeric(split[0].str.strip(), errors="coerce").fillna(0).astype(int)
    df["ag"] = pd.to_numeric(split[1].str.strip(), errors="coerce").fillna(0).astype(int)
    df = df.drop(columns=["_score_clean"])
    return df


def _resolve_columns(df: pd.DataFrame) -> Dict[str, Optional[str]]:
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
        "soth":  col("home_shots_on_target", "shots_on_target_home", "sot_home"),
        "sota":  col("away_shots_on_target", "shots_on_target_away", "sot_away"),
    }


def _prepare_df(df: pd.DataFrame, c: Dict) -> Optional[pd.DataFrame]:
    if c["hg"] and c["ag"]:
        return df
    if c["score"]:
        print(f"[fbref_base] Parsing Score column into hg/ag...")
        df = _parse_score_column(df, c["score"])
        if df.empty:
            print("[fbref_base] No valid score rows after parsing.")
            return None
        return df
    print("[fbref_base] No goal or score columns found.")
    return None


def _find_team_rows(
    df: pd.DataFrame,
    team: str,
    cutoff: datetime,
    c: Dict,
) -> pd.DataFrame:
    """Find last ROLLING_MATCHES for team (home OR away) before cutoff."""
    if not all([c["date"], c["ht"], c["at"]]):
        return pd.DataFrame()

    work = df.copy()
    work[c["date"]] = pd.to_datetime(work[c["date"]], errors="coerce")
    work = work[work[c["date"]] < cutoff]

    all_teams = list(set(
        work[c["ht"]].astype(str).tolist() +
        work[c["at"]].astype(str).tolist()
    ))
    matched = _match_team(team, all_teams)
    if matched is None:
        return pd.DataFrame()

    matched_norm = _norm(matched)
    d1 = work[work[c["ht"]].astype(str).apply(_norm) == matched_norm]
    d2 = work[work[c["at"]].astype(str).apply(_norm) == matched_norm]

    return (
        pd.concat([d1, d2])
        .sort_values(c["date"], ascending=False)
        .head(ROLLING_MATCHES)
    )


def _find_venue_rows(
    df: pd.DataFrame,
    team: str,
    cutoff: datetime,
    c: Dict,
    venue: str,   # "home" or "away"
    n: int = ROLLING_MATCHES,
) -> pd.DataFrame:
    """
    Find last N matches for team in a specific venue context before cutoff.

    venue="home" → rows where team is the home side.
    venue="away" → rows where team is the away side.
    Returns empty DataFrame if fewer than 2 venue-specific matches found.
    """
    if not all([c["date"], c["ht"], c["at"]]):
        return pd.DataFrame()

    work = df.copy()
    work[c["date"]] = pd.to_datetime(work[c["date"]], errors="coerce")
    work = work[work[c["date"]] < cutoff]

    all_teams = list(set(
        work[c["ht"]].astype(str).tolist() +
        work[c["at"]].astype(str).tolist()
    ))
    matched = _match_team(team, all_teams)
    if matched is None:
        return pd.DataFrame()

    matched_norm = _norm(matched)
    col_key = c["ht"] if venue == "home" else c["at"]
    rows = work[work[col_key].astype(str).apply(_norm) == matched_norm]
    return rows.sort_values(c["date"], ascending=False).head(n)


# ── Rolling metric helpers ────────────────────────────────────────────────────

def _goals_per_game(
    rows: pd.DataFrame,
    team_norm: str,
    c_ht: str,
    c_hg: str,
    c_ag: str,
    metric: str = "scored",
) -> float:
    """
    Compute goals scored or conceded per game for a team from a match frame.
    metric: "scored" or "conceded"
    """
    if rows.empty:
        return 0.0
    total = 0
    for _, r in rows.iterrows():
        is_home = _norm(str(r[c_ht])) == team_norm
        hg = int(r[c_hg]) if pd.notnull(r[c_hg]) else 0
        ag = int(r[c_ag]) if pd.notnull(r[c_ag]) else 0
        if metric == "scored":
            total += hg if is_home else ag
        else:
            total += ag if is_home else hg
    return total / len(rows)


def _compute_deg_pressure(
    h_rows: pd.DataFrame,
    a_rows: pd.DataFrame,
    h_norm: str,
    a_norm: str,
    c_ht: str,
    c_hg: str,
    c_ag: str,
    n_recent: int = 3,
) -> float:
    """
    DEG pressure: measures recent defensive deterioration for both teams.

    Computed as the average of each team's trend in goals conceded:
    trend = (last n_recent conceded/game) − (rolling 10 conceded/game)

    Positive result means both teams are conceding more recently → elevated
    degradation pressure → higher probability of goals.
    Range: 0.0–0.80 (clipped).
    """
    def trend(rows: pd.DataFrame, norm: str) -> float:
        if len(rows) < n_recent + 1:
            return 0.0
        ga_recent  = _goals_per_game(rows.head(n_recent), norm, c_ht, c_hg, c_ag, "conceded")
        ga_rolling = _goals_per_game(rows, norm, c_ht, c_hg, c_ag, "conceded")
        return ga_recent - ga_rolling

    h_trend = trend(h_rows, h_norm)
    a_trend = trend(a_rows, a_norm)

    # Average trend, weighted towards positive (degradation) signal
    combined = (h_trend + a_trend) / 2.0
    return round(_clip(combined * 0.6, 0.0, 0.80), 3)


def _compute_team_det(
    rows: pd.DataFrame,
    c_hg: str,
    c_ag: str,
) -> float:
    """
    DET (Detonation) for a single team: normalised std dev of total goals
    in their recent matches.  Higher = more volatile/chaotic matches.
    Range: 0.10–0.80.
    """
    if len(rows) < 4:
        return 0.30

    totals = []
    for _, r in rows.iterrows():
        hg = int(r[c_hg]) if pd.notnull(r[c_hg]) else 0
        ag = int(r[c_ag]) if pd.notnull(r[c_ag]) else 0
        totals.append(hg + ag)

    if not totals:
        return 0.30

    mean_g  = sum(totals) / len(totals)
    std_dev = (sum((x - mean_g) ** 2 for x in totals) / len(totals)) ** 0.5

    # Typical std_dev for football total goals ≈ 1.0–2.0
    return round(_clip(std_dev / 2.5, 0.10, 0.80), 3)


def _compute_eps_stability(
    h_rows: pd.DataFrame,
    a_rows: pd.DataFrame,
    c_hg: str,
    c_ag: str,
) -> float:
    """
    EPS (Epsilon) stability: combined consistency of both teams' matches.
    Derived from the coefficient of variation (std/mean) of total goals.
    Low CV → high stability (high EPS). High CV → low stability (low EPS).
    Range: 0.35–0.95.
    """
    all_rows = pd.concat([h_rows, a_rows]).drop_duplicates()
    if len(all_rows) < 4:
        return 0.65

    totals = []
    for _, r in all_rows.iterrows():
        hg = int(r[c_hg]) if pd.notnull(r[c_hg]) else 0
        ag = int(r[c_ag]) if pd.notnull(r[c_ag]) else 0
        totals.append(hg + ag)

    if not totals:
        return 0.65

    mean_g = sum(totals) / len(totals)
    if mean_g < 0.5:
        return 0.65

    std_dev = (sum((x - mean_g) ** 2 for x in totals) / len(totals)) ** 0.5
    cv      = std_dev / mean_g  # coefficient of variation

    # CV ≈ 0.6 is typical for football. Low CV = more predictable.
    eps = 1.0 - _clip(cv * 0.55, 0.05, 0.60)
    return round(_clip(eps, 0.35, 0.95), 3)


# ── Core feature computation ──────────────────────────────────────────────────

def _compute_features_from_frames(
    H: pd.DataFrame,
    A: pd.DataFrame,
    hname: str,
    aname: str,
    full_df: pd.DataFrame,
    league_code: Optional[str] = None,
    H_home: Optional[pd.DataFrame] = None,
    A_away: Optional[pd.DataFrame] = None,
) -> Dict[str, float]:
    """
    Core feature computation given last-N match frames.

    H, A:        all recent matches (home+away) for each team.
    H_home:      home team's recent HOME games specifically (optional).
    A_away:      away team's recent AWAY games specifically (optional).
    full_df:     full snapshot used to derive league averages.
    league_code: when in INTL_LEAGUE_CODES pins the goal baseline.

    v2.2: home/away venue splits blend venue-specific scoring rates into
    gfh/gfa for better tempo and support delta accuracy.
    v2.2: DEG/DET/EPS computed from rolling data, no longer returning None.
    """
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

    if not all([c_ht, c_at, c_hg, c_ag]):
        return {}

    # ── Find resolved team names in the frame ─────────────────────────
    all_home = list(set(
        H[c_ht].astype(str).tolist() + H[c_at].astype(str).tolist() +
        A[c_ht].astype(str).tolist() + A[c_at].astype(str).tolist()
    ))
    h_matched = _match_team(hname, all_home) or hname
    a_matched = _match_team(aname, all_home) or aname
    h_norm = _norm(h_matched)
    a_norm = _norm(a_matched)

    # ── All-game scoring rates ─────────────────────────────────────────
    gfh = _goals_per_game(H, h_norm, c_ht, c_hg, c_ag, "scored")
    gfa = _goals_per_game(A, a_norm, c_ht, c_hg, c_ag, "scored")

    # ── Venue-specific blend (v2.2) ────────────────────────────────────
    # The home team's attack is more accurately reflected by their HOME
    # game scoring rate; the away team's by their AWAY rate.
    # Blend 65% all-game + 35% venue-specific when enough venue data exists.
    VENUE_BLEND  = 0.35
    VENUE_MIN    = 3     # minimum venue-specific games to activate blend

    if H_home is not None and len(H_home) >= VENUE_MIN:
        gfh_home = _goals_per_game(H_home, h_norm, c_ht, c_hg, c_ag, "scored")
        gfh = gfh * (1 - VENUE_BLEND) + gfh_home * VENUE_BLEND

    if A_away is not None and len(A_away) >= VENUE_MIN:
        gfa_away = _goals_per_game(A_away, a_norm, c_ht, c_hg, c_ag, "scored")
        gfa = gfa * (1 - VENUE_BLEND) + gfa_away * VENUE_BLEND

    # ── SoT ratio ─────────────────────────────────────────────────────
    ratio_sot = 3.2
    if c_soth and c_sota:
        tmp = full_df[[c_hg, c_ag, c_soth, c_sota]].copy()
        tmp["goals"] = tmp[c_hg].fillna(0) + tmp[c_ag].fillna(0)
        tmp["sot"]   = tmp[c_soth].fillna(0) + tmp[c_sota].fillna(0)
        agg = tmp[tmp["goals"] > 0]
        if not agg.empty and agg["goals"].sum() > 0:
            ratio_sot = _clip(agg["sot"].sum() / agg["goals"].sum(), 2.2, 4.5)

    # ── Core match features ────────────────────────────────────────────
    mu_total   = max(0.2, gfh + gfa)
    p0         = math.exp(-mu_total)
    p1         = mu_total * p0
    p_two_plus = 1.0 - (p0 + p1)

    if league_code and league_code in INTL_GOAL_AVERAGES:
        league_mu = INTL_GOAL_AVERAGES[league_code]
    else:
        league_mu = float(
            (full_df[c_hg].fillna(0) + full_df[c_ag].fillna(0)).mean() or 2.5
        )

    # ── DEG / DET / EPS from rolling data (v2.2) ──────────────────────
    deg_pressure  = _compute_deg_pressure(H, A, h_norm, a_norm, c_ht, c_hg, c_ag)
    home_det      = _compute_team_det(H, c_hg, c_ag)
    away_det      = _compute_team_det(A, c_hg, c_ag)
    det_boost     = round((home_det + away_det) / 2.0, 3)
    eps_stability = _compute_eps_stability(H, A, c_hg, c_ag)

    return {
        # Core features
        "p_two_plus":             round(float(p_two_plus), 3),
        "p_home_tt05":            round(float(1.0 - _poisson_p0(gfh)), 3),
        "p_away_tt05":            round(float(1.0 - _poisson_p0(gfa)), 3),
        "tempo_index":            round(_clip(mu_total / 3.0, 0.2, 0.9), 3),
        "sot_proj_total":         round(_clip(mu_total * ratio_sot, 6.0, 16.0), 2),
        "support_idx_over_delta": round(_clip((mu_total - league_mu) * 0.12, -0.15, 0.15), 3),
        # v2.2: module features now computed from rolling data
        "deg_pressure":           deg_pressure,
        "home_det":               home_det,
        "away_det":               away_det,
        "det_boost":              det_boost,
        "eps_stability":          eps_stability,
        # Diagnostic/optional: raw scoring rates for transparency
        "h_scoring_rate":         round(gfh, 3),
        "a_scoring_rate":         round(gfa, 3),
    }


# ── International fallback ────────────────────────────────────────────────────
def _asof_features_intl(
    home_team: str,
    away_team: str,
    match_date: date,
    league_code: str,
    min_matches: int = MIN_MATCHES,
) -> Dict[str, float]:
    snapshots = _load_all_snapshots()
    if not snapshots:
        return {}

    cutoff = datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)

    def best_frame_for_team(team: str):
        best_rows = pd.DataFrame()
        best_full = None
        for snap in snapshots:
            c = _resolve_columns(snap)
            prepared = _prepare_df(snap, c)
            if prepared is None:
                continue
            c2 = _resolve_columns(prepared)
            if not all([c2["date"], c2["ht"], c2["at"], c2["hg"], c2["ag"]]):
                continue
            rows = _find_team_rows(prepared, team, cutoff, c2)
            if len(rows) > len(best_rows):
                best_rows = rows
                best_full = prepared
        return best_rows, best_full

    H, full_H = best_frame_for_team(home_team)
    A, _      = best_frame_for_team(away_team)

    if len(H) < min_matches or len(A) < min_matches:
        print(f"[fbref_base] Intl fallback: not enough data — "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

    print(f"[fbref_base] Intl fallback OK — "
          f"{home_team} ({len(H)} rows), {away_team} ({len(A)} rows)")

    # Venue frames not computed for intl (cross-snapshot complexity)
    return _compute_features_from_frames(
        H, A, home_team, away_team, full_H, league_code=league_code,
    )


# ── Match existence validator ─────────────────────────────────────────────────
def validate_match_existed(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
) -> Tuple[bool, Optional[str]]:
    if league_code in INTL_LEAGUE_CODES:
        return True, None

    df = _load_snapshot(league_code)
    if df is None or df.empty:
        return False, f"No snapshot available for {league_code}. Run the scraper first."

    c = _resolve_columns(df)
    df = _prepare_df(df, c)
    if df is None:
        return False, f"Snapshot for {league_code} could not be parsed."

    c = _resolve_columns(df)
    if not all([c["date"], c["ht"], c["at"]]):
        return False, "Snapshot missing required columns."

    df[c["date"]] = pd.to_datetime(df[c["date"]], errors="coerce")
    day_matches = df[df[c["date"]].dt.date == match_date]

    if day_matches.empty:
        return False, f"No matches found in {league_code} on {match_date}. Check the date."

    all_teams = list(set(
        day_matches[c["ht"]].astype(str).tolist() +
        day_matches[c["at"]].astype(str).tolist()
    ))

    matched_home = _match_team(home_team, all_teams)
    matched_away = _match_team(away_team, all_teams)

    if not matched_home:
        return False, f"{home_team} did not play in {league_code} on {match_date}."
    if not matched_away:
        return False, f"{away_team} did not play in {league_code} on {match_date}."

    h_norm = _norm(matched_home)
    a_norm = _norm(matched_away)

    fixture_exists = any(
        _norm(str(r[c["ht"]])) == h_norm and _norm(str(r[c["at"]])) == a_norm
        for _, r in day_matches.iterrows()
    )

    if not fixture_exists:
        return False, (
            f"{home_team} and {away_team} did not play each other on {match_date}. "
            f"They may have played on that date but against different opponents."
        )

    return True, None


# ── Main entry point ──────────────────────────────────────────────────────────
def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    min_matches: int = MIN_MATCHES,
) -> Dict[str, float]:
    """
    Main entry point for every match prediction.
    Returns {} if data unavailable — callers handle gracefully.

    min_matches: lower this for calibration runs to reduce early-season skips.

    v2.2: checks _SNAPSHOT_OVERRIDE before hitting the DB (Level 1 cache),
    computes venue-specific scoring rate blends, and returns DEG/DET/EPS
    from rolling data instead of returning None/defaults.
    """
    if league_code in INTL_LEAGUE_CODES:
        print(f"[fbref_base] Intl competition ({league_code}) — using domestic fallback.")
        return _asof_features_intl(home_team, away_team, match_date, league_code, min_matches)

    df = _load_snapshot(league_code)
    if df is None or df.empty:
        return {}

    c  = _resolve_columns(df)
    df = _prepare_df(df, c)
    if df is None:
        return {}

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

    H = _find_team_rows(df, home_team, cutoff, c)
    A = _find_team_rows(df, away_team, cutoff, c)

    if len(H) < min_matches or len(A) < min_matches:
        print(f"[fbref_base] Not enough matches — "
              f"{home_team}={len(H)}, {away_team}={len(A)}")
        return {}

    # ── Venue-specific frames (v2.2) ──────────────────────────────────
    # Find home team's recent home games and away team's recent away games.
    # Used to blend venue-specific scoring rates into gfh/gfa.
    H_home = _find_venue_rows(df, home_team, cutoff, c, venue="home")
    A_away = _find_venue_rows(df, away_team, cutoff, c, venue="away")

    print(f"[fbref_base] Computing features: "
          f"{home_team} ({len(H)} all, {len(H_home)} home), "
          f"{away_team} ({len(A)} all, {len(A_away)} away)")

    return _compute_features_from_frames(
        H, A, home_team, away_team, work,
        league_code=league_code,
        H_home=H_home,
        A_away=A_away,
    )
