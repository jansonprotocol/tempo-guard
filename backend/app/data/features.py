"""
As-of feature computation — the engine's input layer.

Given a fixture (league, home, away, date) this produces the point-in-time
feature bundle `evaluate_athena` consumes, derived from each team's last
`ROLLING_MATCHES` games *strictly before* the match date. Nothing after the
cutoff is ever read, so retrosim and calibration are free of lookahead bias.

Ported from the FBref snapshot reader; the feature maths and its tuned
constants are unchanged. What changed is where matches come from: parquet
snapshots in `data/` (see app.data.store) rather than a Postgres table.

Features produced
-----------------
    p_two_plus              Poisson P(total goals >= 2)
    p_home_tt05             P(home team scores)      — team-total over 0.5
    p_away_tt05             P(away team scores)
    tempo_index             normalised expected goal volume
    sot_proj_total          projected shots on target (estimated, see note)
    support_idx_over_delta  expected goals vs league average
    deg_pressure            recent defensive deterioration across both teams
    home_det / away_det     per-team volatility (std dev of match totals)
    det_boost               combined volatility
    eps_stability           consistency of goal totals (inverse CV)

NOTE ON SHOTS ON TARGET. openfootball publishes goals, not shot counts, so
`sot_proj_total` is derived from expected goals via a league-typical
shots-per-goal ratio (SOT_PER_GOAL). It is an estimate, not a measurement. It
feeds only the strict O2.5 add-on gate; every other feature is goal-derived and
therefore exact.
"""
from __future__ import annotations

import math
import re
import unicodedata
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process

from app.data import store

# ── Constants (unchanged from the tuned FBref implementation) ─────────────────
ROLLING_MATCHES = 10
MIN_MATCHES     = 5     # calibration may lower this to reduce early-season skips

# Fuzzy cutoff on token_set_ratio over *canonicalised* names (see _canonical).
# Canonicalisation removes the club-suffix noise that separates "Arsenal" from
# "Arsenal FC", so genuine variants land at or near 100 while different clubs
# score far below. Measured on real openfootball names: true variants >= 100,
# nearest wrong-club pair ("Sunderland" vs "Sheffield United") 38.5 — so 88 sits
# in a wide safe band.
#
# This deliberately favours failing loudly over matching loosely. An earlier
# WRatio-based version silently resolved "Coventry City FC" to "Chelsea FC",
# which would have produced a confident tip built on the wrong club's form.
# A missing tip is recoverable; a wrong one is not.
FUZZY_CUTOFF    = 88

# Club-name decoration that carries no identity: legal forms, sponsor prefixes
# and the like. Stripped before comparison so "AFC Bournemouth", "Bournemouth"
# and "Bournemouth FC" collapse to the same key.
_CLUB_TOKENS = {
    "fc", "afc", "cf", "sc", "ac", "bc", "sk", "fk", "sv", "vfb", "vfl", "tsg",
    "rc", "as", "ss", "ssc", "us", "aj", "ogc", "rcd", "cd", "ud", "sd", "cfc",
    "club", "calcio", "nk", "if", "bk", "de", "cp", "sl", "psv", "bv", "ssv",
    "fsv", "msv", "spvgg", "kv", "rsc", "kaa", "aa", "asd", "acf", "aca",
}

VENUE_BLEND = 0.35      # weight of venue-specific scoring rate in gfh/gfa
VENUE_MIN   = 3         # minimum venue-specific games before blending

# Shots on target per goal — league-typical ratio used to estimate SoT from
# expected goals, since the source data carries no shot counts.
SOT_PER_GOAL = 3.2

# Tempo normalisation. mu_total (expected match goals) realistically spans about
# 1.5-4.5; mapping that onto 0-1 keeps the signal spread out instead of pinned
# at a ceiling. See the tempo_index note in _compute_features.
TEMPO_BASE = 1.5
TEMPO_SPAN = 3.0

INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}

# Historical goals/game baselines for competitions whose own history is too
# short or too uneven to derive a stable league average from.
INTL_GOAL_AVERAGES: Dict[str, float] = {
    "UCL": 2.70, "UEL": 2.50, "UECL": 2.40, "EC": 2.25, "WC": 2.30,
}

# Domestic leagues searched when resolving a cup team's recent club form.
_DOMESTIC_FALLBACK = [
    "ENG-PL", "ESP-LL", "GER-BL", "ITA-SA", "FRA-L1",
    "NED-ED", "POR-PL", "ENG-CH", "GER-B2", "ESP-L2", "ITA-SB", "FRA-L2",
]


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


def _canonical(name: str) -> str:
    """
    Reduce a club name to its identifying core: lowercased, accent-free,
    punctuation-free, with generic club tokens removed.

        "AFC Bournemouth"          -> "bournemouth"
        "Brighton & Hove Albion FC"-> "brighton hove albion"
        "Atlético Madrid"          -> "atletico madrid"

    Falls back to the undecorated name if stripping would empty the string
    (e.g. a club literally named "PSV").
    """
    s = _strip_accents(name.lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = [t for t in s.split() if t not in _CLUB_TOKENS]
    return " ".join(tokens) if tokens else " ".join(s.split())


def _match_team(target: str, candidates: List[str]) -> Optional[str]:
    """
    Resolve a team name against the names present in a dataset.

    Exact, then accent-insensitive, then canonical-exact, then a strict fuzzy
    pass over canonical forms. Returns None rather than guessing when nothing
    clears FUZZY_CUTOFF — callers treat that as "no data for this team", which
    surfaces as a skipped fixture instead of a tip built on the wrong club.
    """
    t_norm = _norm(target)
    t_accent = _norm_accent(target)
    norm_map = {_norm(c): c for c in candidates}
    accent_map = {_norm_accent(c): c for c in candidates}

    if t_norm in norm_map:
        return norm_map[t_norm]
    if t_accent in accent_map:
        return accent_map[t_accent]

    # Canonical exact — resolves pure club-suffix drift with no fuzziness at all.
    canon_map: dict[str, str] = {}
    for c in candidates:
        canon_map.setdefault(_canonical(c), c)
    t_canon = _canonical(target)
    if t_canon in canon_map:
        return canon_map[t_canon]

    keys = list(canon_map.keys())
    if not keys:
        return None
    best = process.extractOne(t_canon, keys, scorer=fuzz.token_set_ratio,
                              score_cutoff=FUZZY_CUTOFF)
    return canon_map[best[0]] if best else None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def _cutoff(match_date: date) -> datetime:
    return datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)


def _team_names(df: pd.DataFrame) -> List[str]:
    return list(set(df["home"].astype(str)) | set(df["away"].astype(str)))


# ── Per-frame index cache ─────────────────────────────────────────────────────
# Replaying a league used to be quadratic: every match re-normalised both team
# columns with a Python-level .map() over the whole frame and rebuilt the team
# name list before fuzzy-matching. On a single season that was tolerable; across
# 27 seasons of history it made a replay unusable.
#
# Instead each frame is indexed once — normalised name columns, the set of names
# present, and a per-team row index — and every subsequent lookup is a dict hit
# plus a date filter.
#
# The cache is keyed by id(df) and *keeps a reference to the frame itself*. That
# reference is load-bearing: without it a frame could be collected and a new one
# allocated at the same address, and the stale index would silently be served
# for the wrong data. Holding the frame keeps every live id unique.
#
# (Stashing the index on df.attrs looks tidier but breaks pandas: attrs
# propagate to slices, and concat compares attrs dicts, which raises as soon as
# the dict contains array values.)
_INDEX_CACHE: dict[int, dict] = {}
_MAX_CACHED_FRAMES = 8


def _frame_index(df: pd.DataFrame) -> dict:
    cached = _INDEX_CACHE.get(id(df))
    if cached is not None and cached["n_rows"] == len(df):
        return cached

    home_norm = df["home"].astype(str).map(_norm)
    away_norm = df["away"].astype(str).map(_norm)
    names = _team_names(df)

    # team_norm -> positional rows where that team played, home or away
    by_team: dict[str, "pd.Index"] = {}
    for norm_name in set(home_norm) | set(away_norm):
        by_team[norm_name] = df.index[(home_norm == norm_name) | (away_norm == norm_name)]

    index = {
        "n_rows": len(df),
        "frame": df,             # keeps id(df) alive and unique — see above
        "home_norm": home_norm,
        "away_norm": away_norm,
        "names": names,
        "by_team": by_team,
        "resolved": {},          # raw team name -> matched dataset name
    }
    if len(_INDEX_CACHE) >= _MAX_CACHED_FRAMES:
        _INDEX_CACHE.clear()
    _INDEX_CACHE[id(df)] = index
    return index


def _resolve_in_frame(df: pd.DataFrame, team: str) -> Optional[str]:
    """Match a team name against a frame, memoised per frame."""
    idx = _frame_index(df)
    if team not in idx["resolved"]:
        idx["resolved"][team] = _match_team(team, idx["names"])
    return idx["resolved"][team]


def _find_team_rows(df: pd.DataFrame, team: str, cutoff: datetime) -> pd.DataFrame:
    """Last ROLLING_MATCHES for a team (home or away) strictly before cutoff."""
    if df.empty:
        return df

    matched = _resolve_in_frame(df, team)
    if matched is None:
        return pd.DataFrame()

    idx = _frame_index(df)
    rows = df.loc[idx["by_team"].get(_norm(matched), df.index[:0])]
    rows = rows[rows["date"] < cutoff]
    return rows.sort_values("date", ascending=False).head(ROLLING_MATCHES)


def _find_venue_rows(
    df: pd.DataFrame, team: str, cutoff: datetime, venue: str,
) -> pd.DataFrame:
    """Last ROLLING_MATCHES for a team in a specific venue context."""
    if df.empty:
        return df

    matched = _resolve_in_frame(df, team)
    if matched is None:
        return pd.DataFrame()

    idx = _frame_index(df)
    col_norm = idx["home_norm"] if venue == "home" else idx["away_norm"]
    rows = df[(col_norm == _norm(matched)) & (df["date"] < cutoff)]
    return rows.sort_values("date", ascending=False).head(ROLLING_MATCHES)


# ── Rolling metrics ───────────────────────────────────────────────────────────

def _goals_per_game(rows: pd.DataFrame, team_norm: str, metric: str = "scored") -> float:
    if rows.empty:
        return 0.0
    total = 0
    for _, r in rows.iterrows():
        is_home = _norm(str(r["home"])) == team_norm
        hg = int(r["hg"]) if pd.notnull(r["hg"]) else 0
        ag = int(r["ag"]) if pd.notnull(r["ag"]) else 0
        if metric == "scored":
            total += hg if is_home else ag
        else:
            total += ag if is_home else hg
    return total / len(rows)


def _match_totals(rows: pd.DataFrame) -> List[int]:
    totals = []
    for _, r in rows.iterrows():
        hg = int(r["hg"]) if pd.notnull(r["hg"]) else 0
        ag = int(r["ag"]) if pd.notnull(r["ag"]) else 0
        totals.append(hg + ag)
    return totals


def _compute_deg_pressure(
    h_rows: pd.DataFrame, a_rows: pd.DataFrame,
    h_norm: str, a_norm: str, n_recent: int = 3,
) -> float:
    """
    Recent defensive deterioration for both teams: the trend in goals conceded
    (last 3 games vs the rolling 10). Positive means both sides are conceding
    more lately, raising the goal outlook. Range 0.0–0.80.
    """
    def trend(rows: pd.DataFrame, norm: str) -> float:
        if len(rows) < n_recent + 1:
            return 0.0
        recent = _goals_per_game(rows.head(n_recent), norm, "conceded")
        rolling = _goals_per_game(rows, norm, "conceded")
        return recent - rolling

    combined = (trend(h_rows, h_norm) + trend(a_rows, a_norm)) / 2.0
    return round(_clip(combined * 0.6, 0.0, 0.80), 3)


def _compute_team_det(rows: pd.DataFrame) -> float:
    """Per-team volatility: normalised std dev of match goal totals. 0.10–0.80."""
    if len(rows) < 4:
        return 0.30
    totals = _match_totals(rows)
    if not totals:
        return 0.30
    mean_g = sum(totals) / len(totals)
    std = (sum((x - mean_g) ** 2 for x in totals) / len(totals)) ** 0.5
    return round(_clip(std / 2.5, 0.10, 0.80), 3)


def _compute_eps_stability(h_rows: pd.DataFrame, a_rows: pd.DataFrame) -> float:
    """Combined consistency from the coefficient of variation. 0.35–0.95."""
    all_rows = pd.concat([h_rows, a_rows]).drop_duplicates()
    if len(all_rows) < 4:
        return 0.65
    totals = _match_totals(all_rows)
    if not totals:
        return 0.65
    mean_g = sum(totals) / len(totals)
    if mean_g < 0.5:
        return 0.65
    std = (sum((x - mean_g) ** 2 for x in totals) / len(totals)) ** 0.5
    cv = std / mean_g
    return round(_clip(1.0 - _clip(cv * 0.55, 0.05, 0.60), 0.35, 0.95), 3)


# ── Core feature computation ──────────────────────────────────────────────────

def _compute_features(
    H: pd.DataFrame, A: pd.DataFrame,
    hname: str, aname: str,
    full_df: pd.DataFrame,
    league_code: Optional[str] = None,
    H_home: Optional[pd.DataFrame] = None,
    A_away: Optional[pd.DataFrame] = None,
) -> Dict[str, float]:
    names = list(set(_team_names(H)) | set(_team_names(A)))
    h_norm = _norm(_match_team(hname, names) or hname)
    a_norm = _norm(_match_team(aname, names) or aname)

    gfh = _goals_per_game(H, h_norm, "scored")
    gfa = _goals_per_game(A, a_norm, "scored")

    # Venue blend: a home side's attack is better described by its home scoring
    # rate, an away side's by its away rate.
    if H_home is not None and len(H_home) >= VENUE_MIN:
        gfh = gfh * (1 - VENUE_BLEND) + _goals_per_game(H_home, h_norm, "scored") * VENUE_BLEND
    if A_away is not None and len(A_away) >= VENUE_MIN:
        gfa = gfa * (1 - VENUE_BLEND) + _goals_per_game(A_away, a_norm, "scored") * VENUE_BLEND

    mu_total = max(0.2, gfh + gfa)
    p0 = math.exp(-mu_total)
    p1 = mu_total * p0
    p_two_plus = 1.0 - (p0 + p1)

    if league_code and league_code in INTL_GOAL_AVERAGES:
        league_mu = INTL_GOAL_AVERAGES[league_code]
    else:
        league_mu = float(
            (full_df["hg"].fillna(0) + full_df["ag"].fillna(0)).mean() or 2.5
        )

    return {
        "p_two_plus":             round(float(p_two_plus), 3),
        "p_home_tt05":            round(float(1.0 - _poisson_p0(gfh)), 3),
        "p_away_tt05":            round(float(1.0 - _poisson_p0(gfa)), 3),
        # Tempo is normalised so a typical fixture lands near the middle of the
        # range. The previous mapping (mu/3.0 clipped at 0.9) saturated: league
        # means sit at 2.4-3.2 goals, so ~63% of matches pinned to the ceiling
        # and the signal was effectively constant. That starved every module
        # gated on low tempo — gate_b, ulr and mfr could fire on a handful of
        # matches per season rather than acting as real controls.
        # TEMPO_BASE is roughly the lowest realistic match total, TEMPO_SPAN the
        # working range above it.
        "tempo_index":            round(_clip((mu_total - TEMPO_BASE) / TEMPO_SPAN,
                                              0.05, 0.95), 3),
        "sot_proj_total":         round(_clip(mu_total * SOT_PER_GOAL, 6.0, 16.0), 2),
        "support_idx_over_delta": round(_clip((mu_total - league_mu) * 0.12, -0.15, 0.15), 3),
        "deg_pressure":           _compute_deg_pressure(H, A, h_norm, a_norm),
        "home_det":               _compute_team_det(H),
        "away_det":               _compute_team_det(A),
        "det_boost":              round((_compute_team_det(H) + _compute_team_det(A)) / 2.0, 3),
        "eps_stability":          _compute_eps_stability(H, A),
        # Diagnostics
        "h_scoring_rate":         round(gfh, 3),
        "a_scoring_rate":         round(gfa, 3),
        "league_mu":              round(league_mu, 3),
        "n_home_matches":         len(H),
        "n_away_matches":         len(A),
    }


# ── International fallback ────────────────────────────────────────────────────

def _asof_features_intl(
    home_team: str, away_team: str, match_date: date,
    league_code: str, min_matches: int,
) -> Dict[str, float]:
    """
    Cup fixtures: clubs' recent form comes from their domestic leagues, since a
    cup campaign alone is far too few matches to compute rolling form from.
    """
    cutoff = _cutoff(match_date)

    def best_frame(team: str) -> tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        best_rows, best_full = pd.DataFrame(), None
        for code in _DOMESTIC_FALLBACK:
            df = store.load_results(code)
            if df.empty:
                continue
            rows = _find_team_rows(df, team, cutoff)
            if len(rows) > len(best_rows):
                best_rows, best_full = rows, df
        return best_rows, best_full

    H, full_H = best_frame(home_team)
    A, _ = best_frame(away_team)

    if len(H) < min_matches or len(A) < min_matches:
        return {}

    return _compute_features(H, A, home_team, away_team,
                             full_H if full_H is not None else H,
                             league_code=league_code)


# ── Main entry point ──────────────────────────────────────────────────────────

def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    min_matches: int = MIN_MATCHES,
) -> Dict[str, float]:
    """
    Compute the feature bundle for a fixture as of its match date.

    Returns {} when there is not enough history — callers treat that as
    "no prediction possible" rather than guessing.
    """
    if league_code in INTL_LEAGUE_CODES:
        return _asof_features_intl(home_team, away_team, match_date,
                                   league_code, min_matches)

    df = store.load_results(league_code)
    if df.empty:
        return {}

    cutoff = _cutoff(match_date)
    work = df[df["date"] < cutoff]
    if work.empty:
        return {}

    H = _find_team_rows(df, home_team, cutoff)
    A = _find_team_rows(df, away_team, cutoff)
    if len(H) < min_matches or len(A) < min_matches:
        return {}

    return _compute_features(
        H, A, home_team, away_team, work,
        league_code=league_code,
        H_home=_find_venue_rows(df, home_team, cutoff, "home"),
        A_away=_find_venue_rows(df, away_team, cutoff, "away"),
    )


def validate_match_existed(
    league_code: str, home_team: str, away_team: str, match_date: date,
) -> Tuple[bool, Optional[str]]:
    """Confirm a fixture really was played on that date before retro-simulating it."""
    df = store.load_results(league_code)
    if df.empty:
        return False, f"No data stored for {league_code}. Run `athena data load` first."

    day = df[df["date"].dt.date == match_date]
    if day.empty:
        return False, f"No {league_code} matches found on {match_date}."

    names = _team_names(day)
    mh = _match_team(home_team, names)
    ma = _match_team(away_team, names)
    if not mh:
        return False, f"{home_team} did not play in {league_code} on {match_date}."
    if not ma:
        return False, f"{away_team} did not play in {league_code} on {match_date}."

    exists = any(
        _norm(str(r["home"])) == _norm(mh) and _norm(str(r["away"])) == _norm(ma)
        for _, r in day.iterrows()
    )
    if not exists:
        return False, (
            f"{home_team} and {away_team} did not play each other on {match_date} "
            f"(both played, but not one another)."
        )
    return True, None


def actual_result(
    league_code: str, home_team: str, away_team: str, match_date: date,
) -> Optional[tuple[int, int]]:
    """Actual (home_goals, away_goals) for a played match, or None."""
    df = store.load_results(league_code)
    if df.empty:
        return None
    day = df[df["date"].dt.date == match_date]
    if day.empty:
        return None
    names = _team_names(day)
    mh = _match_team(home_team, names)
    ma = _match_team(away_team, names)
    if not mh or not ma:
        return None
    for _, r in day.iterrows():
        if _norm(str(r["home"])) == _norm(mh) and _norm(str(r["away"])) == _norm(ma):
            return int(r["hg"]), int(r["ag"])
    return None
