"""
Possession as a per-league adjustment to the goal expectation.

WHY PER-LEAGUE, NOT GLOBAL
=========================
Fitting possession against total goals in three leagues gives coefficients that
disagree in sign, not merely in size:

    COL-PA   possession gap  +0.0055 goals per point   lopsided -> MORE goals
    BRA-SB   possession gap  -0.0062                   lopsided -> FEWER goals
    JPN-J1   possession gap  -0.0092                   lopsided -> FEWER goals

A single global term would average those to roughly nothing, which is probably
why possession looks useless when tested that way. Colombia's football and
Japan's disagree about what one-sided possession means, and the honest model of
that is one coefficient per competition rather than one for the sport.

Two terms are fitted:

    pos_avg   mean possession across both sides. Near-constant at ~50 by
              construction, so it carries little; kept because it is free and
              consistently negative — more deliberate football, fewer goals.

    pos_gap   how lopsided the battle is. This is the term that matters and the
              one whose sign flips by league.

EVERYTHING IS AS-OF
===================
The coefficient for a fixture is fitted only on matches before it. Fitting on a
league's whole history and then scoring the same matches would confirm any
signal, real or not — the failure mode that has already produced one false
result this session. Refits are cached per (league, cutoff-month), which keeps
the cost near zero without letting a later month's fit reach an earlier match.

The adjustment is deliberately capped. A regression fitted on a few thousand
noisy matches should nudge a goal expectation, not overturn it.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from app.data import store

# Matches required before a league gets a fitted coefficient at all. Below this
# the fit describes its own noise.
MIN_FIT = 400

# Rolling window for a team's possession profile, matching the feature layer.
WINDOW = 10
MIN_PRIOR = 8

# Hard bound on how far possession may move the goal expectation, in goals.
# The fitted terms imply roughly 0.1-0.2 on a lopsided fixture; this stops a
# badly-conditioned fit from producing something absurd.
MAX_SHIFT = 0.35

_FIT_CACHE: dict[tuple, Optional[tuple[float, float]]] = {}


def _team_possession(rows: pd.DataFrame, team: str) -> Optional[float]:
    if "hpos" not in rows.columns or rows.empty:
        return None
    at_home = (rows["home"] == team).values
    hp = rows["hpos"].values.astype(float)
    ap = rows["apos"].values.astype(float)
    own = np.where(at_home, hp, ap)
    own = own[np.isfinite(own)]
    if len(own) < MIN_PRIOR:
        return None
    return float(own.mean())


def _fit(league_code: str, cutoff: datetime) -> Optional[tuple[float, float]]:
    """
    Coefficients (pos_avg, pos_gap) for a league, from matches before cutoff.

    Cached per calendar month: a refit every fixture would be wasteful and the
    coefficient moves slowly, but caching by league alone would let a fit made
    late in a season describe matches played early in it.
    """
    key = (league_code, cutoff.year, cutoff.month)
    if key in _FIT_CACHE:
        return _FIT_CACHE[key]

    df = store.load_results(league_code)
    if df.empty or "hpos" not in df.columns:
        _FIT_CACHE[key] = None
        return None
    df = df[(df["date"] < cutoff) & df["hpos"].notna()
            & df["apos"].notna() & df["hg"].notna()]
    df = df.sort_values("date").reset_index(drop=True)
    if len(df) < MIN_FIT:
        _FIT_CACHE[key] = None
        return None

    tot = (df["hg"].astype(float) + df["ag"].astype(float)).values
    rows = []
    for i in range(80, len(df)):
        r = df.iloc[i]
        past = df.iloc[:i]
        h = past[(past["home"] == r["home"]) | (past["away"] == r["home"])].tail(WINDOW)
        a = past[(past["home"] == r["away"]) | (past["away"] == r["away"])].tail(WINDOW)
        hp = _team_possession(h, r["home"])
        ap = _team_possession(a, r["away"])
        if hp is None or ap is None:
            continue
        hs = float(np.where(h["home"] == r["home"], h["hg"], h["ag"]).mean())
        as_ = float(np.where(a["home"] == r["away"], a["hg"], a["ag"]).mean())
        if not np.isfinite(hs) or not np.isfinite(as_):
            continue
        rows.append((hs + as_, (hp + ap) / 2, abs(hp - ap), tot[i]))

    if len(rows) < MIN_FIT:
        _FIT_CACHE[key] = None
        return None

    arr = np.array(rows, dtype=float)
    X = np.column_stack([arr[:, 0], arr[:, 1], arr[:, 2], np.ones(len(arr))])
    try:
        b = np.linalg.lstsq(X, arr[:, 3], rcond=None)[0]
    except np.linalg.LinAlgError:
        _FIT_CACHE[key] = None
        return None
    if not np.all(np.isfinite(b)):
        _FIT_CACHE[key] = None
        return None

    out = (float(b[1]), float(b[2]))     # pos_avg, pos_gap
    _FIT_CACHE[key] = out
    return out


def shift(league_code: str, home: str, away: str,
          match_date: date) -> Optional[float]:
    """
    How far possession moves the goal expectation for this fixture, in goals.

    Returns None when the league has no possession data, too little history to
    fit, or either side lacks a possession profile — the caller then proceeds
    exactly as before rather than on a guess.
    """
    cutoff = datetime.combine(match_date, datetime.min.time())
    coef = _fit(league_code, cutoff)
    if coef is None:
        return None

    df = store.load_results(league_code)
    if df.empty or "hpos" not in df.columns:
        return None
    past = df[df["date"] < cutoff]
    h = past[(past["home"] == home) | (past["away"] == home)].tail(WINDOW)
    a = past[(past["home"] == away) | (past["away"] == away)].tail(WINDOW)
    hp = _team_possession(h, home)
    ap = _team_possession(a, away)
    if hp is None or ap is None:
        return None

    c_avg, c_gap = coef
    # Expressed as a deviation from a balanced 50/50 fixture, so a league where
    # possession says nothing contributes nothing rather than a constant offset.
    delta = c_avg * (((hp + ap) / 2) - 50.0) + c_gap * abs(hp - ap)
    baseline = c_gap * 10.0        # a typical, mildly uneven fixture
    delta -= baseline
    return float(np.clip(delta, -MAX_SHIFT, MAX_SHIFT))
