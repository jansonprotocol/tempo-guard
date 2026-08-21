"""
Season stage as an adjustment to the goal expectation.

WHY THIS ONE IS DIFFERENT
=========================
Every feature rejected so far — possession, shot blends, goal variance, team
tags, referee tendency, rest days — measures how good the two sides are, which
is what `mu` already reads off goals. Adding a second estimate of a quantity
already estimated adds noise and the holdout says so every time.

Season stage is not in that family. It describes what the match is WORTH: how
far into its campaign a fixture falls, which decides whether anything is still
being decided. That is a property of the calendar, not of either squad, and the
engine has no concept of it at all.

It is also the one signal that has survived measurement twice. The
retrospective study found +0.363 goals in final rounds across 295
league-seasons, consistent in 26 of 29 leagues; re-measured using only what is
knowable on match day it is +0.150 [+0.112, +0.188] on 7,653 matches.

EVERYTHING IS AS-OF
===================
The retrospective version located final rounds by looking up when each season
actually ended, which is fine for asking whether the effect is real and useless
as a feature — nobody knows the last date of a season by reading it out of the
future. Two things are knowable on the morning of a match:

    played    how many matches each side has already contested this season
    expected  the league's typical matches per team, from seasons that FINISHED
              before this one began

Their ratio places a fixture in its campaign without reference to anything after
it. Counting per team rather than per league date also handles games in hand,
which a shared cutoff would blur.

That proxy is noisier than the true one, and the gap is visible: it carries 41%
of the effect perfect hindsight saw. A feature can only ever carry the signal
its as-of definition can see, so +0.150 is the honest ceiling here, not +0.363.

THE SHIFT IS GLOBAL, NOT PER LEAGUE
===================================
Per-league coefficients were tried for possession and the per-league fits were
noise: the toggle nets zero. The per-league numbers here scatter the same way —
Germany +0.700 on 135 matches, MLS -0.244 on 175 — which is what small samples
look like, not what twenty-nine different football cultures look like. One
pooled constant on 7,653 matches is the estimate the data supports.

A caution worth keeping in view: the possession shift ran to +/-0.35 and changed
nothing that mattered. This one is smaller and applies to roughly 6% of
fixtures, so a null result is the expected outcome rather than a surprise.
"""
from __future__ import annotations

from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from app.data import store

# Where the effect lives. Below this a fixture is mid-campaign and gets nothing.
FINAL_STRETCH = 0.92

# Pooled as-of estimate, in goals. Deliberately the measured value rather than a
# rounded-up one — the temptation to "help it along" is how a signal becomes a
# thumb on the scale.
SHIFT = 0.150

# A league needs this many completed seasons before its typical length is known
# well enough to place a fixture within one.
MIN_PRIOR_SEASONS = 1
MIN_ROWS = 500

_LEN_CACHE: dict[str, Optional[dict]] = {}


def _expected_lengths(league_code: str) -> Optional[dict]:
    """
    season -> typical matches per team, learned only from seasons that finished
    before that one started.

    Cached per league because it depends on completed seasons alone, so it
    cannot leak anything about a fixture being scored.
    """
    if league_code in _LEN_CACHE:
        return _LEN_CACHE[league_code]

    df = store.load_results(league_code)
    if df.empty or "season" not in df.columns or len(df) < MIN_ROWS:
        _LEN_CACHE[league_code] = None
        return None
    df = df[df["season"].notna()]

    per_season = {}
    for s, g in df.groupby("season"):
        teams = pd.concat([g["home"], g["away"]]).nunique()
        if teams:
            per_season[s] = (2 * len(g) / teams, g["date"].min())

    out = {}
    for s, (_length, start) in per_season.items():
        prior = [v for _k, (v, st) in per_season.items() if st < start]
        if len(prior) >= MIN_PRIOR_SEASONS:
            out[s] = float(np.median(prior))
    if not out:
        _LEN_CACHE[league_code] = None
        return None
    _LEN_CACHE[league_code] = out
    return out


def progress(league_code: str, home: str, away: str,
             match_date: date) -> Optional[float]:
    """
    How far through its season this fixture falls, as a fraction.

    Returns None when the league has too little history, the season cannot be
    identified, or its typical length is not yet learnable. Callers then proceed
    exactly as before rather than on a guess.
    """
    lengths = _expected_lengths(league_code)
    if lengths is None:
        return None

    df = store.load_results(league_code)
    if df.empty:
        return None
    cutoff = datetime.combine(match_date, datetime.min.time())

    # The season a fixture belongs to is the one being played immediately
    # before it, which avoids reading a label off the match itself.
    before = df[df["date"] < cutoff]
    if before.empty:
        return None
    season = before.iloc[-1]["season"]
    n = lengths.get(season)
    if not n:
        return None

    same = before[before["season"] == season]
    ph = int(((same["home"] == home) | (same["away"] == home)).sum())
    pa = int(((same["home"] == away) | (same["away"] == away)).sum())
    return ((ph + pa) / 2.0) / n


def shift(league_code: str, home: str, away: str,
          match_date: date) -> Optional[float]:
    """
    How far season stage moves the goal expectation for this fixture, in goals.

    Zero for most of a campaign by design: the measured effect is confined to
    the closing stretch, and spreading it across the season would dilute a real
    signal into a constant that cancels out.
    """
    p = progress(league_code, home, away, match_date)
    if p is None:
        return None
    return SHIFT if p >= FINAL_STRETCH else 0.0
