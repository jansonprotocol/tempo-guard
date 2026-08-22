"""
Team totals as a second lane.

Every market on the ladder is a MATCH total, so Tip 1 and Tip 2 are always two
rungs of the same bet and are related by set containment — a second lane can
sharpen the first, insure it, or mirror it, but never disagree with it
independently. A team total is the first orthogonal market available here: it
can win on scorelines where the match total loses, without being its opposite.

The probability comes from the engine's own per-side goal expectation, already
computed as-of and published as `p_home_tt05` / `p_away_tt05`. Nothing new is
modelled; these numbers simply had no market attached to them.

MEASURED, NOT ASSUMED
=====================
Over 3,160 fixtures, split chronologically at 2026-04-06:

    train  (2,054)   best floor 0.87   edge +12.75%
    holdout (1,106)  same floor 0.87   edge +20.87%

and every floor held out of sample, each clearing its base rate at the LOW end
of a 95% interval:

    floor 0.70  +5.80%      floor 0.83  +9.60%
    floor 0.75  +6.60%      floor 0.87 +20.90%
    floor 0.79  +7.70%

Edge GROWS with the floor. That is the opposite of the buying-certainty
signature that sank five earlier candidates, where strike rate rose while edge
flattened or fell.

ONLY THE OVER DIRECTION
=======================
`Under 0.5` — a side kept out — is deliberately not offered. The model is well
calibrated where it is confident and badly pessimistic where it is not:

    predicted P(side scores)   actual
      0.00-0.55     49.7%      68.2%     +18.5   <- unusable
      0.55-0.65     60.9%      67.6%      +6.7
      0.75-0.85     79.4%      78.9%      -0.5
      0.85-1.01     88.1%      86.3%      -1.8

A tip that a weak side will be shut out would rest on a number that means
nothing, so the low end of the range is simply not sold.

BASE RATES ARE AS-OF
====================
Edge is strike rate minus the rate at which that side scores in that league,
counted strictly before the match date — the same discipline the rest of the
feature layer follows, so a base rate can never be informed by the fixture it
is used to judge.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

from app.data import store

# Home team and away team, each to score at least once. Named to match how a
# book lists them rather than how the engine stores them.
HOME = "TA O0.5"
AWAY = "TB O0.5"

# Below this the market is not offered. 0.79 matches the match-ladder floor and
# sits inside the calibrated range; 0.83 and above carry more edge on fewer
# fixtures, which is a tuning question rather than a correctness one.
FLOOR = 0.79

# Minimum edge over the as-of base rate. A team total that merely matches how
# often the side normally scores is not a read.
MIN_EDGE = 0.02

# Base rates move slowly; three years matches RECENT_WINDOW_DAYS in features.
_WINDOW_DAYS = 365 * 3
_MIN_SAMPLE = 150
_CACHE: dict[tuple[str, date], tuple[float, float]] = {}


def base_rates(league_code: str, match_date: date) -> Optional[tuple[float, float]]:
    """
    (P home side scores, P away side scores) in this league, strictly before
    `match_date`. None when there is not enough history to mean anything.
    """
    key = (league_code, match_date)
    hit = _CACHE.get(key)
    if hit is not None:
        return hit

    df = store.load_results(league_code)
    if df.empty or "hg" not in df.columns:
        return None

    cutoff = datetime.combine(match_date, datetime.min.time())
    past = df[df["date"] < cutoff]
    if len(past) < _MIN_SAMPLE:
        return None

    window = past[past["date"] >= cutoff - timedelta(days=_WINDOW_DAYS)]
    if len(window) < _MIN_SAMPLE:
        window = past

    out = (float((window["hg"].fillna(0) >= 1).mean()),
           float((window["ag"].fillna(0) >= 1).mean()))
    _CACHE[key] = out
    return out


def candidates(league_code: str, match_date: date,
               p_home: Optional[float], p_away: Optional[float],
               floor: float = FLOOR,
               min_edge: float = MIN_EDGE) -> list[tuple[str, float, float]]:
    """
    Offerable team totals as (market, probability, edge), best edge first.

    Empty when neither side clears the floor, when the edge over the as-of base
    rate is too thin, or when the league has too little history to have a base
    rate at all — a missing benchmark means the edge is unknown, not zero.
    """
    if p_home is None or p_away is None:
        return []
    rates = base_rates(league_code, match_date)
    if rates is None:
        return []
    bh, ba = rates

    out = []
    for market, p, b in ((HOME, float(p_home), bh), (AWAY, float(p_away), ba)):
        if p >= floor and (p - b) >= min_edge:
            out.append((market, p, p - b))
    out.sort(key=lambda c: -c[2])
    return out


def won(market: str, home_goals: int, away_goals: int) -> bool:
    """Settlement. A team total over 0.5 lands when that side scores at all."""
    if market == HOME:
        return int(home_goals) >= 1
    if market == AWAY:
        return int(away_goals) >= 1
    raise ValueError(f"not a team total: {market!r}")
