"""
Team totals as a second lane.

Every market on the match ladder is a MATCH total, so Tip 1 and Tip 2 are
always two rungs of the same bet, related by set containment — a second lane
can sharpen the first, insure it, or mirror it, but never disagree with it
independently. A team total is the first orthogonal market available here.

ONE NUMBER, SEVERAL CUTS
========================
The engine holds a single goal expectation per side, published indirectly as
`p_home_tt05` / `p_away_tt05` = 1 - exp(-gf) and otherwise unused. Recovering
gf = -ln(1 - p_tt05) gives every rung on that side's own ladder. These are not
independent reads; they are five places to cut one estimate, and the only
question worth asking is which cuts the model gets right.

    rung    lands when          predicted   actual     gap
    U0.5    side blanks           27.7%      25.3%    -2.4%
    U1.5    side held to <=1      61.6%      59.7%    -1.9%
    U2.5    side held to <=2      83.9%      83.7%    -0.2%
    O0.5    side scores           72.3%      74.7%    +2.4%
    O1.5    side scores twice+    38.4%      40.3%    +1.9%

All five calibrate. An earlier version of this module offered ONLY `O0.5`,
having rejected the whole Under direction after measuring `U0.5` inside the
extreme tail — sides given under 55% to score, where the Poisson shape is
worst. That was a tail effect generalised into a rule, and it threw away the
best rung on the ladder.

WHAT IS OFFERED, AND WHY
========================
Chronologically split at 2026-04-21, 17,387 train / 9,363 holdout side-obs:

    rung  floor    train edge   holdout edge   holdout hit   fair
    U1.5   0.75      +10.22%        +9.69%        70.48%    1.419
    U1.5   0.80      +11.44%       +12.10%        73.40%    1.362
    O1.5   0.55      +15.63%       +19.01%        60.77%    1.645
    O1.5   0.60      +18.59%       +24.87%        67.44%    1.483
    O0.5   0.80       +7.16%        +9.77%        85.64%    1.168
    O0.5   0.85      +10.67%       +12.74%        88.97%    1.124

    U2.5   0.85       +4.55%        +3.10%        87.51%    1.143   <- degrades
    U2.5   0.90       +5.70%        +3.24%        87.70%    1.140   <- degrades

`U1.5`, `O1.5` and `O0.5` all hold or improve out of sample. `U2.5` is the only
rung whose edge shrinks on the holdout, and it is also the shortest-priced —
87% strike at 1.14 is the buying-certainty shape this project has been fooled
by five times. It is not offered. `U0.5` never clears a floor in practice and
is not offered either.

The three that ship are deliberately different in character:

    U1.5   the weak-attack read. Best combination of edge and price.
    O1.5   the strong-signal read. Largest edge on the board, but a 42% base
           rate means a 0.55 floor already IS a strong claim.
    O0.5   the safe read. Real edge, but 1.12-1.17 leaves nothing after margin,
           so it is ranked last on near-ties.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Optional

from app.data import store

# Market names follow how a book lists them: team A is home, team B is away.
SIDES = ("TA", "TB")

# rung -> (probability given a side expectation, did it land given that side's
#          goals, minimum probability before it is offered at all)
RUNGS: dict[str, tuple] = {
    "U1.5": (lambda gf: math.exp(-gf) * (1 + gf), lambda g: g <= 1, 0.75),
    "O1.5": (lambda gf: 1 - math.exp(-gf) * (1 + gf), lambda g: g >= 2, 0.55),
    "O0.5": (lambda gf: 1 - math.exp(-gf), lambda g: g >= 1, 0.80),
}

# Ranked on near-ties: a rung that pays 1.42 beats one paying 1.14 for the same
# information, and O0.5's price is why it sits last despite a healthy edge.
PREFER = ["O1.5", "U1.5", "O0.5"]
TIE = 0.02

# Minimum edge over the as-of base rate. A rung that merely matches how often it
# lands anyway is not a read.
MIN_EDGE = 0.02

_WINDOW_DAYS = 365 * 3
_MIN_SAMPLE = 150
_CACHE: dict[tuple[str, date], dict[tuple[str, str], float]] = {}


def base_rates(league_code: str, match_date: date) -> Optional[dict]:
    """
    {(side, rung): rate} in this league, counted strictly BEFORE `match_date`.

    As-of for the same reason every other feature is: a benchmark informed by
    the fixture it judges would manufacture edge out of nothing. None when the
    league has too little history — a missing benchmark means the edge is
    unknown, not zero.
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

    out = {}
    for side, col in (("TA", "hg"), ("TB", "ag")):
        goals = window[col].fillna(0).astype(int)
        for rung, (_p, land, _f) in RUNGS.items():
            out[(side, rung)] = float(goals.map(land).mean())
    _CACHE[key] = out
    return out


def candidates(league_code: str, match_date: date,
               p_home: Optional[float], p_away: Optional[float],
               min_edge: float = MIN_EDGE) -> list[tuple[str, float, float]]:
    """
    Offerable team totals as (market, probability, edge), best first.

    Sorted by edge, with near-ties broken toward the longer-priced rung — see
    PREFER. Empty when nothing clears its floor, when the edge over the as-of
    base rate is too thin, or when the league has no usable base rate.
    """
    rates = base_rates(league_code, match_date)
    if rates is None:
        return []

    out = []
    for side, p_tt05 in (("TA", p_home), ("TB", p_away)):
        if p_tt05 is None or not 0.0 < float(p_tt05) < 1.0:
            continue
        gf = -math.log(1 - float(p_tt05))
        for rung, (prob, _land, floor) in RUNGS.items():
            p = prob(gf)
            if p < floor:
                continue
            edge = p - rates[(side, rung)]
            if edge >= min_edge:
                out.append((f"{side} {rung}", p, edge))

    def rank(c):
        rung = c[0].split()[1]
        # Bucket edges so a 2-point difference does not override a 0.3 price
        # difference; inside a bucket the longer-priced rung wins.
        return (-round(c[2] / TIE), PREFER.index(rung) if rung in PREFER else 99)

    out.sort(key=rank)
    return out


def won(market: str, home_goals: int, away_goals: int) -> bool:
    """Settlement, from that side's goals alone."""
    side, rung = market.split()
    if side not in SIDES or rung not in RUNGS:
        raise ValueError(f"not a team total: {market!r}")
    goals = int(home_goals) if side == "TA" else int(away_goals)
    return bool(RUNGS[rung][1](goals))
