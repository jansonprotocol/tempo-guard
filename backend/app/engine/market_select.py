"""
Pick the market from the probability model instead of a threshold flowchart.

WHAT WAS WRONG
==============
The engine does two jobs. It estimates how many goals a fixture will produce,
and it picks which line to play. The first job improved repeatedly this
session; the second never used the result.

`translate_play` decides by asking which side of a hand-tuned threshold a
handful of signals fall on — confidence >= 0.78, p2p >= 0.80, support_delta > 0
— and never looks at the goal estimate itself. So improving that estimate from
2.6 to 2.8 usually leaves every comparison unchanged and the same market comes
out. Measured four times over: the shot blend changed 40% of calls for no
change in edge, the corrected league aggregates changed 78% of calls on a test
matchday and 0.11% of pooled edge, and the tempo/bias sweep only ever bought
hit rate by retreating to a safer line.

That is the signature of a bottleneck downstream of the signal, and it means no
further work on the goal estimate can pay off until this is fixed.

WHAT THIS DOES INSTEAD
======================
The engine already models total goals as Poisson. So for any candidate market
it can state the chance of winning outright:

    P(win | mu) = sum of Poisson(mu) over every total that wins that market

Picking the highest of those alone is useless — it just names the loosest line
available, which wins often and pays nothing. The quantity worth maximising is
the gap against a typical fixture in the same league:

    score(market) = P(win | mu_match) - P(win | mu_league)

That is edge, stated directly: how much more likely this market is to land here
than in an ordinary match of this league. It is positive for Over lines when
the fixture is livelier than its league and for Under lines when it is quieter,
and its size decides how aggressive a rung is justified. Both terms use the
same Poisson assumption, so the approximation largely cancels in the
difference.

WHICH TOTALS WIN IS NOT RESTATED HERE
=====================================
The winning set for each market is derived by asking the grader — the same
`evaluate_market` and `hit_weight` that score a replay — rather than by writing
a second tier table that could drift away from it. Under the full-win
convention a half-win counts as a win, so O2.25/O2.5/O2.75 all collapse to
"3 or more" and U3.25/U3.5/U3.75 to "3 or fewer". Restating that by hand is
exactly the kind of duplicate truth that rots.
"""
from __future__ import annotations

import math
from functools import lru_cache
from typing import Optional, Sequence

from app.util.asian_lines import evaluate_market, hit_weight

# The ladder the selector may choose from. Deliberately the same rungs the old
# flowchart could reach, so a comparison measures the decision rule and not a
# change of vocabulary.
LADDER: tuple[str, ...] = (
    "O1.0", "O1.5", "O1.75", "O2.25", "O2.5", "O2.75",
    "U2.75", "U3.0", "U3.25", "U3.5", "U3.75", "U4.25",
)

# Totals worth summing over. P(11+ goals) at any realistic mu is far below the
# rounding in every number this feeds.
_MAX_TOTAL = 12

# A market must have at least this chance of landing before its edge is worth
# anything. This is the single most consequential number in the module: edge is
# largest for lines in the middle of the goal distribution, where probability
# actually responds to a change in mu, so without a floor the selector runs
# straight to the most volatile line available. At 0.55 it did exactly that and
# won 60% of the time.
#
# Swept in steps of 0.01 over 3,716 unseen matches across 32 leagues:
#
#     floor   strike    edge   leagues >=80%   most-picked line
#      0.55   60.06%  +2.53%       0/32        U2.75 36%
#      0.76   77.83%  +1.11%      11/32        O1.5  33%
#      0.79   80.57%  +1.15%      21/32        U4.25 40%
#      0.82   82.67%  +1.11%      25/32        U4.25 53%
#      0.88   84.45%  +0.86%      29/32        U4.25 65%
#
# Two things that table settles. Between 0.76 and 0.82 the edge column is flat
# to within noise, so strike rate in that band is nearly free — the trade-off
# only bites at the extremes. And the 85% target is reachable at 0.88 only by
# emitting U4.25 in two matches out of three, which scores well precisely
# because it is close to a constant.
#
# 0.79 is the highest floor that clears 80% strike while keeping the most-picked
# line under half of all calls. Above it the gain is concentration rather than
# accuracy.
MIN_WIN_PROB = 0.79

# The other end of the same idea. A market modelled far above this is not a
# forecast, it is an arithmetic fact about the league — in a competition where
# 97% of matches finish under five goals, U4.25 wins nearly always and is priced
# to match. Winning a 1.05 shot 92% of the time loses money while looking
# excellent on a hit-rate report.
#
# The floor refuses bets too risky to want; the ceiling refuses bets too certain
# to price. Together they confine the selector to the band where a market is
# both winnable and buyable, and it self-adjusts by league: at a Serie B
# expectation of 2.2 goals U4.25 models at 0.928 and is excluded, dropping the
# choice to U3.5 at 0.819, while in a high-scoring league it never approaches
# the ceiling and nothing changes.
MAX_WIN_PROB = 0.90


@lru_cache(maxsize=None)
def winning_totals(market: str) -> tuple[int, ...]:
    """
    Which final totals win this market, according to the grader itself.

    Asks `evaluate_market`/`hit_weight` rather than restating the tier table, so
    the selector and the scorer can never disagree about what a win is.
    """
    return tuple(
        t for t in range(_MAX_TOTAL + 1)
        if hit_weight(evaluate_market(market, t, 0)) >= 1.0
    )


@lru_cache(maxsize=8192)
def _poisson_pmf(mu: float, k: int) -> float:
    return math.exp(-mu) * (mu ** k) / math.factorial(k)


def p_win(market: str, mu: float) -> float:
    """Chance this market lands, given an expected total of `mu` goals."""
    if mu <= 0:
        mu = 0.01
    mu = round(mu, 3)          # keeps the pmf cache small and effective
    return sum(_poisson_pmf(mu, t) for t in winning_totals(market))


def score_markets(
    mu: float,
    league_mu: float,
    ladder: Sequence[str] = LADDER,
) -> list[tuple[str, float, float, float]]:
    """
    Score every candidate market for a fixture.

    Returns (market, edge, p_win_here, p_win_typical) sorted by edge, best
    first. Exposed separately from `choose` so the reasoning can be printed and
    inspected rather than taken on trust.
    """
    out = []
    for m in ladder:
        here = p_win(m, mu)
        typical = p_win(m, league_mu)
        out.append((m, here - typical, here, typical))

    # Ties are the normal case, not an edge case, and must not be settled by
    # the order the ladder happens to be written in. Two reasons they arise:
    #
    #   within a tier   O2.25, O2.5 and O2.75 all win on 3+ goals under the
    #                   full-win convention, so their probabilities are
    #                   identical to the last decimal.
    #   ordinary match  when mu equals the league mean every edge is 0.00,
    #                   which is most fixtures.
    #
    # Breaking on probability second means an untradeable fixture resolves to
    # the safest line rather than an arbitrary one.
    out.sort(key=lambda r: (-round(r[1], 6), -r[2]))
    return out


def choose(
    mu: Optional[float],
    league_mu: Optional[float],
    ladder: Sequence[str] = LADDER,
    min_win_prob: Optional[float] = None,
    max_win_prob: Optional[float] = None,
) -> Optional[tuple[str, float, float]]:
    """
    Pick the market with the largest edge that still clears the probability
    floor. Returns (market, edge, p_win) or None when the inputs are missing.

    None is a real answer: without a goal estimate there is nothing to select
    on, and the caller should fall back rather than be handed a guess.
    """
    if mu is None or league_mu is None or mu <= 0 or league_mu <= 0:
        return None

    # Read the floor at call time, not as a default bound at import, so a
    # sweep can vary it without reloading the module.
    if min_win_prob is None:
        min_win_prob = MIN_WIN_PROB

    if max_win_prob is None:
        max_win_prob = MAX_WIN_PROB

    ranked = score_markets(mu, league_mu, ladder)
    for market, edge, here, _typical in ranked:
        if min_win_prob <= here <= max_win_prob:
            return market, edge, here

    # Nothing sits in the band. Which side it missed on decides the fallback.
    above = [(m, e, h) for m, e, h, _ in ranked if h > max_win_prob]
    if above:
        # Everything is too certain to price — an extremely lopsided fixture.
        # Take the least certain of them: closest to being a real bet.
        return min(above, key=lambda r: r[2])

    # Everything is too risky. Take the safest available rather than the
    # highest-edge one, since at this point no line is comfortable.
    safest = max(ladder, key=lambda m: p_win(m, mu))
    return safest, 0.0, p_win(safest, mu)
