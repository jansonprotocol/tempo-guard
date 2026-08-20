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
    "O1.5", "O1.75", "O2.25", "O2.5", "O2.75",
    "U2.75", "U3.25", "U3.5", "U3.75", "U4.25",
)

# Totals worth summing over. P(11+ goals) at any realistic mu is far below the
# rounding in every number this feeds.
_MAX_TOTAL = 12

# A market must have at least this chance of landing before its edge is worth
# anything. Without a floor the selector will happily take a 30% shot because
# it is 8 points better than usual, which is the opposite of the brief.
MIN_WIN_PROB = 0.55


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
    min_win_prob: float = MIN_WIN_PROB,
) -> Optional[tuple[str, float, float]]:
    """
    Pick the market with the largest edge that still clears the probability
    floor. Returns (market, edge, p_win) or None when the inputs are missing.

    None is a real answer: without a goal estimate there is nothing to select
    on, and the caller should fall back rather than be handed a guess.
    """
    if mu is None or league_mu is None or mu <= 0 or league_mu <= 0:
        return None

    ranked = score_markets(mu, league_mu, ladder)
    for market, edge, here, _typical in ranked:
        if here >= min_win_prob:
            return market, edge, here

    # Nothing clears the floor — take the safest available rather than the
    # highest-edge one, since at this point no line is comfortable.
    safest = max(ladder, key=lambda m: p_win(m, mu))
    return safest, 0.0, p_win(safest, mu)
