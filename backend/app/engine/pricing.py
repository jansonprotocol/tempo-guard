"""
What a rung is actually worth, in odds.

Every tip in this log has been published with a "fair price" of `1 / P`, and
that number is wrong for most of the ladder. `P` is the engine's win
probability, and the engine counts a push and a half-win as a win — a
deliberate choice for the HIT-RATE column, where getting your stake back is not
a loss. Money does not work that way. `U4.25` at exactly 4 goals returns half
the stake at odds and half at evens; scoring that as a full win and quoting
`1 / P` claims a price the bet cannot pay.

The error is not small, and it does NOT run one way — which is worse, because
it cannot be corrected with a single fudge factor. At mu = 2.60:

    market    1/P     true      gap    what 1/P got wrong
    O2.75    2.077   2.391   +15.1%    counts the half-win at 3 as a full win
    U3.0     1.359   1.509   +11.1%    counts the push at 3 as a win
    O1.75    1.365   1.440    +5.5%    counts the half-loss at 2 as a win
    U3.25    1.359   1.421    +4.6%    counts the half-win at 3 as a full win
    U4.25    1.140   1.152    +1.1%    counts the half-win at 4 as a full win
    O1.5     1.365   1.365     0.0%    correct - no push possible
    U3.5     1.359   1.359     0.0%    correct - no push possible
    O2.5     2.077   2.077     0.0%    correct - no push possible
    U3.75    1.359   1.263    -7.1%    ignores that 4 only half-loses
    O2.25    2.077   1.816   -12.6%    ignores that 2 only half-loses

Rungs that push or half-win on the boundary were quoted too cheap to buy;
rungs that half-LOSE on the boundary were quoted too dear, so genuine value was
being turned down. Only the `.5` lines were ever right, which is why the `O1.5`
bets in this log priced out sensibly and everything else drifted.

This generalises the log's earlier `O2.25 / O2.5 / O2.75` finding, which worked
the same three prices out by hand for one tier. The rule there — "`1 / P` is
the `O2.5` number" — is this module applied to the over tier.

HOW A LINE SETTLES
==================
An Asian line is one bet or two half-bets:

    .5   one bet, no push possible          U3.5  -> win <=3, lose >=4
    .0   one bet, pushes on the number      U3.0  -> win <=2, PUSH 3, lose >=4
    .25  half on the whole, half on the .5  U3.25 -> half U3.0 + half U3.5
    .75  half on the .5, half on the whole  U3.75 -> half U3.5 + half U4.0

Settling each half separately and averaging gives a stake fraction in
{-1, -0.5, 0, +0.5, +1}, which maps to a return per unit staked at decimal
odds `o`:

    won = max(s, 0)        push = 1 - abs(s)        return = won * o + push

BREAK-EVEN AND WHAT TO ACTUALLY PAY
===================================
Break-even is the price at which expected return is exactly the stake:

    o* = (1 - sum_t p_t * push_t) / (sum_t p_t * won_t)

Paying break-even is a coin flip with extra steps. `buy_from` adds a margin so
the quoted number is a threshold to act on rather than a boundary to sit on.
The default 5% is chosen against this log's own record: bets have averaged
about 1.29 at roughly a 72% strike, which is close enough to level that a
five-point cushion is the difference between grinding and drifting.

Totals are Poisson on `mu`, matching `market_select.p_win` exactly, so the two
modules never disagree about the same fixture.
"""
from __future__ import annotations

import math

# Beyond ~15 goals the Poisson tail contributes less than a rounding error to
# any line on this ladder, and the factorial gets expensive.
_MAX_TOTAL = 16
DEFAULT_MARGIN = 0.05


def _poisson_pmf(mu: float, k: int) -> float:
    return math.exp(-mu) * (mu ** k) / math.factorial(k)


def _settle_half(side: str, line: float, total: int) -> float:
    """One whole or half line. +1 win, 0 push, -1 loss."""
    if total == line:
        return 0.0                      # only reachable on a whole line
    if side == "O":
        return 1.0 if total > line else -1.0
    return 1.0 if total < line else -1.0


def settle_fraction(market: str, total: int) -> float:
    """
    Stake fraction won, in {-1, -0.5, 0, +0.5, +1}.

    Distinct from `asian_lines.evaluate_market`, which reports a push as
    "half_win" so the hit-rate column can count it. Here a push is a push.
    """
    m = market.strip().upper()
    side = m[0]
    if side not in ("O", "U"):
        raise ValueError(f"not an over/under market: {market!r}")
    line = float(m[1:])
    frac = round(line % 1, 2)

    if frac in (0.0, 0.5):
        return _settle_half(side, line, total)

    # Quarter lines split the stake across the two neighbouring lines.
    lo, hi = line - 0.25, line + 0.25
    return (_settle_half(side, lo, total) + _settle_half(side, hi, total)) / 2


def break_even(market: str, mu: float) -> float:
    """
    Decimal odds at which the bet returns the stake on average.

    Raises if the market cannot win at all, which would otherwise return a
    silent infinity and read as a wonderful price.
    """
    won = push = 0.0
    for t in range(_MAX_TOTAL + 1):
        p = _poisson_pmf(mu, t)
        s = settle_fraction(market, t)
        won += p * max(s, 0.0)
        push += p * (1 - abs(s))
    if won <= 0:
        raise ValueError(f"{market} cannot win at mu={mu}")
    return (1 - push) / won


# The winner's-curse haircut. Ranking tips by an ESTIMATE of edge selects the
# fixtures whose estimate came in high, so the top band is overconfident by
# construction rather than by defect — it cannot be shrunk away, only priced.
# Measured twice on separate populations, and stable to a tenth of a point:
#
#     stated edge        n    says    hit    gap        23 Aug     24 Aug
#     under +1%       2704   83.3%  84.7%   +1.4          +1.4       +1.5
#     +1 to +2%        807   82.9%  83.1%   +0.2          +0.2       -0.6
#     +2 to +3.5%     1193   82.0%  81.9%   -0.1          -0.1       +1.2
#     over +3.5%      2872   81.6%  79.1%   -2.5          -2.5       -2.9
#
# Only the top band is out. 79.1 / 81.6 = 0.969 of the stated probability, and
# break-even moves as roughly 1/p, so the price it needs is about 3.2% higher.
# That was README rule 3 — "high-edge tips need about 3% more price" — applied
# by hand on every bet. It is applied here instead, so `buy>=` already carries
# it and the published number is the number to buy at.
#
# Deliberately a step and not a curve: the three bands below the threshold sit
# within noise of zero on both measurements, and fitting a slope through four
# points, two of which disagree in sign between runs, would be fitting noise.
CURSE_EDGE = 0.035
CURSE_HAIRCUT = 0.032


# The buy-from probability is a BLEND of the bet's own stated probability
# and its league's proven playable hitrate — the bettor's rule, stated
# 28 Aug: a tip is one estimate, the league's playable record is thousands
# of settled ones, and the price should listen to both. Below the league's
# record the tip carries 0.4 and the league 0.6, which pulls the required
# price DOWN and makes lower-probability lanes reachable at real-world
# odds; above it the tip carries 0.8 and the league 0.2, which asks a
# little extra of the "easy" lanes. The two weights meet exactly at the
# league's own number, so the blend is continuous. Predictions never read
# this — it is a decision-layer rule about what to PAY, like the margin
# and the haircut beside it.
BUY_BLEND_BELOW = 0.4
BUY_BLEND_ABOVE = 0.8


def blend_p(p: float, league_play_hit: float | None) -> float:
    """The probability the buy-from price is computed on: the tip blended
    with its league's playable record. No record, no blend."""
    if not league_play_hit or p <= 0:
        return p
    w = BUY_BLEND_ABOVE if p >= league_play_hit else BUY_BLEND_BELOW
    return w * p + (1 - w) * league_play_hit


def buy_from(market: str, mu: float, margin: float = DEFAULT_MARGIN,
             stated_edge: float | None = None) -> float:
    """
    The lowest price worth taking: break-even, a margin, and the curse haircut.

    `stated_edge` is the tip's own published edge over its market's base rate,
    as a fraction. Pass it and a tip above CURSE_EDGE is quoted the extra price
    the top band has twice been measured to need. Omit it and the haircut does
    not apply — callers that do not compute an edge get the old number, so this
    can never silently reprice a lane that never claimed an edge at all.
    """
    price = break_even(market, mu) * (1 + margin)
    if stated_edge is not None and stated_edge > CURSE_EDGE:
        price *= 1 + CURSE_HAIRCUT
    return price


def expected_value(market: str, mu: float, odds: float) -> float:
    """Expected return per unit staked, minus the stake. 0.05 = +5%."""
    ev = 0.0
    for t in range(_MAX_TOTAL + 1):
        p = _poisson_pmf(mu, t)
        s = settle_fraction(market, t)
        ev += p * (max(s, 0.0) * odds + (1 - abs(s)))
    return ev - 1
