"""
Is the Poisson assumption good enough for the rungs this engine sells?

The bet ledger threw up a suspicious pattern: bets priced 1.20-1.39 hit 22
points below their stated probability, and most of those were `U3.x` rungs.
The natural structural story is overdispersion — real football totals having
fatter tails than Poisson, producing more 4+ goal games than the model expects.
That would hurt `U3.x` more than anything else, because its boundary sits
closest to the typical total, where the shape of the distribution matters most.

It is a good story and it is FALSE. On 272,857 matches:

    mean total 2.623   variance 2.713   var/mean = 1.034   (Poisson => 1.000)

    goals   actual  Poisson    diff
        0    7.95%    7.26%   +0.69
        1   18.72%   19.04%   -0.32
        2   24.52%   24.97%   -0.45
        3   21.74%   21.83%   -0.09
        4   14.11%   14.32%   -0.20
        5    7.64%    7.51%   +0.12
       6+    5.31%    5.07%   +0.24

There IS mild overdispersion — slightly more 0-0s and slightly more high-
scoring games than Poisson — but it is worth almost nothing at the rungs
actually traded:

    rung    Poisson says   actually     gap
    U2.75          51.3%      51.2%    -0.1
    U3.0           73.1%      72.9%    -0.2
    U3.5           73.1%      72.9%    -0.2
    U4.25          87.4%      87.1%    -0.4
    O1.5           73.7%      73.3%    -0.4
    O2.5           48.7%      48.8%    +0.1

Every rung is within half a point, and `U3.x` is not singled out — it is the
BEST calibrated of the unders. So the -19 point gap on `U3.x` bets is not a
distributional artefact, and there is no shape correction to apply. That leaves
variance on a small, outcome-selected sample as the likely explanation, which
is a reason NOT to turn the pattern into a rule.

Worth keeping because it is a negative result that closes a line of enquiry:
before adding any dispersion parameter to the engine, re-run this.

Usage:  python scripts/dispersion.py
"""
from __future__ import annotations

import sys
from math import exp, factorial
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.data import store

# Rungs the engine actually sells, and the totals that win them under the
# full-win convention (a push counts, matching `market_select.p_win`).
RUNGS = [
    ("U2.75", lambda t: t <= 2),
    ("U3.0", lambda t: t <= 3),
    ("U3.5", lambda t: t <= 3),
    ("U4.25", lambda t: t <= 4),
    ("U4.5", lambda t: t <= 4),
    ("O1.5", lambda t: t >= 2),
    ("O2.5", lambda t: t >= 3),
]
_TAIL = 17


def totals() -> pd.Series:
    frames = []
    for lg in store.available_leagues():
        try:
            d = store.load_results(lg)
        except Exception:
            continue
        if d is None or not len(d):
            continue
        frames.append(d[["hg", "ag"]])
    df = pd.concat(frames).dropna()
    return (df.hg + df.ag).astype(int)


def main() -> None:
    t = totals()
    mu, var = t.mean(), t.var()
    print(f"{len(t):,} matches")
    print(f"mean {mu:.3f}   variance {var:.3f}   var/mean {var / mu:.4f}"
          f"   (Poisson => 1.0000)\n")

    pmf = lambda k: exp(-mu) * mu ** k / factorial(k)
    print(f"{'goals':>6}{'actual':>9}{'Poisson':>9}{'diff':>8}")
    for k in range(9):
        a = (t == k).mean()
        print(f"{k:6}{a * 100:8.2f}%{pmf(k) * 100:8.2f}%{(a - pmf(k)) * 100:+7.2f}")
    a = (t >= 9).mean()
    p = 1 - sum(pmf(k) for k in range(9))
    print(f"{'9+':>6}{a * 100:8.2f}%{p * 100:8.2f}%{(a - p) * 100:+7.2f}")

    print(f"\n{'rung':7}{'Poisson says':>14}{'actually':>11}{'gap':>8}")
    for rung, wins in RUNGS:
        act = t.map(wins).mean()
        pois = sum(pmf(k) for k in range(_TAIL) if wins(k))
        print(f"{rung:7}{pois * 100:13.1f}%{act * 100:10.1f}%"
              f"{(act - pois) * 100:+7.1f}")


if __name__ == "__main__":
    main()
