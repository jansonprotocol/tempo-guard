"""
Is ONE SIDE's goal count Poisson, the way the match total is?

`scripts/dispersion.py` settled this for the match total on 272,857 matches:
variance/mean 1.034, every traded rung within half a point of Poisson. That
result is why the engine prices totals from a single mu and why mu, not the
distribution, was the thing worth fixing.

The team lane assumes the same of a single side, and the lane-level calibration
says it should not:

    O0.5  P(>=1)   +0.7    the zero is right
    U1.5  P(<=1)   -5.3    claims too many low-scoring sides
    O1.5  P(>=2)   +5.8    claims too few 2+ sides

U1.5 and O1.5 are complements, so those are one error seen twice: the model puts
too much mass on exactly one goal and not enough on two or more, while getting
P(0) right. That is what over-dispersion looks like with the zero pinned — and a
side has structure a match total does not, since a team parking the bus and a
team chasing a game are different processes averaged into one rate.

Counted here directly. Every stored match contributes two side-observations,
each compared against Poisson with that side's own league-and-venue mean.

Usage:  python scripts/side_dispersion.py [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from scripts.team_shrink_sweep import LEAGUES


def main() -> None:
    args = sys.argv[1:]
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)

    counts: Counter = Counter()
    n = 0
    tot = 0.0
    sq = 0.0
    for lg in codes:
        df = store.load_results(lg)
        if df is None or df.empty:
            continue
        for col in ("hg", "ag"):
            g = df[col].dropna().astype(int)
            for v in g:
                counts[min(v, 6)] += 1
            n += len(g)
            tot += float(g.sum())
            sq += float((g ** 2).sum())

    mean = tot / n
    var = sq / n - mean ** 2
    print(f"{n} side-observations   mean {mean:.4f}   variance {var:.4f}   "
          f"var/mean {var/mean:.4f}\n")
    print(f"{'goals':>7}{'actual':>10}{'Poisson':>10}{'diff':>9}")
    for gcount in range(7):
        if gcount < 6:
            p = math.exp(-mean) * mean ** gcount / math.factorial(gcount)
            label = str(gcount)
        else:
            p = 1.0 - sum(math.exp(-mean) * mean ** i / math.factorial(i)
                          for i in range(6))
            label = "6+"
        act = counts[gcount] / n
        print(f"{label:>7}{act*100:9.2f}%{p*100:9.2f}%{(act-p)*100:+9.2f}")

    print(f"\n{'rung':>7}{'actual':>10}{'Poisson':>10}{'diff':>9}")
    for label, lo, hi in (("O0.5", 1, 99), ("U1.5", 0, 1), ("O1.5", 2, 99)):
        act = sum(c for g, c in counts.items() if lo <= g <= hi) / n
        p = sum(math.exp(-mean) * mean ** i / math.factorial(i)
                for i in range(0, 40) if lo <= i <= hi)
        print(f"{label:>7}{act*100:9.2f}%{p*100:9.2f}%{(act-p)*100:+9.2f}")
    print("\nvar/mean above 1 means a side's goals are MORE spread than Poisson.")


if __name__ == "__main__":
    main()
