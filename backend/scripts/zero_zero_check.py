"""
Is the goal model optimistic about 0-0?

O1.0 became 41% of picks in the capped leagues, and it fails on exactly one
scoreline. Its price comes from a Poisson model, and low-scoring leagues are
often underdispersed — totals cluster tighter than Poisson assumes — so the
concern was that O1.0 had been made the most common tip in five leagues on the
back of an assumption that fails hardest precisely there.

Measured, 2019 onward, actual 0-0 rate against exp(-mu):

    BRA-SB   11.4% vs 11.7%   0.98x        ENG-PL    5.6% vs  5.7%   0.98x
    ARG-PD   12.8% vs 11.5%   1.11x        GER-BL    5.1% vs  4.2%   1.21x
    ARG-CLP  11.8% vs  9.5%   1.24x        NED-ED    5.1% vs  4.5%   1.12x
    ITA-SB    8.7% vs  8.4%   1.04x
    GRE-SL   10.0% vs  8.6%   1.16x

    capped mean 1.11x        normal mean 1.10x

The concern was wrong in its specific form and right in general. 0-0 does run
about 11% above the Poisson expectation, but equally in both groups, so this is
not a low-scoring-league effect and O1.0 was not made riskier by the caps —
BRA-SB is the best-behaved league of the five.

What it does mean: a published 90% is nearer 89%. Every win probability the
engine prints is slightly generous in the tail, which makes MIN_WIN_PROB
slightly generous too. The bias is small, one-directional, and worth
remembering before trusting a modelled probability to the nearest point.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from app.data import store

CAPPED = ["BRA-SB", "ARG-PD", "ARG-CLP", "ITA-SB", "GRE-SL"]
NORMAL = ["ENG-PL", "GER-BL", "NED-ED", "ITA-SA", "TUR-SL", "FRA-L1"]
SINCE = "2019-01-01"


def main() -> None:
    print(f"  {'league':9s} {'n':>6} {'mu':>6} {'actual 0-0':>11} {'Poisson':>9} {'ratio':>7}")
    print("  " + "-" * 54)
    groups = {}
    for label, codes in (("capped", CAPPED), ("normal", NORMAL)):
        ratios = []
        for lg in codes:
            df = store.load_results(lg)
            df = df[df["date"] >= SINCE]
            if len(df) < 300:
                continue
            t = (df["hg"] + df["ag"]).dropna()
            mu = float(t.mean())
            actual = float((t == 0).mean())
            expected = float(np.exp(-mu))
            ratios.append(actual / expected)
            print(f"  {lg:9s} {len(t):6d} {mu:6.2f} {actual:11.1%} "
                  f"{expected:9.1%} {actual / expected:7.2f}x")
        groups[label] = ratios

    print()
    for label, r in groups.items():
        if r:
            print(f"  {label} leagues  mean ratio {np.mean(r):.2f}x")
    print("\n  Equal ratios in both groups means this is a general Poisson")
    print("  limitation, not a low-scoring-league effect. A published 90% is")
    print("  nearer 89%.")


if __name__ == "__main__":
    main()
