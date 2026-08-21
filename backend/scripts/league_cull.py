"""
Which leagues are worth playing — and does "worth playing" even persist?

The spread between the best and worst league is 13.5 points of hit rate, six
times the entire headroom available from better team modelling. So cutting the
leagues the engine cannot read is the largest lever measured so far, and it
costs a config edit rather than a feature.

THE TRAP
========
Ranking 48 leagues by hit rate and keeping the top half is selecting on noise.
At 300 fixtures a league's strike carries a standard error near 2.3 points, and
with 48 of them the best few are partly just the luckiest few. Doing that would
manufacture a confident-looking play list that fails immediately out of sample —
the same mistake as reading an 83-fixture tail as a 92.8% lane.

So the first question is not "which leagues are best" but "is a league's quality
a stable property at all". Each league is split chronologically, ranked on the
older half, and scored on the newer one. If the two halves agree, the ranking is
real and a cull is justified. If they do not, then league quality is mostly
noise at this sample size and the honest move is to cut only on a much cruder
signal, or not at all.

Reported per league: strike and edge in each half, plus the rank correlation
across leagues. Then a proposed play list built ONLY from the older half and
scored on the newer, so the quoted improvement is out-of-sample rather than the
selection restated.

WHAT IS BEING OPTIMISED
=======================
Hit rate, because that is the brief. But edge is printed beside it because the
two come apart in a way that matters here: Nigeria hits 88.3% at +1.02% edge and
Brazilian Serie B 84.8% at -0.40%. Those are not the engine reading the league,
they are low-scoring competitions where the safe rung nearly always lands — and
a near-certain line is priced accordingly. A league kept for hit rate alone
should be recognised as a line-position play rather than a prediction.

Reads the cached replay, so it costs seconds.
"""
from __future__ import annotations

import sys
from collections import Counter
from math import sqrt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

from app.util.asian_lines import evaluate_market, hit_weight

CACHE = Path(__file__).resolve().parents[1] / ".cache" / "tail_rows_300_dom.csv"

# A league needs this many fixtures per half before it is ranked at all.
MIN_HALF = 100

# Candidate cut lines, applied to the OLDER half only.
STRIKE_FLOORS = [0.76, 0.78, 0.80, 0.82]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(mk, tt) -> float:
    if not len(mk) or not len(tt):
        return 0.0
    n = len(tt)
    return sum(c * sum(1 for t in tt if won(m, t)) / n
               for m, c in Counter(mk).items()) / len(mk)


def score(g) -> tuple[int, int, float, float, float, float]:
    """hits, n, strike, edge, goals per match, base rate."""
    mk, tt = list(g["market"]), list(g["total"])
    h = sum(1 for m, t in zip(mk, tt) if won(m, t))
    s = h / len(mk)
    b = base_of(mk, tt)
    return h, len(mk), s, s - b, float(np.mean(tt)), b


def main() -> None:
    if not CACHE.exists():
        print(f"no cache at {CACHE}\nrun: python scripts/abstain_tail.py 300")
        return
    df = pd.read_csv(CACHE).sort_values("date")

    halves = {}
    for code, g in df.groupby("code"):
        g = g.sort_values("date")
        cut = len(g) // 2
        old, new = g.iloc[:cut], g.iloc[cut:]
        if len(old) < MIN_HALF or len(new) < MIN_HALF:
            continue
        halves[code] = (score(old), score(new))

    print(f"{len(halves)} leagues with >= {MIN_HALF} fixtures per half\n")
    print(f"{'league':10s} {'OLD strike':>11} {'OLD edge':>9}   "
          f"{'NEW strike':>11} {'NEW edge':>9}   {'drift':>7}")
    print("-" * 68)
    rows = []
    for code, (a, b) in sorted(halves.items(), key=lambda kv: -kv[1][0][2]):
        s1, e1, s2, e2 = a[2], a[3], b[2], b[3]
        print(f"{code:10s} {s1:10.1%} {e1:+9.2%}   {s2:10.1%} {e2:+9.2%}   "
              f"{s2 - s1:+7.1%}")
        rows.append((code, s1, e1, s2, e2))

    # WHICH PROPERTIES OF A LEAGUE ARE STABLE AT ALL
    #
    # Ordered deliberately. Scoring rate is a fact about the competition;
    # base rate follows from it because it decides where the safe rung sits;
    # strike and edge are what the engine did. If the first two persist and
    # the last two do not, then leagues differ in how easy they are to BET,
    # not in how well they are READ — and a play list must be built from the
    # stable half.
    o_g = np.array([halves[c][0][4] for c in [r[0] for r in rows]])
    n_g = np.array([halves[c][1][4] for c in [r[0] for r in rows]])
    o_b = np.array([halves[c][0][5] for c in [r[0] for r in rows]])
    n_b = np.array([halves[c][1][5] for c in [r[0] for r in rows]])
    o_s = np.array([r[1] for r in rows])
    n_s = np.array([r[3] for r in rows])
    o_e = np.array([r[2] for r in rows])
    n_e = np.array([r[4] for r in rows])

    def r_of(a, b):
        return float(np.corrcoef(a, b)[0, 1])

    noise = 1 / sqrt(len(rows) - 3)
    print("-" * 68)
    print(f"older vs newer half across {len(rows)} leagues "
          f"(+/-{noise:.2f} is indistinguishable from nothing)\n")
    print(f"  goals per match (structural)  r = {r_of(o_g, n_g):+.3f}")
    print(f"  base rate (line position)     r = {r_of(o_b, n_b):+.3f}")
    print(f"  strike (what we bet)          r = {r_of(o_s, n_s):+.3f}")
    print(f"  edge (engine skill)           r = {r_of(o_e, n_e):+.3f}")

    per_half = len(df) // len(rows) // 2
    expect = 2 * 2.5 * sqrt(0.8 * 0.2 / per_half)
    print(f"\n  spread in older half {max(o_s) - min(o_s):.1%}; "
          f"pure noise at n={per_half} would give about {expect:.1%}")

    # What a cull chosen on the older half actually delivers on the newer one.
    print(f"\n  cut on OLDER half, measured on NEWER half:")
    print(f"  {'rule':16s} {'keeps':>6}  {'NEW strike':>11}  {'vs all':>8}  "
          f"{'NEW edge':>9}")
    print("  " + "-" * 60)
    all_new = pd.concat([newer_half(df, c) for c in halves])
    base_s, base_e = score(all_new)[2:4]
    print(f"  {'play everything':16s} {len(halves):6d}  {base_s:10.1%}  "
          f"{'--':>8}  {base_e:+9.2%}")

    def measure(label, keep):
        if not keep:
            print(f"  {label:16s} keeps nothing")
            return
        s, e = score(pd.concat([newer_half(df, c) for c in keep]))[2:4]
        print(f"  {label:16s} {len(keep):6d}  {s:10.1%}  "
              f"{s - base_s:+8.2%}  {e:+9.2%}")

    for fl in STRIKE_FLOORS:
        measure(f"strike >= {fl:.2f}", [r[0] for r in rows if r[1] >= fl])
    for fl in (0.0, 0.02):
        measure(f"edge >= {fl:+.2f}", [r[0] for r in rows if r[2] >= fl])

    # SELECTING ON WHAT IS KNOWABLE IN ADVANCE
    #
    # Past strike is not predictive, but scoring rate is. This ranks leagues by
    # the OLDER half's goals per match — a fact available before any of the
    # newer half was played — and scores the newer half. It is the only cull
    # rule here that is not selecting on its own noise.
    print(f"\n  select on OLDER-half goals per match, score on NEWER half:")
    print(f"  {'rule':26s} {'keeps':>5}  {'NEW strike':>11} {'vs all':>8} "
          f"{'NEW edge':>9}")
    print("  " + "-" * 64)
    print(f"  {'play everything':26s} {len(halves):5d}  {base_s:10.1%} "
          f"{'--':>8} {base_e:+9.2%}")
    order = sorted(halves, key=lambda c: halves[c][0][4])
    for k in (8, 12, 16, 24):
        keep = order[:k]
        s, e = score(pd.concat([newer_half(df, c) for c in keep]))[2:4]
        lo, hi = halves[keep[0]][0][4], halves[keep[-1]][0][4]
        print(f"  {'lowest-scoring ' + str(k):26s} {k:5d}  {s:10.1%} "
              f"{s - base_s:+8.2%} {e:+9.2%}   ({lo:.2f}-{hi:.2f} gpm)")
    for k in (8, 12):
        keep = order[-k:]
        s, e = score(pd.concat([newer_half(df, c) for c in keep]))[2:4]
        print(f"  {'highest-scoring ' + str(k):26s} {k:5d}  {s:10.1%} "
              f"{s - base_s:+8.2%} {e:+9.2%}")



def newer_half(df: pd.DataFrame, code: str) -> pd.DataFrame:
    g = df[df["code"] == code].sort_values("date")
    return g.iloc[len(g) // 2:]


if __name__ == "__main__":
    main()
