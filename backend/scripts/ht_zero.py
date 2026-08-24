"""
A 0-0 at half time: how often does the second half produce a goal?

This is the one live question worth answering from stored football data, and it
needs no model at all. Every result in the store carries a half-time score, so
the bet "over 0.5 goals, bought at 0-0 with 45 minutes left" can be settled
directly against history: count the matches that reached the break goalless and
see how many finished 0-0.

Two things make this worth measuring rather than assuming:

**A 0-0 at half time is itself information.** The pre-match expectation said
this match would produce ~2.9 goals. Forty-five minutes of nothing is evidence
the match is quieter than that read — worse pitch, cagey game state, whatever
the model could not see. Pricing the second half off the pre-match mu ignores
that and overstates the chance of a goal.

**Second halves are not half a match.** They carry slightly more goals than
first halves, so a flat 50/50 split understates it in the other direction. The
two effects push opposite ways and only the data settles which wins.

Reported by pre-match expectation band, because a goalless half in a low-tempo
league means something different from one in a high-scoring league:

    n         matches that were 0-0 at the break
    2H goal   how many produced at least one goal after it
    needs     the break-even price for backing over 0.5 at half time

Usage:  python scripts/ht_zero.py [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store

BANDS = [(0.0, 2.5), (2.5, 2.9), (2.9, 3.3), (3.3, 9.9)]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if not n:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - m) / d, (c + m) / d)


def main() -> None:
    args = sys.argv[1:]
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))

    # (league goal average, was 0-0 at HT, finished 0-0)
    rows = []
    per_league, defective = {}, []
    for lg in codes:
        df = store.load_results(lg)
        if df is None or df.empty or "hthg" not in df:
            continue
        d = df.dropna(subset=["hthg", "htag"])
        if len(d) < 100:
            continue

        # Some sources supply a half-time score for every match EXCEPT the ones
        # that finished 0-0. Dropping nulls then removes exactly the losing
        # cases, and the league reports a flawless 100% - the answer is an
        # artefact of which rows survived. Detect it by comparing 0-0 finishes
        # inside the half-time subset against the league as a whole: a league
        # with plenty of them overall and none here is not clean, it is
        # censored. Excluded rather than corrected, because the missing rows
        # cannot be recovered.
        all_00 = int(((df["hg"] == 0) & (df["ag"] == 0)).sum())
        sub_00 = int(((d["hg"] == 0) & (d["ag"] == 0)).sum())
        if all_00 >= 10 and sub_00 <= all_00 * 0.1:
            defective.append((lg, all_00, sub_00))
            continue
        lmu = (d["hg"] + d["ag"]).mean()
        goalless = d[(d["hthg"] == 0) & (d["htag"] == 0)]
        if goalless.empty:
            continue
        scored = int(((goalless["hg"] + goalless["ag"]) > 0).sum())
        per_league[lg] = (scored, len(goalless), lmu)
        for _, r in goalless.iterrows():
            rows.append((lmu, int(r["hg"]) + int(r["ag"]) > 0))

    n = len(rows)
    k = sum(1 for _, s in rows if s)
    print(f"{n} matches reached half time at 0-0, across {len(per_league)} leagues\n")
    if defective:
        print(f"EXCLUDED — half-time score missing on 0-0 finishes "
              f"({len(defective)} leagues):")
        for lg, a, s in defective:
            print(f"  {lg:9} {a:4} matches finished 0-0, {s} kept a half-time "
                  f"score")
        print()

    print(f"{'league goal avg':18}{'n':>7}{'2H goal':>10}{'95% CI':>13}{'needs':>9}")
    for lo, hi in BANDS:
        b = [s for m, s in rows if lo <= m < hi]
        if len(b) < 50:
            continue
        kk = sum(1 for s in b if s)
        p = kk / len(b)
        w = wilson(kk, len(b))
        print(f"{lo:.1f} - {hi:.1f}{'':>8}{len(b):7}{p*100:9.1f}%"
              f"   [{w[0]*100:.0f}-{w[1]*100:.0f}]{1/p:9.3f}")

    p = k / n
    w = wilson(k, n)
    print(f"\n{'ALL':18}{n:7}{p*100:9.1f}%   [{w[0]*100:.0f}-{w[1]*100:.0f}]"
          f"{1/p:9.3f}")

    print(f"\nby league (n >= 30 goalless halves):")
    for lg in sorted(per_league, key=lambda x: -per_league[x][0] / per_league[x][1]):
        s, t, lmu = per_league[lg]
        if t < 30:
            continue
        print(f"  {lg:9}{t:5}{s / t * 100:8.1f}%   avg {lmu:.2f}"
              f"   needs {t / s:.3f}")


if __name__ == "__main__":
    main()
