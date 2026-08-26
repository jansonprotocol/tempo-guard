"""
Are the team lane's two splits one defect, or two?

`team_calibration` reports both, and both pool away to +0.4:

    rung   O0.5 +0.7   O1.5 +5.8   U1.5 -5.3
    side   TA (home) +2.9          TB (away) -3.0

Read separately they suggest two faults. But the rungs are not evenly spread
across the sides — a home team scores more often than an away team, so `O1.5`
should be predominantly a HOME lane and `U1.5` predominantly an AWAY one. If
that is the whole story, the rung split IS the side split wearing a different
label, and fixing the side fixes both. If instead `O1.5` runs high on BOTH
sides and `U1.5` runs low on BOTH, they are independent and need separate work.

That is the one question this answers: cross-tabulate. Each cell reports what
the engine said, what landed, and the gap, so the pattern can be read off the
grid rather than inferred from two margins.

The instrument is `team_calibration`'s own collection — same lanes, same two
windows, same top-candidate rule — so the margins here must reproduce the ones
that motivated the question. They are printed alongside as a check: if they do
not match, the cross-tab is measuring a different population and says nothing.

Usage:  python scripts/team_rung_side.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.team_calibration import collect
from scripts.team_shrink_sweep import LEAGUES, wilson

RUNGS = ("O0.5", "O1.5", "U1.5")
SIDES = (("TA", "home"), ("TB", "away"))


def cell(rows: list) -> tuple[int, float, float, float]:
    n = len(rows)
    if not n:
        return 0, 0.0, 0.0, 0.0
    hit = sum(1 for r in rows if r[1]) / n
    says = sum(r[0] for r in rows) / n
    return n, says, hit, hit - says


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)

    rows = []
    for back in (0, n):
        for lg in codes:
            try:
                rows += collect(lg, n, back)
            except Exception as exc:
                print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    if not rows:
        return
    # market is "TA O1.5" / "TB U1.5" — side and rung in one string.
    print(f"{len(rows)} team lanes, both windows pooled\n")

    print(f"{'':10}{'HOME (TA)':>30}{'AWAY (TB)':>30}")
    print(f"{'rung':10}{'n':>6}{'says':>8}{'hit':>8}{'gap':>8}"
          f"{'n':>6}{'says':>8}{'hit':>8}{'gap':>8}")
    for rung in RUNGS:
        line = f"{rung:10}"
        for side, _label in SIDES:
            c = cell([r for r in rows if r[2] == f"{side} {rung}"])
            line += (f"{c[0]:6}{c[1]*100:7.1f}%{c[2]*100:7.1f}%{c[3]*100:+8.1f}"
                     if c[0] >= 40 else f"{c[0]:6}{'—':>8}{'—':>8}{'—':>8}")
        print(line)

    # The two margins that motivated the question, recomputed here so the
    # cross-tab can be trusted to describe the same lanes.
    print("\nmargins, as a check against team_calibration")
    for label, keys in (("rung", [(r, lambda x, r=r: x[2].endswith(r)) for r in RUNGS]),
                        ("side", [(s, lambda x, s=s: x[2].startswith(s)) for s, _ in SIDES])):
        for name, pred in keys:
            c = cell([r for r in rows if pred(r)])
            if c[0] < 40:
                continue
            w = wilson(sum(1 for r in rows if pred(r) and r[1]), c[0])
            print(f"  {label} {name:6}{c[0]:6}{c[1]*100:7.1f}%{c[2]*100:7.1f}%"
                  f"{c[3]*100:+8.1f}   [{w[0]*100:.0f}-{w[1]*100:.0f}]")

    # If the rung split is the side split relabelled, the rungs must be
    # lopsidedly distributed across the sides. Quantified rather than assumed.
    print("\nhow each rung splits across the sides")
    for rung in RUNGS:
        a = sum(1 for r in rows if r[2] == f"TA {rung}")
        b = sum(1 for r in rows if r[2] == f"TB {rung}")
        if a + b < 40:
            continue
        print(f"  {rung:6} home {a:5} ({a/(a+b)*100:4.1f}%)   away {b:5} "
              f"({b/(a+b)*100:4.1f}%)")


if __name__ == "__main__":
    main()
