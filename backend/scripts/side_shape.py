"""
Is one side's goal count Poisson at the boundary the team lane actually cuts?

`team_rung_side` showed the team lane has ONE defect, not two. `O1.5` runs
+5.9 at home and +5.0 away — the same on both sides, so it is not venue — and
`U1.5` runs -5.6 away. The side margins that looked like a venue split are pure
composition: `O1.5` is 86.5% home lanes and `U1.5` is 93.0% away lanes.

Both surviving gaps say one thing. A side reaches two goals MORE often than the
model expects:

    O1.5   side scores twice+     says 60.2%   landed 65.9%
    U1.5   side held to <=1       says 78.7%   landed 73.5%

And there is a mechanism that predicts exactly this, including why `O0.5` is
fine. `team_total` recovers `gf = -ln(1 - p_tt05)` and prices every rung as
Poisson(gf). That construction pins `P(>=1)` to the feature BY DEFINITION — it
is the equation being inverted — so `O0.5` cannot be wrong however wrong the
shape is. All the shape error is pushed onto the >=2 boundary, which is the
only other place the ladder cuts.

So this asks the question directly, and free of lane selection: bucket every
priceable side-observation by its own fitted `gf`, and compare Poisson against
what happened at each threshold.

    >=1    must agree. It is the anchor; disagreement means the feature is
           broken rather than the shape, and nothing below can be read.
    >=2    the test. The rungs that ship are decided here.
    >=3    confirmation. Over-dispersion fattens the tail progressively, so a
           real shape error grows with the threshold rather than jumping once.

Every fixture the engine can price contributes two observations. No floors, no
ranking, no offer — the population cannot move with the thing being measured.

Usage:  python scripts/side_shape.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES, wilson

# Buckets on the fitted rate, not on probability: gf is what the ladder is
# priced from, and the rungs that ship sit around gf 0.9 to 1.7.
BANDS = [(0.0, 0.9), (0.9, 1.2), (1.2, 1.5), (1.5, 1.9), (1.9, 9.9)]


def poisson_at_least(k: int, gf: float) -> float:
    """P(X >= k) for X ~ Poisson(gf), the exact form the engine uses."""
    if k <= 0:
        return 1.0
    below = sum(math.exp(-gf) * gf ** i / math.factorial(i) for i in range(k))
    return 1.0 - below


def collect(lg: str, n: int, back: int) -> list[tuple[float, int]]:
    """(fitted gf, goals scored) for both sides of every priceable fixture."""
    df = store.load_results(lg)
    if df is None or len(df) < 260:
        return []
    ordered = df.sort_values("date")
    if back:
        ordered = ordered.iloc[:-back]
    out = []
    for _, r in ordered.tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(lg, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if req is None or req.p_home_tt05 is None or req.p_away_tt05 is None:
            continue
        for p, goals in ((req.p_home_tt05, r["hg"]), (req.p_away_tt05, r["ag"])):
            p = float(p)
            if not 0.0 < p < 1.0:
                continue
            out.append((-math.log(1.0 - p), int(goals)))
    return out


def line(label: str, rows: list, k: int) -> None:
    if len(rows) < 60:
        return
    says = sum(poisson_at_least(k, gf) for gf, _g in rows) / len(rows)
    hits = sum(1 for _gf, g in rows if g >= k)
    hit = hits / len(rows)
    w = wilson(hits, len(rows))
    print(f"{label:14}{len(rows):7}{says*100:7.1f}%{hit*100:7.1f}%"
          f"{(hit-says)*100:+8.1f}   [{w[0]*100:.0f}-{w[1]*100:.0f}]")


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
    print(f"{len(rows)} side-observations, no selection, both windows\n")

    for k in (1, 2, 3):
        print(f"P(side scores >= {k})")
        print(f"{'fitted gf':14}{'n':>7}{'poisson':>8}{'actual':>8}{'gap':>8}"
              f"{'95% CI':>13}")
        for lo, hi in BANDS:
            line(f"  {lo:.1f}-{hi:.1f}", [r for r in rows if lo <= r[0] < hi], k)
        line("  ALL", rows, k)
        print()

    # Dispersion, stated plainly. Poisson forces var == mean; a ratio above 1
    # fattens both tails, and the ladder only cuts one of them.
    mean = sum(g for _gf, g in rows) / len(rows)
    var = sum((g - mean) ** 2 for _gf, g in rows) / (len(rows) - 1)
    print(f"one side's goals: mean {mean:.3f}  var {var:.3f}  "
          f"var/mean {var / mean:.3f}   (Poisson = 1.000)")


if __name__ == "__main__":
    main()
