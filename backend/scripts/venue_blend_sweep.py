"""
`VENUE_BLEND = 0.35` has never been swept. What does it actually buy?

It is the weight given to a side's VENUE-SPECIFIC scoring rate over its overall
form — 35% of a home team's attack comes from its home matches, 65% from all
matches. The number was inherited and every venue fix on the record was built
around it rather than testing it: the residual de-bias exists precisely because
0.35 leaves `gfh` about 0.113 goals light, and its own strength is written as
`edge * (1 - (venue_h + venue_a) / 2)`, which goes to zero at a blend of 1.0.
So the constant and its correction are entangled, and only a sweep separates
them.

TWO LANES, NOT ONE
==================
This is the difference from `TEAM_SHRINK` and `TEAM_RATE_FLOOR`, and it decides
how the result has to be read. Those two touch `p_*_tt05` alone, so the match
ladder cannot move. `VENUE_BLEND` changes `gfh` and `gfa` BEFORE they are
summed, so it moves `mu_total` as well — a value that sharpens the team lane
while blunting the match lane is not an improvement, it is a transfer. Both are
scored here and both are reported.

    SIDE    P(that side scores), per venue. The split between home and away is
            the number the venue work has been chasing; pooled hides it.
    MATCH   P(total >= 2 / 3 / 4) against Poisson(mu_total). Selection-free, so
            it cannot move with the thing being changed the way a lane-level
            calibration figure does.

Both windows are kept separate. A blend that wins on recent data and not on
held-back data is a fit — the standard this project already applies to every
other constant, and the one that caught two claims about the team lane.

Usage:  python scripts/venue_blend_sweep.py [--n 150] [--values 0,0.2,0.35]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import features, store
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES

# Spans both directions and includes 0.0, because "no venue split at all" is
# the honest null: if the blend is worth nothing, that is the finding.
CANDIDATES = [0.0, 0.20, 0.35, 0.50, 0.65, 0.80]
THRESHOLDS = (2, 3, 4)


def poisson_at_least(k: int, mu: float) -> float:
    if k <= 0:
        return 1.0
    return 1.0 - sum(math.exp(-mu) * mu ** i / math.factorial(i)
                     for i in range(k))


def collect(lg: str, n: int, back: int) -> list[tuple]:
    """(p_home, hg, p_away, ag, mu_total, total) per priceable fixture."""
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
        hg, ag = int(r["hg"]), int(r["ag"])
        out.append((float(req.p_home_tt05), hg,
                    float(req.p_away_tt05), ag,
                    float(req.mu_total), hg + ag))
    return out


def score(rows: list[tuple]) -> tuple[float, float, float, float]:
    """(home gap, away gap, |split|, match gap) in points."""
    n = len(rows)
    home = sum(1 for r in rows if r[1] >= 1) / n - sum(r[0] for r in rows) / n
    away = sum(1 for r in rows if r[3] >= 1) / n - sum(r[2] for r in rows) / n
    # The match figure pools the three thresholds the ladder actually cuts on,
    # as a MEAN ABSOLUTE gap: signed gaps at different thresholds cancel, which
    # is the exact way this project has hidden defects from itself before.
    match = sum(
        abs(sum(1 for r in rows if r[5] >= k) / n
            - sum(poisson_at_least(k, r[4]) for r in rows) / n)
        for k in THRESHOLDS) / len(THRESHOLDS)
    return home * 100, away * 100, abs(home - away) * 100, match * 100


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    values = ([float(v) for v in args[args.index("--values") + 1].split(",")]
              if "--values" in args else CANDIDATES)

    original = features.VENUE_BLEND
    print(f"{'blend':8}{'window':12}{'n':>7}{'home':>8}{'away':>8}"
          f"{'|split|':>9}{'match':>8}")
    try:
        for v in values:
            features.VENUE_BLEND = v
            for wname, back in (("recent", 0), ("held-back", n)):
                rows = []
                for lg in LEAGUES:
                    try:
                        rows += collect(lg, n, back)
                    except Exception as exc:
                        print(f"{lg:9} FAILED {exc}", file=sys.stderr)
                if len(rows) < 200:
                    continue
                h, a, s, m = score(rows)
                mark = "  <- live" if v == original else ""
                print(f"{v:<8.2f}{wname:12}{len(rows):7}{h:+8.1f}{a:+8.1f}"
                      f"{s:9.1f}{m:8.2f}{mark}")
            print()
    finally:
        features.VENUE_BLEND = original


if __name__ == "__main__":
    main()
