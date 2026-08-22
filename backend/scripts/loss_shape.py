"""
What do the losses have in common, and is there a better rung to be buying?

Runs on every settled tip in the README rather than only the fixtures that were
backed, so nothing here is selected on whether a bet was placed.

WHAT THE MISSES LOOK LIKE
=========================
All 14 are boundary events. Ten are Unders that went over, four are Overs that
landed on exactly one goal, and there are only two genuine blowouts (Nantes 7
goals on mu 2.55, Śląsk 6 on 2.08). The engine is not losing because it reads
matches wrongly; it is losing by a goal.

THE ONE-RUNG-SAFER TEST
=======================
Buying one rung up the ladder rescues 8 of the 14 and breaks 0 winners.

**The "breaks 0" half is arithmetic, not evidence.** A safer rung's winning
totals are a strict superset of the tip's, so it CANNOT turn a winner into a
loser — the same set-containment trap that produced three fake zeros in the
tip-pair families earlier. The only real question is whether 8 rescues are worth
the price given up, which averages 8.6% (break-even 1.204 -> 1.100).

Holding the margin fixed so only the rung changes:

    margin    as issued    one safer    diff
       0%       +0.61%       +2.36%    +1.76
       5%       +5.32%       +7.16%    +1.84
      10%      +10.03%      +11.95%    +1.92

If the engine were perfectly calibrated this difference would be exactly zero —
the rescues would cost precisely what they are worth. The +1.84 IS the measured
miscalibration at the boundary, and it is small: overall calibration is +0.6
points across 95 tips. Suggestive, not established, and measured in-sample.

Hit rate moves further than ROI does: 85.3% -> roughly 93.7%.

CAN THE PRICE BE HAD?
=====================
The obvious objection is that safer rungs are short and books are stingy there.
Measured against the actual bet book, the opposite is true:

    fair price     n   avg margin offered   reached +5%
    under 1.15     6         +3.6%             2/6
    1.15-1.24     33         +6.5%            15/33
    1.25-1.34     21         +2.6%             5/21
    1.35+         10         -1.7%             2/10

The most generous band is 1.15-1.24, which is exactly where the safer rungs
sit. The stingiest is 1.35+, where the tips themselves are priced.

Usage:  python scripts/loss_shape.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing
from app.util.asian_lines import evaluate_market
from scripts.backfill_buyfrom import mu_for
from scripts.ledger import read_fixtures

# One rung up the ladder — a strictly larger set of winning totals.
SAFER = {
    "U2.75": "U3.5", "U3.0": "U4.25", "U3.25": "U4.25", "U3.5": "U4.25",
    "U4.25": "U4.5",
    "O1.5": "O1.0", "O1.75": "O1.5", "O2.25": "O1.75", "O2.5": "O2.25",
    "O2.75": "O2.5",
}


def settled():
    for name, f in read_fixtures().items():
        if f["hg"] is None:
            continue
        mu = mu_for(f["rung"], f["p"])
        if mu is None:
            continue
        r = evaluate_market(f["rung"], f["hg"], f["ag"])
        if r is None:
            continue
        yield dict(name=name, rung=f["rung"], p=f["p"], mu=mu,
                   tot=f["hg"] + f["ag"], hit=r is True or r == "half_win")


def roi(rows, pick, margin: float) -> tuple[int, float]:
    ret = n = 0.0
    for r in rows:
        m = pick(r)
        if m is None:
            continue
        try:
            odds = pricing.break_even(m, r["mu"]) * (1 + margin)
        except ValueError:
            continue
        s = pricing.settle_fraction(m, r["tot"])
        ret += max(s, 0.0) * odds + (1 - abs(s))
        n += 1
    return int(n), (ret / n - 1) * 100


def main() -> None:
    rows = list(settled())
    misses = [r for r in rows if not r["hit"]]
    print(f"{len(rows)} settled tips, {len(misses)} misses\n")

    print("every miss, by how far:")
    print(f"{'fixture':34}{'tip':7}{'mu':>6}{'actual':>8}")
    for r in sorted(misses, key=lambda r: -(r["tot"] - r["mu"])):
        print(f"{r['name'][:33]:34}{r['rung']:7}{r['mu']:6.2f}{r['tot']:8}")

    resc = brk = 0
    for r in rows:
        s2 = SAFER.get(r["rung"])
        if not s2:
            continue
        h2 = evaluate_market(s2, r["tot"], 0)
        h2 = h2 is True or h2 == "half_win"
        resc += (not r["hit"]) and h2
        brk += r["hit"] and not h2
    print(f"\none rung safer: rescues {resc}, breaks {brk}")
    print("  (breaks 0 is arithmetic — a safer rung's winning totals are a")
    print("   superset, so it cannot lose a match the tip won)")

    print(f"\n{'margin':>8}{'as issued':>12}{'one safer':>12}{'diff':>8}")
    for m in (0.0, 0.05, 0.10, 0.15):
        _, a = roi(rows, lambda r: r["rung"], m)
        _, b = roi(rows, lambda r: SAFER.get(r["rung"]), m)
        print(f"{m * 100:7.0f}%{a:+11.2f}%{b:+11.2f}%{b - a:+8.2f}")

    hit2 = sum(
        1 for r in rows
        if (lambda h: h is True or h == "half_win")(
            evaluate_market(SAFER.get(r["rung"], r["rung"]), r["tot"], 0))
    )
    print(f"\nhit rate  as issued {sum(r['hit'] for r in rows)}/{len(rows)}"
          f" = {sum(r['hit'] for r in rows) / len(rows) * 100:.1f}%"
          f"   one safer {hit2}/{len(rows)} = {hit2 / len(rows) * 100:.1f}%")


if __name__ == "__main__":
    main()
