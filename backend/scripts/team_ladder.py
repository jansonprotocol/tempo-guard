"""
The whole team-total ladder, not just the 0.5 line.

The first pass tested only `Over 0.5` and `Under 0.5` and rejected the Under
direction on calibration. That rejection was correct for that rung and wrong as
a conclusion about the direction, because 0.5 is the extreme tail of the
distribution — the one place a Poisson shape error bites hardest. `Under 1.5`
sits near the middle of the same distribution and should behave completely
differently.

WHAT IS BEING SLICED
====================
The engine holds ONE number per side: that side's goal expectation, published
indirectly as `p_home_tt05` / `p_away_tt05` = 1 - exp(-gf). Every rung below is
a different cut of that same estimate, recovered as gf = -ln(1 - p_tt05):

    TA U0.5   side scores 0          P(0)
    TA U1.5   side scores 0 or 1     P(<=1)
    TA U2.5   side scores 0, 1 or 2  P(<=2)
    TA O0.5   side scores at all     P(>=1)
    TA O1.5   side scores twice+     P(>=2)

So these are not five independent opinions. They are five places to cut one
opinion, and the question is which cuts the model gets right. If real goal
counts are over-dispersed relative to Poisson — more blanks and more hauls than
the shape predicts — the tails will mis-calibrate while the middle holds. That
is a testable claim rather than a guess, and it is what the calibration table
below answers.

Reported per rung: calibration against outcome, strike rate, the as-of base
rate of that rung in that league, and the difference. A rung is only worth
offering if its probability means something AND it beats picking that rung
blind.
"""
from __future__ import annotations

import math
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 150
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "ITA-SB", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL",
           "SUI-SL", "GRE-SL", "POL-EK", "BRA-SA", "ESP-L2"]

# rung -> (probability from a side expectation gf, did it land given goals g)
RUNGS = {
    "U0.5": (lambda gf: math.exp(-gf),
             lambda g: g == 0),
    "U1.5": (lambda gf: math.exp(-gf) * (1 + gf),
             lambda g: g <= 1),
    "U2.5": (lambda gf: math.exp(-gf) * (1 + gf + gf * gf / 2),
             lambda g: g <= 2),
    "O0.5": (lambda gf: 1 - math.exp(-gf),
             lambda g: g >= 1),
    "O1.5": (lambda gf: 1 - math.exp(-gf) * (1 + gf),
             lambda g: g >= 2),
}
ORDER = ["U0.5", "U1.5", "U2.5", "O0.5", "O1.5"]


def main() -> None:
    obs = []          # (league, rung, p, landed)
    goals = defaultdict(list)   # league -> [(home goals, away goals)]
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        for req, (hg, ag) in pairs:
            ph, pa = req.p_home_tt05, req.p_away_tt05
            if ph is None or pa is None or ph >= 1 or pa >= 1:
                continue
            hg, ag = int(hg), int(ag)
            goals[code].append((hg, ag))
            d = req.match_date
            for p_tt05, g in ((float(ph), hg), (float(pa), ag)):
                gf = -math.log(1 - p_tt05)
                for name in ORDER:
                    prob, land = RUNGS[name]
                    obs.append((code, name, prob(gf), land(g), d))
        print(f"  {code}: {len(pairs)}", flush=True)

    n_fx = sum(len(v) for v in goals.values())
    print(f"\n  {n_fx} fixtures, {n_fx * 2} sides\n")

    # As-of-free base rate per league per rung, over the same pool.
    base = {}
    for code, gs in goals.items():
        flat = [g for pair in gs for g in pair]
        for name in ORDER:
            _prob, land = RUNGS[name]
            base[(code, name)] = sum(land(g) for g in flat) / len(flat)

    print("  CALIBRATION — predicted vs actual, by rung")
    print(f"  {'rung':>6s} {'predicted':>10s} {'actual':>8s} {'gap':>8s}")
    for name in ORDER:
        sel = [(p, w) for _c, r, p, w, _d in obs if r == name]
        pred = sum(p for p, _ in sel) / len(sel)
        act = sum(w for _p, w in sel) / len(sel)
        flag = "  <- unusable" if abs(act - pred) > 0.05 else ""
        print(f"  {name:>6s} {pred:10.1%} {act:8.1%} {act - pred:+8.1%}{flag}")

    print("\n  EDGE AT A PROBABILITY FLOOR — offer the rung only when confident")
    print(f"  {'rung':>6s} {'floor':>6s} {'picks':>7s} {'hit':>8s} {'base':>8s} "
          f"{'edge':>8s} {'fair':>7s}")
    for name in ORDER:
        for floor in (0.60, 0.70, 0.75, 0.80, 0.85, 0.90):
            sel = [(c, p, w) for c, r, p, w, _d in obs if r == name and p >= floor]
            if len(sel) < 60:
                continue
            hit = sum(w for _c, _p, w in sel) / len(sel)
            bas = sum(base[(c, name)] for c, _p, _w in sel) / len(sel)
            print(f"  {name:>6s} {floor:6.2f} {len(sel):7d} {hit:8.2%} "
                  f"{bas:8.2%} {hit - bas:+8.2%} {1/hit if hit else 0:7.3f}")
        print()


    # ── Chronological holdout on the two rungs worth shipping ────────────
    print("  CHRONOLOGICAL HOLDOUT")
    obs.sort(key=lambda o: o[4])
    cut = int(len(obs) * 0.65)
    train, hold = obs[:cut], obs[cut:]
    print(f"  train {len(train)} side-observations (to {train[-1][4]}), "
          f"holdout {len(hold)} (from {hold[0][4]})\n")

    def m(sel, name, floor):
        s = [(c, p, w) for c, r, p, w, _d in sel if r == name and p >= floor]
        if len(s) < 40:
            return None
        hit = sum(w for _c, _p, w in s) / len(s)
        bas = sum(base[(c, name)] for c, _p, _w in s) / len(s)
        return len(s), hit, bas, hit - bas

    for name, floors in (("U1.5", (0.70, 0.75, 0.80)),
                         ("O1.5", (0.55, 0.60, 0.65)),
                         ("O0.5", (0.80, 0.85)),
                         ("U2.5", (0.85, 0.90))):
        for f in floors:
            a, b = m(train, name, f), m(hold, name, f)
            if not a or not b:
                continue
            print(f"  {name} floor {f:.2f}   train {a[0]:5d} edge {a[3]:+7.2%}"
                  f"   |   HOLDOUT {b[0]:5d} hit {b[1]:6.2%} base {b[2]:6.2%} "
                  f"edge {b[3]:+7.2%}")
        print()


if __name__ == "__main__":
    main()
