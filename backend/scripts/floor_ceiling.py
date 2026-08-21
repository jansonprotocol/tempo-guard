"""
Where does a league's hit rate stop rising, and is the engine earning it?

Raising the probability floor makes the selector refuse anything it is not
confident about. Strike goes up almost by construction — the question is
whether it goes up faster than the base rate of what it is now buying. If it
does not, the engine has stopped predicting and started buying certainty, and
a flat bet on the safest rung would do the same job for free.

So three things are printed side by side per floor:

    strike   how often the published market lands
    base     how often those same markets land across the sample
    edge     the difference, i.e. what the engine contributed

and underneath, flat-bet references: always U4.25, always U3.0, always O1.5.
A floor whose strike never beats the best flat bet is a floor that has been
tuned into a constant.

Holdout-split, because a floor swept and read on the same matches is a floor
chosen to fit them.

Usage:  python scripts/floor_ceiling.py JPN-J1 500
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUE = sys.argv[1] if len(sys.argv) > 1 else "JPN-J1"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 500
HOLDOUT = 0.35
FLOORS = [0.79, 0.82, 0.85, 0.88, 0.91, 0.94]
FLAT = ["U4.25", "U3.5", "U3.0", "O1.5"]


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals):
    if not markets or not totals:
        return 0.0
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def run(pairs, cfg, flags):
    markets, totals = [], []
    for req, (hg, ag) in pairs:
        markets.append(predict_fixture(req, cfg, module_flags=flags)
                       .translated_play.market)
        totals.append(int(hg) + int(ag))
    hits = sum(1 for m, t in zip(markets, totals) if won(m, t))
    return hits, markets, totals


def main() -> None:
    pairs = _requests_for(LEAGUE, None, None, CALIB_MIN_MATCHES, limit=N)
    pairs.sort(key=lambda p: p[0].match_date)
    cut = int(len(pairs) * (1 - HOLDOUT))
    train, hold = pairs[:cut], pairs[cut:]
    cfg = config.get(LEAGUE)
    flags = ModuleFlags(**(cfg.module_overrides or {}))

    print(f"{LEAGUE}: {len(pairs)} matches "
          f"({len(train)} train / {len(hold)} holdout), "
          f"{pairs[0][0].match_date} to {pairs[-1][0].match_date}\n")
    print(f"  {'floor':>6}  {'train':>14}  {'HOLDOUT':>14}  {'base':>7}  "
          f"{'edge':>8}   mix")
    print("  " + "-" * 92)

    for fl in FLOORS:
        c = deepcopy(cfg)
        c.min_win_prob = fl
        th, _, _ = run(train, c, flags)
        hh, hm, ht = run(hold, c, flags)
        base = base_of(hm, ht)
        strike = hh / len(hold)
        mix = "  ".join(f"{m}:{n * 100 // len(hold)}%"
                        for m, n in Counter(hm).most_common(3))
        print(f"  {fl:6.2f}  {th:3d}/{len(train):3d} = {th / len(train):5.1%}  "
              f"{hh:3d}/{len(hold):3d} = {hh / len(hold):5.1%}  {base:6.1%}  "
              f"{strike - base:+7.2%}   {mix}")

    totals = [int(hg) + int(ag) for _, (hg, ag) in hold]
    print(f"\n  flat bets on the same {len(hold)} holdout matches (no engine):")
    for m in FLAT:
        w = sum(1 for t in totals if won(m, t))
        print(f"    always {m:6s} {w:3d}/{len(totals)} = {w / len(totals):5.1%}")


if __name__ == "__main__":
    main()
