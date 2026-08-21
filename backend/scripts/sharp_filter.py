"""
Fewer, more confident sharp plays — without breaking what already works.

The brief: raise the sharp lane's strike rate by offering fewer plays, keeping
the ones it is most sure of.

Two obvious routes are already ruled out. Tightening the z-score gate was
measured earlier and backfires — the lane falls from 62.2% at 0.7 sigma to
52.4% at 1.3 sigma, because the engine is least reliable at its own extremes,
so demanding a more unusual fixture selects worse matches rather than better
ones. And swapping to pure probability selection loses nearly two points of
edge at a matched fire rate.

What the sharp lane comparison did establish is why the z-gate wins: it demands
two independent conditions before speaking — the fixture is unusual for its
league AND the lean agrees. Conjunctions of conservative filters selected better
than the model's own edge ranking.

So this keeps the z-gate and its market choice, and adds a third condition
rather than replacing either: the chosen market must also clear a minimum
modelled chance of landing. Plays the gate likes but the goal model considers a
coin flip get dropped.

Note what this deliberately does NOT do. It never lets the probability model
CHOOSE the market — that is the part measured to be overconfident. It only lets
it VETO. Ranking by predicted edge underperformed; using the same numbers as a
floor is the one role the safe lane showed they are reliable in.

Reported per threshold: how often the lane still fires, its strike rate, and
its edge, so the cost of each drop in volume is visible.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "FRA-L2",
    "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL", "SCO-PL",
    "SUI-SL", "CZE-FL", "FIN-VL", "IRL-PD", "ENG-CH", "ENG-L2",
    "SCO-CH", "BRA-SA", "ARG-PD", "COL-PA", "MEX-LMX", "MLS",
    "JPN-J1", "CHN-SL",
]
LIMIT = 400
THRESHOLDS = [0.00, 0.45, 0.50, 0.55, 0.60, 0.65, 0.70, 0.75]


def won(market, total):
    return hit_weight(evaluate_market(market, total, 0)) >= 1.0


def base_of(markets, totals):
    if not markets or not totals:
        return 0.0
    return sum(
        sum(1 for t in totals if won(m, t)) / len(totals) for m in markets
    ) / len(markets)


def main():
    rows = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            pred = predict_fixture(req, cfg, module_flags=flags)
            L = getattr(pred, "lanes", None)
            sharp = L.sharp.market if (L and L.sharp) else None
            rows.append({
                "total": hg + ag,
                "sharp": sharp,
                "mu": req.mu_total,
                "pwin": market_select.p_win(sharp, req.mu_total) if (sharp and req.mu_total) else None,
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return

    n = len(rows)
    print(f"\n{n} fixtures\n")
    print(f"  {'min win chance':>15} {'fires':>8} {'plays':>7} {'strike':>8} {'edge':>8}   mix")
    print("  " + "-" * 76)

    for th in THRESHOLDS:
        fired = [r for r in rows
                 if r["sharp"] and r["pwin"] is not None and r["pwin"] >= th]
        if len(fired) < 30:
            print(f"  {th:15.2f} {len(fired) / n:8.1%}  (too few to judge)")
            continue
        markets = [r["sharp"] for r in fired]
        totals = [r["total"] for r in rows]
        hits = sum(1 for r in fired if won(r["sharp"], r["total"]))
        strike = hits / len(fired)
        edge = strike - base_of(markets, totals)
        mix = " ".join(f"{m}:{c * 100 // len(fired)}%"
                       for m, c in Counter(markets).most_common(3))
        label = "none (current)" if th == 0 else f"{th:.2f}"
        print(f"  {label:>15} {len(fired) / n:8.1%} {len(fired):7d} "
              f"{strike:8.1%} {edge:+8.2%}   {mix}", flush=True)


if __name__ == "__main__":
    main()
