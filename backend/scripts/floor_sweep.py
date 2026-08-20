"""
How safe must a bet be before it is allowed?

The first run of the probability selector tripled edge (+0.72% -> +2.53%) and
sent the strike rate through the floor (79.7% -> 60.1%). The cause is in the
scoring rule: edge is defined as the gap between this fixture's win chance and
a typical one's, and that gap is naturally largest for lines sitting in the
middle of the goal distribution, because those are the ones whose odds move
most when the goal estimate moves. Tail lines like U4.25 win ~86% of the time
almost regardless of the fixture, so they can never show much edge.

So "maximise the gap" quietly means "take the most volatile line available".
With the floor at 0.55 it did exactly that, and won 60% of the time.

The floor is what bounds this. Raising it restricts the search to bets that
clear a given strike rate and takes the best edge among those. This sweeps it
against the brief: 80-85% strike, with as much edge as can be had there.

Both numbers are reported because they trade off, and the market mix is
reported because a high floor can collapse the selector into a single safe line
— which scores well and is worth nothing.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "FRA-L2",
    "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL", "SCO-PL",
    "SUI-SL", "DEN-SL", "SWE-AL", "NOR-EL", "POL-EK", "CZE-FL",
    "FIN-VL", "IRL-PD", "RUS-PL", "ENG-CH", "ENG-L2", "SCO-CH",
    "BRA-SA", "BRA-SB", "ARG-PD", "COL-PA", "MEX-LMX", "MLS",
    "JPN-J1", "CHN-SL",
]
LIMIT = 400
HOLDOUT_FRACTION = 0.30
FLOORS = [0.55, 0.62, 0.68, 0.72, 0.76, 0.80, 0.84, 0.88]


def base_rate(markets, totals) -> float:
    if not markets or not totals:
        return 0.0
    return sum(
        sum(1 for t in totals if hit_weight(evaluate_market(m, t, 0)) >= 1.0) / len(totals)
        for m in markets
    ) / len(markets)


def main() -> None:
    data = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 60:
            continue
        pairs.sort(key=lambda p: p[0].match_date)
        cut = int(len(pairs) * (1 - HOLDOUT_FRACTION))
        data.append((code, config.get(code), pairs[cut:]))

    n = sum(len(h) for _, _, h in data)
    print(f"{len(data)} leagues, pooled holdout {n} matches\n", flush=True)
    print(f"  {'floor':>6} {'hit':>7} {'base':>7} {'EDGE':>7} {'ROI~':>7}  "
          f"{'leagues>=80%':>12}  top market")
    print("  " + "-" * 74)

    def run(prob, floor=None):
        if floor is not None:
            market_select.MIN_WIN_PROB = floor
        hits = tot = 0
        mk = Counter()
        edges, over80 = [], 0
        for code, cfg, hold in data:
            f = dict(cfg.module_overrides or {})
            f["prob_select"] = prob
            r = replay(code, cfg, _pairs=hold, module_flags=ModuleFlags(**f))
            if not r.sample:
                continue
            hits += r.hits
            tot += r.sample
            totals = [o.total_goals for o in r.outcomes]
            markets = [o.market for o in r.outcomes]
            for m in markets:
                mk[m] += 1
            edges.append((base_rate(markets, totals), r.sample))
            over80 += r.hit_rate >= 0.80
        b = sum(e[0] * e[1] for e in edges) / sum(e[1] for e in edges)
        return hits / tot, b, mk, over80, len(edges)

    hit, base, mk, o80, nl = run(False)
    top = mk.most_common(1)[0]
    roi = (hit - base) / base * 100 if base else 0
    print(f"  {'OLD':>6} {hit:7.2%} {base:7.2%} {hit - base:+7.2%} {roi:+6.1f}%  "
          f"{o80:6d}/{nl:<5d}  {top[0]} {top[1] / sum(mk.values()):.0%}", flush=True)

    for fl in FLOORS:
        hit, base, mk, o80, nl = run(True, fl)
        top = mk.most_common(1)[0]
        share = top[1] / sum(mk.values())
        roi = (hit - base) / base * 100 if base else 0
        warn = "  <- near-constant" if share > 0.55 else ""
        print(f"  {fl:6.2f} {hit:7.2%} {base:7.2%} {hit - base:+7.2%} {roi:+6.1f}%  "
              f"{o80:6d}/{nl:<5d}  {top[0]} {share:.0%}{warn}", flush=True)


if __name__ == "__main__":
    main()
