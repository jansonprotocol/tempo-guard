"""
Does picking the market from the probability model beat the flowchart?

The flowchart (`translate_play`) never reads the goal estimate, which is why
four separate improvements to that estimate produced no change in edge. This
scores the replacement against it on the same holdout, with the same scoring
rule, so the only difference is the decision rule.

Judged on edge, not hit rate. That distinction has already mattered once: the
tempo sweep found a setting with the best hit rate in the whole search (81.6%)
and an edge of +0.01%, because it had quietly become a machine for emitting
U4.25. The new selector can fail exactly the same way — on a fixture that looks
completely ordinary every market has zero edge and it falls back to the safest
line — so the market mix is reported alongside the numbers and a collapse
toward one market is a failure regardless of what the hit rate says.

Also reports the flip ledger (rescued vs broken), since a change that wins the
same number of matches by different means is not an improvement.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
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


def base_rate(markets, totals) -> float:
    """What this mix of markets would score against the league's own totals."""
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
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 60:
            continue
        pairs.sort(key=lambda p: p[0].match_date)
        cut = int(len(pairs) * (1 - HOLDOUT_FRACTION))
        data.append((code, config.get(code), pairs[cut:]))

    n = sum(len(h) for _, _, h in data)
    print(f"{len(data)} leagues, pooled holdout {n} matches\n", flush=True)

    def evaluate(prob: bool):
        per, mk = {}, Counter()
        H = T = 0
        for code, cfg, hold in data:
            base_flags = dict(cfg.module_overrides or {})
            base_flags["prob_select"] = prob
            r = replay(code, cfg, _pairs=hold, module_flags=ModuleFlags(**base_flags))
            per[code] = r.outcomes
            for o in r.outcomes:
                mk[o.market] += 1
            H += r.hits
            T += r.sample
        return per, mk, H, T

    old_per, old_mk, oH, oT = evaluate(False)
    new_per, new_mk, nH, nT = evaluate(True)

    def pooled_edge(per):
        hits = tot = 0
        edges = []
        for code, outs in per.items():
            if not outs:
                continue
            totals = [o.total_goals for o in outs]
            markets = [o.market for o in outs]
            h = sum(o.hit for o in outs)
            hits += h
            tot += len(outs)
            edges.append((code, h / len(outs), base_rate(markets, totals), len(outs)))
        b = sum(e[2] * e[3] for e in edges) / sum(e[3] for e in edges)
        return hits / tot, b, edges

    o_hit, o_base, o_edges = pooled_edge(old_per)
    n_hit, n_base, n_edges = pooled_edge(new_per)

    print(f"  {'rule':16s} {'hit':>7} {'base':>7} {'EDGE':>7}")
    print("  " + "-" * 42)
    print(f"  {'flowchart':16s} {o_hit:7.2%} {o_base:7.2%} {o_hit - o_base:+7.2%}")
    print(f"  {'probability':16s} {n_hit:7.2%} {n_base:7.2%} {n_hit - n_base:+7.2%}")

    resc = brok = 0
    for code in old_per:
        for a, b in zip(old_per[code], new_per[code]):
            if not a.hit and b.hit:
                resc += 1
            elif a.hit and not b.hit:
                brok += 1
    print(f"\n  flips: rescued {resc}, broken {brok}, net {resc - brok:+d}")

    for label, mk, tot in (("flowchart", old_mk, oT), ("probability", new_mk, nT)):
        print(f"\n  {label} market mix:")
        for m, c in mk.most_common(8):
            print(f"    {m:8s} {c:5d}  {c / tot:5.1%}")
        top = mk.most_common(1)[0]
        if top[1] / tot > 0.50:
            print(f"    ^ {top[0]} is {top[1] / tot:.0%} of all calls — "
                  f"this is close to a constant, not a prediction")

    print(f"\n  per-league edge")
    print(f"    {'league':8s} {'n':>4} {'flowchart':>10} {'prob':>8} {'delta':>8}")
    better = worse = 0
    om = {e[0]: e for e in o_edges}
    for code, hit, base, cnt in n_edges:
        oe = om[code][1] - om[code][2]
        ne = hit - base
        d = ne - oe
        better += d > 0.005
        worse += d < -0.005
        print(f"    {code:8s} {cnt:4d} {oe:+10.1%} {ne:+8.1%} {d:+8.1%}")
    print(f"\n    better {better}   worse {worse}   unchanged {len(n_edges) - better - worse}")


if __name__ == "__main__":
    main()
