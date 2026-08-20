"""
Does the 0.79 floor hold up on matches it was not chosen from?

The floor was swept over each league's most recent 400 matches, scored on the
last 30% of those. Those numbers are honest out-of-sample for the *engine* —
nothing peeked at a result — but not for the *choice of floor*, which was made
by reading that very table. Picking the best row of a table and then quoting
that row is how the earlier "85% leagues" came to lose two thirds of their edge
the moment they were re-measured somewhere else.

So this scores the same settings on the ~1,200 matches immediately BEFORE that
window. Those matches had no influence on which floor was chosen, so selection
cannot follow the measurement into them.

Three things are compared on that era:

    flowchart     the rule cascade that was live this morning
    floor 0.79    what is live now
    floor 0.82    the neighbour, as a check that the curve is flat here rather
                  than 0.79 being a lucky point on a noisy line

If 0.79 holds its ~+1 point of strike rate and its edge over the flowchart, it
is a property of the engine. If it collapses to the flowchart, the sweep found
noise and the default should go back.
"""
from __future__ import annotations

import math
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
    "SUI-SL", "DEN-SL", "SWE-AL", "POL-EK", "CZE-FL", "FIN-VL",
    "IRL-PD", "RUS-PL", "ENG-CH", "ENG-L2", "SCO-CH", "BRA-SA",
    "ARG-PD", "COL-PA", "MEX-LMX", "MLS", "JPN-J1", "CHN-SL",
]
WINDOW = 1600      # build this many
RECENT = 400       # drop the most recent 400 — the era the floor came from


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
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=WINDOW)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < RECENT + 300:
            print(f"{code}: skipped (only {len(pairs)} replayable)", flush=True)
            continue
        pairs.sort(key=lambda p: p[0].match_date)
        data.append((code, config.get(code), pairs[:-RECENT]))
        print(f"  built {code} ({len(pairs) - RECENT} in the confirmation era)",
              flush=True)

    if not data:
        print("no data")
        return

    n = sum(len(h) for _, _, h in data)
    span = (min(h[0][0].match_date for _, _, h in data),
            max(h[-1][0].match_date for _, _, h in data))
    print(f"\n{len(data)} leagues, {n} matches, {span[0]} to {span[1]}")
    print("None of these influenced the choice of floor.\n")

    def run(prob: bool, floor: float | None):
        if floor is not None:
            market_select.MIN_WIN_PROB = floor
        hits = tot = 0
        mk = Counter()
        per, bases = {}, []
        for code, cfg, hold in data:
            f = dict(cfg.module_overrides or {})
            f["prob_select"] = prob
            r = replay(code, cfg, _pairs=hold, module_flags=ModuleFlags(**f))
            if not r.sample:
                continue
            hits += r.hits
            tot += r.sample
            markets = [o.market for o in r.outcomes]
            totals = [o.total_goals for o in r.outcomes]
            for m in markets:
                mk[m] += 1
            bases.append((base_rate(markets, totals), r.sample))
            per[code] = (r.hit_rate, base_rate(markets, totals), r.sample,
                         [o.hit for o in r.outcomes])
        b = sum(x * c for x, c in bases) / sum(c for _, c in bases)
        return hits / tot, b, mk, per, tot

    results = {}
    print(f"  {'rule':16s} {'hit':>7} {'base':>7} {'EDGE':>7} {'95% CI on hit':>18}  top market")
    print("  " + "-" * 76)
    for label, prob, floor in (("flowchart", False, None),
                               ("floor 0.79", True, 0.79),
                               ("floor 0.82", True, 0.82)):
        hit, base, mk, per, tot = run(prob, floor)
        results[label] = (hit, base, per, tot)
        se = math.sqrt(hit * (1 - hit) / tot)
        top = mk.most_common(1)[0]
        print(f"  {label:16s} {hit:7.2%} {base:7.2%} {hit - base:+7.2%} "
              f"  {hit - 1.96 * se:6.2%}..{hit + 1.96 * se:6.2%}  "
              f"{top[0]} {top[1] / tot:.0%}", flush=True)

    # Paired comparison: same matches, so the flip ledger is the sharper test.
    fh, fb, fper, _ = results["flowchart"]
    nh, nb, nper, _ = results["floor 0.79"]
    resc = brok = 0
    for code in fper:
        if code not in nper:
            continue
        for a, b in zip(fper[code][3], nper[code][3]):
            if not a and b:
                resc += 1
            elif a and not b:
                brok += 1
    net = resc - brok
    print(f"\n  0.79 vs flowchart on the same matches: "
          f"rescued {resc}, broken {brok}, net {net:+d}")
    print(f"  strike {fh:.2%} -> {nh:.2%} ({nh - fh:+.2f} pts), "
          f"edge {fh - fb:+.2%} -> {nh - nb:+.2%}")

    o80_old = sum(1 for c in fper if fper[c][0] >= 0.80)
    o80_new = sum(1 for c in nper if nper[c][0] >= 0.80)
    print(f"  leagues at 80%+: {o80_old} -> {o80_new} (of {len(fper)})")

    print("\n  per league (flowchart -> 0.79)")
    print(f"    {'league':8s} {'n':>5} {'flow':>7} {'0.79':>7} {'delta':>7}")
    better = worse = 0
    for code in sorted(fper):
        if code not in nper:
            continue
        a, b = fper[code][0], nper[code][0]
        d = b - a
        better += d > 0.005
        worse += d < -0.005
        print(f"    {code:8s} {fper[code][2]:5d} {a:7.1%} {b:7.1%} {d:+7.1%}")
    print(f"\n    better {better}   worse {worse}   "
          f"unchanged {len(fper) - better - worse}")

    print()
    if net > 0 and (nh - nb) >= (fh - fb) - 0.002:
        print("  VERDICT: holds up on data it was not chosen from.")
    elif abs(net) <= max(10, 0.01 * sum(fper[c][2] for c in fper)):
        print("  VERDICT: indistinguishable from the flowchart here. The sweep's "
              "advantage did not reproduce; treat 0.79 as no better than what it "
              "replaced.")
    else:
        print("  VERDICT: worse on unseen data. The sweep fitted noise and the "
              "default should revert.")


if __name__ == "__main__":
    main()
