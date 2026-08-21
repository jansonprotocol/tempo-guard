"""
Do the top leagues survive a era they were not selected on?

Six leagues were reported as 82-87% with +4 to +10% edge. That list was the top
six of thirty-two, ranked on ~120 holdout matches each. Both facts undermine it:
ranking noisy samples and quoting the winners guarantees inflation, and at n=120
a hit rate near 86% carries a 95% interval of roughly six points either way.

Neither is a reason to disbelieve the numbers. Both are reasons to re-measure
them somewhere the selection could not reach.

Design: the earlier reading used each league's most recent 400 matches and
scored the last 30% of those. This scores the ~1,200 matches immediately
BEFORE that window — same leagues, same settings, an era that had no influence
on which six got picked. Selection cannot survive into data it never saw.

  holds up      the edge is a property of the league, and 85% is real there
  collapses     the ranking was luck and the six were the lucky tail

Reported against base rate throughout, since hit rate alone was already shown
to be purchasable by retreating to safer markets.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
from app.engine.types import ModuleFlags
from app.util.asian_lines import evaluate_market, hit_weight

# The six reported, plus the weak leagues as a control: if the confirmation
# era simply scores higher for everyone, that shows up here too.
SELECTED = ["TUR-SL", "ITA-SA", "NOR-EL", "BEL-PL", "NED-ED", "GER-BL"]
CONTROL = ["GRE-SL", "FRA-L2", "IRL-PD", "ENG-PL", "ESP-LL", "POL-EK"]

WINDOW = 1600      # build this many, then drop the most recent 400
RECENT = 400       # the era the original ranking came from


def base_rate_of(code: str, markets: list[str], totals: list[int]) -> float:
    """
    How often the chosen markets win on this league's own goal distribution,
    ignoring which fixture each was assigned to. This is the score a coin-flip
    version of the engine would post by emitting the same mix of markets.
    """
    if not markets:
        return 0.0
    hits = 0
    for m in markets:
        hits += sum(1 for t in totals if hit_weight(evaluate_market(m, t, 0)) >= 1.0) / len(totals)
    return hits / len(markets)


def run(code: str) -> tuple | None:
    pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=WINDOW)
    if len(pairs) < RECENT + 300:
        return None
    pairs.sort(key=lambda p: p[0].match_date)
    earlier = pairs[:-RECENT]          # never touched by the original ranking

    cfg = config.get(code)
    r = replay(code, cfg, _pairs=earlier,
               module_flags=ModuleFlags(**(cfg.module_overrides or {})))
    if not r.sample:
        return None

    totals = [o.total_goals for o in r.outcomes]
    markets = [o.market for o in r.outcomes]
    base = base_rate_of(code, markets, totals)
    hit = r.hit_rate
    se = math.sqrt(hit * (1 - hit) / r.sample)
    span = (r.outcomes[0].match_date, r.outcomes[-1].match_date)
    return code, r.sample, hit, base, hit - base, se, span


def main() -> None:
    print(f"Confirmation era: the {WINDOW - RECENT} matches before each league's "
          f"most recent {RECENT}.\n")
    print(f"  {'league':8s} {'n':>5} {'hit':>7} {'base':>7} {'edge':>7} "
          f"{'95% CI on hit':>18}   period")
    print("  " + "-" * 78)

    for label, group in (("SELECTED (reported as top six)", SELECTED),
                         ("CONTROL (weak + large leagues)", CONTROL)):
        print(f"\n  {label}")
        for code in group:
            try:
                res = run(code)
            except Exception as exc:
                print(f"  {code:8s} skipped ({exc})", flush=True)
                continue
            if res is None:
                print(f"  {code:8s} not enough history", flush=True)
                continue
            c, n, hit, base, edge, se, (d0, d1) = res
            lo, hi = hit - 1.96 * se, hit + 1.96 * se
            print(f"  {c:8s} {n:5d} {hit:7.1%} {base:7.1%} {edge:+7.1%} "
                  f"  {lo:6.1%}..{hi:6.1%}   {d0} to {d1}", flush=True)


if __name__ == "__main__":
    main()
