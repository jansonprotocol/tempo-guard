"""
What do the configured under-line caps actually cost?

The caps encode a judgement the engine cannot verify: that U3.75 and U4.25 are
unbuyable in certain leagues. Nothing in the results data can confirm that, so
the honest thing to measure is the price paid for acting on it — the strike rate
and edge given up by refusing rungs the model would otherwise choose.

Toggling is done by clearing max_under_line on a copy of each league's config,
not by a module flag. An earlier version of this comparison flipped an attribute
that had been deleted in a refactor, so both arms ran identical code and the
table showed a change of exactly zero on every row.
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

CAPPED = ["BRA-SB", "ARG-PD", "ARG-CLP", "ITA-SB", "GRE-SL"]
LIMIT = 400


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals):
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def main():
    data = []
    for code in CAPPED:
        pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        if len(pairs) < 100:
            continue
        data.append((code, config.get(code), pairs))
        print(f"  built {code} ({len(pairs)})", flush=True)

    print(f"\n{len(data)} capped leagues, "
          f"{sum(len(p) for _, _, p in data)} fixtures\n")
    print(f"  {'setting':>10} {'strike':>8} {'edge':>8}   mix")
    print("  " + "-" * 66)

    for label, capped in (("uncapped", False), ("capped", True)):
        hits = tot = 0
        mk, totals = [], []
        for _code, cfg, pairs in data:
            c = deepcopy(cfg)
            if not capped:
                c.max_under_line = None
                c.min_over_line = None
            flags = ModuleFlags(**(c.module_overrides or {}))
            for req, (hg, ag) in pairs:
                m = predict_fixture(req, c, module_flags=flags).translated_play.market
                t = hg + ag
                hits += won(m, t)
                tot += 1
                mk.append(m)
                totals.append(t)
        strike = hits / tot
        mix = " ".join(f"{m}:{n * 100 // tot}%" for m, n in Counter(mk).most_common(5))
        print(f"  {label:>10} {strike:8.1%} {strike - base_of(mk, totals):+8.2%}   {mix}",
              flush=True)


if __name__ == "__main__":
    main()
