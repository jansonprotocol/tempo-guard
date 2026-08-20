"""
Two loose ends from the global sweep, both of which change what the proposals
mean.

1. IS bias_shift A DEAD DIAL?
   The sweep returned byte-identical holdout hit rates for every bias_shift
   from -0.8 to +0.4 across 3,952 matches. That is not "a small effect", that
   is no effect. But it was measured at one tempo_factor, so it could in
   principle be a dial that only bites elsewhere. Testing it at the current
   default settles it.

   This matters because four of the eleven proposals (JPN-J1, FRA-L2, POL-EK,
   SCO-CH) rest on bias_shift. If the dial does nothing, those gains were the
   greedy search draping noise over an inert knob — and that is direct evidence
   about how much of the rest of the table to believe.

2. WHERE DOES tempo_factor ACTUALLY BOTTOM OUT?
   The sweep was monotonic all the way down to 0.25, the bottom of the extended
   grid — so it hit the boundary a second time. Either the optimum is below
   0.25, or the curve is flattening onto a plateau. Extending the grid down to
   0.05 tells which, and a plateau that reaches near zero would mean the tempo
   signal is being scaled down toward irrelevance, which is a statement about
   the signal rather than about its multiplier.
"""
from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
from app.engine.types import ModuleFlags

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "FRA-L2",
    "NED-ED", "POR-PL", "TUR-SL", "GRE-SL", "POL-EK", "JPN-J1",
    "SCO-CH", "BRA-SB", "COL-PA", "MLS",
]
LIMIT = 400
HOLDOUT_FRACTION = 0.30

LOW_TEMPOS = [0.0, 0.05, 0.10, 0.15, 0.20, 0.25, 0.35, 0.56]
SHIFTS = [-0.8, -0.5, -0.2, 0.0, 0.2, 0.5]


def score(code, cfg, pairs):
    r = replay(code, cfg, _pairs=pairs,
               module_flags=ModuleFlags(**(cfg.module_overrides or {})))
    return r.hits, r.sample


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

    def pooled(mutate):
        h = s = 0
        for code, cfg, hold in data:
            a, b = score(code, mutate(deepcopy(cfg)), hold)
            h += a
            s += b
        return h / s if s else 0.0

    print("1. bias_shift at the CURRENT tempo_factor")
    seen = set()
    for v in SHIFTS:
        def m(c, v=v):
            c.base_over_bias = round(min(1.0, max(0.0, 0.5 + v / 2)), 3)
            c.base_under_bias = round(min(1.0, max(0.0, 0.5 - v / 2)), 3)
            return c
        r = pooled(m)
        seen.add(round(r, 6))
        print(f"   {v:+5.2f}   {r:6.2%}", flush=True)
    print(f"   -> {'DEAD DIAL: identical at every value' if len(seen) == 1 else 'has an effect'}\n")

    print("2. tempo_factor, grid extended toward zero")
    for t in LOW_TEMPOS:
        def m(c, t=t):
            c.tempo_factor = t
            return c
        mark = "   <- current" if abs(t - 0.56) < 1e-9 else ""
        print(f"   {t:5.2f}   {pooled(m):6.2%}{mark}", flush=True)


if __name__ == "__main__":
    main()
