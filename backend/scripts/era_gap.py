"""
Why is edge smaller on recent matches than on older ones?

The confirmation run scored +2.37% edge over 2014-2025 and the recent holdout
+0.72%. Read carelessly that says the engine's signal is decaying. Decomposed,
it says something else:

    historical   strike 79.89%   base 77.51%   edge +2.37%
    recent       strike 79.66%   base 78.94%   edge +0.72%

The strike rates differ by 0.23 points — nothing. The base rate differs by 1.43.
Edge is strike minus base, so the entire gap comes from the benchmark rising,
not from the engine falling.

Which leaves two candidates, and they need different responses:

  matches changed   football itself moved — totals cluster differently, so the
                    lines the engine plays land more often by default. Nothing
                    to fix; the edge was always partly a gift from a more
                    variable era, and the honest expectation for the future is
                    the lower number.

  mix changed       the engine now picks different markets than it used to, and
                    the new ones have higher base rates. That is a property of
                    today's selector, and if those markets are also easier to
                    beat it is fine — if not, it is buying strike with base rate
                    exactly as the tempo sweep did.

The clean separator is a FIXED market. U4.25's base rate depends only on the
matches, not on what the engine chose, so tracking it per era isolates the first
explanation from the second.

RESULT: NEITHER. THE ERAS WERE NEVER THE VARIABLE.
==================================================
On a fixed set of 12 leagues:

    2015-2017      883   strike 78.9%  base 77.8%  edge +1.17%   (too thin)
    2018-2020     5641   strike 81.9%  base 77.5%  edge +4.40%
    2021-2023    11912   strike 81.8%  base 78.3%  edge +3.49%
    2024-2026    10152   strike 82.7%  base 78.9%  edge +3.80%

Edge is stable between +3.5% and +4.4% from 2018 on, and the most recent era
beats 2021-2023. Strike rate rises across the period. Nothing decays.

The apparent gap came from comparing different LEAGUE SETS, not different eras.
Over the same recent period, these 12 leagues score +3.80% while the 32-league
holdout scores +0.72% — because the latter includes CHN-SL, IRL-PD, FIN-VL and
the others already measured at negative edge. The confirmation run, meanwhile,
covered 27 leagues rather than 32 only because DEN-SL, SWE-AL and POL-EK lacked
the history for its window, and those skips silently dropped weak leagues and
inflated its number.

Two samples differing in composition were read as a difference in time. Any
future comparison across periods has to hold the league set fixed, or it is
measuring which leagues were included.

KNOWN FLAWS IN THIS SCRIPT
==========================
The 2,400-match window per league starves the early eras — 2015-2017 gets 883
fixtures and is not comparable to the rest. Per-era windows would fix it.

`_requests_for` is called inside the era loop, so features are rebuilt for every
era: ~115,000 builds where 29,000 would do, turning a four-minute job into
fifty. Build once, filter by date.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, store
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "NED-ED",
    "POR-PL", "BEL-PL", "TUR-SL", "SCO-PL", "ENG-CH", "GRE-SL",
]
ERAS = [
    ("2015-2017", "2015-01-01", "2018-01-01"),
    ("2018-2020", "2018-01-01", "2021-01-01"),
    ("2021-2023", "2021-01-01", "2024-01-01"),
    ("2024-2026", "2024-01-01", "2027-01-01"),
]
WINDOW = 2400
FIXED = ["U4.25", "U3.5", "O1.5", "O2.5"]


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals):
    if not markets or not totals:
        return 0.0
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def main() -> None:
    # Part 1 — the matches alone, no engine involved. Base rate of fixed lines.
    print("MATCHES ONLY — base rate of a fixed line, by era")
    print(f"  {'era':10s} {'n':>7} {'mu':>6} {'sd':>6} " +
          " ".join(f"{m:>7}" for m in FIXED))
    print("  " + "-" * 62)
    for label, lo, hi in ERAS:
        tot = []
        for lg in LEAGUES:
            d = store.load_results(lg)
            d = d[(d["date"] >= lo) & (d["date"] < hi)]
            tot.extend((d["hg"] + d["ag"]).dropna().tolist())
        if len(tot) < 500:
            print(f"  {label:10s} {len(tot):7d}   (too few)")
            continue
        a = np.array(tot)
        rates = " ".join(f"{np.mean([won(m, t) for t in a]):7.1%}" for m in FIXED)
        print(f"  {label:10s} {len(a):7d} {a.mean():6.2f} {a.std():6.2f} {rates}",
              flush=True)

    # Part 2 — the engine, on the same eras.
    print("\nENGINE — strike, base and edge by era")
    print(f"  {'era':10s} {'n':>6} {'strike':>8} {'base':>8} {'edge':>8}   mix")
    print("  " + "-" * 72)
    for label, lo, hi in ERAS:
        mk, totals, hits = [], [], 0
        for lg in LEAGUES:
            try:
                pairs = _requests_for(lg, None, None, CALIB_MIN_MATCHES,
                                      limit=WINDOW)
            except Exception:
                continue
            cfg = config.get(lg)
            flags = ModuleFlags(**(cfg.module_overrides or {}))
            for req, (hg, ag) in pairs:
                if not (str(lo) <= str(req.match_date) < str(hi)):
                    continue
                m = predict_fixture(req, cfg, module_flags=flags).translated_play.market
                t = hg + ag
                hits += won(m, t)
                mk.append(m)
                totals.append(t)
        if len(mk) < 300:
            print(f"  {label:10s} {len(mk):6d}   (too few)")
            continue
        strike = hits / len(mk)
        base = base_of(mk, totals)
        mix = " ".join(f"{m}:{n * 100 // len(mk)}%"
                       for m, n in Counter(mk).most_common(4))
        print(f"  {label:10s} {len(mk):6d} {strike:8.1%} {base:8.1%} "
              f"{strike - base:+8.2%}   {mix}", flush=True)


if __name__ == "__main__":
    main()
