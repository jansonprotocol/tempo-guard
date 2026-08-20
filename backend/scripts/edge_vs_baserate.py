"""
Hit rate against the only benchmark that matters: the market's own base rate.

The engine's most-emitted market is U4.25, which wins whenever a match stays
under five goals. That happens 85.7% of the time in Ligue 1 and 88.8% in Serie
A *by itself* — no model, no features, no football knowledge. So an 85% hit
rate is not automatically a good result; picking U4.25 every week reaches it
while predicting nothing at all.

What separates a prediction from a constant is edge:

    edge = hit rate - the base rate of the markets actually chosen

A model that picks U4.25 on 100 fixtures and wins 86 has done nothing. A model
that picks U3.5 (65% base) on 100 fixtures and wins 74 has found nine points of
real signal, despite the lower headline number.

This is not the same as pricing against a bookmaker. Base rates are computed
from stored match results — football data, not market data — so this stays
inside the constraint that ATHENA reason only from the game itself.

Reported per setting: the raw hit rate, the base rate its own market mix would
have produced by chance, and the difference.
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config, store
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

SETTINGS = [
    ("current", None, None),
    ("tempo 0.25 / bias -0.80", 0.25, -0.80),
    ("tempo 0.35 / bias -0.80", 0.35, -0.80),
    ("tempo 0.40 / bias -0.20", 0.40, -0.20),
]


def base_rates(code: str) -> dict[str, float]:
    """How often each market would win in this league regardless of fixture."""
    df = store.load_results(code)
    if df.empty:
        return {}
    out = {}
    for m in ("O1.5", "O1.75", "O2.25", "O2.5", "O2.75",
              "U2.5", "U2.75", "U3.25", "U3.5", "U3.75", "U4.25"):
        w = [hit_weight(evaluate_market(m, int(h), int(a)))
             for h, a in zip(df["hg"], df["ag"])]
        w = [x for x in w if x >= 0]
        out[m] = sum(1 for x in w if x >= 1.0) / len(w) if w else 0.0
    return out


def main() -> None:
    data, rates = [], {}
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
        rates[code] = base_rates(code)

    n = sum(len(h) for _, _, h in data)
    print(f"{len(data)} leagues, pooled holdout {n} matches\n", flush=True)
    print(f"  {'setting':26s} {'hit':>7} {'base':>7} {'EDGE':>7}   markets")
    print("  " + "-" * 64)

    for label, t, s in SETTINGS:
        hits = tot = 0
        expected = 0.0
        mk = Counter()
        per_league = {}
        for code, cfg, hold in data:
            c = deepcopy(cfg)
            if t is not None:
                c.tempo_factor = t
                c.base_over_bias = round(min(1.0, max(0.0, 0.5 + s / 2)), 3)
                c.base_under_bias = round(min(1.0, max(0.0, 0.5 - s / 2)), 3)
            r = replay(code, c, _pairs=hold,
                       module_flags=ModuleFlags(**(c.module_overrides or {})))
            lh = le = 0.0
            for o in r.outcomes:
                tot += 1
                hits += o.hit
                mk[o.market] += 1
                b = rates[code].get(o.market, 0.0)
                expected += b
                lh += o.hit
                le += b
            if r.sample:
                per_league[code] = (r.hit_rate, le / r.sample, r.sample)

        hr = hits / tot
        br = expected / tot
        top = ", ".join(f"{m} {c / tot:.0%}" for m, c in mk.most_common(3))
        print(f"  {label:26s} {hr:7.2%} {br:7.2%} {hr - br:+7.2%}   {top}", flush=True)

        if t is None:
            baseline_league = per_league
        else:
            best_league = per_league

    # Per-league edge at the candidate, since the goal is per-league.
    print(f"\n  per-league EDGE (hit - base) at 'tempo 0.25 / bias -0.80'")
    print(f"  {'league':8s} {'n':>4} {'hit':>7} {'base':>7} {'edge':>7}    "
          f"{'was':>7} {'edge':>7}")
    pos = 0
    for code in sorted(best_league):
        h, b, sm = best_league[code]
        h0, b0, _ = baseline_league[code]
        pos += (h - b) > 0
        print(f"  {code:8s} {sm:4d} {h:7.1%} {b:7.1%} {h - b:+7.1%}    "
              f"{h0:7.1%} {h0 - b0:+7.1%}")
    print(f"\n  leagues with positive edge: {pos}/{len(best_league)}")


if __name__ == "__main__":
    main()
