"""
Score candidate settings the way the bet actually pays: net flips.

A hit-rate delta hides the trade being made. "+2.4%" could be 10 misses
rescued at the cost of 1 new one, or 90 rescued at the cost of 68 — same
headline, completely different change. What matters is the ledger:

    rescued   a miss under the current settings that becomes a win
    broken    a win under the current settings that becomes a miss
    net       rescued - broken

A change is worth making when it rescues clearly more than it breaks. Seven
for two is excellent; four for three is noise wearing a positive sign.

ALSO: A SANITY CHECK ON THE FLAT TAIL
=====================================
The tempo sweep returned an identical 81.65% for every tempo_factor from 0.00
to 0.20 — five different settings, same hit rate to four decimals. Identical
output across different inputs usually means the input stopped being read, not
that five settings are equally good. The likely cause is saturation: scale the
tempo signal down far enough and every fixture lands in the same bucket, so the
engine stops discriminating and just repeats one market.

That would also explain why bias_shift looked inert in the global sweep — it
was measured at tempo_factor 0.25, near that dead zone.

So this counts distinct markets emitted at each setting. A high hit rate on one
market is not a working engine; it is a constant, and it will not survive
contact with a league whose base rate differs.
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
from app.engine.types import ModuleFlags

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

# Around the peak the probe found, not below the dead zone.
TEMPOS = [0.25, 0.30, 0.35, 0.40]
SHIFTS = [-0.8, -0.6, -0.4, -0.2, 0.0]


def outcomes(code, cfg, pairs) -> list[bool]:
    r = replay(code, cfg, _pairs=pairs,
               module_flags=ModuleFlags(**(cfg.module_overrides or {})))
    return r


def tune(cfg, tempo, shift):
    c = deepcopy(cfg)
    c.tempo_factor = tempo
    c.base_over_bias = round(min(1.0, max(0.0, 0.5 + shift / 2)), 3)
    c.base_under_bias = round(min(1.0, max(0.0, 0.5 - shift / 2)), 3)
    return c


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

    # Baseline: current settings, per match.
    base: dict[str, list] = {}
    for code, cfg, hold in data:
        base[code] = outcomes(code, cfg, hold).outcomes
    b_hits = sum(o.hit for v in base.values() for o in v)
    b_n = sum(len(v) for v in base.values())
    print(f"baseline {b_hits}/{b_n} = {b_hits / b_n:.2%}\n")

    print("  tempo  shift   hit     rescued  broken     net   markets")
    print("  " + "-" * 56)
    best = None
    for t in TEMPOS:
        for s in SHIFTS:
            hits = tot = resc = brok = 0
            mk = Counter()
            for code, cfg, hold in data:
                r = outcomes(code, tune(cfg, t, s), hold)
                for old, new in zip(base[code], r.outcomes):
                    tot += 1
                    hits += new.hit
                    mk[new.market] += 1
                    if not old.hit and new.hit:
                        resc += 1
                    elif old.hit and not new.hit:
                        brok += 1
            net = resc - brok
            print(f"  {t:5.2f}  {s:+5.2f}  {hits / tot:6.2%}  {resc:7d}  {brok:6d}  "
                  f"{net:+6d}   {len(mk):2d}", flush=True)
            if best is None or net > best[0]:
                best = (net, t, s, mk, hits / tot)

    net, t, s, mk, hr = best
    print(f"\nBEST NET: tempo_factor {t}, bias_shift {s:+.2f}")
    print(f"  {hr:.2%} pooled, net {net:+d} flips vs baseline")
    print("  markets emitted:")
    for m, c in mk.most_common():
        print(f"    {m:8s} {c:5d}  {c / sum(mk.values()):5.1%}")

    # Per league at the winner, since the target is per-league not pooled.
    print("\n  per league at that setting (target: 85%)")
    over = 0
    for code, cfg, hold in data:
        r = outcomes(code, tune(cfg, t, s), hold)
        old = base[code]
        o_hit = sum(x.hit for x in old)
        resc = sum(1 for a, b in zip(old, r.outcomes) if not a.hit and b.hit)
        brok = sum(1 for a, b in zip(old, r.outcomes) if a.hit and not b.hit)
        hrl = r.hit_rate
        over += hrl >= 0.85
        flag = "  <-- 85%+" if hrl >= 0.85 else ""
        print(f"    {code:8s} {r.sample:4d}  {o_hit / len(old):6.1%} -> {hrl:6.1%}  "
              f"(+{resc} -{brok}){flag}")
    print(f"\n  leagues at or above 85%: {over}/{len(data)}")


if __name__ == "__main__":
    main()
