"""
Re-tune the probability floor now that mu has been shrunk.

`MIN_WIN_PROB = 0.79` carries this justification in market_select:

    "0.79 is the highest floor that clears 80% strike while keeping the
     most-picked line under half of all calls."

That was measured against an over-spread mu. Shrinkage moved every fixture
toward its league mean, so far fewer rungs now clear an ABSOLUTE 79% — and the
selector falls through to the safest buyable one. The result is a floor that
used to keep the top line under 50% of calls and now pushes `U4.25` to 88-95%
of tips in five leagues. The floor did not change; what it is applied to did.

The two constants have to be tuned together. This sweeps the floor at the
shipped shrink and reports the same criteria the original tuning used, plus the
one that cannot be gamed:

    hit     strike rate, push counted
    base    how often the CHOSEN markets land across all fixtures of their own
            league — what a bettor picking those same markets blind would get
    EDGE    hit - base. The only figure that does not improve by retreating.
    top     share of calls going to the single most-picked market

A floor is only better if EDGE holds up AND `top` comes back under half.

Usage:  python scripts/floor_after_shrink.py [--n 150] [--leagues ...]
"""
from __future__ import annotations

import sys
import dataclasses
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market

FLOORS = (0.79, 0.75, 0.70, 0.65, 0.60)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else
             ["ENG-CH", "ESP-L2", "TUR-SL", "CHI-PD", "SAU-PL", "COL-PA",
              "MLS", "JPN-J1", "BRA-SB", "CHN-SL"])

    cache = []
    for lg in codes:
        df = store.load_results(lg)
        if df is None or len(df) < 200:
            continue
        cfg = config.get(lg)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for _, r in df.sort_values("date").tail(n).iterrows():
            d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
            try:
                req = build_request(lg, str(r["home"]), str(r["away"]), d)
            except Exception:
                continue
            if req is None:
                continue
            cache.append((lg, req, cfg, flags, int(r["hg"]) + int(r["ag"])))

    totals: dict[str, list[int]] = {}
    for lg, _r, _c, _f, t in cache:
        totals.setdefault(lg, []).append(t)
    bc: dict[tuple[str, str], float] = {}

    def base_rate(lg: str, mk: str) -> float:
        if (lg, mk) not in bc:
            ts = totals[lg]
            bc[(lg, mk)] = sum(
                1 for t in ts
                if evaluate_market(mk, t, 0) in (True, "half_win")) / len(ts)
        return bc[(lg, mk)]

    print(f"{len(cache)} fixtures replayed\n")
    print(f"{'floor':>7}{'n':>6}{'hit':>8}{'base':>8}{'EDGE':>8}"
          f"{'top':>7}   market mix")
    for fl in FLOORS:
        hits = tips = 0
        base_sum = 0.0
        mix: Counter = Counter()
        for lg, req, cfg, flags, total in cache:
            c2 = dataclasses.replace(cfg, min_win_prob=fl)
            try:
                mk = predict_fixture(req, c2,
                                     module_flags=flags).translated_play.market
            except Exception:
                continue
            if not mk:
                continue
            res = evaluate_market(mk, total, 0)
            if res is None:
                continue
            tips += 1
            base_sum += base_rate(lg, mk)
            hits += res is True or res == "half_win"
            mix[mk] += 1
        if not tips:
            continue
        hit, base = hits / tips, base_sum / tips
        top = mix.most_common(1)[0][1] / tips
        shown = "  ".join(f"{m} {c*100//tips}%" for m, c in mix.most_common(3))
        print(f"{fl:7.2f}{tips:6}{hit*100:7.1f}%{base*100:7.1f}%"
              f"{(hit-base)*100:+8.2f}{top*100:6.0f}%   {shown}", flush=True)


if __name__ == "__main__":
    main()
