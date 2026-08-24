"""
Does finishing the shrink fix the tail, on the SAME fixtures?

`joint_sweep.py` said no: MU_SHRINK 0.35 -> 0.25 moved the tail band from -2.8
to -2.6. That comparison is confounded, and the confound runs the wrong way.
The band is defined by stated edge over +3.5%, and shrinking mu lowers every
probability — so fewer fixtures qualify, and the ones that survive are the most
extreme of the extremes. The band gets HARDER as k falls, which masks whatever
improvement the shrink delivers.

So the fixtures are frozen. The tail is defined ONCE, by the current engine, and
those same fixtures are then re-priced at each k and scored against the same
outcomes. Nothing enters or leaves the band.

    gap @ k    hit rate of the frozen tail minus what it claims at that k

If the gap closes as k falls, residual mu over-spread IS the mechanism and the
earlier null was an artefact of a moving denominator. If it does not, the
mechanism is dead for a second time and on a test built to be fair to it.

Usage:  python scripts/tail_paired.py [--n 120] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, features, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.edge_bands import MARKETS

KS = [0.20, 0.25, 0.30, 0.35]


def base_rates(code: str) -> dict[str, float]:
    df = store.load_results(code)
    if df is None or df.empty:
        return {}
    out = {}
    for m in MARKETS:
        w = [hit_weight(evaluate_market(m, int(h), int(a)))
             for h, a in zip(df["hg"], df["ag"])]
        w = [x for x in w if x >= 0]
        out[m] = sum(1 for x in w if x >= 1.0) / len(w) if w else 0.0
    return out


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 120
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    k0 = features.MU_SHRINK

    # Pass 1 at the current k: fix the tail membership and the outcomes.
    frozen = []
    for lg in codes:
        df = store.load_results(lg)
        if df is None or len(df) < 200:
            continue
        cfg = config.get(lg)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        rates = base_rates(lg)
        for _, r in df.sort_values("date").tail(n).iterrows():
            d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
            try:
                req = build_request(lg, str(r["home"]), str(r["away"]), d)
                if req is None:
                    continue
                mk = predict_fixture(req, cfg,
                                     module_flags=flags).translated_play.market
            except Exception:
                continue
            if not mk:
                continue
            res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
            if res is None:
                continue
            base = rates.get(mk)
            if base is None:
                continue
            p = market_select.p_win(mk, req.mu_total)
            if (p - base) * 100 >= 3.5:
                frozen.append((lg, str(r["home"]), str(r["away"]), d, mk,
                               res is True or res == "half_win"))
    print(f"tail frozen at k={k0}: {len(frozen)} fixtures\n")
    if not frozen:
        return

    print(f"{'k':>6}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}")
    for k in KS:
        features.MU_SHRINK = k
        features._INDEX_CACHE.clear()
        says = []
        hits = []
        for lg, h, a, d, mk, won in frozen:
            try:
                req = build_request(lg, h, a, d)
                if req is None:
                    continue
            except Exception:
                continue
            says.append(market_select.p_win(mk, req.mu_total))
            hits.append(won)
        if not says:
            continue
        s = sum(says) / len(says)
        hit = sum(hits) / len(hits)
        mark = "  <- current" if abs(k - k0) < 1e-9 else ""
        print(f"{k:6.2f}{len(says):7}{s*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-s)*100:+8.1f}{mark}")
    features.MU_SHRINK = k0
    print("\nSame fixtures, same outcomes, same market at every k — only the "
          "probability moves.")


if __name__ == "__main__":
    main()
