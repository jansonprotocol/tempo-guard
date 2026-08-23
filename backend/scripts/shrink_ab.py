"""
Does shrinking mu toward the league mean actually fix the overconfidence?

`calibrate_mu.py` found the cause: regressing what happened on what was
predicted gives a slope of **0.424** pooled over ~2,000 fixtures. The engine's
per-fixture spread is roughly two and a half times too wide. Its lowest-mu
quintile claims 1.99 goals and delivers 2.54; its highest claims 3.60 and
delivers 3.26. Bias is negligible (+0.08), so this is not a level error — every
tip is selected on exactly the extremes that are wrong.

The implied correction is one line:

    mu' = league_mu + k * (mu - league_mu)

with k the regression slope. This script replays fixtures at several values of
k and reports what each does to the two numbers that matter:

    gap    hit rate minus the probability the engine claimed. This is what
           should go to zero — it is the whole defect.
    hit    the raw strike rate, which must not collapse in the process.

A warning the numbers cannot give on their own: shrinking mu pulls every
fixture toward the league mean, so the engine differentiates less and the
market mix narrows toward the safest rung. A k that fixes the gap by making
every tip the same tip has not fixed anything — it has stopped predicting. The
market mix is printed for that reason.

Usage:  python scripts/shrink_ab.py [--n 150] [--leagues ...]
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market

KS = (0.60,)


def run(codes: list[str], n: int) -> None:
    # Build every request once; only mu changes between arms.
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
    # Empirical base rate: how often each market lands across ALL sampled
    # fixtures of its own league. Hit rate alone is purchasable by retreating to
    # a safer rung, so the number that matters is strike MINUS base — what the
    # read is worth over picking that same market blind.
    totals: dict[str, list[int]] = {}
    for lg, _r, _c, _f, t in cache:
        totals.setdefault(lg, []).append(t)
    base_cache: dict[tuple[str, str], float] = {}

    def base_rate(lg: str, mk: str) -> float:
        key = (lg, mk)
        if key not in base_cache:
            ts = totals[lg]
            w = sum(1 for t in ts
                    if evaluate_market(mk, t, 0) in (True, "half_win"))
            base_cache[key] = w / len(ts)
        return base_cache[key]

    print(f"{len(cache)} fixtures replayed\n")
    print(f"{'k':>6}{'n':>6}{'says':>8}{'hit':>8}{'gap':>8}"
          f"{'base':>8}{'EDGE':>8}   market mix")

    for k in KS:
        hits = tips = 0
        p_sum = 0.0
        mix: Counter = Counter()
        base_sum = 0.0
        for lg, req, cfg, flags, total in cache:
            mu = req.league_mu + k * (req.mu_total - req.league_mu)
            r2 = req.model_copy(update={"mu_total": mu})
            try:
                mk = predict_fixture(r2, cfg, module_flags=flags).translated_play.market
            except Exception:
                continue
            if not mk:
                continue
            res = evaluate_market(mk, total, 0)
            if res is None:
                continue
            tips += 1
            p_sum += market_select.p_win(mk, mu)
            base_sum += base_rate(lg, mk)
            hits += res is True or res == "half_win"
            mix[mk] += 1
        if not tips:
            continue
        says, hit, base = p_sum / tips, hits / tips, base_sum / tips
        top = "  ".join(f"{m} {c*100//tips}%" for m, c in mix.most_common(2))
        print(f"{k:6.2f}{tips:6}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+7.1f}{base*100:8.1f}%{(hit-base)*100:+8.2f}"
              f"   {top}", flush=True)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else
             ["ENG-CH", "ESP-L2", "TUR-SL", "CHN-SL", "CHI-PD", "SAU-PL",
              "COL-PA", "MLS", "JPN-J1", "BRA-SB"])
    run(codes, n)


if __name__ == "__main__":
    main()
