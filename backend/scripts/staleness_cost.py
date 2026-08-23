"""
What does a stale store actually cost, and should the freshness gate bite?

`league_status.py` marks seven leagues as not cleared for futurematch — ENG-PL,
FRA-L1, GER-BL, ITA-SA, ITA-SB, GRE-SL, COPA-L, all 86-107 days behind — and
the tip path ignores it. Four of them were tipped last weekend, on form ending
in May 2026. They went 17/19, which is not evidence of harm, but nineteen
fixtures is not evidence of anything.

Retrosimming those leagues answers the wrong question: replaying their own
history prices each fixture with data that was FRESH at the time, so it measures
the league, not the staleness.

The right experiment prices the same fixture twice:

    FRESH   as of the match date, which is what the engine normally does
    STALE   as of the match date minus N days, so the form window is forced to
            end N days early — exactly the situation a lagging store creates

Both are scored against the same real result. The difference is the cost of the
lag, isolated from every property of the league. Nothing about the fixture,
the market ladder or the calibration changes between the two arms.

Usage:  python scripts/staleness_cost.py [--n 200] [--lags 30,60,90]
"""
from __future__ import annotations

import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market

DEFAULT_LAGS = (0, 30, 60, 90, 120)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 200
    lags = ([int(x) for x in args[args.index("--lags") + 1].split(",")]
            if "--lags" in args else DEFAULT_LAGS)
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else
             ["ENG-CH", "ESP-L2", "TUR-SL", "BEL-PL", "POR-PL", "GER-B2",
              "NED-ED", "FRA-L2"])

    fixtures = []
    for lg in codes:
        df = store.load_results(lg)
        if df is None or len(df) < 200:
            continue
        cfg = config.get(lg)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for _, r in df.sort_values("date").tail(n).iterrows():
            d = r["date"]
            fixtures.append((lg, cfg, flags, str(r["home"]), str(r["away"]),
                             d, int(r["hg"]) + int(r["ag"])))
    print(f"{len(fixtures)} fixtures, each priced at every lag\n")
    print(f"{'lag':>6}{'n':>7}{'skip%':>8}{'says':>8}{'hit':>8}{'gap':>8}"
          f"{'vs fresh':>10}")

    baseline = None
    for lag in lags:
        hits = tips = skips = 0
        p_sum = 0.0
        for lg, cfg, flags, home, away, d, total in fixtures:
            asof = d - timedelta(days=lag)
            day = asof.date() if hasattr(asof, "date") else asof
            try:
                req = build_request(lg, home, away, day)
            except Exception:
                skips += 1
                continue
            if req is None:
                skips += 1
                continue
            try:
                mk = predict_fixture(
                    req, cfg, module_flags=flags).translated_play.market
            except Exception:
                skips += 1
                continue
            if not mk:
                skips += 1
                continue
            res = evaluate_market(mk, total, 0)
            if res is None:
                skips += 1
                continue
            tips += 1
            p_sum += market_select.p_win(mk, req.mu_total)
            hits += res is True or res == "half_win"
        if not tips:
            continue
        hit = hits / tips
        says = p_sum / tips
        if baseline is None:
            baseline = hit
        print(f"{lag:5}d{tips:7}{skips/(tips+skips)*100:7.0f}%{says*100:7.1f}%"
              f"{hit*100:7.1f}%{(hit-says)*100:+8.1f}"
              f"{(hit-baseline)*100:+9.1f}", flush=True)


if __name__ == "__main__":
    main()
