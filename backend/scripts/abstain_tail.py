"""
Is the high-confidence tail real, or 83 lucky fixtures?

The abstain-vs-floor comparison found the two moves roughly equivalent in the
middle of the range and sharply different at the top: refusing everything below
0.91 scored 92.8% on 83 plays at +14.3% edge, where raising the floor to the
same 0.91 scored 86.6% on all of them. If that holds it is the whole case for
building a no-bet, because it is the one region the existing dial cannot reach.

But 83 fixtures carries a standard error near 2.8 points, so the interval runs
from the high eighties to nearly 98 and the finding is suggestive rather than
settled. This widens the sample across every configured league and prints the
tail at finer steps, with the error bar next to each so a thin bucket cannot be
read as a result.

Split chronologically as well. A confidence band that works on the older half
and not the newer one is a band fitted to history.
"""
from __future__ import annotations

import sys
from collections import Counter
from math import sqrt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = 600
LEVELS = [0.79, 0.85, 0.88, 0.90, 0.91, 0.92, 0.93]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals) -> float:
    if not markets or not totals:
        return 0.0
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def line(label, picks, pool):
    if not picks:
        print(f"  {label:12s} never plays")
        return
    markets = [m for m, _ in picks]
    totals = [t for _, t in picks]
    hits = sum(1 for m, t in picks if won(m, t))
    n = len(picks)
    s = hits / n
    se = sqrt(max(s * (1 - s), 1e-9) / n)
    base = base_of(markets, totals)
    mix = " ".join(f"{m}:{c * 100 // n}%" for m, c in Counter(markets).most_common(3))
    print(f"  {label:12s} {n:5d} ({n / pool:5.1%})  strike {s:6.1%} +/-{se:.1%}  "
          f"edge {s - base:+7.2%}   {mix}")


def main() -> None:
    codes = sorted(config.load_all().keys())
    rows = []
    for code in codes:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            market = predict_fixture(req, cfg, module_flags=flags) \
                .translated_play.market
            pw = next((h for m, _e, h, _t
                       in market_select.score_markets(req.mu_total, req.league_mu)
                       if m == market), 0.0)
            rows.append({"code": code, "date": req.match_date, "market": market,
                         "total": int(hg) + int(ag), "p_win": pw})
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return
    rows.sort(key=lambda r: r["date"])
    n = len(rows)
    half = n // 2
    print(f"\n{n} fixtures across {len(set(r['code'] for r in rows))} leagues, "
          f"{rows[0]['date']} to {rows[-1]['date']}\n")

    for title, sub in (("ALL", rows),
                       ("OLDER HALF", rows[:half]),
                       ("NEWER HALF", rows[half:])):
        print(f"  {title}")
        for lv in LEVELS:
            line(f"abstain {lv:.2f}",
                 [(r["market"], r["total"]) for r in sub if r["p_win"] >= lv],
                 len(sub))
        print()

    # Where the tail actually comes from — a band concentrated in two leagues
    # is a league finding wearing a confidence badge.
    tail = [r for r in rows if r["p_win"] >= 0.91]
    if tail:
        print("  tail (>=0.91) by league:")
        for code, c in Counter(r["code"] for r in tail).most_common(12):
            t = [r for r in tail if r["code"] == code]
            h = sum(1 for r in t if won(r["market"], r["total"]))
            print(f"    {code:10s} {c:4d}  {h}/{c} = {h / c:5.1%}")


if __name__ == "__main__":
    main()
