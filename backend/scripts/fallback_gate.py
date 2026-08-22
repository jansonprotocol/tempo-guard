"""
Is the U4.25 fallback separable from the rest — and does refusing it help?

The live log shows 20/22 on everything that is not U4.25 and 6/8 on U4.25 at
negative edge. Eight fixtures is nothing, so this asks the same question of the
stored record.

Two things are deliberately kept apart, because conflating them is how the
earlier "raise the floor" result went wrong:

    the MARKET chosen    U4.25 is the safest buyable rung, so the selector
                         lands there when nothing else clears the floor
    the EDGE carried     how far the chosen market beats its own base rate

A negative edge means the engine found nothing worth backing and took the
safest thing on the board. That is a different object from a positive-edge
U4.25, which is a genuine read that happens to land on the same rung.

Reported per bucket: hit rate, base rate of the markets in that bucket, and the
difference. A bucket whose hit rate is high but whose edge is ~0 is not skill,
it is the rung being easy — which is the whole reason edge exists here.
"""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 150
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL",
           "SUI-SL", "GRE-SL", "SWE-AS", "POL-EK"]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals) -> float:
    """Weighted over DISTINCT markets — the naive form is O(n^2)."""
    if not markets or not totals:
        return 0.0
    n = len(totals)
    return sum(c * sum(1 for t in totals if won(m, t)) / n
               for m, c in Counter(markets).items()) / len(markets)


def main() -> None:
    rows = []
    totals_by_league = defaultdict(list)
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 50:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            tot = int(hg) + int(ag)
            totals_by_league[code].append(tot)
            m = predict_fixture(req, cfg, module_flags=flags).translated_play.market
            edge = next((e for mk, e, _p, _q in
                         market_select.score_markets(req.mu_total, req.league_mu)
                         if mk == m), 0.0)
            rows.append((code, m, edge, tot, won(m, tot)))
        print(f"  {code}: {len(pairs)}", flush=True)

    print(f"\n  {len(rows)} fixtures\n")

    def report(label, sel):
        if not sel:
            print(f"  {label:<34s}      -")
            return
        hits = sum(r[4] for r in sel)
        n = len(sel)
        base = sum(base_of([r[1] for r in sel if r[0] == c],
                           totals_by_league[c]) * len([r for r in sel if r[0] == c])
                   for c in totals_by_league) / n
        print(f"  {label:<34s} {hits:5d}/{n:<5d} {hits/n:6.2%}"
              f"   base {base:6.2%}   edge {hits/n - base:+6.2%}")

    print("  BY MARKET AND EDGE SIGN")
    report("everything", rows)
    report("  not U4.25", [r for r in rows if r[1] != "U4.25"])
    report("  U4.25, edge >= 0", [r for r in rows if r[1] == "U4.25" and r[2] >= 0])
    report("  U4.25, edge < 0", [r for r in rows if r[1] == "U4.25" and r[2] < 0])
    print()
    report("  any market, edge < 0", [r for r in rows if r[2] < 0])
    report("  any market, edge >= 0", [r for r in rows if r[2] >= 0])

    print("\n  WHAT REFUSING EACH BUCKET WOULD DO")
    all_h, all_n = sum(r[4] for r in rows), len(rows)
    print(f"  {'bet everything':<34s} {all_h}/{all_n} = {all_h/all_n:.2%}")
    for label, keep in (
        ("refuse U4.25 at negative edge", lambda r: not (r[1] == "U4.25" and r[2] < 0)),
        ("refuse ALL negative edge",      lambda r: r[2] >= 0),
        ("refuse ALL U4.25",              lambda r: r[1] != "U4.25"),
    ):
        kept = [r for r in rows if keep(r)]
        h, n = sum(x[4] for x in kept), len(kept)
        print(f"  {label:<34s} {h}/{n} = {h/n:.2%}   "
              f"({all_n - n} refused, {all_n and (all_n-n)/all_n:.0%} of the book)")

    print("\n  MARKET MIX")
    for m, c in Counter(r[1] for r in rows).most_common():
        print(f"    {m:<8s} {c:5d}  {c/len(rows):6.1%}")


if __name__ == "__main__":
    main()
