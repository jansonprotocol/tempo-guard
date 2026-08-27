"""
Per-league retrosim: is any league dragging the hit rate down?

The live log answers this badly. Ninety-seven settled tips spread over
twenty-three leagues means most leagues have three or four, and at that size a
league showing 50% and one showing 100% are not distinguishable from each
other. Anything read off that table is noise with a name attached.

So each league is replayed instead: take its most recent N results, price each
one strictly as-of (nothing after the match date is read), and score the tip
the engine would actually have issued. Same code path as a live tip — the only
difference is that the answer is already known.

Reported per league:

    n         fixtures that produced a tip
    skip%     fixtures withheld — thin history, unresolved names, no playable rung
    says      mean probability the engine attached to its own tips
    hit       what actually landed, push counted as a hit (the log's convention)
    gap       hit - says. NEGATIVE means the league is overconfident, which is
              the failure that costs money; positive means it is leaving edge on
              the table but is not lying.

A league is only worth acting on when the gap is large AND n is big enough to
mean something. Wilson intervals are printed for exactly that reason.

Usage:  python scripts/retrosim.py [--n 150] [--leagues MLS,JPN-J1]
                                  [--min 150]   lower the 200-row floor
"""
from __future__ import annotations

import math
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market

DEFAULT_N = 150
# A league needs at least this many priced fixtures before its gap is reported
# as anything other than noise.
MIN_N = 40


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if not n:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - m) / d, (c + m) / d)


def replay(league: str, n: int, back: int = 0, min_rows: int = 200,
           days: int = 0) -> dict:
    # The 200-row floor keeps thin domestic leagues out of a sweep, where a
    # league with 60 results would be read as a calibration verdict. The cup
    # QUALIFIERS sit just under it by nature — UCL-Q carries 182 results in
    # total — and were silently returning nothing, which is how their badges
    # went a day stale without anyone noticing. So the floor is a parameter
    # now: the sweep keeps 200, the cup rebuild passes what it means.
    df = store.load_results(league)
    if df is None or len(df) < min_rows:
        return {}
    # `back` drops the most recent `back` matches before taking the window, so
    # a mid-season stretch can be scored instead of the season restart. The last
    # 120 matches of most leagues straddle the summer break, where the rolling
    # form window reaches across it and describes teams that no longer exist in
    # that shape. Measuring that as a league's calibration confuses "early
    # season is hard" with "this league is badly modelled".
    ordered = df.sort_values("date")
    if back:
        ordered = ordered.iloc[:-back]
    # Two seasons is this project's widest validation window; a table row
    # must not quietly reach past it just because a league plays often.
    if days:
        cut = ordered["date"].max() - timedelta(days=days)
        ordered = ordered[ordered["date"] >= cut]
    recent = ordered.tail(n)
    cfg = config.get(league)
    flags = ModuleFlags(**(cfg.module_overrides or {}))

    hits = tips = skips = 0
    p_sum = 0.0
    buys: list[float] = []
    for _, r in recent.iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            skips += 1
            continue
        if req is None:
            skips += 1
            continue
        try:
            mk = predict_fixture(req, cfg, module_flags=flags).translated_play.market
        except Exception:
            skips += 1
            continue
        if not mk:
            skips += 1
            continue
        res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
        if res is None:
            skips += 1
            continue
        tips += 1
        # The PUBLISHED probability, debits included — the table grades
        # the number a visitor actually sees, not the raw engine one.
        p_st = market_select.stated(league, mk,
                                    market_select.p_win(mk, req.mu_total))
        p_sum += p_st
        # And the buy-from a card would have printed for this tip — the
        # price below which it is not worth money, which is what decides
        # whether a lane can ever be BOUGHT, not just whether it lands.
        from scripts.two_tips import buy_value
        edge = p_st - market_select.p_win(mk, req.league_mu) \
            if req.league_mu else None
        bv = buy_value(mk, req.mu_total, p_st, edge, league)
        if bv is not None:
            buys.append(bv)
        hits += res is True or res == "half_win"

    if not tips:
        return {}
    lo, hi = wilson(hits, tips)
    return dict(league=league, n=tips, skip=skips / (tips + skips),
                says=p_sum / tips, hit=hits / tips, lo=lo, hi=hi,
                buy=sum(buys) / len(buys) if buys else None)


def main() -> None:
    args = sys.argv[1:]
    n = DEFAULT_N
    if "--n" in args:
        n = int(args[args.index("--n") + 1])
    back = int(args[args.index("--back") + 1]) if "--back" in args else 0
    min_rows = int(args[args.index("--min") + 1]) if "--min" in args else 200
    days = int(args[args.index("--days") + 1]) if "--days" in args else 0
    if "--leagues" in args:
        codes = args[args.index("--leagues") + 1].split(",")
    else:
        codes = sorted(store.available_leagues())

    rows = []
    for lg in codes:
        try:
            out = replay(lg, n, back, min_rows, days)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
            continue
        if out:
            rows.append(out)
            r = out
            line = (f"{r['league']:9}{r['n']:5}{r['skip']*100:6.0f}%"
                    f"{r['says']*100:8.1f}%{r['hit']*100:8.1f}%"
                    f"{(r['hit']-r['says'])*100:+7.1f}"
                    f"   [{r['lo']*100:.0f}-{r['hi']*100:.0f}]")
            if r.get("buy"):
                line += f"   buy {r['buy']:.2f}"
            print(line, flush=True)

    if not rows:
        return
    tot_n = sum(r["n"] for r in rows)
    w_says = sum(r["says"] * r["n"] for r in rows) / tot_n
    w_hit = sum(r["hit"] * r["n"] for r in rows) / tot_n
    print(f"\n{len(rows)} leagues, {tot_n} priced fixtures")
    print(f"weighted   says {w_says*100:.1f}%   hit {w_hit*100:.1f}%"
          f"   gap {(w_hit-w_says)*100:+.1f}")

    bad = [r for r in rows if r["n"] >= MIN_N and r["hit"] - r["says"] < -0.04]
    print(f"\noverconfident by more than 4 points on n>={MIN_N}: {len(bad)}")
    for r in sorted(bad, key=lambda r: r["hit"] - r["says"]):
        print(f"   {r['league']:9}{r['n']:5}  says {r['says']*100:5.1f}%"
              f"  hit {r['hit']*100:5.1f}%  gap {(r['hit']-r['says'])*100:+.1f}")


if __name__ == "__main__":
    main()
