"""
Abstaining vs just raising the floor — they key on the same number, not the same act.

`p_win` is already the floor's currency, so "refuse below 0.85" sounds like the
dial the engine has. It is not. Raising MIN_WIN_PROB does not decline a fixture;
it makes the selector RETREAT to a safer rung on that same fixture. On J1 that
took the strike from 82.2% to 84.5% and collapsed the market mix to 93% U4.25,
with edge turning negative — the engine stopped predicting and started buying a
near-certainty at a price that cannot pay.

Abstention is the other move: play the same markets you always would, on fewer
matches. If the two are equivalent, no-bet is not worth building and the floor
is enough. If abstention reaches the same strike while KEEPING its market mix
and its edge, then it is doing something the floor cannot.

Matched on strike rate rather than on setting, since that is the comparison that
means anything: at a given hit rate, which arm still has a spread of markets and
a positive edge?

    floor X      raise MIN_WIN_PROB to X, bet every fixture
    abstain X    keep the floor at its configured value, skip fixtures whose
                 chosen market wins with probability below X

Also tested: history as a second gate, since the abstention probe found it
separates independently of p_win.
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = ["ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED", "TUR-SL",
           "JPN-J1", "BRA-SA", "POR-PL", "MLS", "FRA-L2", "ENG-CH"]
LIMIT = 300
LEVELS = [0.79, 0.82, 0.85, 0.88, 0.91]
MIN_HISTORY = 12


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals) -> float:
    """
    Average win rate of the chosen markets across the sample.

    Weighted over DISTINCT markets rather than iterated per pick. The naive
    form re-scans every total once per fixture, which is O(n^2): at 11,000
    fixtures that is 2.7 minutes for a single call and it silently turned a
    summary into an hour of arithmetic. The ladder has twelve rungs, so
    counting them collapses it to O(12n) for an identical number.
    """
    if not markets or not totals:
        return 0.0
    n = len(totals)
    return sum(c * sum(1 for t in totals if won(m, t)) / n
               for m, c in Counter(markets).items()) / len(markets)


def report(label, picks):
    """picks: (market, total) for fixtures actually played."""
    if not picks:
        print(f"  {label:14s} never plays")
        return
    markets = [m for m, _ in picks]
    totals = [t for _, t in picks]
    hits = sum(1 for m, t in picks if won(m, t))
    strike = hits / len(picks)
    base = base_of(markets, totals)
    mix = " ".join(f"{m}:{c * 100 // len(picks)}%"
                   for m, c in Counter(markets).most_common(3))
    print(f"  {label:14s} plays {len(picks):4d}  strike {strike:6.1%}  "
          f"base {base:6.1%}  edge {strike - base:+6.2%}   {mix}")


def main() -> None:
    rows = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        df = store.load_results(code)
        df = df[df["hg"].notna() & df["ag"].notna()].sort_values("date")

        for req, (hg, ag) in pairs:
            total = int(hg) + int(ag)
            market = predict_fixture(req, cfg, module_flags=flags) \
                .translated_play.market
            pw = next((h for m, _e, h, _t
                       in market_select.score_markets(req.mu_total, req.league_mu)
                       if m == market), 0.0)
            past = df[df["date"] < np.datetime64(req.match_date)]
            hist = min(
                int(((past["home"] == req.home_team) |
                     (past["away"] == req.home_team)).sum()),
                int(((past["home"] == req.away_team) |
                     (past["away"] == req.away_team)).sum()),
            )
            row = {"code": code, "total": total, "market": market,
                   "p_win": pw, "history": hist}
            # What the same fixture becomes at each raised floor.
            for lv in LEVELS:
                c = deepcopy(cfg)
                c.min_win_prob = lv
                row[f"floor{lv}"] = predict_fixture(req, c, module_flags=flags) \
                    .translated_play.market
            rows.append(row)
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return
    print(f"\n{len(rows)} fixtures\n")

    print("  RAISE THE FLOOR — same fixtures, safer rungs")
    for lv in LEVELS:
        report(f"floor {lv:.2f}", [(r[f"floor{lv}"], r["total"]) for r in rows])

    print("\n  ABSTAIN — same rungs, fewer fixtures")
    for lv in LEVELS:
        report(f"abstain {lv:.2f}",
               [(r["market"], r["total"]) for r in rows if r["p_win"] >= lv])

    print("\n  ABSTAIN + require history")
    for lv in LEVELS:
        report(f"abstain {lv:.2f}",
               [(r["market"], r["total"]) for r in rows
                if r["p_win"] >= lv and r["history"] >= MIN_HISTORY])


if __name__ == "__main__":
    main()
