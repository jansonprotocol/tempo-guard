"""
Possession A/B on one league's most recent fixtures.

Both arms replay the SAME fixtures through the SAME engine; the only
difference is whether the fitted per-league possession coefficient is allowed
to move the goal expectation before a market is chosen.

The number that decides it is the net flip ledger, not the headline strike:

    rescued   a match the current engine loses and possession wins
    broken    a match the current engine wins and possession loses

A toggle earns its place when rescued > broken. Everything else — the strike
rate, the market mix, the size of the shifts — is there to explain the ledger,
not to override it.

Coverage is reported because it bounds the whole result. Possession returns
None when a league has too little fitted history or a side has no profile, and
on those fixtures the two arms are identical by construction. A toggle that
only touches 40 of 150 matches cannot move the strike rate much in either
direction, and reading a small net as a signal would be reading noise.

Usage:  python scripts/possession_ab.py JPN-J1 150
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, possession
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUE = sys.argv[1] if len(sys.argv) > 1 else "JPN-J1"
N = int(sys.argv[2]) if len(sys.argv) > 2 else 150


def won(market: str, total: int) -> bool:
    return hit_weight(evaluate_market(market, total, 0)) >= 1.0


def base_of(markets, totals) -> float:
    """How often the chosen markets win across this sample — the bar to clear."""
    if not markets or not totals:
        return 0.0
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def replay(pairs, cfg, flags):
    out = []
    for req, (hg, ag) in pairs:
        pred = predict_fixture(req, cfg, module_flags=flags)
        m = pred.translated_play.market
        t = int(hg) + int(ag)
        out.append({
            "home": req.home_team, "away": req.away_team,
            "date": req.match_date, "market": m, "total": t,
            "won": won(m, t),
        })
    return out


def report(label, rows):
    markets = [r["market"] for r in rows]
    totals = [r["total"] for r in rows]
    hits = sum(1 for r in rows if r["won"])
    strike = hits / len(rows)
    base = base_of(markets, totals)
    mix = "  ".join(f"{m}:{c * 100 // len(rows)}%"
                    for m, c in Counter(markets).most_common(4))
    print(f"  {label:14s} {hits:3d}/{len(rows)} = {strike:6.1%}   "
          f"base {base:6.1%}   edge {strike - base:+6.2%}   {mix}")
    return strike, base


def main() -> None:
    pairs = _requests_for(LEAGUE, None, None, CALIB_MIN_MATCHES, limit=N)
    pairs.sort(key=lambda p: p[0].match_date)
    if not pairs:
        print(f"{LEAGUE}: no replayable fixtures")
        return

    cfg_off = config.get(LEAGUE)
    flags = ModuleFlags(**(cfg_off.module_overrides or {}))
    cfg_on = deepcopy(cfg_off)
    cfg_on.use_possession = True

    print(f"{LEAGUE}: {len(pairs)} matches, "
          f"{pairs[0][0].match_date} to {pairs[-1][0].match_date}")
    print(f"  config: floor={cfg_off.min_win_prob or 0.79} "
          f"max_under={cfg_off.max_under_line} min_over={cfg_off.min_over_line} "
          f"possession={cfg_off.use_possession}\n")

    # How far the coefficient actually moves each fixture, and how often it
    # declines to say anything at all.
    shifts = []
    for req, _ in pairs:
        s = possession.shift(LEAGUE, req.home_team, req.away_team, req.match_date)
        shifts.append(s)
    live = [s for s in shifts if s is not None]
    if live:
        print(f"  possession covers {len(live)}/{len(pairs)} fixtures  "
              f"(shift min {min(live):+.3f}  mean {sum(live) / len(live):+.3f}  "
              f"max {max(live):+.3f})\n")
    else:
        print("  possession returns nothing on every fixture here — "
              "no fitted coefficient or no team profiles.\n")

    print(f"  {'arm':14s} {'strike':>16}   {'base':>10}   {'edge':>11}   mix")
    print("  " + "-" * 88)
    off = replay(pairs, cfg_off, flags)
    on = replay(pairs, cfg_on, flags)
    report("possession OFF", off)
    report("possession ON", on)

    changed = [(a, b, s) for a, b, s in zip(off, on, shifts)
               if a["market"] != b["market"]]
    rescued = [(a, b, s) for a, b, s in changed if b["won"] and not a["won"]]
    broken = [(a, b, s) for a, b, s in changed if a["won"] and not b["won"]]

    print(f"\n  market changed on {len(changed)}/{len(pairs)} fixtures")
    print(f"  rescued {len(rescued)}   broken {len(broken)}   "
          f"NET {len(rescued) - len(broken):+d}")

    for title, group in (("RESCUED", rescued), ("BROKEN", broken)):
        if not group:
            continue
        print(f"\n  {title}")
        for a, b, s in group[:12]:
            sh = f"{s:+.2f}" if s is not None else "  n/a"
            print(f"    {a['date']}  {a['home'][:20]:20s} v {a['away'][:20]:20s} "
                  f"{a['market']:>5s} -> {b['market']:<5s}  total {a['total']}"
                  f"  shift {sh}")
    print()


if __name__ == "__main__":
    main()
