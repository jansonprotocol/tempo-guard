"""
What produces the 41% U4.25 concentration, and what would break it?

Not a feature and not a bias — the probability floor. `market_select.choose`
takes the highest-edge market whose win probability clears MIN_WIN_PROB, and
falls back to the safest buyable rung when nothing clears it. U4.25 IS that
rung, so every fixture where the ladder has nothing convincing lands there.

That makes the concentration a dial, not a defect, and the dial is one number.
This sweeps it and reports what each setting buys and costs: the market mix, the
hit rate, and the realised edge against the base rate of the markets actually
chosen. Hit rate and edge move in OPPOSITE directions across the sweep, which is
the whole point — a lower floor surfaces more opinionated markets that win less
often but beat their own baseline by more.
"""
from __future__ import annotations

import sys
from collections import Counter, defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300
FLOORS = [0.95, 0.93, 0.91, 0.89, 0.87, 0.85, 0.83, 0.79, 0.75, 0.70]
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL"]


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals):
    if not markets or not totals:
        return 0.0
    n = len(totals)
    return sum(c * sum(1 for t in totals if won(m, t)) / n
               for m, c in Counter(markets).items()) / len(markets)


def main():
    fixtures = []
    totals_by_league = defaultdict(list)
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        cfg = config.get(code)
        for req, (hg, ag) in pairs:
            tot = int(hg) + int(ag)
            totals_by_league[code].append(tot)
            fixtures.append((code, cfg, req.mu_total, req.league_mu, tot))
        print(f"  {code}: {len(pairs)}", flush=True)
    print(f"\n  {len(fixtures)} fixtures\n")

    orig = market_select.MIN_WIN_PROB
    print(f"  {'floor':>6s} {'hit':>7s} {'base':>7s} {'edge':>7s}   market mix")
    print("  " + "-" * 78)
    for f in FLOORS:
        picks, hits = [], 0
        by_league = defaultdict(list)
        for code, cfg, mu, lmu, tot in fixtures:
            # keyword args: the third POSITIONAL parameter is `ladder`, and
            # passing a line cap there silently replaces the whole ladder.
            sel = market_select.choose(mu, lmu, min_win_prob=f,
                                       max_under=cfg.max_under_line,
                                       min_over=cfg.min_over_line)
            if sel is None:
                continue
            m = sel[0]
            picks.append(m)
            by_league[code].append(m)
            hits += won(m, tot)
        n = len(picks)
        base = sum(base_of(by_league[c], totals_by_league[c]) * len(by_league[c])
                   for c in by_league) / n
        mix = "  ".join(f"{m} {c/n:.0%}"
                        for m, c in Counter(picks).most_common(4))
        star = " <- shipping" if abs(f - orig) < 1e-9 else ""
        print(f"  {f:6.2f} {hits/n:7.2%} {base:7.2%} {hits/n - base:+7.2%}   {mix}{star}")


if __name__ == "__main__":
    main()
