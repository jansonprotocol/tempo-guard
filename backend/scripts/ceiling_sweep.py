"""
A bet that always wins is not a bet — sweep the playability ceiling.

Everything measured so far scored a market on whether it lands. That ignores
whether it can be bought. In a league where 97% of matches finish under five
goals, U4.25 wins almost always and is priced accordingly; a 90% strike rate on
a 1.05 shot is a losing product with an excellent-looking hit rate.

Low-scoring leagues are where this bites. Brazilian Serie B, Argentina and
Italian Serie B all cluster tightly enough that the safest under rung becomes
unplayable, and the engine reaches for it constantly because the floor rewards
safety and nothing penalises shortness.

The fix mirrors the floor. MIN_WIN_PROB refuses bets too risky to want;
MAX_WIN_PROB refuses bets too certain to price. Between them the selector is
confined to the band where a market is both winnable and buyable, and it
self-adjusts per league: in a tight league U4.25 breaches the ceiling and the
choice falls to U3.5 or U3.25, while in a high-scoring one it never gets near.

The ladder also gains two rungs this needs. O1.0 wins at 1+ goals under the
full-win convention, so it is the *safer* Over — exactly the rung that is
playable in a league where 2+ is a genuine question. U3.0 wins at 3 or fewer via
the push, same tier as U3.5 at a different price.

Reported per ceiling: strike, edge, and the market mix, for a set of tight
leagues and a set of ordinary ones, since a ceiling that helps the first must
not wreck the second.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

TIGHT = ["BRA-SB", "ARG-PD", "ITA-SB", "ARG-CLP", "GRE-SL"]
NORMAL = ["ENG-PL", "GER-BL", "NED-ED", "ITA-SA", "TUR-SL", "BRA-SA"]
LIMIT = 400
CEILINGS = [1.00, 0.92, 0.90, 0.88, 0.86, 0.84]


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals):
    if not markets or not totals:
        return 0.0
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def build(codes):
    out = []
    for code in codes:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"  {code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        out.append((code, cfg, ModuleFlags(**(cfg.module_overrides or {})), pairs))
        print(f"  built {code} ({len(pairs)})", flush=True)
    return out


def score(group, ceiling):
    market_select.MAX_WIN_PROB = ceiling
    hits = tot = 0
    mk, totals = [], []
    for _code, cfg, flags, pairs in group:
        for req, (hg, ag) in pairs:
            pred = predict_fixture(req, cfg, module_flags=flags)
            m = pred.translated_play.market
            t = hg + ag
            hits += won(m, t)
            tot += 1
            mk.append(m)
            totals.append(t)
    strike = hits / tot
    return strike, strike - base_of(mk, totals), Counter(mk), tot


def main():
    print("building tight leagues")
    tight = build(TIGHT)
    print("building normal leagues")
    normal = build(NORMAL)

    for label, group in (("TIGHT (low-scoring)", tight), ("NORMAL", normal)):
        if not group:
            continue
        print(f"\n{label} — {sum(len(p) for _, _, _, p in group)} fixtures")
        print(f"  {'ceiling':>8} {'strike':>8} {'edge':>8}   mix")
        print("  " + "-" * 70)
        for c in CEILINGS:
            strike, edge, mk, tot = score(group, c)
            mix = " ".join(f"{m}:{n * 100 // tot}%" for m, n in mk.most_common(4))
            lab = "none" if c >= 1.0 else f"{c:.2f}"
            print(f"  {lab:>8} {strike:8.1%} {edge:+8.2%}   {mix}", flush=True)


if __name__ == "__main__":
    main()
