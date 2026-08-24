"""
Does Tip 2 rescue Tip 1, shadow it, or contradict it? The answer is mostly
logic, and the measurement only fills in how often each case comes up.

Every (Tip 1, Tip 2) pair falls into one of three families, decided entirely by
which totals each market wins on:

    SHARPEN   Tip 2's winning totals are a SUBSET of Tip 1's
              e.g. U4.25 -> U3.75, O1.5 -> O2.75, U3.0 -> U2.75
              Tip 2 winning implies Tip 1 won. It can NEVER rescue a loss.
              It is the same opinion at a longer price and a lower strike.

    NET       Tip 1's winning totals are a subset of TIP 2's
              e.g. U3.0 -> U4.25
              Tip 2 wins whenever Tip 1 does, and on some totals where Tip 1
              fails. Strictly higher strike, strictly shorter price.

    FLIP      neither contains the other — opposite sides of the book
              e.g. U4.25 -> O1.75
              The only family where the two can disagree in both directions.

So "does Tip 2 also win?" is not one question. In SHARPEN it is a strictly
harder bet; in NET a strictly easier one; only in FLIP is it a different read.

Reported per family and per exact pair: how often each tip lands, and the four
joint outcomes.
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.two_tips import MAX_TIP2_GAP, MIN_EDGE, MIN_TIP2_ABS, PREFER

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 200
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL",
           "SUI-SL", "GRE-SL", "POL-EK", "SWE-AS"]
MAXT = 12


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def wins_on(m):
    return frozenset(t for t in range(MAXT) if won(m, t))


def family(a, b):
    A, B = wins_on(a), wins_on(b)
    if B < A:
        return "SHARPEN"
    if A < B:
        return "NET"
    return "FLIP"


def pick_two(mu, lmu, cfg, t1):
    """Tip 2 exactly as scripts/two_tips.py chooses it."""
    sc = [(m, e, p) for m, e, p, _q in market_select.score_markets(mu, lmu)
          if market_select.playable(m, cfg.max_under_line, cfg.min_over_line)]
    by = {m: (e, p) for m, e, p in sc}
    if t1 not in by:
        return None
    _e1, p1 = by[t1]
    cands = [(m, e, p) for m, e, p in sc
             if abs(p - p1) > 1e-9 and e >= MIN_EDGE
             and p >= max(MIN_TIP2_ABS, p1 - MAX_TIP2_GAP)]
    if not cands:
        return None
    top = max(p for _m, _e, p in cands)
    tier = [c for c in cands if abs(c[2] - top) < 1e-9]
    tier.sort(key=lambda c: PREFER.index(c[0]) if c[0] in PREFER else 99)
    return tier[0][0]


def main():
    rows = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            tot = int(hg) + int(ag)
            t1 = predict_fixture(req, cfg, module_flags=flags).translated_play.market
            t2 = pick_two(req.mu_total, req.league_mu, cfg, t1)
            rows.append((t1, t2, tot))
        print(f"  {code}: {len(pairs)}", flush=True)

    withtwo = [r for r in rows if r[1]]
    print(f"\n  {len(rows)} fixtures, {len(withtwo)} carried a Tip 2 "
          f"({len(withtwo)/len(rows):.0%})\n")

    def block(label, sel):
        if not sel:
            return
        n = len(sel)
        h1 = sum(won(a, t) for a, b, t in sel)
        h2 = sum(won(b, t) for a, b, t in sel)
        both = sum(won(a, t) and won(b, t) for a, b, t in sel)
        only1 = sum(won(a, t) and not won(b, t) for a, b, t in sel)
        only2 = sum(won(b, t) and not won(a, t) for a, b, t in sel)
        neither = n - both - only1 - only2
        print(f"  {label:<26s} n={n:5d}  T1 {h1/n:6.1%}  T2 {h2/n:6.1%}   "
              f"both {both/n:5.1%}  T1only {only1/n:5.1%}  "
              f"T2only {only2/n:5.1%}  neither {neither/n:5.1%}")

    print("  BY FAMILY")
    for fam in ("SHARPEN", "NET", "FLIP"):
        block(fam, [r for r in withtwo if family(r[0], r[1]) == fam])

    print("\n  BY EXACT PAIR (>= 40 fixtures)")
    by_pair = defaultdict(list)
    for r in withtwo:
        by_pair[(r[0], r[1])].append(r)
    for (a, b), sel in sorted(by_pair.items(), key=lambda kv: -len(kv[1])):
        if len(sel) >= 40:
            block(f"{a} -> {b}  [{family(a, b)[:4]}]", sel)

    print("\n  HOW OFTEN CAN TIP 2 RESCUE A TIP 1 LOSS?")
    for fam in ("SHARPEN", "NET", "FLIP"):
        sel = [r for r in withtwo if family(r[0], r[1]) == fam]
        if not sel:
            continue
        lost1 = [r for r in sel if not won(r[0], r[2])]
        saved = sum(won(b, t) for a, b, t in lost1)
        print(f"    {fam:<9s} Tip 1 lost {len(lost1):4d} times, "
              f"Tip 2 won {saved:4d} of those"
              + (f" ({saved / len(lost1):.0%})" if lost1 else ""))


if __name__ == "__main__":
    main()
