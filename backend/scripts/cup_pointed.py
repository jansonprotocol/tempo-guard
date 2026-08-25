"""
Can the DNB-style pointed read reopen part of the cup board?

The domestic backtest (`dnb_confluence.py`) proved the pointed team lane is
real: when a strong rung names a side — its own 2+ goals, or the opponent's
silence — that side avoids defeat 84% of the time at home, and the read
holds both windows. The question here is whether the same shape can serve
as a CONFIDENCE GATE in cups: not DNB lanes, but "when the fixture is
pointed, is the composite's total line trustworthy enough to tip?"

The side split comes from the club composite itself: the strength gap IS
goals of supremacy by construction (the ratings were fitted on it), so

    supremacy s  = 0.40 + (str_h − str_a)        cup home adv, measured
    mu_home      = (mu + s) / 2,  mu_away = mu − mu_home   (floored 0.1)

and a fixture is POINTED when a side clears the same floors the domestic
strong rungs demand: P(X scores 2+) ≥ 0.55 (direct) or P(Y ≤ 1) ≥ 0.75
(elimination). One honesty note stated up front: in cups both the total
and the split come from the SAME strength numbers, so pointedness is
agreement between two cuts of one estimate, not two independent strands —
which is exactly why it is measured rather than assumed.

Two measurements, Swiss era only (the only era the composite survives):

    GATE   the symmetry check re-run on pointed vs unpointed tips. If the
           pointed subset calibrates in BOTH directions where the full set
           did not (−0.6 / −4.0), the gate earns a probationary cup board.
    SIDE   descriptive: does the pointed side avoid defeat in cups at
           domestic-like rates at all?

VERDICT (25 Aug 2026): the gate helps in BOTH directions — pointed beats
unpointed forward (+0.7 vs −2.3) and reverse (−3.4 vs −4.6) — a
consistent-sign improvement, which is more than most cup ideas managed.
But the reverse window still fails at −3.4: the 24-25 miss is a LEVEL
error, and no selection rule can fix a mis-levelled mu. Cups stay off;
re-run alongside cup_composite.py as Swiss-era data thickens. The SIDE
read, meanwhile, transfers: 77.4% avoid-defeat on 168 pointed cup
fixtures (home 79.8 / break-even 1.32, away 69.2 / 1.57) — the domestic
Rule 5 structure, a few points weaker.

Usage:  python scripts/cup_pointed.py
"""
from __future__ import annotations

import math
import statistics as st
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config
from app.engine import market_select
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.cup_composite import SPLIT, build_rows, fit
from scripts.team_shrink_sweep import wilson

CUP_HOME_ADV = 0.40          # measured over 4,825 bridges (cup_strength.py)
P2PLUS_FLOOR = 0.55          # domestic O1.5 floor
SILENCE_FLOOR = 0.75         # domestic U1.5 floor


def sides(mu: float, sh: float, sa: float) -> tuple[float, float]:
    s = CUP_HOME_ADV + (sh - sa)
    gh = max(0.1, min(mu - 0.1, (mu + s) / 2))
    return gh, mu - gh


def pointed(gh: float, ga: float) -> tuple[str, str] | None:
    """(X, how) when a side clears a domestic strong-rung floor, else None.

    Direct outranks elimination when both fire, mirroring the domestic rule
    where the attack's own claim is the primary read.
    """
    for x, g in (("H", gh), ("A", ga)):
        if 1 - math.exp(-g) * (1 + g) >= P2PLUS_FLOOR:
            return x, "direct"
    for y, g, x in (("H", gh, "A"), ("A", ga, "H")):
        if math.exp(-g) * (1 + g) >= SILENCE_FLOOR:
            return x, "elim"
    return None


def grade(rows, b0, b1, b2):
    got = []
    for _d, c, sh, sa, b, hg, ag in rows:
        mu = b + b0 + b1 * abs(sh - sa) + b2 * (sh + sa)
        if not (0.5 < mu < 6):
            continue
        cfg = config.get(c)
        best = None
        for m, _e, p, _q in market_select.score_markets(mu, b):
            if not market_select.playable(m, cfg.max_under_line,
                                          cfg.min_over_line):
                continue
            if p < market_select.MIN_WIN_PROB or math.isnan(p):
                continue
            if best is None or p > best[1]:
                best = (m, p)
        if best is None:
            continue
        res = evaluate_market(best[0], hg, ag)
        if res is None:
            continue
        gh, ga = sides(mu, sh, sa)
        got.append(dict(m=best[0], p=best[1], hit=hit_weight(res) >= 1.0,
                        pt=pointed(gh, ga),
                        wdl="W" if hg > ag else "D" if hg == ag else "L",
                        hg=hg, ag=ag))
    return got


def show(label, g):
    if len(g) < 30:
        print(f"  {label:30} too few: {len(g)}")
        return
    k = sum(1 for r in g if r["hit"])
    hit, says = k / len(g), st.mean(r["p"] for r in g)
    w = wilson(k, len(g))
    mix = defaultdict(int)
    for r in g:
        mix[r["m"]] += 1
    top = " ".join(f"{m}:{n}" for m, n in
                   sorted(mix.items(), key=lambda x: -x[1])[:3])
    print(f"  {label:30} {len(g):4}  says {says*100:5.1f}  hit {hit*100:5.1f}"
          f"  gap {(hit-says)*100:+5.1f}  [{w[0]*100:.0f}-{w[1]*100:.0f}]"
          f"  {top}")


def side_show(label, rs):
    if len(rs) < 20:
        print(f"  {label:30} too few: {len(rs)}")
        return
    n = len(rs)
    w = sum(1 for r in rs if (r["hg"] > r["ag"]) == (r["pt"][0] == "H")
            and r["hg"] != r["ag"])
    d = sum(1 for r in rs if r["hg"] == r["ag"])
    lo = n - w - d
    be = (w + lo) / w if w else float("inf")
    print(f"  {label:30} {n:4}   W {w/n*100:4.1f}  D {d/n*100:4.1f}  "
          f"L {lo/n*100:4.1f}   avoid-defeat {(w+d)/n*100:5.1f}   "
          f"break-even {be:.2f}")


def main() -> None:
    rows = build_rows()
    s1 = [r for r in rows if r[0] < SPLIT]
    s2 = [r for r in rows if r[0] >= SPLIT]

    print("GATE: symmetry check, pointed vs unpointed tips")
    for lab, tr, va in (("fit 24-25 -> 25-26", s1, s2),
                        ("fit 25-26 -> 24-25", s2, s1)):
        b0, b1, b2 = fit(tr)
        g = grade(va, b0, b1, b2)
        print(f"{lab}:")
        show("all", g)
        show("pointed", [r for r in g if r["pt"]])
        show("  direct", [r for r in g if r["pt"] and r["pt"][1] == "direct"])
        show("  elim", [r for r in g if r["pt"] and r["pt"][1] == "elim"])
        show("unpointed", [r for r in g if not r["pt"]])

    print("\nSIDE: pointed side's own result in cups (pooled betas, "
          "descriptive)")
    b0, b1, b2 = fit(s1 + s2)
    g = grade(s1 + s2, b0, b1, b2)
    pt = [r for r in g if r["pt"]]
    side_show("pointed side, all", pt)
    side_show("pointed at home side", [r for r in pt if r["pt"][0] == "H"])
    side_show("pointed at away side", [r for r in pt if r["pt"][0] == "A"])


if __name__ == "__main__":
    main()
