"""
The cup spiderweb, final stage: club-level strength inside the Swiss era.

Every earlier cup variant trained across the 2024 format break and died on
it (the as-of refit exposed the sign flip: early −3.7 / late +3.3). This one
never touches the old world. The store holds two complete Swiss-format
seasons — 2024-25 and 2025-26 — so train on one, validate on the other,
entirely within the new regime, and then swap the roles: a model that only
works in one direction learned a season, not a structure.

The strength number is per CLUB, not per league — the BIG_MATCH strand
applied continentally:

    club_str = league_rating          as-of the break, trailing 8y bridges
             + 0.8 * (domestic PPG − 1.45)   trailing 365 days, ≥10 games
             + 0.3 * own_cup_form     rolling GD/game, last 10 own cup
                                      matches, needs ≥6 (else omitted)

    mu = rolling_3y_base + b0 + b1*|str_h − str_a| + b2*(str_h + str_a)

Tips selected offline exactly as market_select would (playable rung over
the MIN_WIN_PROB floor, per-cup config limits).

VERDICT (25 Aug 2026): the closest any cup model has come, and still short.

    fit 24-25 -> validate 25-26   182 tips  says 85.8  hit 85.2  gap −0.6
    fit 25-26 -> validate 24-25   125 tips  says 84.8  hit 80.8  gap −4.0

Forward calibrates; the reverse fails; pooled ≈ −2 on 307 tips. The bar is
both directions or nothing, so CUP_TIPS_ENABLED stays False. But the
trajectory is real — the family started at −11.4 — and the club-level
composite beat league-only ratings in every cut. Coverage is the other
honest limit: ~307 of ~1,100 Swiss-era fixtures clear the feature gates
(mapped clubs, rated leagues, ≥10 domestic games). Each finished matchday
thickens both windows; re-run this before the next verdict.

Usage:  python scripts/cup_composite.py
"""
from __future__ import annotations

import json
import math
import statistics as st
import sys
from collections import defaultdict, deque
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.cup_asof import CUPS, fit_ratings
from scripts.team_shrink_sweep import wilson

BREAK = pd.Timestamp("2024-07-01")           # Swiss era begins
SPLIT = pd.Timestamp("2025-07-01")           # season 1 | season 2
PPG_WEIGHT = 0.8
OWN_FORM_WEIGHT = 0.3
PPG_CENTRE = 1.45                            # typical PPG, so avg club ~ 0


def build_rows():
    """Every Swiss-era fixture with full features, built chronologically so
    nothing is priced on data that saw it."""
    cleaned = json.loads(
        (Path(__file__).resolve().parents[2] / "config"
         / "club_leagues.json").read_text())

    frames, matches = {}, []
    for code in CUPS:
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        frames[code] = df.dropna(subset=["hg", "ag"]).sort_values("date")
        for r in frames[code].itertuples():
            matches.append((code, str(r.home), str(r.away),
                            cleaned.get(str(r.home)), cleaned.get(str(r.away)),
                            int(r.hg), int(r.ag), r.date))
    matches.sort(key=lambda m: m[7])

    bridges = [(li, lj, hg, ag) for _c, _h, _a, li, lj, hg, ag, d in matches
               if li and lj and li != lj and d < BREAK
               and d >= BREAK - pd.Timedelta(days=2920)]
    rating, den = fit_ratings(bridges)

    dom = {}

    def ppg(club, when):
        lg = cleaned.get(club)
        if lg is None:
            return None
        if lg not in dom:
            d = store.load_results(lg)
            dom[lg] = d.dropna(subset=["hg", "ag"]) if d is not None else None
        df = dom[lg]
        if df is None:
            return None
        w = df[(df.date < when) & (df.date >= when - pd.Timedelta(days=365))]
        pts = games = 0
        for r in w.itertuples():
            if str(r.home) == club:
                pts += 3 if r.hg > r.ag else 1 if r.hg == r.ag else 0
                games += 1
            elif str(r.away) == club:
                pts += 3 if r.ag > r.hg else 1 if r.hg == r.ag else 0
                games += 1
        return pts / games - PPG_CENTRE if games >= 10 else None

    own_hist = defaultdict(lambda: deque(maxlen=10))

    def own_form(club):
        h = own_hist[club]
        return (sum(h) / len(h)) if len(h) >= 6 else None

    def rolling_base(code, when):
        df = frames[code]
        w = df[(df.date < when) & (df.date >= when - pd.Timedelta(days=1095))]
        if len(w) < 40:
            w = df[df.date < when]
        return float((w.hg + w.ag).mean()) if len(w) >= 30 else None

    rows = []
    for c, hcl, acl, li, lj, hg, ag, d in matches:
        if (d >= BREAK and li and lj
                and den.get(li, 0) >= 15 and den.get(lj, 0) >= 15):
            b = rolling_base(c, d)
            ph, pa = ppg(hcl, d), ppg(acl, d)
            oh, oa = own_form(hcl), own_form(acl)
            if b is not None and ph is not None and pa is not None:
                sh = (rating[li] + PPG_WEIGHT * ph
                      + (OWN_FORM_WEIGHT * oh if oh is not None else 0))
                sa = (rating[lj] + PPG_WEIGHT * pa
                      + (OWN_FORM_WEIGHT * oa if oa is not None else 0))
                rows.append((d, c, sh, sa, b, hg, ag))
        own_hist[hcl].append(hg - ag)      # updates AFTER use, never before
        own_hist[acl].append(ag - hg)
    return rows


def fit(rows):
    X = np.array([[abs(r[2] - r[3]), r[2] + r[3], 1.0] for r in rows])
    y = np.array([r[5] + r[6] - r[4] for r in rows])
    b1, b2, b0 = np.linalg.lstsq(X, y, rcond=None)[0]
    return b0, b1, b2


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
        got.append((best[0], best[1], hit_weight(res) >= 1.0))
    return got


def show(label, g):
    if len(g) < 30:
        print(f"{label:34} too few: {len(g)}")
        return
    k = sum(1 for r in g if r[2])
    hit, says = k / len(g), st.mean(r[1] for r in g)
    w = wilson(k, len(g))
    mix = defaultdict(int)
    for m, _p, _h in g:
        mix[m] += 1
    top = " ".join(f"{m}:{n}" for m, n in
                   sorted(mix.items(), key=lambda x: -x[1])[:4])
    print(f"{label:34} {len(g):4}  says {says*100:5.1f}  hit {hit*100:5.1f}  "
          f"gap {(hit-says)*100:+5.1f}  [{w[0]*100:.0f}-{w[1]*100:.0f}]  {top}")


def main() -> None:
    rows = build_rows()
    s1 = [r for r in rows if r[0] < SPLIT]
    s2 = [r for r in rows if r[0] >= SPLIT]
    print(f"Swiss era with full features: {len(s1)} (24-25)  "
          f"{len(s2)} (25-26)\n")

    b0, b1, b2 = fit(s1)
    print(f"betas from 24-25: {b0:+.3f} {b1:+.3f}*|gap| {b2:+.3f}*sum")
    show("fit 24-25 -> validate 25-26", grade(s2, b0, b1, b2))
    b0, b1, b2 = fit(s2)
    print(f"betas from 25-26: {b0:+.3f} {b1:+.3f}*|gap| {b2:+.3f}*sum")
    show("fit 25-26 -> validate 24-25", grade(s1, b0, b1, b2))
    b0, b1, b2 = fit(s1 + s2)
    print(f"\npooled betas: {b0:+.3f} {b1:+.3f}*|gap| {b2:+.3f}*sum")


if __name__ == "__main__":
    main()
