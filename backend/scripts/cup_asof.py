"""
The as-of refit: does letting the ratings breathe fix the early window?

The strength prototype validated at +1.1 on the newest holdout and failed at
−3.7 on 2022-24 — with ratings frozen at the 2022 training cut. The lead
suspect is drift: league strength is not a constant (the post-2022 window
contains, among other things, an entire league's attack moving to Saudi
Arabia). So this re-runs the same validation with everything AS-OF:

    ratings   refitted at each month boundary, on the trailing eight years
              of bridges strictly before that month — a fixture is never
              priced on a rating that saw it
    baseline  the rolling three-year competition mean, as before
    betas     FROZEN from the pre-2022 fit. Structure ("mismatch means
              goals") should transfer; levels should drift. If the early
              window still fails, that assumption falls next.

Same bar as everything here: both windows calibrated with a live market mix,
or it does not ship.

Usage:  python scripts/cup_asof.py
"""
from __future__ import annotations

import json
import math
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.team_shrink_sweep import wilson

CUPS = ("UCL", "UEL", "UECL", "UCL-Q", "UEL-Q", "UECL-Q")
CUT = pd.Timestamp("2022-07-01")
BRIDGE_WINDOW = pd.Timedelta(days=2920)          # eight years
MIN_BRIDGES = 15


def fit_ratings(bridges):
    rating = defaultdict(float)
    home_adv = 0.3
    den = defaultdict(int)
    for _ in range(120):
        num = defaultdict(float)
        den = defaultdict(int)
        ha_n = ha_d = 0.0
        for li, lj, hg, ag in bridges:
            s = hg - ag
            ha_n += s - (rating[li] - rating[lj])
            ha_d += 1
            resid = s - home_adv
            num[li] += resid + rating[lj]
            den[li] += 1
            num[lj] += rating[li] - resid
            den[lj] += 1
        home_adv = ha_n / ha_d
        new = {lg: num[lg] / den[lg] for lg in num if den[lg] >= MIN_BRIDGES}
        m = sum(new.values()) / len(new)
        rating = defaultdict(float, {lg: v - m for lg, v in new.items()})
    return rating, den


def main() -> None:
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
            li, lj = cleaned.get(str(r.home)), cleaned.get(str(r.away))
            matches.append((code, li, lj, int(r.hg), int(r.ag), r.date))
    matches.sort(key=lambda m: m[5])

    def bridges_before(when):
        return [(li, lj, hg, ag) for _c, li, lj, hg, ag, d in matches
                if li and lj and li != lj and when - BRIDGE_WINDOW <= d < when]

    def rolling_base(code, when):
        df = frames[code]
        w = df[(df.date < when) & (df.date >= when - pd.Timedelta(days=1095))]
        if len(w) < 60:
            w = df[df.date < when]
        return float((w.hg + w.ag).mean()) if len(w) >= 30 else None

    # Frozen betas from the pre-CUT fit, on pre-CUT as-of ratings (one fit at
    # the cut is close enough for coefficients — structure, not level).
    r0, d0 = fit_ratings(bridges_before(CUT))
    X, y = [], []
    for c, li, lj, hg, ag, d in matches:
        if d >= CUT or not (li and lj and d0.get(li, 0) >= MIN_BRIDGES
                            and d0.get(lj, 0) >= MIN_BRIDGES):
            continue
        b = rolling_base(c, d)
        if b is None:
            continue
        X.append([abs(r0[li] - r0[lj]), r0[li] + r0[lj], 1.0])
        y.append(hg + ag - b)
    b1, b2, b0 = np.linalg.lstsq(np.array(X), np.array(y), rcond=None)[0]
    print(f"frozen betas: {b0:+.3f} {b1:+.3f}*|gap| {b2:+.3f}*sum "
          f"(fit on {len(X)})")

    # Walk the test era month by month, ratings refitted as-of.
    test = [m for m in matches if m[5] >= CUT]
    months = sorted({(m[5].year, m[5].month) for m in test})
    got = []
    for ym in months:
        start = pd.Timestamp(year=ym[0], month=ym[1], day=1)
        rating, den = fit_ratings(bridges_before(start))
        for c, li, lj, hg, ag, d in test:
            if (d.year, d.month) != ym:
                continue
            if not (li and lj and den.get(li, 0) >= MIN_BRIDGES
                    and den.get(lj, 0) >= MIN_BRIDGES):
                continue
            b = rolling_base(c, d)
            if b is None:
                continue
            mu = (b + b0 + b1 * abs(rating[li] - rating[lj])
                  + b2 * (rating[li] + rating[lj]))
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
            got.append((d, c, best[0], best[1], hit_weight(res) >= 1.0))

    mid = sorted(g[0] for g in got)[len(got) // 2]

    def show(label, rows):
        if len(rows) < 30:
            print(f"{label:12} too few: {len(rows)}")
            return
        k = sum(1 for r in rows if r[4])
        hit, says = k / len(rows), sum(r[3] for r in rows) / len(rows)
        w = wilson(k, len(rows))
        mix = defaultdict(int)
        for _d, _c, m, _p, _h in rows:
            mix[m] += 1
        top = " ".join(f"{m}:{n}" for m, n in
                       sorted(mix.items(), key=lambda x: -x[1])[:4])
        print(f"{label:12} {len(rows):4} tips  says {says*100:5.1f}%  "
              f"hit {hit*100:5.1f}%  gap {(hit-says)*100:+5.1f}  "
              f"[{w[0]*100:.0f}-{w[1]*100:.0f}]  {top}")

    show("test-early", [g for g in got if g[0] < mid])
    show("test-late", [g for g in got if g[0] >= mid])
    print()
    for code in CUPS:
        show(code, [g for g in got if g[1] == code])


if __name__ == "__main__":
    main()
