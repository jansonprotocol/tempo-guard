"""
Does anything add to Elo on cup totals? The orthogonality test.

The cup lane prices from Club Elo alone, and the old domestic-form path
died with a slope of 0.017 — but that path used form as the PRIMARY
signal. This asks the different question: on top of a strength model,
does any form term carry information Elo cannot?

The hypothesis worth testing is not "form" in general but TEMPO. Elo
rates a club by results, so two 1600-rated clubs might be a 1-0 pair or
a 3-2 pair and Elo reads them identically. A club's own goal rate is
the obvious candidate for what strength cannot say.

Five candidates, all as-of, all measured against the Elo baseline
(mu = base + b0 + b1·|Δelo| + b2·Σelo):

    dom_tempo   both clubs' domestic goals per game, trailing 365 days
    cup_tempo   both clubs' own cup matches' goal totals, last 8
    ppg_sum     both clubs' domestic points per game (the composite's
                form term, re-asked as an ADDITION rather than a rival)
    ppg_gap     the mismatch in domestic form
    elo_mom     each club's Elo change over the previous 90 days —
                form as the rating itself moves

Reported first as coefficients with standard errors on the pooled Swiss
era (a t below ~2 is noise), then — for anything that survives — the
project's actual bar: tips graded in both symmetry directions against
the Elo-only model.

VERDICT (26 Aug 2026): the tempo hypothesis is half right and still
does not ship.

    dom_tempo  n  251  +0.429 ± 0.165   t +2.60   variance +2.67%
    cup_tempo  n 1064  +0.112 ± 0.053   t +2.13   variance +0.43%
    ppg_sum    n  251  +0.253 ± 0.219   t +1.15   variance +0.54%
    ppg_gap    n  251  +0.129 ± 0.337   t +0.38   variance +0.06%
    elo_mom    n 1561  -0.011 ± 0.101   t -0.11   variance +0.00%

Goal-rate terms carry signal Elo cannot (both positive, both past t=2),
exactly as the orthogonality argument predicted — while every
results-based term is flat, which independently re-confirms the old
0.017 finding and shows Elo levels already price momentum.

But signal is not edge. Converted into tips and graded both directions,
the terms change the mix without improving it:

    dom_tempo   -0.6 -> -0.8   |   -6.8 -> -7.0
    cup_tempo   -1.2 -> -2.9   |   -1.4 -> -1.2
    both        +1.1 -> -0.1   |   -5.3 -> -9.4

Not one window improves. A term explaining 0.4-2.7% of residual variance
moves mu by a few hundredths of a goal — far less than the distance
between rungs — so it reshuffles selection at the margin and mostly
picks worse. The cup lane stays strength-only; this is VENUE_BLEND
again, measured and closed.

Usage:  python scripts/cup_elo_form.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from scripts.cup_asof import CUPS
from scripts.cup_composite import SPLIT, grade, show
from scripts.cup_elo import BREAK, ELO_END, SCALE, asof, elo_series

ROOT = Path(__file__).resolve().parents[2]
TERMS = ("dom_tempo", "cup_tempo", "ppg_sum", "ppg_gap", "elo_mom")


def build():
    names = json.loads((ROOT / "config" / "club_elo_names.json").read_text())
    leagues = json.loads((ROOT / "config" / "club_leagues.json").read_text())
    series = elo_series()

    frames = {}
    for code in CUPS:
        df = store.load_results(code)
        if df is not None and not df.empty:
            frames[code] = df.dropna(subset=["hg", "ag"]).sort_values("date")

    dom_cache = {}

    def dom(club, when):
        """(goals per game, points per game) in the club's own league."""
        lg = leagues.get(club)
        if lg is None:
            return None, None
        if lg not in dom_cache:
            d = store.load_results(lg)
            dom_cache[lg] = (d.dropna(subset=["hg", "ag"]) if d is not None
                             else None)
        df = dom_cache[lg]
        if df is None:
            return None, None
        w = df[(df.date < when) & (df.date >= when - pd.Timedelta(days=365))]
        tot = pts = games = 0
        for r in w.itertuples():
            home = str(r.home) == club
            away = str(r.away) == club
            if not (home or away):
                continue
            games += 1
            tot += int(r.hg) + int(r.ag)
            gf, ga = (r.hg, r.ag) if home else (r.ag, r.hg)
            pts += 3 if gf > ga else 1 if gf == ga else 0
        if games < 10:
            return None, None
        return tot / games, pts / games

    # Own cup history, walked forward so a fixture never sees itself.
    cup_hist = {}

    def elo(club, when):
        ser = series.get(names.get(club, club)) or series.get(club)
        return asof(ser, when) if ser else None

    rows = []
    allm = []
    for code, df in frames.items():
        for r in df.itertuples():
            allm.append((code, r))
    allm.sort(key=lambda x: x[1].date)

    for code, r in allm:
        h, a = str(r.home), str(r.away)
        tot = int(r.hg) + int(r.ag)
        if BREAK <= r.date <= ELO_END:
            eh, ea = elo(h, r.date), elo(a, r.date)
            base_df = frames[code]
            w = base_df[(base_df.date < r.date)
                        & (base_df.date >= r.date - pd.Timedelta(days=1095))]
            if len(w) < 40:
                w = base_df[base_df.date < r.date]
            base = float((w.hg + w.ag).mean()) if len(w) >= 30 else None
            gh, ph = dom(h, r.date)
            ga_, pa = dom(a, r.date)
            ch = cup_hist.get(h, [])
            ca = cup_hist.get(a, [])
            if eh and ea and base:
                # Each term is optional: a fixture missing one still tests
                # the others, so every candidate gets its largest honest
                # sample instead of the intersection of all five.
                mh = elo(h, r.date - pd.Timedelta(days=90))
                ma = elo(a, r.date - pd.Timedelta(days=90))
                rows.append(dict(
                    d=r.date, code=code, hg=int(r.hg), ag=int(r.ag),
                    base=base, eh=eh / SCALE, ea=ea / SCALE,
                    dom_tempo=((gh + ga_) - 5.4) if (gh and ga_) else None,
                    cup_tempo=((float(np.mean(ch[-8:]))
                                + float(np.mean(ca[-8:]))) - 5.4)
                    if (len(ch) >= 5 and len(ca) >= 5) else None,
                    ppg_sum=((ph + pa) - 2.9) if (ph and pa) else None,
                    ppg_gap=abs(ph - pa) if (ph and pa) else None,
                    elo_mom=((((eh - mh) if mh else 0.0)
                              + ((ea - ma) if ma else 0.0)) / SCALE)
                    if (mh or ma) else None))
        cup_hist.setdefault(h, []).append(tot)
        cup_hist.setdefault(a, []).append(tot)
    return rows


def design(rows, extra=()):
    X = [[abs(r["eh"] - r["ea"]), r["eh"] + r["ea"], 1.0]
         + [r[t] for t in extra] for r in rows]
    y = [r["hg"] + r["ag"] - r["base"] for r in rows]
    return np.array(X), np.array(y)


def fit_se(X, y):
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    dof = len(y) - X.shape[1]
    s2 = float(resid @ resid) / dof
    cov = s2 * np.linalg.pinv(X.T @ X)
    return beta, np.sqrt(np.diag(cov)), float(resid @ resid)


def main() -> None:
    rows = build()
    print(f"{len(rows)} Swiss-era cup fixtures with Elo AND full form "
          f"coverage\n")
    if len(rows) < 200:
        print("too few to test"); return

    X0, y = design(rows)
    b0, se0, rss0 = fit_se(X0, y)
    print(f"Elo baseline on all {len(rows)}: {b0[2]:+.3f} "
          f"{b0[0]:+.3f}*|gap| {b0[1]:+.3f}*sum\n")
    print("each candidate, on its own coverage, against Elo alone there:")
    keep = []
    for term in TERMS:
        sub = [r for r in rows if r[term] is not None]
        if len(sub) < 250:
            print(f"  {term:10} too little coverage: {len(sub)}")
            continue
        Xa, ya = design(sub)
        _, _, rss_a = fit_se(Xa, ya)
        Xb, _ = design(sub, (term,))
        b1, se1, rss_b = fit_se(Xb, ya)
        coef, se = b1[3], se1[3]
        t = coef / se if se else 0.0
        drop = (rss_a - rss_b) / rss_a * 100
        flag = "  <-- survives" if abs(t) >= 2 else ""
        print(f"  {term:10} n {len(sub):4}  {coef:+.4f} ± {se:.4f}   "
              f"t {t:+5.2f}   variance {drop:+.2f}%{flag}")
        if abs(t) >= 2:
            keep.append(term)

    if not keep:
        print("\nNothing clears t=2. Elo already carries what these terms "
              "know; the cup lane stays strength-only.")
        return

    print(f"\nsurvivors: {keep} — now the two-window bar\n")

    def graded(train, test, extra):
        Xt, yt = design(train, extra)
        bt, _, _ = fit_se(Xt, yt)
        out = []
        for r in test:
            mu = (r["base"] + bt[2] + bt[0] * abs(r["eh"] - r["ea"])
                  + bt[1] * (r["eh"] + r["ea"])
                  + sum(bt[3 + i] * r[k] for i, k in enumerate(extra)))
            # grade() rebuilds mu as base + b0 + b1|sh-sa| + b2(sh+sa);
            # with b0 = b1 = 0 and b2 = 1, handing it sh = sa = (mu-base)/2
            # reproduces mu exactly instead of adding the base twice.
            half = (mu - r["base"]) / 2
            out.append((r["d"], r["code"], half, half, r["base"],
                        r["hg"], r["ag"]))
        return grade(out, 0.0, 0.0, 1.0)

    for term in keep + ([tuple(keep)] if len(keep) > 1 else []):
        extra = (term,) if isinstance(term, str) else term
        sub = [r for r in rows if all(r[k] is not None for k in extra)]
        s1 = [r for r in sub if r["d"] < SPLIT]
        s2 = [r for r in sub if r["d"] >= SPLIT]
        print(f"— {'+'.join(extra)} (n {len(sub)}) —")
        for lab, tr, te in (("24-25 -> 25-26", s1, s2),
                            ("25-26 -> 24-25", s2, s1)):
            show(f"  {lab}  Elo only", graded(tr, te, ()))
            show(f"  {lab}  + form  ", graded(tr, te, extra))
        print()


if __name__ == "__main__":
    main()
