"""
Does the domestic mu miss what the cup mu missed?

B3 fixed a hole peculiar to cups: that lane prices from |elo gap| and
elo sum, and |gap| is symmetric, so it could not tell a strong home side
from a strong away side. The domestic path cannot have that hole — it
builds mu from each side's own scoring rate at its own venue. But the
question behind it generalises: is there information in a fixture's
CONTEXT that the engine's rates do not already carry?

Four candidates, each measured as the residual of the engine's own mu
(actual total minus mu_total), all as-of:

    ppg_signed   home PPG minus away PPG, trailing 365 days — the
                 domestic echo of B3: does knowing WHICH side is the
                 better one add anything after the rates?
    ppg_gap      |the same|, the lopsidedness rather than the direction
    pos_signed   league-table positions, away rank minus home rank —
                 stakes and stature rather than form
    stature      each club's mean points-per-game over the THREE prior
                 seasons, signed — "Barcelona is always top three" as a
                 number, which rolling form forgets after a bad month
    reverse      the goal total of this season's earlier meeting between
                 the same clubs — the domestic version of the two-legged
                 tie question

Reported as coefficient ± standard error against the mu residual, then
split into an older and a newer half. The bar is the project's usual:
significant, stable, and present in both windows, or it does not ship.

Usage:  python scripts/domestic_context.py [--n 300]
"""
from __future__ import annotations

import math
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine.types import ModuleFlags
from app.predict import build_request

# A spread of sizes and cultures, not just the big five.
LEAGUES = ("ENG-PL", "ESP-LL", "GER-BL", "ITA-SA", "FRA-L1", "NED-ED",
           "POR-PL", "BEL-PL", "TUR-SL", "BRA-SA", "SAU-PL", "ENG-CH",
           "SCO-PL", "DEN-SL", "NOR-EL", "SWE-AL")
TERMS = ("ppg_signed", "ppg_gap", "pos_signed", "stature", "reverse")


def league_rows(code: str, n: int) -> list[dict]:
    df = store.load_results(code)
    if df is None or len(df) < 400:
        return []
    df = df.dropna(subset=["hg", "ag"]).sort_values("date")
    cfg = config.get(code)
    ModuleFlags(**(cfg.module_overrides or {}))

    # Walked forward: form, table and stature are built only from rows
    # already seen, so nothing is priced on its own result.
    pts = defaultdict(float)
    games = defaultdict(int)
    season_pts: dict = defaultdict(lambda: defaultdict(float))
    season_games: dict = defaultdict(lambda: defaultdict(int))
    hist = []
    met: dict = {}
    out = []
    target = set(df.index[-n:])

    for r in df.itertuples():
        h, a = str(r.home), str(r.away)
        season = str(getattr(r, "season", "") or "")
        if r.Index in target:
            try:
                req = build_request(code, h, a, r.date.date())
            except Exception:
                req = None
            if req is not None and req.mu_total:
                # trailing-365d PPG
                cut = r.date - pd.Timedelta(days=365)
                recent = [x for x in hist if x[0] >= cut]
                pp = defaultdict(float)
                gg = defaultdict(int)
                for _d, hh, aa, hgg, agg in recent:
                    for team, gf, ga in ((hh, hgg, agg), (aa, agg, hgg)):
                        pp[team] += 3 if gf > ga else 1 if gf == ga else 0
                        gg[team] += 1
                if gg[h] >= 10 and gg[a] >= 10:
                    ppg_h, ppg_a = pp[h] / gg[h], pp[a] / gg[a]
                    # current-season table position
                    tbl = sorted(season_pts[season].items(),
                                 key=lambda kv: -kv[1])
                    rank = {t: i + 1 for i, (t, _v) in enumerate(tbl)}
                    ph, pa = rank.get(h), rank.get(a)
                    # stature: PPG across the three PRIOR seasons
                    prior = [s for s in season_pts if s and s < season]
                    prior = sorted(prior)[-3:]
                    sh = sa = None
                    if prior:
                        num_h = sum(season_pts[s].get(h, 0) for s in prior)
                        den_h = sum(season_games[s].get(h, 0) for s in prior)
                        num_a = sum(season_pts[s].get(a, 0) for s in prior)
                        den_a = sum(season_games[s].get(a, 0) for s in prior)
                        if den_h >= 20 and den_a >= 20:
                            sh, sa = num_h / den_h, num_a / den_a
                    rev = met.get((season, frozenset((h, a))))
                    out.append(dict(
                        d=r.date, dev=int(r.hg) + int(r.ag) - req.mu_total,
                        ppg_signed=ppg_h - ppg_a,
                        ppg_gap=abs(ppg_h - ppg_a),
                        pos_signed=((pa - ph) / 10.0
                                    if ph and pa and len(tbl) >= 8 else None),
                        stature=(sh - sa) if sh is not None else None,
                        reverse=(rev - 2.7) if rev is not None else None))
        # bookkeeping AFTER use
        hist.append((r.date, h, a, int(r.hg), int(r.ag)))
        for team, gf, ga in ((h, r.hg, r.ag), (a, r.ag, r.hg)):
            p = 3 if gf > ga else 1 if gf == ga else 0
            pts[team] += p
            games[team] += 1
            season_pts[season][team] += p
            season_games[season][team] += 1
        met[(season, frozenset((h, a)))] = int(r.hg) + int(r.ag)
    return out


def stats(rows, term):
    sub = [r for r in rows if r.get(term) is not None]
    if len(sub) < 200:
        return None
    x = np.array([r[term] for r in sub])
    y = np.array([r["dev"] for r in sub])
    X = np.column_stack([x, np.ones(len(x))])
    beta, *_ = np.linalg.lstsq(X, y, rcond=None)
    resid = y - X @ beta
    s2 = float(resid @ resid) / (len(y) - 2)
    se = math.sqrt(s2 * np.linalg.pinv(X.T @ X)[0, 0])
    return len(sub), beta[0], se, beta[0] / se if se else 0.0


def show(label, rows, term):
    got = stats(rows, term)
    if got is None:
        print(f"  {label:22} too little coverage")
        return
    n, b, se, t = got
    flag = "  <--" if abs(t) >= 2 else ""
    print(f"  {label:22} n {n:5}  {b:+.4f} ± {se:.4f}   t {t:+5.2f}{flag}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 300
    rows = []
    for code in LEAGUES:
        try:
            got = league_rows(code, n)
        except Exception as exc:
            print(f"{code}: FAILED {exc}", file=sys.stderr)
            continue
        print(f"{code}: {len(got)}", file=sys.stderr)
        rows += got
    rows.sort(key=lambda r: r["d"])
    print(f"\n{len(rows)} domestic fixtures, residual against the engine's "
          f"own mu\n")
    mid = rows[len(rows) // 2]["d"]
    for term in TERMS:
        print(f"{term}:")
        show("all", rows, term)
        show("older half", [r for r in rows if r["d"] < mid], term)
        show("newer half", [r for r in rows if r["d"] >= mid], term)


if __name__ == "__main__":
    main()
