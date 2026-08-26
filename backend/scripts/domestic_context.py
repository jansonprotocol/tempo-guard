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

VERDICT (26 Aug 2026): one term clears the residual bar and still does
not ship — the fourth "signal without edge" of the day.

    ppg_gap      +0.241 ± 0.063  t 3.81   windows t 3.17 / 2.23  <-- passes
    pos_signed   +0.103 ± 0.035  t 2.94   windows t 2.42 / 1.75
    stature      +0.118 ± 0.057  t 2.06   windows t 1.17 / 1.79
    ppg_signed   +0.094 ± 0.039  t 2.40   windows t 1.92 / 1.46
    reverse      +0.025 ± 0.019  t 1.32   nothing

Graded as tips in the live shape, the mismatch term LOSES hitrate in
both directions — 86.4 -> 85.9 and 86.4 -> 85.7 — while moving the gap
by a tenth either way. The residual is real; the tips are not better for
it. The reason is visible in the same table: the domestic board already
grades 86.4% against a stated 85.2-85.6, so it is mildly UNDER-confident
already. Pushing mu up on mismatched fixtures shifts selection toward
looser rungs, and those pick worse than the correction gains.

The shrinkage hypothesis is also refuted. If MU_SHRINK were
over-correcting the extremes, the residual would concentrate where mu
sits far from the league mean. It does not: near the mean +0.116
(t 1.06), middling +0.358 (t 3.08), far +0.251 (t 2.40) — strongest in
the MIDDLE. Whatever the mismatch term knows, it is not a shrink error.

Usage:  python scripts/domestic_context.py [--n 300] [--grade]
                                           [--cache rows.pkl]
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
                        d=r.date, code=code, hg=int(r.hg), ag=int(r.ag),
                        mu=float(req.mu_total),
                        lmu=float(req.league_mu or 0) or None,
                        dev=int(r.hg) + int(r.ag) - req.mu_total,
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


def graded(rows, b, term="ppg_gap"):
    """Re-select each tip offline with mu shifted by b * term, exactly as
    market_select would, and grade it."""
    from app.engine import market_select
    from app.util.asian_lines import evaluate_market, hit_weight
    out = []
    for r in rows:
        if r.get(term) is None or not r.get("lmu"):
            continue
        cfg = config.get(r["code"])
        mu = r["mu"] + b * r[term]
        best = None
        for m, _e, p, _q in market_select.score_markets(mu, r["lmu"]):
            if not market_select.playable(m, cfg.max_under_line,
                                          cfg.min_over_line):
                continue
            if p < (cfg.min_win_prob or market_select.MIN_WIN_PROB):
                continue
            if best is None or p > best[1]:
                best = (m, p)
        if best is None:
            continue
        res = evaluate_market(best[0], r["hg"], r["ag"])
        if res is None:
            continue
        out.append((best[1], hit_weight(res) >= 1.0))
    return out


def grade_show(label, g):
    if len(g) < 100:
        print(f"  {label:26} too few: {len(g)}")
        return
    k = sum(1 for x in g if x[1])
    says = sum(x[0] for x in g) / len(g)
    print(f"  {label:26} {len(g):5} tips  says {says*100:5.1f}  "
          f"hit {k/len(g)*100:5.1f}  gap {(k/len(g)-says)*100:+5.1f}")


def main() -> None:
    import pickle
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 300
    cache = Path(args[args.index("--cache") + 1]) if "--cache" in args \
        else None
    if cache and cache.exists():
        rows = pickle.loads(cache.read_bytes())
    else:
        rows = []
        for code in LEAGUES:
            try:
                got = league_rows(code, n)
            except Exception as exc:
                print(f"{code}: FAILED {exc}", file=sys.stderr)
                continue
            print(f"{code}: {len(got)}", file=sys.stderr)
            rows += got
        if cache:
            cache.write_bytes(pickle.dumps(rows))
    rows.sort(key=lambda r: r["d"])
    print(f"\n{len(rows)} domestic fixtures, residual against the engine's "
          f"own mu\n")
    mid = rows[len(rows) // 2]["d"]
    for term in TERMS:
        print(f"{term}:")
        show("all", rows, term)
        show("older half", [r for r in rows if r["d"] < mid], term)
        show("newer half", [r for r in rows if r["d"] >= mid], term)

    if "--grade" not in sys.argv:
        return

    # WHY would a mismatch add goals the rates missed? The suspect is
    # shrinkage: MU_SHRINK pulls every fixture toward the league mean, and
    # a lopsided fixture is exactly the one that sits far from it. If the
    # residual is concentrated where mu already departs from the mean,
    # the gap term is really a shrinkage correction.
    print("\nMECHANISM — residual by how far mu sits from the league mean")
    have = [r for r in rows if r.get("lmu")]
    have.sort(key=lambda r: abs(r["mu"] - r["lmu"]))
    third = len(have) // 3
    for lab, part in (("mu near the mean", have[:third]),
                      ("middling", have[third:2 * third]),
                      ("mu far from the mean", have[2 * third:])):
        show(lab, part, "ppg_gap")

    # The bar: does it survive as TIPS, in both windows?
    print("\nGRADED — the live shape, betas frozen from the other window")
    old_h = [r for r in rows if r["d"] < mid]
    new_h = [r for r in rows if r["d"] >= mid]
    for lab, train, test in (("older -> newer", old_h, new_h),
                             ("newer -> older", new_h, old_h)):
        got = stats(train, "ppg_gap")
        b = got[1] if got else 0.0
        grade_show(f"{lab}  as-is", graded(test, 0.0))
        grade_show(f"{lab}  + gap {b:+.3f}", graded(test, b))


if __name__ == "__main__":
    main()
