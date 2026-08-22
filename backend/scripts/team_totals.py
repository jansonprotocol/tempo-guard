"""
Could Tip 2 be a TEAM total rather than a match total?

The idea being tested: when the match total is a coin-flip the engine cannot
resolve — the fixtures where Tip 1 falls back to a negative-edge `U4.25` — one
SIDE may still be readable. "Neither of us knows how many goals this game has,
but Team A scoring at all is 80%."

Why it is worth testing rather than dismissing: every existing Tip 2 is a rung
on the SAME ladder as Tip 1, so the two are related by set containment. A team
total is the first genuinely orthogonal market available — it can win on totals
where Tip 1 loses and lose where Tip 1 wins, without being the opposite side of
the same bet.

The inputs already exist. `features.asof_features` computes per-side goal
expectations and publishes them as `p_home_tt05` / `p_away_tt05`, the Poisson
probability that each side scores at least once. They feed one add-on gate and
are otherwise unused. No new modelling is required to test this — only a
measurement of whether those numbers beat their own base rate.

WHAT IS MEASURED
================
    calibration   predicted P(side scores) against how often it happened,
                  bucketed. A market is only worth offering if its probability
                  means something.
    edge          strike rate minus the base rate of the same market in the
                  same league — the benchmark used everywhere else here,
                  because "home team scores" is ~75% by itself and a 75% hit
                  rate would be worth precisely nothing.
    where it bites  the same numbers restricted to fixtures where Tip 1 is a
                  fallback (negative edge), since that is the case the idea is
                  actually aimed at.

Both directions are scored: OVER 0.5 (the side scores) and UNDER 0.5 (the side
is kept out), because the second is the low-probability, high-edge lane a
defensive read would produce.
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

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 200
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL",
           "SUI-SL", "GRE-SL", "POL-EK", "BRA-SA"]


def main() -> None:
    rows = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            ph, pa = req.p_home_tt05, req.p_away_tt05
            if ph is None or pa is None:
                continue
            m = predict_fixture(req, cfg, module_flags=flags).translated_play.market
            edge = next((e for mk, e, _p, _q in
                         market_select.score_markets(req.mu_total, req.league_mu)
                         if mk == m), 0.0)
            rows.append(dict(lg=code, ph=float(ph), pa=float(pa),
                             hs=int(hg) >= 1, as_=int(ag) >= 1,
                             t1=m, t1edge=edge, date=req.match_date))
        print(f"  {code}: {len(pairs)}", flush=True)

    print(f"\n  {len(rows)} fixtures\n")

    # Base rate: how often each side scores at all, per league.
    base = {}
    for lg in {r["lg"] for r in rows}:
        sel = [r for r in rows if r["lg"] == lg]
        base[lg] = (sum(r["hs"] for r in sel) / len(sel),
                    sum(r["as_"] for r in sel) / len(sel))
    bh = sum(base[lg][0] for lg in base) / len(base)
    ba = sum(base[lg][1] for lg in base) / len(base)
    print(f"  BASE RATES   home side scores {bh:.1%}   away side scores {ba:.1%}")
    print("  (a team-total tip must beat THESE, not 50%)\n")

    print("  CALIBRATION — predicted P(side scores) vs what happened")
    print(f"  {'bucket':>12s} {'n':>6s} {'predicted':>10s} {'actual':>8s} {'gap':>7s}")
    buckets = [(0.0, .55), (.55, .65), (.65, .75), (.75, .85), (.85, 1.01)]
    for lo, hi in buckets:
        sel = [(r["ph"], r["hs"]) for r in rows if lo <= r["ph"] < hi]
        sel += [(r["pa"], r["as_"]) for r in rows if lo <= r["pa"] < hi]
        if len(sel) < 30:
            continue
        pred = sum(p for p, _ in sel) / len(sel)
        act = sum(h for _, h in sel) / len(sel)
        print(f"  {lo:.2f}-{hi:.2f} {len(sel):8d} {pred:10.1%} {act:8.1%} "
              f"{act - pred:+7.1%}")

    def score(label, sel, floor):
        """Best available team-total OVER 0.5 at a probability floor."""
        picks = []
        for r in sel:
            cands = []
            if r["ph"] >= floor:
                cands.append((r["ph"], r["hs"], base[r["lg"]][0]))
            if r["pa"] >= floor:
                cands.append((r["pa"], r["as_"], base[r["lg"]][1]))
            if cands:
                picks.append(max(cands))
        if not picks:
            print(f"  {label:<34s} floor {floor:.2f}   no picks")
            return
        n = len(picks)
        hit = sum(h for _p, h, _b in picks) / n
        bas = sum(b for _p, _h, b in picks) / n
        print(f"  {label:<34s} floor {floor:.2f} {n:6d} picks  "
              f"hit {hit:6.1%}  base {bas:6.1%}  edge {hit - bas:+6.2%}")

    print("\n  TEAM TOTAL OVER 0.5 — take the more likely side above a floor")
    for floor in (0.70, 0.75, 0.79, 0.83, 0.87):
        score("all fixtures", rows, floor)

    print("\n  RESTRICTED TO FIXTURES WHERE TIP 1 IS A FALLBACK (negative edge)")
    fb = [r for r in rows if r["t1edge"] < 0]
    print(f"  ({len(fb)} of {len(rows)} fixtures, {len(fb)/len(rows):.0%})")
    for floor in (0.70, 0.75, 0.79, 0.83):
        score("Tip 1 negative edge", fb, floor)

    print("\n  TEAM TOTAL UNDER 0.5 — the side is kept out")
    for ceil in (0.55, 0.50, 0.45, 0.40):
        picks = []
        for r in rows:
            cands = []
            if r["ph"] <= ceil:
                cands.append((1 - r["ph"], not r["hs"], 1 - base[r["lg"]][0]))
            if r["pa"] <= ceil:
                cands.append((1 - r["pa"], not r["as_"], 1 - base[r["lg"]][1]))
            if cands:
                picks.append(max(cands))
        if not picks:
            continue
        n = len(picks)
        hit = sum(h for _p, h, _b in picks) / n
        bas = sum(b for _p, _h, b in picks) / n
        print(f"  {'side kept out':<34s} P<= {ceil:.2f} {n:6d} picks  "
              f"hit {hit:6.1%}  base {bas:6.1%}  edge {hit - bas:+6.2%}")


    # ── Chronological holdout ────────────────────────────────────────────
    # A sweep over floors on one pool proves nothing by this repository's own
    # standard; the best of several thresholds is high by construction. Split
    # by DATE and check the floor chosen on the earlier half survives on the
    # later one.
    rows.sort(key=lambda r: r["date"])
    cut = int(len(rows) * 0.65)
    train, hold = rows[:cut], rows[cut:]
    print(f"\n  CHRONOLOGICAL HOLDOUT   train {len(train)} "
          f"(to {train[-1]['date']})   holdout {len(hold)} (from {hold[0]['date']})")

    def measure(sel, floor):
        picks = []
        for r in sel:
            c = []
            if r["ph"] >= floor:
                c.append((r["ph"], r["hs"], base[r["lg"]][0]))
            if r["pa"] >= floor:
                c.append((r["pa"], r["as_"], base[r["lg"]][1]))
            if c:
                picks.append(max(c))
        if not picks:
            return None
        n = len(picks)
        hit = sum(h for _p, h, _b in picks) / n
        bas = sum(b for _p, _h, b in picks) / n
        return n, hit, bas, hit - bas

    best, bf = None, None
    for floor in (0.70, 0.75, 0.79, 0.83, 0.87):
        m = measure(train, floor)
        if m and (best is None or m[3] > best[3]):
            best, bf = m, floor
    print(f"  best floor on TRAIN: {bf:.2f}  "
          f"{best[0]} picks hit {best[1]:.1%} base {best[2]:.1%} edge {best[3]:+.2%}")
    h = measure(hold, bf)
    print(f"  same floor on HOLDOUT: {h[0]} picks hit {h[1]:.1%} "
          f"base {h[2]:.1%} edge {h[3]:+.2%}")
    print("\n  every floor on the holdout:")
    for floor in (0.70, 0.75, 0.79, 0.83, 0.87):
        m = measure(hold, floor)
        if m:
            print(f"    floor {floor:.2f}  {m[0]:5d} picks  hit {m[1]:6.1%}  "
                  f"base {m[2]:6.1%}  edge {m[3]:+6.2%}")


if __name__ == "__main__":
    main()
