"""
Can the board's weakest leagues be lifted the way the cups were?

The bottom of the retrosim table — ITA-SB 77.2, TUR-SL 78.3, MLS 78.8 —
is not a calibration problem: most of those rows SAY ~80 and HIT ~79,
which is an engine telling the truth about leagues it finds hard. The
proven lever for that is not a fix to mu but a change in what gets
offered: raise the league's probability floor and selection climbs the
ladder to safer rungs. That is exactly what took the cups from ~80 to
~83 at their 0.82 floor — and the same measurement showed its mirror
image, that tightening EDGE instead loses (the winner's curse).

Per league, per candidate floor, the two-season window is replayed
as-of: one build_request per fixture, then one cheap predict per floor
with the league's config copied and only min_win_prob changed — the
identical machinery live tips go through, Rule 6 translation included.
Because the floor is a PER-LEAGUE override in config/leagues.json,
shipping one cannot move any other league by construction.

The bar to ship, per league:

    hit improves in BOTH halves of the window   (the standing rule)
    pooled improvement of at least +1.0         (not noise-chasing)
    at least ~60% of the tips survive           (a lane, not a relic)

Usage:  python scripts/floor_lift.py [--leagues A,B] [--floors 0.78,0.80,0.82]
"""
from __future__ import annotations

import copy
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

# The bottom of the table, worst first — everything under ~81 at 27 Aug.
LOW = ("ITA-SB", "TUR-SL", "MLS", "FRA-L2", "MEX-LMX", "ITA-SA", "SCO-CH",
       "CZE-FL", "POL-EK", "PER-L1", "ROU-L1", "FIN-VL", "CRO-1L",
       "ENG-L2", "CHI-PD", "RUS-PL", "JPN-J1", "GRE-SL")
FLOORS = (None, 0.78, 0.80, 0.82)      # None = the league's current floor


def sweep(code: str, floors) -> list[dict]:
    df = store.load_results(code)
    if df is None or len(df) < 150:
        return []
    df = df.dropna(subset=["hg", "ag"]).sort_values("date")
    cut = df["date"].max() - timedelta(days=730)
    recent = df[df["date"] >= cut].tail(800)
    cfg = config.get(code)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    cfgs = []
    for fl in floors:
        c = copy.copy(cfg)
        if fl is not None:
            c.min_win_prob = fl
        cfgs.append(c)

    out = [dict(code=code, floor=fl, rows=[]) for fl in floors]
    for r in recent.itertuples():
        try:
            req = build_request(code, str(r.home), str(r.away), r.date.date())
        except Exception:
            continue
        if req is None or not req.mu_total:
            continue
        for slot, c in zip(out, cfgs):
            try:
                mk = predict_fixture(req, c,
                                     module_flags=flags).translated_play.market
            except Exception:
                continue
            if not mk:
                continue
            res = evaluate_market(mk, int(r.hg), int(r.ag))
            if res is None:
                continue
            w = hit_weight(res)
            if w < 0:
                continue
            slot["rows"].append((r.date, w >= 1.0))
    return out


def main() -> None:
    args = sys.argv[1:]
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else list(LOW))
    floors = (tuple(None if x == "cfg" else float(x) for x in
                    args[args.index("--floors") + 1].split(","))
              if "--floors" in args else FLOORS)

    print(f"{'league':8}{'floor':>7}{'n':>6}{'keep':>6}{'hit':>7}"
          f"{'older':>7}{'newer':>7}   verdict")
    for code in codes:
        got = sweep(code, floors)
        if not got:
            print(f"{code:8}  too little data")
            continue
        base = got[0]["rows"]
        if not base:
            continue
        mid = sorted(d for d, _ in base)[len(base) // 2]
        bh = sum(1 for _d, h in base if h) / len(base) * 100
        bo = [h for d, h in base if d < mid]
        bn = [h for d, h in base if d >= mid]
        bho = sum(bo) / len(bo) * 100 if bo else 0
        bhn = sum(bn) / len(bn) * 100 if bn else 0
        for slot in got:
            rows = slot["rows"]
            if not rows:
                continue
            hit = sum(1 for _d, h in rows if h) / len(rows) * 100
            o = [h for d, h in rows if d < mid]
            n_ = [h for d, h in rows if d >= mid]
            ho = sum(o) / len(o) * 100 if o else 0
            hn = sum(n_) / len(n_) * 100 if n_ else 0
            keep = len(rows) / len(base) * 100
            if slot["floor"] is None:
                verdict = "baseline"
            else:
                both = ho > bho and hn > bhn
                verdict = ("SHIPS" if both and hit - bh >= 1.0 and keep >= 60
                           else "both halves up" if both
                           else "one window short" if (ho > bho) != (hn > bhn)
                           else "no lift")
            fl = "cfg" if slot["floor"] is None else f"{slot['floor']:.2f}"
            print(f"{code:8}{fl:>7}{len(rows):>6}{keep:>5.0f}%{hit:>7.1f}"
                  f"{ho:>7.1f}{hn:>7.1f}   {verdict}", flush=True)
        print()


if __name__ == "__main__":
    main()
