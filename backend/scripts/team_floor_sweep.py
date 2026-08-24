"""
Raise the team-lane floors instead of the shrink, and see if hit rate follows.

`team_shrink_sweep.py` found that lowering TEAM_SHRINK raises both hit rate and
real edge — 75.6% and +12.0 at k=0.62 against 78.6% and +13.2 at 0.54. Tempting,
and the wrong lever. Lowering k works by shrinking probabilities until they fall
under the rung floors, so fewer lanes qualify; the filtering is a side effect of
breaking the calibration. At 0.54 the lane is +2.5 points UNDER-confident, so
`buy from` demands about 1.34 where the true break-even is 1.27, and most of the
promised volume never gets bought.

One constant is doing two jobs. This separates them: hold k at its calibrated
0.62 and raise the floor a lane must clear before it is offered at all. If the
hit rate climbs the same way, the selectivity was the whole story and it can be
had without touching a probability that is currently honest.

    bump      added to every rung's floor in team_total.RUNGS
    n         lanes surviving
    says/hit  what the lane claimed and returned — with k fixed, `says` should
              stay honest at every bump, which is the entire point
    EDGE      hit minus the base rate of the rungs actually chosen

Usage:  python scripts/team_floor_sweep.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import team_total
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from scripts.team_shrink_sweep import LEAGUES

BUMPS = [0.00, 0.03, 0.06, 0.09, 0.12, 0.15]


def collect(lg: str, n: int, back: int) -> list:
    df = store.load_results(lg)
    if df is None or len(df) < 260:
        return []
    ordered = df.sort_values("date")
    if back:
        ordered = ordered.iloc[:-back]
    cfg = config.get(lg)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    out = []
    for _, r in ordered.tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(lg, str(r["home"]), str(r["away"]), d)
            if req is None or req.p_home_tt05 is None or req.p_away_tt05 is None:
                continue
            predict_fixture(req, cfg, module_flags=flags)
            cands = team_total.candidates(lg, d, req.p_home_tt05, req.p_away_tt05)
        except Exception:
            continue
        if not cands:
            continue
        out.append((cands, int(r["hg"]), int(r["ag"])))
    return out


def report(label: str, rows: list) -> None:
    print(f"\n=== {label} — {len(rows)} fixtures with a lane at bump 0 ===")
    print(f"{'bump':>6}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'base':>8}"
          f"{'EDGE':>8}{'needs':>8}")
    for bump in BUMPS:
        n = hits = 0
        p_sum = base_sum = 0.0
        for cands, hg, ag in rows:
            # Re-apply the ranking after the floor bump: the best surviving
            # candidate is not always the one that led before it.
            keep = [c for c in cands
                    if c[1] >= team_total.RUNGS[c[0].split()[1]][2] + bump]
            if not keep:
                continue
            market, p, e = keep[0]
            n += 1
            p_sum += p
            base_sum += p - e
            hits += team_total.won(market, hg, ag)
        if not n:
            continue
        says, hit, base = p_sum / n, hits / n, base_sum / n
        print(f"{bump:6.2f}{n:7}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+8.1f}{base*100:7.1f}%{(hit-base)*100:+8.1f}"
              f"{1/hit:8.3f}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)
    for label, back in (("RECENT", 0), ("HELD BACK", n)):
        rows = []
        for lg in codes:
            try:
                rows += collect(lg, n, back)
            except Exception as exc:
                print(f"{lg:9} FAILED {exc}", file=sys.stderr)
        if rows:
            report(label, rows)


if __name__ == "__main__":
    main()
