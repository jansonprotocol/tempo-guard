"""
Is the team lane's probability correctly SLOPED, or only correctly centred?

`team_floor_sweep.py` raised the bar a lane must clear and watched the surviving
lanes over-deliver by more and more: +1.6 points at a small bump, +7.6 at a
larger one. At no bump at all the average gap is ~0. A level that is right while
every filtered subset runs high is the signature of a slope error — confident
lanes under-stated, unconfident ones over-stated, the two cancelling in the mean.

That cancelling is why it has gone unnoticed: every aggregate check this project
runs on the team lane reports it as well calibrated, and it is, on average.

Measured directly here rather than inferred: bucket every offered lane by the
probability it was given, and compare that against what it returned. A correctly
sloped model sits on the diagonal in every bucket. A flat one runs low at the
top and high at the bottom.

    says     mean probability the engine attached, within the bucket
    hit      what those lanes returned
    gap      hit - says. The pattern ACROSS buckets is the finding, not any
             single row.

Usage:  python scripts/team_calibration.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import team_total
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from scripts.team_shrink_sweep import LEAGUES, wilson

BUCKETS = [(0.50, 0.60), (0.60, 0.68), (0.68, 0.75), (0.75, 0.82), (0.82, 1.01)]


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
        market, p, _e = cands[0]
        out.append((p, team_total.won(market, int(r["hg"]), int(r["ag"])), market))
    return out


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)
    rows = []
    for back in (0, n):
        for lg in codes:
            try:
                rows += collect(lg, n, back)
            except Exception as exc:
                print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    if not rows:
        return
    print(f"{len(rows)} team lanes, both windows pooled\n")
    print(f"{'stated p':14}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'95% CI':>13}")
    for lo, hi in BUCKETS:
        b = [r for r in rows if lo <= r[0] < hi]
        if len(b) < 40:
            continue
        k = sum(1 for r in b if r[1])
        hit, says = k / len(b), sum(r[0] for r in b) / len(b)
        w = wilson(k, len(b))
        print(f"{lo*100:.0f}-{hi*100:.0f}%{'':<8}{len(b):7}{says*100:7.1f}%"
              f"{hit*100:7.1f}%{(hit-says)*100:+8.1f}"
              f"   [{w[0]*100:.0f}-{w[1]*100:.0f}]")
    k = sum(1 for r in rows if r[1])
    print(f"\n{'ALL':14}{len(rows):7}"
          f"{sum(r[0] for r in rows)/len(rows)*100:7.1f}%{k/len(rows)*100:7.1f}%"
          f"{(k/len(rows) - sum(r[0] for r in rows)/len(rows))*100:+8.1f}")
    print("\nA slope error shows as gap RISING across the buckets while ALL sits "
          "near zero.")

    # By RUNG, because the floor sweep filters against each rung's own floor
    # (U1.5 0.75, O1.5 0.55, O0.5 0.80) rather than an absolute probability.
    # Raising the bump therefore changes which rungs survive, so an apparent
    # confidence effect can really be one rung being mispriced.
    print(f"\n{'rung':14}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'95% CI':>13}")
    rungs = sorted({r[2].split()[1] for r in rows})
    for rung in rungs:
        b = [r for r in rows if r[2].split()[1] == rung]
        if len(b) < 40:
            continue
        k = sum(1 for r in b if r[1])
        hit, says = k / len(b), sum(r[0] for r in b) / len(b)
        w = wilson(k, len(b))
        print(f"{rung:14}{len(b):7}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+8.1f}   [{w[0]*100:.0f}-{w[1]*100:.0f}]")
    print(f"\n{'side':14}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}")
    for side in ("TA", "TB"):
        b = [r for r in rows if r[2].split()[0] == side]
        if len(b) < 40:
            continue
        k = sum(1 for r in b if r[1])
        hit, says = k / len(b), sum(r[0] for r in b) / len(b)
        print(f"{side + ' (home)' if side == 'TA' else side + ' (away)':14}"
              f"{len(b):7}{says*100:7.1f}%{hit*100:7.1f}%{(hit-says)*100:+8.1f}")


if __name__ == "__main__":
    main()
