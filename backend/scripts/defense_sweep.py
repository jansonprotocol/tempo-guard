"""
Does opponent defense earn its way into the team lane? DEFENSE_BLEND, swept.

Stage 1 (scratch, 206,676 fixture-sides) showed the raw signal: holding own
attack fixed, the opponent's conceded rate still separates P(side scores) by
7-11 points. The engine's side rate is attack-only, so that is the largest
untapped strand in the store — bigger in raw terms than the venue split was.

Raw separation is not engine-incremental value, so this sweeps the constant
through the ENGINE's own path: every priceable fixture in the sweep leagues,
two windows (recent and held-back), each side scored against what actually
happened. Two verdict layers, same as the floor's validation:

    shape    P(side >= 1) and P(side >= 2) gap, says minus hit, per window
    lanes    what the offered team rungs (O0.5 / O1.5 / U1.5) claim vs return

A weight that helps the recent window and not the held-back one is a fit and
dies here — the bar TEAM_SHRINK, TEAM_RATE_FLOOR and BIG_MATCH_DEBIT cleared.

Usage:  python scripts/defense_sweep.py [--weights 0,0.3,0.5,0.7,1.0]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import features, store
from app.engine import team_total
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES

N = 260


def collect(weight: float):
    """(window, p_home, p_away, hg, ag, league, date) at this blend weight."""
    features.DEFENSE_BLEND = weight
    out = []
    for wname, back in (("recent", 0), ("held-back", N)):
        for lg in LEAGUES:
            df = store.load_results(lg)
            if df is None or len(df) < 260 + N:
                continue
            o = df.dropna(subset=["hg", "ag"]).sort_values("date")
            if back:
                o = o.iloc[:-back]
            for _, r in o.tail(N).iterrows():
                d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
                try:
                    req = build_request(lg, str(r["home"]), str(r["away"]), d)
                except Exception:
                    continue
                if (req is None or req.p_home_tt05 is None
                        or req.p_away_tt05 is None):
                    continue
                out.append((wname, float(req.p_home_tt05),
                            float(req.p_away_tt05),
                            int(r["hg"]), int(r["ag"]), lg, d))
    return out


def main() -> None:
    args = sys.argv[1:]
    weights = ([float(x) for x in args[args.index("--weights") + 1].split(",")]
               if "--weights" in args else [0.0, 0.3, 0.5, 0.7, 1.0])

    for w in weights:
        rows = collect(w)
        print(f"\nDEFENSE_BLEND = {w}   ({len(rows)} fixtures)")
        print(f"{'window':12}{'n':>7}{'says>=1':>9}{'hit>=1':>9}{'gap':>7}"
              f"{'says>=2*':>10}{'hit>=2':>9}")
        for wname in ("recent", "held-back"):
            b = [r for r in rows if r[0] == wname]
            says = hit = says2 = hit2 = 0.0
            n = 0
            for _w, ph, pa, hg, ag, _lg, _d in b:
                for p, g in ((ph, hg), (pa, ag)):
                    gf = -math.log(max(1e-9, 1 - min(p, 1 - 1e-9)))
                    says += p
                    hit += g >= 1
                    says2 += 1 - math.exp(-gf) * (1 + gf)
                    hit2 += g >= 2
                    n += 1
            print(f"{wname:12}{n:7}{says/n*100:8.1f}%{hit/n*100:8.1f}%"
                  f"{(hit-says)/n*100:+7.1f}{says2/n*100:9.1f}%"
                  f"{hit2/n*100:8.1f}%")

        # Lane consequence: what the OFFERED rungs claim vs return.
        print(f"{'rung':6}{'window':12}{'n':>6}{'says':>8}{'hit':>8}{'gap':>8}")
        for wname in ("recent", "held-back"):
            got = {}
            for _w, ph, pa, hg, ag, lg, d in [r for r in rows if r[0] == wname]:
                try:
                    c = team_total.candidates(lg, d, ph, pa)
                except Exception:
                    continue
                if not c:
                    continue
                m, p, _e = c[0]
                got.setdefault(m.split()[1], []).append(
                    (p, team_total.won(m, hg, ag)))
            for rung in ("O0.5", "O1.5", "U1.5"):
                b = got.get(rung, [])
                if len(b) < 40:
                    continue
                hitr = sum(1 for _p, x in b if x) / len(b)
                saysr = sum(p for p, _x in b) / len(b)
                print(f"{rung:6}{wname:12}{len(b):6}{saysr*100:7.1f}%"
                      f"{hitr*100:7.1f}%{(hitr-saysr)*100:+8.1f}")


if __name__ == "__main__":
    main()
