"""
Is the U1.5 / O1.5 split caused by SELECTION rather than by the probabilities?

Lane-level calibration gives `U1.5` -5.3 and `O1.5` +5.8, both at z = 3.5, on a
feature measured clean at +0.8 / +0.3. The two rungs are complements for one
side, so it is one error seen twice. Side-level over-dispersion was measured and
rejected: 293,114 observations give a rung effect of 0.62 points pointing the
wrong way, against 5.3 observed.

That leaves the selector. `team_total.candidates` returns every eligible rung
ranked by edge and the engine offers the top one — a max over rungs AND sides.
A max is biased toward whichever estimate came in high, and these two rungs sit
on opposite tails of the same distribution: `U1.5` is offered on low-scoring
sides (floor 0.75), `O1.5` on high-scoring ones (floor 0.55). If that is the
mechanism, the bias belongs to being CHOSEN, not to the rung.

The test separates the two:

    TOP   the lane the engine actually offers — what today's split measures
    ALL   every eligible candidate, scored whether or not it was chosen

Selection predicts the split appears in TOP and vanishes in ALL. A genuine
mispricing of the rungs predicts it survives in both, since ALL contains the
same rungs without the max applied.

Usage:  python scripts/lane_selection.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.engine import team_total
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES, wilson


def collect(lg: str, n: int, back: int) -> list:
    df = store.load_results(lg)
    if df is None or len(df) < 260:
        return []
    ordered = df.sort_values("date")
    if back:
        ordered = ordered.iloc[:-back]
    out = []
    for _, r in ordered.tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(lg, str(r["home"]), str(r["away"]), d)
            if req is None or req.p_home_tt05 is None:
                continue
            cands = team_total.candidates(lg, d, req.p_home_tt05, req.p_away_tt05)
        except Exception:
            continue
        if not cands:
            continue
        hg, ag = int(r["hg"]), int(r["ag"])
        for i, (m, p, _e) in enumerate(cands):
            out.append((m.split()[1], p, team_total.won(m, hg, ag), i == 0))
    return out


def show(label: str, rows: list) -> None:
    print(f"\n{label}")
    print(f"{'rung':>8}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'95% CI':>13}")
    for rung in ("O0.5", "O1.5", "U1.5"):
        b = [r for r in rows if r[0] == rung]
        if len(b) < 40:
            continue
        k = sum(1 for r in b if r[2])
        hit, says = k / len(b), sum(r[1] for r in b) / len(b)
        w = wilson(k, len(b))
        print(f"{rung:>8}{len(b):7}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+8.1f}   [{w[0]*100:.0f}-{w[1]*100:.0f}]")


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
    print(f"{len(rows)} eligible candidates, "
          f"{sum(1 for r in rows if r[3])} of them selected")
    show("TOP — the lane actually offered", [r for r in rows if r[3]])
    show("ALL — every eligible candidate", rows)
    print("\nSplit in TOP but not ALL means the selector, not the rung.")


if __name__ == "__main__":
    main()
