"""
Does `p_home_tt05` actually predict that the home team scores?

The lane-level calibration test cannot answer this. It scores only fixtures
that produced an offer, and the offer depends on the very probability being
judged — raise `p_home_tt05` and more marginal home lanes clear their floors, so
the population under test changes with the thing under test. That is exactly
what happened when the per-side shrink target was corrected: the home gap looked
unchanged while home lanes went 1,004 to 1,248 and mean stated probability fell.

This drops the lane entirely. Every fixture the engine can price contributes two
observations — `p_home_tt05` against whether the home team scored, and
`p_away_tt05` against whether the away team did. No floors, no rung ranking, no
selection. If the feature is honest, both sides sit on the diagonal.

    says   mean probability the feature gave
    hit    how often that side actually scored
    gap    hit - says, per side and per probability band

`--old` restores the pre-fix behaviour — both sides shrunk toward `league_mu/2`
— by forcing the home share to 0.5. Run with and without it to A/B the per-side
shrink target on identical fixtures, which is the only way to tell whether that
change helped, since the lane-level test moves its own population.

Usage:  python scripts/tt05_calibration.py [--n 150] [--leagues A,B] [--old]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import features, store
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES, wilson

BANDS = [(0.0, 0.65), (0.65, 0.75), (0.75, 0.82), (0.82, 0.88), (0.88, 1.01)]


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
        except Exception:
            continue
        if req is None or req.p_home_tt05 is None or req.p_away_tt05 is None:
            continue
        out.append(("home", req.p_home_tt05, int(r["hg"]) >= 1))
        out.append(("away", req.p_away_tt05, int(r["ag"]) >= 1))
    return out


def show(label: str, rows: list) -> None:
    if not rows:
        return
    k = sum(1 for r in rows if r[2])
    hit, says = k / len(rows), sum(r[1] for r in rows) / len(rows)
    w = wilson(k, len(rows))
    print(f"{label:14}{len(rows):7}{says*100:7.1f}%{hit*100:7.1f}%"
          f"{(hit-says)*100:+8.1f}   [{w[0]*100:.0f}-{w[1]*100:.0f}]")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)
    if "--old" in args:
        features._home_share = lambda df, code, cutoff: 0.5
        print("OLD BEHAVIOUR: both sides shrunk toward league_mu / 2\n")
    rows = []
    for back in (0, n):
        for lg in codes:
            try:
                rows += collect(lg, n, back)
            except Exception as exc:
                print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    if not rows:
        return
    print(f"{len(rows)//2} fixtures, {len(rows)} side-observations, no selection\n")
    print(f"{'':14}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'95% CI':>13}")
    show("HOME", [r for r in rows if r[0] == "home"])
    show("AWAY", [r for r in rows if r[0] == "away"])
    show("BOTH", rows)
    for side in ("home", "away"):
        print(f"\n{side.upper()} by stated probability")
        for lo, hi in BANDS:
            b = [r for r in rows if r[0] == side and lo <= r[1] < hi]
            if len(b) >= 60:
                show(f"  {lo*100:.0f}-{hi*100:.0f}%", b)


if __name__ == "__main__":
    main()
