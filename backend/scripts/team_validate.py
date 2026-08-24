"""
Validate the team-total lane where it was NOT fitted.

`TEAM_SHRINK = 0.62` was measured on the most recent 200 fixtures of six
leagues — ENG-CH, ESP-L2, TUR-SL, MLS, JPN-J1, CHI-PD. That is exactly the
setup that made the first match-side pass look better than it was, so the same
number has to survive two independent moves away from where it was fitted:

    HELD-OUT WINDOW    same leagues, an earlier stretch that had no influence
                       on the constant
    HELD-OUT LEAGUES   leagues never used in the fit at all

Two things are reported, and they answer different questions.

    residual slope     regress a side's actual goals on its predicted rate.
                       1.0 means the spread is right. This is the quantity
                       TEAM_SHRINK was fitted to, so seeing it hold elsewhere
                       is the direct test.

    rung calibration   for each rung actually offered (`O0.5`, `O1.5`, `U1.5`),
                       the probability claimed against the frequency observed.
                       This is what a bet settles on, and a slope can look fine
                       while a particular rung is still off — the match side
                       had exactly that shape at the boundary.

Usage:  python scripts/team_validate.py [--n 200] [--back 0] [--leagues ...]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.predict import build_request

# Rungs the team lane actually offers, with the side-goal counts that win them.
RUNGS = (
    ("O0.5", lambda gf: 1 - math.exp(-gf), lambda g: g >= 1),
    ("O1.5", lambda gf: 1 - math.exp(-gf) * (1 + gf), lambda g: g >= 2),
    ("U1.5", lambda gf: math.exp(-gf) * (1 + gf), lambda g: g <= 1),
)

FITTED_ON = {"ENG-CH", "ESP-L2", "TUR-SL", "MLS", "JPN-J1", "CHI-PD"}


def slope(xs: list[float], ys: list[float]) -> float:
    n = len(xs)
    mx, my = sum(xs) / n, sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx if sxx else 0.0


def collect(league: str, n: int, back: int) -> list[tuple[float, int]]:
    """(predicted side rate, actual side goals), two rows per fixture."""
    df = store.load_results(league)
    if df is None or len(df) < 200:
        return []
    ordered = df.sort_values("date")
    if back:
        ordered = ordered.iloc[:-back]
    out = []
    for _, r in ordered.tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if req is None:
            continue
        for p, g in ((req.p_home_tt05, int(r["hg"])),
                     (req.p_away_tt05, int(r["ag"]))):
            if p is None or not (0 < p < 1):
                continue
            out.append((-math.log(1 - p), g))
    return out


def report(label: str, rows: list[tuple[float, int]]) -> None:
    if len(rows) < 100:
        print(f"{label}: too few observations ({len(rows)})")
        return
    b = slope([x for x, _ in rows], [float(g) for _, g in rows])
    print(f"\n{label}   n={len(rows)}   residual slope {b:.3f}")
    print(f"  {'rung':6}{'says':>9}{'actual':>9}{'gap':>8}")
    for name, prob, wins in RUNGS:
        says = sum(prob(gf) for gf, _ in rows) / len(rows)
        act = sum(1 for _, g in rows if wins(g)) / len(rows)
        print(f"  {name:6}{says*100:8.1f}%{act*100:8.1f}%{(act-says)*100:+8.1f}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 200
    back = int(args[args.index("--back") + 1]) if "--back" in args else 0

    if "--leagues" in args:
        codes = args[args.index("--leagues") + 1].split(",")
        rows: list[tuple[float, int]] = []
        for lg in codes:
            rows += collect(lg, n, back)
        report(",".join(codes), rows)
        return

    fitted, held = [], []
    for lg in sorted(FITTED_ON):
        fitted += collect(lg, n, back)
    for lg in ("ENG-L1", "ENG-L2", "BEL-PL", "POR-PL", "GER-B2", "SCO-PL",
               "ARG-PD", "NED-ED"):
        held += collect(lg, n, back)
    report(f"FITTED-ON leagues (back={back})", fitted)
    report(f"HELD-OUT leagues (back={back})", held)


if __name__ == "__main__":
    main()
