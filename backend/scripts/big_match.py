"""
Do big matches score fewer goals than their leagues — and does the engine know?

Born from a retrosim: the engine tipped `O1.5` at 77% on the November 2010
Manchester derby and it finished 0-0. The suspicion is structural, not one
match: the feature set reads ten games of form per side and a league baseline,
so it cannot see OCCASION — two well-matched sides at the top setting up not
to lose to each other. If such fixtures run systematically tighter, every one
of them is an Over tip issued a little too confidently.

"Derby" is not derivable from results data (the store has names, not cities).
What IS derivable, strictly as-of, is the league table — points accumulated
from results before the fixture — so the flags are stakes-based:

    TOP CLASH      both sides in the as-of top 4, at least 6 rounds played
    TOP-6 CLASH    both in the top 6 (the softer version)
    1 v 2          the literal title fixture
    BOTTOM CLASH   both in the bottom 4 with 60% of the season gone — the
                   relegation six-pointer, the fear-driven mirror case

Each flagged group is compared to ITS OWN league-season's goals per game, so a
high-scoring league cannot masquerade as an effect. Reported as goals/game
delta and as the O1.5 / O2.5 landing rates the ladder actually trades on.

This is stage 1: raw and engine-free, over every stored season, so the sample
is enormous and nothing is selected. If the effect is real, stage 2 asks the
harder question — whether the engine's mu already absorbs it through form —
and only a gap THERE earns an adjustment.

Usage:  python scripts/big_match.py
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

DATA = Path(__file__).resolve().parents[2] / "data"
SKIP = {"UCL", "UCL-Q", "UEL", "UEL-Q", "UECL", "UECL-Q", "COPA-L"}

MIN_ROUNDS = 6          # a table before six rounds is noise, not stakes
LATE = 0.60             # the relegation flag needs a season mostly played


def season_flags(df: pd.DataFrame):
    """Yield (flags, total_goals) per fixture, table computed strictly as-of."""
    pts: dict[str, int] = defaultdict(int)
    played: dict[str, int] = defaultdict(int)
    n_rows = len(df)
    teams_guess = df[["home", "away"]].stack().nunique()

    for i, r in enumerate(df.sort_values("date").itertuples()):
        if pd.isna(r.hg) or pd.isna(r.ag):
            continue
        hg, ag = int(r.hg), int(r.ag)
        h, a = str(r.home), str(r.away)

        flags = set()
        if min(played[h], played[a]) >= MIN_ROUNDS:
            table = sorted(pts, key=lambda t: -pts[t])
            pos = {t: k + 1 for k, t in enumerate(table)}
            ph, pa = pos.get(h, 99), pos.get(a, 99)
            if ph <= 4 and pa <= 4:
                flags.add("top4")
            if ph <= 6 and pa <= 6:
                flags.add("top6")
            if {ph, pa} == {1, 2}:
                flags.add("1v2")
            nt = max(teams_guess, len(pts))
            if (i / n_rows) >= LATE and ph > nt - 4 and pa > nt - 4:
                flags.add("bottom4")
        yield flags, hg + ag

        for t, gf, ga in ((h, hg, ag), (a, ag, hg)):
            pts[t] += 3 if gf > ga else 1 if gf == ga else 0
            played[t] += 1


def main() -> None:
    groups = defaultdict(lambda: [0, 0.0, 0, 0])   # n, goal-delta sum, o15, o25
    base_n = 0
    for lg_dir in sorted(DATA.iterdir()):
        if not lg_dir.is_dir() or lg_dir.name in SKIP:
            continue
        for f in sorted(lg_dir.glob("*.parquet")):
            df = pd.read_parquet(f)
            if len(df) < 60 or "hg" not in df:
                continue
            good = df.dropna(subset=["hg", "ag"])
            if len(good) < 60:
                continue
            season_mu = float((good["hg"] + good["ag"]).mean())
            for flags, total in season_flags(good):
                keys = flags if flags else {"control"}
                for k in keys:
                    g = groups[k]
                    g[0] += 1
                    g[1] += total - season_mu
                    g[2] += total >= 2
                    g[3] += total >= 3
                base_n += 1

    print(f"{base_n} fixtures across every stored domestic season\n")
    print(f"{'group':12}{'n':>8}{'goals vs season':>17}{'O1.5 lands':>12}"
          f"{'O2.5 lands':>12}")
    order = ["control", "top6", "top4", "1v2", "bottom4"]
    ctrl = groups["control"]
    for k in order:
        g = groups[k]
        if g[0] < 50:
            continue
        se = (2.9 / g[0] ** 0.5)      # sd of a match total is ~1.7^2 -> ~2.9var
        print(f"{k:12}{g[0]:8}{g[1]/g[0]:+13.3f} ±{se:.3f}"
              f"{g[2]/g[0]*100:11.1f}%{g[3]/g[0]*100:11.1f}%")
    print("\ncontrol O-rates are the baseline the flags must beat DOWNWARD "
          "for the tighter-big-match claim to hold.")


if __name__ == "__main__":
    main()
