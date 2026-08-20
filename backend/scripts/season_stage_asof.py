"""
Re-measure the season-stage effect using only what is knowable on match day.

The first measurement found +0.363 goals in final rounds across 295
league-seasons. It located those rounds by looking up when each season actually
ended — which is fine for asking whether the effect exists, and useless as a
feature. On the morning of a match nobody knows the season's last date by
reading it out of the future, and building the feature that way would
reintroduce exactly the lookahead just removed from the league aggregates.

What IS knowable on match day:

    matches this team has already played this season   (count of prior results)
    the league's typical matches per team              (from COMPLETED seasons
                                                        before this one)

Their ratio places a fixture in its season without reference to anything after
it. Counting per team rather than per league-date also handles games in hand,
which a shared cutoff would blur.

This proxy is noisier than the true one — season length varies, some leagues
change size, a few run split formats — so the effect measured through it can
legitimately be smaller than +0.363. That difference is the point of running
this: the feature can only ever carry the signal the as-of definition can see,
so that is the number worth knowing, not the retrospective one.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

from app.data import store

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "FRA-L2",
    "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL", "SCO-PL",
    "SUI-SL", "DEN-SL", "SWE-AL", "NOR-EL", "POL-EK", "CZE-FL",
    "FIN-VL", "IRL-PD", "RUS-PL", "ENG-CH", "ENG-L1", "ENG-L2",
    "BRA-SA", "ARG-PD", "COL-PA", "MEX-LMX", "MLS", "JPN-J1",
]
SINCE = "2010-01-01"

# Progress bands. The final stretch is where the retrospective study found the
# effect, so the bands are cut to make that comparison directly.
BANDS = [
    ("early    (0-40%)",   0.00, 0.40),
    ("middle  (40-80%)",   0.40, 0.80),
    ("late    (80-92%)",   0.80, 0.92),
    ("closing (92-97%)",   0.92, 0.97),
    ("final    (97%+)",    0.97, 9.99),
]


def league_frame(code: str) -> pd.DataFrame | None:
    df = store.load_results(code)
    if df.empty or "season" not in df.columns:
        return None
    df = df[(df["date"] >= SINCE) & df["season"].notna()].copy()
    if len(df) < 500:
        return None
    return df.sort_values("date").reset_index(drop=True)


def expected_length(df: pd.DataFrame) -> dict[str, float]:
    """
    Typical matches per team, derived only from seasons that finished BEFORE
    each season began. The first season has no prior to learn from and is
    dropped rather than guessed at.
    """
    per_season = {}
    for s, g in df.groupby("season"):
        teams = pd.concat([g["home"], g["away"]]).nunique()
        if teams:
            per_season[s] = (2 * len(g) / teams, g["date"].min())

    out = {}
    for s, (_, start) in per_season.items():
        prior = [v for k, (v, st) in per_season.items() if st < start]
        if prior:
            out[s] = float(np.median(prior))
    return out


def main() -> None:
    rows = []
    for code in LEAGUES:
        df = league_frame(code)
        if df is None:
            continue
        exp = expected_length(df)
        if not exp:
            continue

        played: dict[tuple, int] = {}
        for r in df.itertuples(index=False):
            s = r.season
            n = exp.get(s)
            if not n:
                continue
            # Count BEFORE this match, so the fixture itself is never included.
            ph = played.get((s, r.home), 0)
            pa = played.get((s, r.away), 0)
            progress = ((ph + pa) / 2.0) / n
            played[(s, r.home)] = ph + 1
            played[(s, r.away)] = pa + 1

            if pd.isna(r.hg) or pd.isna(r.ag):
                continue
            rows.append({
                "league": code, "season": s,
                "progress": progress, "total": float(r.hg) + float(r.ag),
            })

    if not rows:
        print("no data")
        return
    r = pd.DataFrame(rows)
    print(f"{r['league'].nunique()} leagues, {len(r)} matches\n")

    # Compare each band against its own season's average, so decade-scale drift
    # in scoring cannot be read as a stage effect.
    r["season_mean"] = r.groupby(["league", "season"])["total"].transform("mean")
    r["delta"] = r["total"] - r["season_mean"]

    print(f"  {'band':20s} {'matches':>8} {'goals':>8} {'delta':>8} {'95% CI':>18}")
    print("  " + "-" * 68)
    for label, lo, hi in BANDS:
        sub = r[(r["progress"] >= lo) & (r["progress"] < hi)]
        if len(sub) < 50:
            print(f"  {label:20s} {len(sub):8d}   (too few)")
            continue
        d = sub["delta"].mean()
        se = sub["delta"].std() / np.sqrt(len(sub))
        print(f"  {label:20s} {len(sub):8d} {sub['total'].mean():8.3f} "
              f"{d:+8.3f} {d - 1.96 * se:+8.3f}..{d + 1.96 * se:+7.3f}")

    fin = r[r["progress"] >= 0.97]
    if len(fin) >= 50:
        d = fin["delta"].mean()
        se = fin["delta"].std() / np.sqrt(len(fin))
        print(f"\n  FINAL STRETCH (as-of): {d:+.3f} goals "
              f"[{d - 1.96 * se:+.3f}, {d + 1.96 * se:+.3f}] on {len(fin)} matches")
        print(f"  retrospective study found +0.363 using the true season end date")
        print(f"  -> the as-of proxy carries "
              f"{d / 0.363:.0%} of the effect the perfect-hindsight version saw")

        print("\n  per league (final stretch delta):")
        for code, g in fin.groupby("league"):
            if len(g) < 30:
                continue
            print(f"    {code:8s} {g['delta'].mean():+6.3f}  ({len(g)} matches)")


if __name__ == "__main__":
    main()
