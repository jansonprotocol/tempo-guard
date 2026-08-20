"""
Does the closing stage of a season score differently?

Raised as an explanation for a cluster of Over misses on the final Ligue 1
matchday, then stated with more confidence than it had earned. "Dead rubbers
are low-scoring" is folk wisdom, and the opposite story — nothing to play for,
open defending, more goals — is told just as often. Neither belongs in the
engine on the strength of a plausible story.

The engine has no concept of what a match is *for*: no title race, no
relegation, no stakes. If the closing rounds genuinely score differently, that
is a feature it is missing. If they do not, the Ligue 1 misses were ordinary
variance and the idea should be dropped.

Method: bucket every match by how far it sits from the end of its season, then
compare mean total goals in each bucket against the same season's own average.
Comparing within a season matters — leagues drift over decades, and pooling
across years would let that drift masquerade as a stage effect.

Season boundaries are inferred from gaps in the fixture calendar rather than
assumed, since the registry mixes autumn-spring leagues with calendar-year ones.

A real effect should be consistent in sign across leagues. One league with a
big number is noise; twenty leagues leaning the same way is a feature.
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

# A break longer than this ends a season.
GAP_DAYS = 45


def seasons(df: pd.DataFrame) -> pd.Series:
    d = df["date"].sort_values()
    gap = d.diff().dt.days.fillna(0) > GAP_DAYS
    return gap.cumsum().reindex(df.index)


def main() -> None:
    rows = []
    for code in LEAGUES:
        df = store.load_results(code)
        if df.empty:
            continue
        df = df[df["date"] >= SINCE].copy()
        if len(df) < 500:
            continue
        df = df.sort_values("date").reset_index(drop=True)
        df["season"] = seasons(df)
        df["total"] = df["hg"].astype(float) + df["ag"].astype(float)

        for s, g in df.groupby("season"):
            if len(g) < 100:
                continue
            end = g["date"].max()
            g = g.assign(to_end=(end - g["date"]).dt.days)
            avg = g["total"].mean()
            for label, sel in (
                ("final round", g["to_end"] <= 3),
                ("last 2 weeks", (g["to_end"] > 3) & (g["to_end"] <= 17)),
                ("mid season", g["to_end"] > 17),
            ):
                sub = g[sel]
                if len(sub) >= 5:
                    rows.append({
                        "league": code, "season": s, "stage": label,
                        "n": len(sub), "mean": sub["total"].mean(),
                        "delta": sub["total"].mean() - avg,
                    })

    if not rows:
        print("no data")
        return
    r = pd.DataFrame(rows)

    print("GOALS BY SEASON STAGE (delta vs that season's own average)\n")
    print(f"  {'stage':14s} {'matches':>8} {'mean goals':>11} {'delta':>8}")
    print("  " + "-" * 44)
    for stage in ("mid season", "last 2 weeks", "final round"):
        sub = r[r["stage"] == stage]
        n = int(sub["n"].sum())
        mean = float(np.average(sub["mean"], weights=sub["n"]))
        delta = float(np.average(sub["delta"], weights=sub["n"]))
        print(f"  {stage:14s} {n:8d} {mean:11.3f} {delta:+8.3f}")

    fin = r[r["stage"] == "final round"]
    n_seasons = len(fin)
    up = int((fin["delta"] > 0).sum())
    mean_d = float(np.average(fin["delta"], weights=fin["n"]))
    sd = float(fin["delta"].std())
    se = sd / np.sqrt(n_seasons)

    print(f"\nFINAL ROUND, across {n_seasons} league-seasons")
    print(f"  mean delta      {mean_d:+.3f} goals")
    print(f"  95% interval    {mean_d - 1.96 * se:+.3f} .. {mean_d + 1.96 * se:+.3f}")
    print(f"  seasons higher  {up}/{n_seasons}  ({up / n_seasons:.0%})")

    print("\n  per league (final round delta, seasons):")
    for code, g in fin.groupby("league"):
        d = float(np.average(g["delta"], weights=g["n"]))
        print(f"    {code:8s} {d:+6.3f}   ({len(g)} seasons, {int(g['n'].sum())} matches)")

    lo, hi = mean_d - 1.96 * se, mean_d + 1.96 * se
    print()
    if lo > 0.05:
        print("  -> final rounds score HIGHER; worth a feature")
    elif hi < -0.05:
        print("  -> final rounds score LOWER; worth a feature")
    else:
        print("  -> no effect worth modelling; the Ligue 1 misses were variance")


if __name__ == "__main__":
    main()
