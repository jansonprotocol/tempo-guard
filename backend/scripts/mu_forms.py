"""
Is the engine's goal expectation the best one available from the same data?

`mu` today is additive and symmetric: each side's rolling scoring rate, summed.
That form cannot express one team's attack meeting the other's defence, so a
side conceding two a game reads as a low-scoring fixture whenever its own attack
is poor. Vila Nova v Ponte Preta was called U3.0 at 89% and finished 6-0 for
exactly that reason.

The standard alternative is multiplicative, the form every Poisson goal model
uses:

    expected home goals = league_avg x home_attack_strength x away_defence_weakness
    expected away goals = league_avg x away_attack_strength x home_defence_weakness

where strength and weakness are each team's rate divided by the league's. It
uses precisely the same inputs — goals scored, goals conceded, rolling window —
and costs nothing extra to compute. The only difference is that it multiplies
where the current form adds.

Scored here against the actual total, per league, so the comparison is like for
like on identical fixtures and windows. Correlation is the right measure at this
stage: it asks whether the quantity carries more information, before any
question of how a market gets picked from it.
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
    "ENG-CH", "ITA-SB", "BRA-SA", "BRA-SB", "ARG-PD", "COL-PA",
    "MEX-LMX", "MLS", "JPN-J1", "SUI-SL", "DEN-SL", "AUT-BL",
]
SINCE = "2021-01-01"
WINDOW = 10
MIN_PRIOR = 8


def rates(rows: pd.DataFrame, team: str) -> tuple[float, float]:
    at_home = (rows["home"] == team).values
    hg = rows["hg"].fillna(0).values
    ag = rows["ag"].fillna(0).values
    return (float(np.where(at_home, hg, ag).mean()),
            float(np.where(at_home, ag, hg).mean()))


def main() -> None:
    print(f"  {'league':9s} {'n':>5}  {'additive':>9} {'multiplicative':>15} {'gain':>8}")
    print("  " + "-" * 54)
    wins = 0
    total = 0
    for lg in LEAGUES:
        df = store.load_results(lg)
        if df.empty:
            continue
        df = df[df["date"] >= SINCE].sort_values("date").reset_index(drop=True)
        if len(df) < 400:
            continue
        df["tot"] = df["hg"].fillna(0) + df["ag"].fillna(0)

        recs = []
        for i, row in df.iterrows():
            past = df.iloc[:i]
            if len(past) < 60:
                continue
            h = past[(past["home"] == row["home"]) | (past["away"] == row["home"])].tail(WINDOW)
            a = past[(past["home"] == row["away"]) | (past["away"] == row["away"])].tail(WINDOW)
            if len(h) < MIN_PRIOR or len(a) < MIN_PRIOR:
                continue
            lg_avg = float(past.tail(400)["tot"].mean()) / 2
            if lg_avg <= 0:
                continue
            hs, hc = rates(h, row["home"])
            as_, ac = rates(a, row["away"])

            additive = hs + as_
            # Attack strength and defensive weakness, each relative to the league.
            mult = lg_avg * ((hs / lg_avg) * (ac / lg_avg)
                             + (as_ / lg_avg) * (hc / lg_avg))
            recs.append((additive, mult, row["tot"]))

        if len(recs) < 300:
            continue
        r = pd.DataFrame(recs, columns=["add", "mult", "tot"])
        ca = r["add"].corr(r["tot"])
        cm = r["mult"].corr(r["tot"])
        total += 1
        wins += cm > ca
        flag = "  <-" if cm > ca + 0.01 else ""
        print(f"  {lg:9s} {len(r):5d}  {ca:+9.3f} {cm:+15.3f} {cm - ca:+8.3f}{flag}",
              flush=True)

    print(f"\n  multiplicative better in {wins}/{total} leagues")


if __name__ == "__main__":
    main()
