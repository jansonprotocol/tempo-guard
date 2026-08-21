"""
Do the two goal-expectation forms complement each other, or is the pattern noise?

The additive and multiplicative forms each win in half the leagues, and the gain
from switching correlates -0.655 with how well additive already does. Two
explanations fit that equally well and imply opposite actions:

  complementarity   the forms carry different information, so where one is weak
                    the other picks up. A blend should then beat BOTH, and the
                    best mixing weight should be stable when re-fitted on a
                    different stretch of matches.

  mean reversion    per-league correlations are noisy. Leagues where additive
                    scores high are partly lucky, so any alternative looks worse
                    there and better where additive scored low. The -0.655
                    appears with no shared information at all, a blend lands
                    between the two rather than above them, and the best weight
                    is unstable across halves.

Both tests are run here. Each league's matches are split chronologically; the
blend weight is swept on the first half and the winner re-scored on the second.
Fitting and testing on the same matches would confirm complementarity even when
none exists, which is the error this codebase has already made once today with
the probability floor.
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
    "ENG-CH", "ITA-SB", "BRA-SA", "BRA-SB", "ARG-PD", "MEX-LMX",
    "MLS", "JPN-J1",
]
SINCE = "2021-01-01"
WINDOW = 10
MIN_PRIOR = 8
WEIGHTS = [0.0, 0.25, 0.5, 0.75, 1.0]      # 0 = pure additive, 1 = pure multiplicative


def rates(rows: pd.DataFrame, team: str) -> tuple[float, float]:
    at_home = (rows["home"] == team).values
    hg = rows["hg"].fillna(0).values
    ag = rows["ag"].fillna(0).values
    return (float(np.where(at_home, hg, ag).mean()),
            float(np.where(at_home, ag, hg).mean()))


def build(lg: str) -> pd.DataFrame | None:
    df = store.load_results(lg)
    if df.empty:
        return None
    df = df[df["date"] >= SINCE].sort_values("date").reset_index(drop=True)
    if len(df) < 500:
        return None
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
        recs.append((hs + as_,
                     lg_avg * ((hs / lg_avg) * (ac / lg_avg)
                               + (as_ / lg_avg) * (hc / lg_avg)),
                     row["tot"]))
    if len(recs) < 400:
        return None
    return pd.DataFrame(recs, columns=["add", "mult", "tot"])


def corr_at(r: pd.DataFrame, w: float) -> float:
    blend = (1 - w) * r["add"] + w * r["mult"]
    if blend.std() == 0:
        return 0.0
    return float(blend.corr(r["tot"]))


def main() -> None:
    print(f"  {'league':9s} {'best w':>7} {'train':>7} | "
          f"{'add':>7} {'mult':>7} {'blend':>7}  {'beats both?':>12}")
    print("  " + "-" * 72)
    beats = 0
    total = 0
    weights_first, weights_second = [], []

    for lg in LEAGUES:
        r = build(lg)
        if r is None:
            continue
        cut = len(r) // 2
        first, second = r.iloc[:cut], r.iloc[cut:]

        # Fit the weight on the first half only.
        best_w = max(WEIGHTS, key=lambda w: corr_at(first, w))
        train = corr_at(first, best_w)

        # Score everything on the held-out second half.
        c_add = corr_at(second, 0.0)
        c_mul = corr_at(second, 1.0)
        c_bld = corr_at(second, best_w)
        won = c_bld > max(c_add, c_mul) + 1e-9
        beats += won
        total += 1

        weights_first.append(best_w)
        weights_second.append(max(WEIGHTS, key=lambda w: corr_at(second, w)))

        print(f"  {lg:9s} {best_w:7.2f} {train:+7.3f} | {c_add:+7.3f} {c_mul:+7.3f} "
              f"{c_bld:+7.3f}  {'YES' if won else 'no':>12}", flush=True)

    print(f"\n  blend beats both pure forms on unseen matches: {beats}/{total}")
    wf = np.array(weights_first, dtype=float)
    ws = np.array(weights_second, dtype=float)
    agree = int((wf == ws).sum())
    print(f"  best weight identical in both halves: {agree}/{total}")
    if len(wf) > 2 and wf.std() and ws.std():
        print(f"  corr(best weight first half, second half) = "
              f"{np.corrcoef(wf, ws)[0, 1]:+.3f}")
    print()
    if beats > total * 0.6:
        print("  -> complementary: a per-league blend carries real information")
    elif beats < total * 0.4:
        print("  -> not complementary. The -0.655 pattern was mean reversion, and")
        print("     a blend adds nothing over picking one form.")
    else:
        print("  -> inconclusive at this sample; the blend is no better than a")
        print("     coin flip between the two forms.")


if __name__ == "__main__":
    main()
