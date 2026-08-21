"""
Which unused inputs are ORTHOGONAL to team strength — and which are just it again?

Every feature tried so far has failed the same way: possession, shot blends, goal
variance, team tags. The pattern is not bad luck. All of them are proxies for how
good the two sides are at scoring and conceding, which is precisely what `mu`
already reads off goals. Adding a second measurement of a quantity you already
measure adds noise, not information, and the holdout says so every time.

So the question is not "what other data is there" but "what moves total goals
WITHOUT being a restatement of team quality". Three candidates cost nothing
because the data is already stored:

    referee     assigned before kickoff, varies in penalties and cards, and has
                nothing to do with either team's ability.
    rest days   days since each side last played. Computable from the date column
                alone. Congestion is a property of the calendar, not the squad.
    stage       how far into the season the match falls. Already measured once
                (+0.363 goals in closing rounds across 295 league-seasons) and
                never wired into anything.

TEST: SPLIT-HALF, NOT CORRELATION
=================================
Correlating referee goal averages against goals would confirm a signal that does
not exist — a referee who happened to take six high-scoring matches looks like a
high-scoring referee. The honest test is whether the tendency PERSISTS: split
each referee's matches into odd and even, and correlate the two halves. A real
tendency shows up in both. Noise does not, by construction.

The same split is applied to the raw effects so the reported size is out-of-half.
Nothing here changes the engine; it decides what is worth building.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

from app.data import config, store

# Below this a referee's average is his own noise.
MIN_PER_REF = 12
MIN_REFS = 15

# Rest-day buckets. Three days or fewer is a genuine turnaround; eight or more
# is a break.
CONGESTED = 3
RESTED = 8


def league_codes() -> list[str]:
    return list(config.load_all().keys())


def split_half(groups: dict[str, list[float]]) -> tuple[float, int]:
    """Correlation between odd and even halves of each group's values."""
    a, b = [], []
    for _k, vals in groups.items():
        if len(vals) < MIN_PER_REF:
            continue
        odd = vals[0::2]
        even = vals[1::2]
        if len(odd) < 4 or len(even) < 4:
            continue
        a.append(float(np.mean(odd)))
        b.append(float(np.mean(even)))
    if len(a) < MIN_REFS:
        return float("nan"), len(a)
    return float(np.corrcoef(a, b)[0, 1]), len(a)


def referee_probe(df: pd.DataFrame) -> tuple[float, int, float]:
    """Split-half persistence of referee goal tendency, and its spread."""
    if "referee" not in df.columns:
        return float("nan"), 0, float("nan")
    d = df[df["referee"].notna() & (df["referee"].astype(str).str.len() > 2)]
    if d.empty:
        return float("nan"), 0, float("nan")
    tot = (d["hg"].astype(float) + d["ag"].astype(float)).values
    groups: dict[str, list[float]] = {}
    for ref, t in zip(d["referee"].astype(str).values, tot):
        groups.setdefault(ref, []).append(float(t))
    r, n = split_half(groups)
    means = [np.mean(v) for v in groups.values() if len(v) >= MIN_PER_REF]
    spread = float(np.std(means)) if len(means) >= MIN_REFS else float("nan")
    return r, n, spread


def rest_probe(df: pd.DataFrame) -> tuple[float, float, int, int]:
    """Goals when at least one side is congested vs when both are rested."""
    d = df.sort_values("date").reset_index(drop=True)
    last: dict[str, pd.Timestamp] = {}
    rows = []
    for r in d.itertuples(index=False):
        h, a, dt = r.home, r.away, r.date
        hp, ap = last.get(h), last.get(a)
        if hp is not None and ap is not None:
            hr = (dt - hp).days
            ar = (dt - ap).days
            if 0 < hr < 40 and 0 < ar < 40:
                rows.append((min(hr, ar), float(r.hg) + float(r.ag)))
        last[h] = dt
        last[a] = dt
    if len(rows) < 400:
        return float("nan"), float("nan"), 0, 0
    tight = [t for m, t in rows if m <= CONGESTED]
    easy = [t for m, t in rows if m >= RESTED]
    if len(tight) < 60 or len(easy) < 60:
        return float("nan"), float("nan"), len(tight), len(easy)
    return float(np.mean(tight)), float(np.mean(easy)), len(tight), len(easy)


def main() -> None:
    codes = league_codes()
    print(f"{'league':10s} {'n':>6}  {'ref r':>7} {'refs':>5} {'ref sd':>7}   "
          f"{'congested':>10} {'rested':>8} {'delta':>7}")
    print("-" * 76)

    ref_rs, deltas = [], []
    for code in sorted(codes):
        try:
            df = store.load_results(code)
        except Exception:
            continue
        if df.empty or len(df) < 800:
            continue
        df = df[df["hg"].notna() & df["ag"].notna()]

        r, n, sd = referee_probe(df)
        tight, easy, nt, ne = rest_probe(df)

        rs = f"{r:+.3f}" if np.isfinite(r) else "     --"
        sds = f"{sd:.3f}" if np.isfinite(sd) else "     --"
        if np.isfinite(tight) and np.isfinite(easy):
            dl = tight - easy
            deltas.append(dl)
            rest = f"{tight:10.2f} {easy:8.2f} {dl:+7.2f}"
        else:
            rest = f"{'--':>10} {'--':>8} {'--':>7}"
        if np.isfinite(r):
            ref_rs.append(r)
        print(f"{code:10s} {len(df):6d}  {rs:>7} {n:5d} {sds:>7}   {rest}")

    print("-" * 76)
    if ref_rs:
        print(f"referee split-half, {len(ref_rs)} leagues: "
              f"mean {np.mean(ref_rs):+.3f}   median {np.median(ref_rs):+.3f}   "
              f"positive in {sum(1 for x in ref_rs if x > 0)}/{len(ref_rs)}")
    if deltas:
        print(f"congested minus rested, {len(deltas)} leagues: "
              f"mean {np.mean(deltas):+.3f} goals   "
              f"negative in {sum(1 for x in deltas if x < 0)}/{len(deltas)}")


if __name__ == "__main__":
    main()
