"""
Club Elo as the cup strength number: the externally-maintained rating,
same symmetry bar.

The home-grown composite (league rating + as-of PPG + own cup form) got
cups to −0.6 / −4.0 — closest yet, one window short. Club Elo (clubelo.com)
is the professional version of that composite: club-level, updated after
every match in every competition, and available AS-OF daily — so it prices
exactly what our three-strand proxy approximates, with none of the
coverage gates (no domestic-history minimum, no league-rating minimum).

Data: daily snapshots mirrored by github.com/tonyelhabr/club-rankings
(the clubelo API itself is unreachable from this environment), trimmed to
config/club_elo.parquet — 860 clubs, 2023-03-27 → 2026-01-14: all of Swiss
season one, season two through the league phase. Source for refreshes:

  https://github.com/tonyelhabr/club-rankings/releases/download/club-rankings/clubelo-club-rankings.csv

Store-name → clubelo-name mapping lives in config/club_elo_names.json
(380 clubs, auto-matched plus a hand pass; clubelo transliterates umlauts
as ue/oe and truncates: Zuerich, Malmoe, Bueyueksehir, Steaua for FCSB).

Two models, same offline tip selection as every cup instrument:

  FROZEN   mu = rolling_3y_base + b0 + b1·|Δelo| + b2·(elo sum), betas
           fitted on one Swiss season, validated on the other, both ways.
  WALKED   the honest live shape: slopes frozen from the other season,
           intercept refit monthly on the trailing 180 days of Swiss rows
           — because the frozen intercepts differ by half a goal between
           seasons (−0.83 vs −0.36): the LEVEL drifts, the slopes do not.

VERDICT (25 Aug 2026): the best cup result of the project, by a distance.

    frozen   fit 24-25 -> 25-26   745  gap −2.5      reverse  818  −1.7
    walked   25-26  −1.8  (halves −3.6 / −0.3)
             24-25  −2.4  (halves −3.4 / −1.5)

92% coverage (1,563 of 1,701 fixtures — the composite managed ~28%), a
real market mix (~25% O1.5 against the U4.25 base rung), and for the
first time EVERY window carries the same small negative sign: a stable
~−2 overstatement, no sign flips, no pooled cancellation. A −2 says-debit
measured on one season and confirmed on the other would pass the same
two-window bar every shipped constant passed. Staleness is a non-issue:
Elo lagged 30 and 60 days grades identically (−1.8/−2.1, −2.1/−1.9), so
a committed snapshot refreshed every few weeks is operationally enough.
Per-competition intercepts were tried and REJECTED (−1.8/−3.7, per-cup
gaps whipsaw between directions — they absorb season noise, not level).

Not wired into the live engine: that is a build decision, not a constant
— CUP_TIPS_ENABLED stays False until it is taken deliberately.

Usage:  python scripts/cup_elo.py [--csv path/to/clubelo.csv]
"""
from __future__ import annotations

import bisect
import json
import sys
from pathlib import Path

import numpy as np
import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from scripts.cup_asof import CUPS
from scripts.cup_composite import SPLIT, fit, grade, show

BREAK = pd.Timestamp("2024-07-01")
ELO_END = pd.Timestamp("2026-01-14")         # last mirrored snapshot
PARQUET = Path(__file__).resolve().parents[2] / "config" / "club_elo.parquet"
SCALE = 100.0                                # betas per 100 Elo, readable


def elo_series(csv_path: str | None = None) -> dict[str, tuple[list, list]]:
    """clubelo name -> (sorted snapshot dates, elos), for as-of lookup."""
    if csv_path:
        df = pd.read_csv(csv_path, usecols=["Club", "Elo", "date"])
        df["date"] = pd.to_datetime(df["date"])
        df = df.sort_values("date").drop_duplicates(["Club", "date"],
                                                    keep="last")
    else:
        df = pd.read_parquet(PARQUET)
    out = {}
    for club, g in df.groupby("Club", observed=True):
        out[str(club)] = (list(g["date"]), list(g["Elo"]))
    return out


def asof(series, when):
    dates, elos = series
    i = bisect.bisect_left(dates, when)
    return elos[i - 1] if i else None


def build_rows(csv_path: str | None = None):
    names = json.loads(
        (Path(__file__).resolve().parents[2] / "config"
         / "club_elo_names.json").read_text())
    series = elo_series(csv_path)

    frames = {}
    rows, total = [], 0
    for code in CUPS:
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        frames[code] = df.dropna(subset=["hg", "ag"]).sort_values("date")

    def rolling_base(code, when):
        df = frames[code]
        w = df[(df.date < when) & (df.date >= when - pd.Timedelta(days=1095))]
        if len(w) < 40:
            w = df[df.date < when]
        return float((w.hg + w.ag).mean()) if len(w) >= 30 else None

    for code, df in frames.items():
        for r in df.itertuples():
            if not (BREAK <= r.date <= ELO_END):
                continue
            total += 1
            sh = series.get(names.get(str(r.home), ""))
            sa = series.get(names.get(str(r.away), ""))
            if sh is None or sa is None:
                continue
            eh, ea = asof(sh, r.date), asof(sa, r.date)
            b = rolling_base(code, r.date)
            if eh is None or ea is None or b is None:
                continue
            rows.append((r.date, code, eh / SCALE, ea / SCALE, b,
                         int(r.hg), int(r.ag)))
    print(f"Swiss era to {ELO_END.date()}: {total} fixtures, "
          f"{len(rows)} with as-of Elo for both clubs "
          f"({len(rows)/total*100:.0f}% coverage)\n")
    return rows


def walk(train, test, all_rows, label):
    """Slopes from `train`, intercept refit monthly on trailing 180 days."""
    b0_fit, b1, b2 = fit(train)
    got = []
    for ym in sorted({(r[0].year, r[0].month) for r in test}):
        start = pd.Timestamp(year=ym[0], month=ym[1], day=1)
        tr = [r for r in all_rows
              if start - pd.Timedelta(days=180) <= r[0] < start]
        if len(tr) >= 100:
            b0 = float(np.mean([r[5] + r[6] - r[4]
                                - (b1 * abs(r[2] - r[3])
                                   + b2 * (r[2] + r[3])) for r in tr]))
        else:
            b0 = b0_fit
        got.extend(grade([r for r in test
                          if (r[0].year, r[0].month) == ym], b0, b1, b2))
    show(label, got)
    return got


def main() -> None:
    args = sys.argv[1:]
    csv = args[args.index("--csv") + 1] if "--csv" in args else None

    rows = build_rows(csv)
    rows.sort(key=lambda r: r[0])
    s1 = [r for r in rows if r[0] < SPLIT]
    s2 = [r for r in rows if r[0] >= SPLIT]
    print(f"season windows: {len(s1)} (24-25)  {len(s2)} (25-26 to Jan)\n")

    print("FROZEN (betas from one season, applied unchanged to the other)")
    b0, b1, b2 = fit(s1)
    print(f"betas from 24-25: {b0:+.3f} {b1:+.3f}*|gap| {b2:+.3f}*sum "
          f"(per 100 Elo)")
    show("fit 24-25 -> validate 25-26", grade(s2, b0, b1, b2))
    b0, b1, b2 = fit(s2)
    print(f"betas from 25-26: {b0:+.3f} {b1:+.3f}*|gap| {b2:+.3f}*sum")
    show("fit 25-26 -> validate 24-25", grade(s1, b0, b1, b2))

    print("\nWALKED (slopes frozen, intercept tracked monthly — live shape)")
    for lab, tr, te in (("walk 25-26", s1, s2), ("walk 24-25", s2, s1)):
        walk(tr, te, rows, lab)
        mid = sorted(r[0] for r in te)[len(te) // 2]
        walk(tr, [r for r in te if r[0] < mid], rows, "  older half")
        walk(tr, [r for r in te if r[0] >= mid], rows, "  newer half")


if __name__ == "__main__":
    main()
