"""
What each Over rung is worth once a match is 0-0 at half time.

A live price that has drifted looks generous and often is not, because the thing
that moved it — 45 minutes with no goal — moved the true probability further.
The pre-match tip cannot help here: it priced 90 minutes, and the question is
about 45.

The trap is that rungs decay at completely different rates. At a league average
near 2.7 goals, a match still 0-0 at the break reaches ONE goal about 76% of the
time and TWO about 42%. Backing `O0.5` on the drift is betting on something that
still probably happens; backing `O1.5` is a coin flip that loses. Both look like
"the same tip, better odds".

Counted straight from stored results — every match that reached half time
goalless, split by how high-scoring its league is, since tempo is the one thing
that genuinely carries into the second half. Leagues that drop the half-time
score on 0-0 finishes are excluded (see `scripts/ht_zero.py`); left in, they
report an impossible 100%.

Usage:  python scripts/live_ht_table.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store

BANDS = [(0.0, 2.5, "under 2.5"), (2.5, 2.8, "2.5 - 2.8"),
         (2.8, 3.1, "2.8 - 3.1"), (3.1, 9.9, "over 3.1")]


def main() -> None:
    rows = []
    for lg in sorted(store.available_leagues()):
        df = store.load_results(lg)
        if df is None or df.empty or "hthg" not in df:
            continue
        d = df.dropna(subset=["hthg", "htag"])
        if len(d) < 100:
            continue
        all00 = int(((df["hg"] == 0) & (df["ag"] == 0)).sum())
        sub00 = int(((d["hg"] == 0) & (d["ag"] == 0)).sum())
        if all00 >= 10 and sub00 <= all00 * 0.1:
            continue
        lmu = float((d["hg"] + d["ag"]).mean())
        g = d[(d["hthg"] == 0) & (d["htag"] == 0)]
        for _, r in g.iterrows():
            rows.append((lmu, int(r["hg"]) + int(r["ag"])))

    print(f"{len(rows)} matches that reached half time at 0-0\n")
    print(f"{'league tempo':14}{'n':>7}"
          f"{'O0.5':>9}{'buy≥':>8}{'O1.5':>9}{'buy≥':>8}{'O2.5':>9}{'buy≥':>8}")
    for lo, hi, label in BANDS:
        b = [t for m, t in rows if lo <= m < hi]
        if len(b) < 200:
            continue
        line = f"{label:14}{len(b):7}"
        for need in (1, 2, 3):
            p = sum(1 for t in b if t >= need) / len(b)
            line += f"{p*100:8.1f}%{1/p*1.05:8.2f}"
        print(line)
    print("\nbuy≥ is break-even plus the usual 5% margin.")
    print("A rung needing TWO more goals is worth roughly half one needing one.")


if __name__ == "__main__":
    main()
