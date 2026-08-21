"""
Which leagues can actually be run, and for what?

Two different questions, often confused:

  retro     is there enough stored history to replay past fixtures? Needs depth
            and a rolling window of prior matches, not freshness.

  future    can it predict an upcoming fixture? Needs the CURRENT season loaded.
            A league whose data stopped a year ago will still retro-simulate
            happily and produce meaningless futurematches, which is exactly what
            Brazilian Serie B did before it was repointed at ESPN.

So freshness is reported separately from depth, and a league is only cleared for
futurematch when its most recent result is recent enough that the rolling form
window describes the teams playing now.
"""
from __future__ import annotations

import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import sources, store

# A team's form window is 10 matches. Most leagues play weekly, so a gap beyond
# roughly two months means the stored form is describing a different squad.
FRESH_DAYS = 45
STALE_DAYS = 120

# Enough history for the rolling features plus a replay worth reading.
RETRO_MIN = 300

TODAY = date.today()


def main() -> None:
    rows = []
    for code in sorted(sources.LEAGUES):
        src = sources.LEAGUES[code]
        try:
            df = store.load_results(code)
        except Exception:
            df = None
        if df is None or df.empty:
            rows.append((code, src.name, 0, None, None, 0, "NO DATA", "NO DATA"))
            continue

        n = len(df)
        last = df["date"].max().date()
        age = (TODAY - last).days
        shots = int(df["hst"].notna().sum()) if "hst" in df.columns else 0

        retro = "yes" if n >= RETRO_MIN else f"thin ({n})"
        if age <= FRESH_DAYS:
            future = "yes"
        elif age <= STALE_DAYS:
            future = f"stale {age}d"
        else:
            future = f"NO {age}d"
        rows.append((code, src.name, n, last, age, shots, retro, future))

    print(f"  {'code':9s} {'league':28s} {'rows':>6} {'last result':>12} "
          f"{'age':>6} {'shots':>7}  {'retro':>10}  {'future':>9}")
    print("  " + "-" * 104)
    for code, name, n, last, age, shots, retro, future in rows:
        sh = f"{shots / n:.0%}" if n else "-"
        print(f"  {code:9s} {name[:28]:28s} {n:6d} "
              f"{str(last) if last else '-':>12} {str(age) + 'd' if age is not None else '-':>6} "
              f"{sh:>7}  {retro:>10}  {future:>9}")

    ok_future = [r for r in rows if r[7] == "yes"]
    ok_retro = [r for r in rows if r[6] == "yes"]
    print(f"\n  retro-capable:  {len(ok_retro)}/{len(rows)}")
    print(f"  future-capable: {len(ok_future)}/{len(rows)}")
    dead = [r for r in rows if r[7].startswith("NO")]
    if dead:
        print(f"\n  NOT usable for futurematch ({len(dead)}):")
        for r in dead:
            print(f"    {r[0]:9s} {r[1][:34]:34s} last {r[3]}  ({r[4]}d)")


if __name__ == "__main__":
    main()
