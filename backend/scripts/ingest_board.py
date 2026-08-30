"""
Merge the board's completed matches into the results bank.

Until now the store ended where the last provider snapshot ended (22-24
Aug), and the fixtures this project itself graded — scores read off
ESPN or the bettor's own screen — never fed back. The bettor asked for
the merge (30 Aug) so the retrosim windows include the freshest week.
As-of discipline is untouched: every replay still reads only results
dated before the fixture it prices, so nothing can train on the match
it is being scored against.

Guard rails, in order:
  - only domestic leagues the store already carries (cups keep their
    own provider conventions and are skipped);
  - only rows with a real final score (the FT — no source mark stays
    out);
  - both team names must resolve against the store's own names through
    the engine's resolver — an unresolved name is printed and skipped,
    never guessed, so no duplicate identity can be created;
  - store.save() drops (date, home, away) duplicates, so re-running
    after the next sweep is safe and idempotent.

Usage:  python scripts/ingest_board.py            merge and report
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.data.features import _match_team

FIXTURES = Path(__file__).resolve().parents[2] / "config" / "fixtures.tsv"
SCORE = re.compile(r"(\d+)-(\d+)")
SEASON = "2026-27"
SKIP_PREFIX = ("UCL", "UEL", "UECL")


def main() -> None:
    stored = set(store.available_leagues())
    added: dict[str, int] = {}
    skipped: list[str] = []
    rows: dict[str, list[dict]] = {}

    for ln in FIXTURES.read_text().splitlines():
        c = ln.split("\t")
        if ln.startswith("#") or len(c) < 7:
            continue
        code, teams, status = c[1], c[3], c[6]
        if code not in stored or code.startswith(SKIP_PREFIX):
            continue
        if not status or status.startswith("LIVE") or "no source" in status:
            continue
        m = SCORE.search(status)
        if not m or " v " not in teams:
            continue
        h, a = teams.split(" v ", 1)
        df = store.load_results(code)
        names = sorted(set(df["home"]) | set(df["away"]))
        rh, ra = _match_team(h, names), _match_team(a, names)
        if rh is None or ra is None:
            skipped.append(f"{code}: {teams} — "
                           f"{'home' if rh is None else 'away'} unresolved")
            continue
        rows.setdefault(code, []).append(dict(
            date=pd.Timestamp(c[0].split(" ")[0]), home=rh, away=ra,
            hg=int(m.group(1)), ag=int(m.group(2)), season=SEASON,
            league_code=code, country="", status="result"))

    for code, new in sorted(rows.items()):
        cur = store.load(code, SEASON)
        have = set(zip(cur["date"], cur["home"], cur["away"])) \
            if not cur.empty else set()
        fresh = [r for r in new
                 if (r["date"], r["home"], r["away"]) not in have]
        if not fresh:
            continue
        add = pd.DataFrame(fresh)
        merged = pd.concat([cur, add], ignore_index=True) \
            if not cur.empty else add
        store.save(code, SEASON, merged)
        added[code] = len(fresh)

    for code, n in sorted(added.items()):
        print(f"  {code:9} +{n}")
    print(f"merged {sum(added.values())} results into "
          f"{len(added)} leagues")
    if skipped:
        print("skipped (name unresolved — never guessed):")
        for s in skipped:
            print(f"  {s}")


if __name__ == "__main__":
    main()
