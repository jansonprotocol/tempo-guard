"""
The other half of a two-legged tie, written down.

Athena prices ONE match's goal total. It has no concept of a tie, an
aggregate score, or what a side needs on the night — and that is a real
blind spot, because a team chasing a deficit plays differently from one
protecting a lead. Rather than pretend otherwise, the board states the
tie context next to the tip and labels it as context.

This fetches completed first legs from ESPN for the cup fixtures on the
board and writes config/first_legs.tsv (fixture, first-leg line). The
file is typed data like fixtures.tsv; the renderers read it, and
nothing in the engine does.

Usage:  python scripts/first_legs.py [--days 8]
"""
from __future__ import annotations

import json
import subprocess
import sys
from datetime import date, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "first_legs.tsv"
SLUG = {"UCL-Q": "uefa.champions_qual", "UEL-Q": "uefa.europa_qual",
        "UECL-Q": "uefa.europa.conf_qual", "UCL": "uefa.champions",
        "UEL": "uefa.europa", "UECL": "uefa.europa.conf"}


from scripts.liveline import same_club as _same


def fetch(code: str, lo: date, hi: date) -> list[tuple]:
    slug = SLUG.get(code)
    if not slug:
        return []
    url = (f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}"
           f"/scoreboard?dates={lo:%Y%m%d}-{hi:%Y%m%d}")
    try:
        raw = subprocess.run(["curl", "-sS", "--max-time", "30", url],
                             capture_output=True, text=True).stdout
        data = json.loads(raw)
    except Exception:
        return []
    out = []
    for e in data.get("events", []):
        c = e["competitions"][0]
        if not e["status"]["type"]["completed"]:
            continue
        h = next(x for x in c["competitors"] if x["homeAway"] == "home")
        a = next(x for x in c["competitors"] if x["homeAway"] == "away")
        out.append((h["team"]["displayName"], int(h["score"]),
                    int(a["score"]), a["team"]["displayName"]))
    return out


def main() -> None:
    from scripts.board import load
    args = sys.argv[1:]
    days = int(args[args.index("--days") + 1]) if "--days" in args else 8

    fixtures = [f for f in load() if f.code in SLUG and " v " in f.teams]
    cache, rows = {}, []
    for f in fixtures:
        day = date.fromisoformat(f.kickoff.split(" ")[0])
        if f.code not in cache:
            cache[f.code] = fetch(f.code, day - timedelta(days=days),
                                  day - timedelta(days=2))
        home, away = (x.strip() for x in f.teams.split(" v ", 1))
        # The first leg is the reverse fixture: today's away side hosted it.
        for lh, hg, ag, la in cache[f.code]:
            if _same(lh, away) and _same(la, home):
                rows.append((f.teams, f"{lh} {hg}-{ag} {la}"))
                break

    head = ("# The first leg of each two-legged tie on the board, from "
            "ESPN.\n"
            "# Athena does not read this — it prices one match's goals and "
            "has no\n"
            "# concept of a tie. The board shows it as context beside the "
            "tip.\n"
            "# Written by scripts/first_legs.py. fixture\tfirst leg\n")
    OUT.write_text(head + "".join(f"{a}\t{b}\n" for a, b in sorted(rows)))
    print(f"{len(rows)} first legs written for {len(fixtures)} cup fixtures")


if __name__ == "__main__":
    main()
