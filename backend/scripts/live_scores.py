"""
Live scores for the fixtures on the board, from ESPN's public scoreboard.

Sofascore blocks this environment (403 at their edge); ESPN does not, and it
is already this repo's fallback data provider, so live states come from the
same family of source as the stored results. This polls the scoreboard for
every competition the board currently has fixtures in and prints the matches
it can pair with `config/fixtures.tsv` rows — pairing by the same resolver
rules the engine uses would be overkill for a scoreboard glance, so it is a
loose token match, and a row it cannot pair is listed rather than guessed.

Read-only: nothing is written. Grading stays a human act in fixtures.tsv.

Usage:  python scripts/live_scores.py             sweep the whole board
        python scripts/live_scores.py Malmö       one match, full stat sheet
                                                  and the goal/card timeline
"""
from __future__ import annotations

import json
import sys
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.board import load

# Board league code -> ESPN scoreboard slug. Extend as slates need it.
ESPN = {
    "ITA-SA": "ita.1", "SAU-PL": "ksa.1", "DEN-SL": "den.1", "SWE-AL": "swe.1",
    "ESP-L2": "esp.2", "ESP-LL": "esp.1", "NED-D2": "ned.2", "ENG-PL": "eng.1",
    "POR-PL": "por.1", "FRA-L2": "fra.2", "TUR-SL": "tur.1", "BRA-SB": "bra.2",
    "BRA-SA": "bra.1", "CHI-PD": "chi.1", "COL-PA": "col.1",
    "UCL-Q": "uefa.champions_qual",
}


def fetch(slug: str) -> list[tuple[str, str, str, str]]:
    url = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}/scoreboard"
    try:
        with urllib.request.urlopen(url, timeout=12) as r:
            d = json.load(r)
    except Exception:
        return []
    out = []
    for e in d.get("events", []):
        c = e["competitions"][0]
        a, b = c["competitors"]
        h, aw = (a, b) if a["homeAway"] == "home" else (b, a)
        out.append((h["team"]["displayName"], aw["team"]["displayName"],
                    f"{h.get('score', '?')}-{aw.get('score', '?')}",
                    e["status"]["type"]["shortDetail"]))
    return out


def tokens(s: str) -> set[str]:
    return {w.lower().strip(".") for w in s.split() if len(w) > 3}


STATS = [
    ("possessionPct", "Possession %"), ("totalShots", "Shots"),
    ("shotsOnTarget", "On target"), ("blockedShots", "Blocked"),
    ("saves", "Keeper saves"), ("wonCorners", "Corners"),
    ("foulsCommitted", "Fouls"), ("yellowCards", "Yellow cards"),
    ("redCards", "Red cards"), ("offsides", "Offsides"),
    ("totalPasses", "Passes"), ("passPct", "Pass accuracy"),
    ("totalTackles", "Tackles"), ("interceptions", "Interceptions"),
]


def detail(query: str) -> None:
    """Sofascore-style sheet for one board fixture, from ESPN's summary."""
    q = tokens(query) or {query.lower()}
    fx = next((f for f in load() if q & tokens(f.teams)
               or query.lower() in f.teams.lower()), None)
    if fx is None:
        print(f"no board fixture matches {query!r}")
        return
    slug = ESPN.get(fx.code)
    if slug is None:
        print(f"no ESPN slug for {fx.code}")
        return
    base = f"https://site.api.espn.com/apis/site/v2/sports/soccer/{slug}"
    with urllib.request.urlopen(f"{base}/scoreboard", timeout=12) as r:
        events = json.load(r).get("events", [])
    ev = next((e for e in events
               if tokens(e["name"]) & tokens(fx.teams)), None)
    if ev is None:
        print(f"{fx.teams}: not on ESPN's current board")
        return
    with urllib.request.urlopen(f"{base}/summary?event={ev['id']}",
                                timeout=12) as r:
        s = json.load(r)

    c = ev["competitions"][0]
    a, b = c["competitors"]
    h, aw = (a, b) if a["homeAway"] == "home" else (b, a)
    print(f"{h['team']['displayName']} {h.get('score','?')}-"
          f"{aw.get('score','?')} {aw['team']['displayName']}   "
          f"[{ev['status']['type']['shortDetail']}]\n")

    teams = s.get("boxscore", {}).get("teams", [])
    if len(teams) == 2:
        cols = []
        for tm in teams:
            cols.append({st.get("name"): st.get("displayValue", "-")
                         for st in tm.get("statistics", [])})
        th, ta = ((cols[0], cols[1])
                  if teams[0]["team"]["displayName"] == h["team"]["displayName"]
                  else (cols[1], cols[0]))
        for key, label in STATS:
            if key in th or key in ta:
                print(f"  {th.get(key,'-'):>8}   {label:<16}"
                      f"{ta.get(key,'-'):>8}")

    keys = s.get("keyEvents", [])
    if keys:
        print("\n  timeline:")
        for k in keys:
            mins = k.get("clock", {}).get("displayValue", "?")
            print(f"    {mins:>4}  {k.get('text', k.get('type',{}).get('text',''))[:70]}")


def main() -> None:
    if len(sys.argv) > 1 and not sys.argv[1].startswith("-"):
        detail(" ".join(sys.argv[1:]))
        return

    fixtures = [f for f in load() if not f.settled]
    by_code: dict[str, list] = {}
    for f in fixtures:
        by_code.setdefault(f.code, []).append(f)

    unmatched = []
    for code, rows in sorted(by_code.items()):
        slug = ESPN.get(code)
        board = {id(f): f for f in rows}
        if slug:
            for h, a, score, state in fetch(slug):
                hit = next((f for f in board.values()
                            if tokens(h) & tokens(f.teams)
                            or tokens(a) & tokens(f.teams)), None)
                if hit is not None:
                    print(f"  {score:>5}  {state:12}  {hit.teams}  ({hit.league})")
                    del board[id(hit)]
        unmatched += [f for f in board.values()]

    if unmatched:
        print("\nno live pairing (not on ESPN's board, or not today):")
        for f in unmatched:
            print(f"    {f.kickoff}  {f.teams}  ({f.league})")


if __name__ == "__main__":
    main()
