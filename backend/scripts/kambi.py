"""
Unibet (NL) prices for the leagues the odds feed does not carry.

THE BETTOR, 4 Oct: "can we not work in something for unquoted cards ...
this is the Eerste Divisie on Unibet ... can't you use that for any other
missing league without quotes?" Unibet's site is built on Kambi, whose
public offering API serves the same prices as JSON: a league's match list
(listView) and every market on one match (betoffer/event). This module
reads the Total Goals ladder from it, for cards in leagues the feed has no
market for, so those cards get a price, a play bar check and a first-sight
stamp like any other — and the price is one at a book he actually uses.

Odds never enter the prediction path: this prices cards, nothing else.
Only full-match .5 lines exist on this ladder (O/U 1.5 ... 5.5); a quarter
line is quoted on the line the bettor strikes (odds_api.bought), and a
whole line such as O1.0 has no Kambi price and stays unquoted.
"""
from __future__ import annotations

import datetime as dt
import json
import time
import urllib.request
from zoneinfo import ZoneInfo

BASE = "https://eu-offering-api.kambicdn.com/offering/v2018/ubnl"
Q = "lang=en_GB&market=NL"
BOOK = "Unibet (NL)"
AMS = ZoneInfo("Europe/Amsterdam")

# Board league code -> Kambi listView path. Only leagues the odds feed
# does not carry need a row; a league Unibet does not list stays unquoted.
PATHS = {
    "NED-D2": "football/netherlands/eerste_divisie",
    "COL-PA": "football/colombia/liga_betplay_dimayor",
    "PER-L1": "football/peru/liga_1",
    "INT-CONCACAF": "football/concacaf/nations_league",
}

_CACHE: dict = {}


def _get(url: str):
    if url in _CACHE:
        return _CACHE[url]
    for attempt in range(3):
        try:
            req = urllib.request.Request(url, headers={"User-Agent": "tempo-guard"})
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.load(r)
            _CACHE[url] = data
            return data
        except Exception:
            time.sleep(1 + attempt * 2)
    _CACHE[url] = None
    return None


def events(code: str) -> list[dict]:
    """[{id, home, away, day}] for a league's upcoming matches; day is the
    Amsterdam date the board files a kickoff under."""
    path = PATHS.get(code)
    if not path:
        return []
    d = _get(f"{BASE}/listView/{path}/all/matches.json?{Q}&useCombined=true")
    out = []
    for e in (d or {}).get("events", []):
        ev = e.get("event") or {}
        try:
            ko = dt.datetime.fromisoformat(ev["start"].replace("Z", "+00:00"))
        except (KeyError, ValueError):
            continue
        out.append(dict(id=ev.get("id"), home=ev.get("homeName", ""),
                        away=ev.get("awayName", ""),
                        day=ko.astimezone(AMS).date().isoformat()))
    return out


def find(code: str, teams: str, day: str) -> dict | None:
    from scripts.liveline import same_club
    if " v " not in teams:
        return None
    home, away = (x.strip() for x in teams.split(" v ", 1))
    d0 = dt.date.fromisoformat(day)
    for ev in events(code):
        if abs((dt.date.fromisoformat(ev["day"]) - d0).days) > 1:
            continue
        if same_club(ev["home"], home) and same_club(ev["away"], away):
            return ev
    return None


def totals(event_id) -> dict:
    """{'O1.5': price, 'U3.5': price, ...} from the full-match Total Goals
    market of one Kambi event."""
    d = _get(f"{BASE}/betoffer/event/{event_id}.json?{Q}")
    out = {}
    for bo in (d or {}).get("betOffers", []):
        if (bo.get("criterion") or {}).get("englishLabel") != "Total Goals":
            continue
        for o in bo.get("outcomes", []):
            lab = (o.get("englishLabel") or "")[:1]
            if lab not in "OU" or "line" not in o or "odds" not in o:
                continue
            if o.get("status", "OPEN") != "OPEN":
                continue
            line = o["line"] / 1000
            out[f"{lab}{line:g}" if line % 1 else f"{lab}{line:.1f}"] = o["odds"] / 1000
    return out


def lane_price(code: str, teams: str, day: str, want: str) -> dict | None:
    """A quote in odds_api.lane_price's shape, from Unibet alone."""
    ev = find(code, teams, day)
    if not ev:
        return None
    price = totals(ev["id"]).get(want)
    if not price or price <= 1.0:
        return None
    return dict(lane=want, consensus=price, best=price, book=BOOK,
                unibet_nl=price, n=1, top=[(BOOK, price)])
