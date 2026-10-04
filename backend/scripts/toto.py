"""
TOTO prices, added to every card as one more book.

THE BETTOR, 4 Oct: "TonyBet can sometimes price better, it's one of the
NL/EU better books. Also TOTO has good ones." TOTO's sportsbook site reads
its prices from a public JSON API (sport-api.toto.nl): a league's match
list (POST /event/request) and one match's full market set (GET
/cms/content?route=Event&eventId=...). This module reads the full-match
Total Goals ladder from it (O/U 0.5 ... 5.5) for the board's leagues, so
TOTO's price stands beside the feed's on every card it covers — and,
where it is the best, it is the card's best price (and so its
first-sight stamp, since 4 Oct). TonyBet is not read yet: its platform
API wants a filter format that could not be confirmed from outside.

Odds never enter the prediction path: this prices cards, nothing else.
"""
from __future__ import annotations

import datetime as dt
import json
import time
import urllib.error
import urllib.request
from zoneinfo import ZoneInfo

API = "https://sport-api.toto.nl"
BOOK = "TOTO"
AMS = ZoneInfo("Europe/Amsterdam")
HEAD = {"Content-Type": "application/json", "Accept": "application/json",
        "User-Agent": "Mozilla/5.0", "Origin": "https://sport.toto.nl",
        "Referer": "https://sport.toto.nl/"}

# Board league code -> TOTO competition drilldown ids (sport-api
# /cms/navigation, level 4 under Voetbal). Built 4 Oct; a league TOTO does
# not list (ALG-L1, MAR-BP, RUS-PL) has no row.
LEAGUES = {
    "ENG-PL": ["567"], "ENG-CH": ["691"], "ENG-L1": ["578"], "ENG-L2": ["688"],
    "ESP-LL": ["570"], "ESP-L2": ["587"], "ITA-SA": ["644"], "ITA-SB": ["726"],
    "GER-BL": ["577"], "GER-B2": ["573"], "FRA-L1": ["911"], "FRA-L2": ["924"],
    "NED-ED": ["1176"], "NED-D2": ["1053"], "BEL-PL": ["914"], "POR-PL": ["586"],
    "TUR-SL": ["593"], "SCO-PL": ["874"], "AUT-BL": ["572"], "SUI-SL": ["664"],
    "GRE-SL": ["580"], "DEN-SL": ["574"], "SWE-AL": ["591"], "NOR-EL": ["584"],
    "POL-EK": ["604"], "IRL-PD": ["926"], "SAU-PL": ["991"], "MLS": ["596"],
    "MEX-LMX": ["889"], "BRA-SA": ["972"], "BRA-SB": ["1002"], "ARG-PD": ["2736"],
    "CHI-PD": ["1133"], "COL-PA": ["1226"], "PER-L1": ["2206"], "JPN-J1": ["582"],
    "CHN-SL": ["882"], "INT-UEFA": ["9641", "7296"],
    "INT-CONCACAF": ["9343", "7296"],
    "INT-FR": ["12069", "1451"],
}

_CACHE: dict = {}

# TOTO names national sides in Dutch; the board in English. Read before
# the club matcher, so "Frankrijk" meets "France".
DUTCH = {
    "frankrijk": "France", "belgie": "Belgium", "belgië": "Belgium",
    "italie": "Italy", "italië": "Italy", "turkije": "Türkiye",
    "duitsland": "Germany", "spanje": "Spain", "engeland": "England",
    "nederland": "Netherlands", "schotland": "Scotland", "wales": "Wales",
    "noord-ierland": "Northern Ireland", "ierland": "Republic of Ireland",
    "zwitserland": "Switzerland", "oostenrijk": "Austria", "polen": "Poland",
    "tsjechie": "Czechia", "tsjechië": "Czechia", "slowakije": "Slovakia",
    "slovenie": "Slovenia", "slovenië": "Slovenia", "kroatie": "Croatia",
    "kroatië": "Croatia", "servie": "Serbia", "servië": "Serbia",
    "bosnie en herzegovina": "Bosnia-Herzegovina",
    "bosnië en herzegovina": "Bosnia-Herzegovina",
    "montenegro": "Montenegro", "albanie": "Albania", "albanië": "Albania",
    "noord-macedonie": "North Macedonia", "noord-macedonië": "North Macedonia",
    "griekenland": "Greece", "bulgarije": "Bulgaria", "roemenie": "Romania",
    "roemenië": "Romania", "hongarije": "Hungary", "oekraine": "Ukraine",
    "oekraïne": "Ukraine", "wit-rusland": "Belarus", "rusland": "Russia",
    "moldavie": "Moldova", "moldavië": "Moldova", "litouwen": "Lithuania",
    "letland": "Latvia", "estland": "Estonia", "finland": "Finland",
    "zweden": "Sweden", "noorwegen": "Norway", "denemarken": "Denmark",
    "ijsland": "Iceland", "faeroer": "Faroe Islands", "faeröer": "Faroe Islands",
    "faroer eilanden": "Faroe Islands", "kazachstan": "Kazakhstan",
    "georgie": "Georgia", "georgië": "Georgia", "armenie": "Armenia",
    "armenië": "Armenia", "azerbeidzjan": "Azerbaijan", "cyprus": "Cyprus",
    "malta": "Malta", "luxemburg": "Luxembourg", "andorra": "Andorra",
    "san marino": "San Marino", "liechtenstein": "Liechtenstein",
    "gibraltar": "Gibraltar", "kosovo": "Kosovo", "israel": "Israel",
    "portugal": "Portugal", "verenigde staten": "United States",
    "mexico": "Mexico", "canada": "Canada", "brazilie": "Brazil",
    "brazilië": "Brazil", "argentinie": "Argentina", "argentinië": "Argentina",
    "colombia": "Colombia", "peru": "Peru", "chili": "Chile",
    "japan": "Japan", "zuid-korea": "South Korea", "china": "China",
    "australie": "Australia", "australië": "Australia", "marokko": "Morocco",
    "egypte": "Egypt", "tunesie": "Tunisia", "tunesië": "Tunisia",
    "algerije": "Algeria", "kameroen": "Cameroon", "nigeria": "Nigeria",
    "ivoorkust": "Ivory Coast", "zuid-afrika": "South Africa",
    "saoedi-arabie": "Saudi Arabia", "saoedi-arabië": "Saudi Arabia",
    "jordanie": "Jordan", "jordanië": "Jordan", "oezbekistan": "Uzbekistan",
    "tadzjikistan": "Tajikistan", "dominicaanse republiek": "Dominican Republic",
    "trinidad en tobago": "Trinidad and Tobago", "curacao": "Curaçao",
    "jamaica": "Jamaica", "haiti": "Haiti", "honduras": "Honduras",
    "costa rica": "Costa Rica", "panama": "Panama", "guatemala": "Guatemala",
    "el salvador": "El Salvador", "nicaragua": "Nicaragua", "suriname": "Suriname",
    "kaaimaneilanden": "Cayman Islands", "bahama's": "Bahamas",
    "amerikaanse maagdeneilanden": "US Virgin Islands",
    "britse maagdeneilanden": "British Virgin Islands",
    "turks- en caicoseilanden": "Turks and Caicos Islands",
    "sint-kitts en nevis": "St. Kitts and Nevis", "saint lucia": "St. Lucia",
    "guadeloupe": "Guadeloupe", "martinique": "Martinique", "cuba": "Cuba",
    "bermuda": "Bermuda", "barbados": "Barbados", "grenada": "Grenada",
    "frans-guyana": "French Guiana", "belize": "Belize", "aruba": "Aruba",
    "venezuela": "Venezuela", "bolivia": "Bolivia", "gambia": "Gambia",
    "rwanda": "Rwanda", "kenia": "Kenya", "oeganda": "Uganda",
    "dr congo": "Congo DR", "congo dr": "Congo DR", "ghana": "Ghana",
    "mali": "Mali", "senegal": "Senegal",
}


def _en(name: str) -> str:
    return DUTCH.get(name.strip().lower(), name)


def _req(url: str, body: dict | None = None):
    key = (url, json.dumps(body, sort_keys=True) if body else None)
    if key in _CACHE:
        return _CACHE[key]
    data = None
    for attempt in range(3):
        try:
            req = urllib.request.Request(
                url, data=json.dumps(body).encode() if body is not None else None,
                headers=HEAD, method="POST" if body is not None else "GET")
            with urllib.request.urlopen(req, timeout=20) as r:
                data = json.load(r)
            break
        except urllib.error.HTTPError as e:
            if e.code != 429:
                break
            time.sleep(2 + attempt * 3)
        except Exception:
            time.sleep(1 + attempt * 2)
    # A failed call is not cached: one throttled request must not blank a
    # whole league for the rest of the run.
    if data is not None:
        _CACHE[key] = data
    time.sleep(0.05)
    return data


def _walk_events(o):
    if isinstance(o, dict):
        if "startTime" in o and "teams" in o and "id" in o:
            yield o
        for v in o.values():
            yield from _walk_events(v)
    elif isinstance(o, list):
        for v in o:
            yield from _walk_events(v)


def events(code: str) -> list[dict]:
    out, seen = [], set()
    for sel in LEAGUES.get(code, []):
        d = _req(f"{API}/event/request", {
            "includedIds": [{"selectionId": sel}], "isLive": False,
            "isPreMatch": True, "order": "START_TIME", "grouping": "NONE",
            "sortCode": "MTCH", "includeMarketFilters": False,
            "excludeSpecials": True, "hasLiveStream": False})
        for e in _walk_events(d or {}):
            if e["id"] in seen:
                continue
            seen.add(e["id"])
            side = {t.get("side"): t.get("name", "") for t in e.get("teams", [])}
            try:
                ko = dt.datetime.fromisoformat(e["startTime"].replace("+0000", "+00:00"))
            except ValueError:
                continue
            out.append(dict(id=e["id"], home=side.get("HOME", ""),
                            away=side.get("AWAY", ""),
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
        if same_club(_en(ev["home"]), home) and same_club(_en(ev["away"]), away):
            return ev
    return None


_TOTALS: dict = {}


def totals(event_id) -> dict:
    """{'O1.5': price, 'U4.5': price, ...} from TOTO's full match page.
    Parsed once per match per run; the 270 KB page is not kept."""
    if event_id in _TOTALS:
        return _TOTALS[event_id]
    url = f"{API}/cms/content?route=Event&eventId={event_id}"
    d = _req(url)
    _CACHE.pop((url, None), None)
    out: dict = {}

    def walk(o):
        if isinstance(o, dict):
            if o.get("groupCode") == "TOTAL_GOALS_OVER/UNDER" and "outcomes" in o:
                line = o.get("handicapValue")
                if line is not None and o.get("status", "ACTIVE") == "ACTIVE":
                    for oc in o["outcomes"]:
                        side = (oc.get("name") or "")[:1]
                        pr = (oc.get("prices") or [{}])[0].get("decimal")
                        if side in ("O", "U") and pr and oc.get("active", True):
                            out[f"{side}{float(line):g}" if float(line) % 1
                                else f"{side}{float(line):.1f}"] = float(pr)
            for v in o.values():
                walk(v)
        elif isinstance(o, list):
            for v in o:
                walk(v)
    walk(d or {})
    if d is not None:
        _TOTALS[event_id] = out
    return out


def price(code: str, teams: str, day: str, want: str) -> float | None:
    ev = find(code, teams, day)
    if not ev:
        return None
    p = totals(ev["id"]).get(want)
    return p if p and p > 1.0 else None
