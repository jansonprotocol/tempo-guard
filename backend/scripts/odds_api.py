"""
Real bookmaker prices for the board — the decision layer, never the engine.

Athena has never seen a price and still does not. This module sits entirely
downstream: it fetches what the market is actually offering, derives a price
for each lane a card prints, and reports the consensus and the best available
so a reader can compare their own screen against it. Nothing here writes to a
probability, and no function in `app/engine/` imports this file.

Why it exists: measured over two seasons and 8,071 ladder lanes, taking the
market MAXIMUM price instead of the market AVERAGE was worth +3.58 points of
ROI (-5.46% -> -1.88%). That is a bigger lever than any model change this
project has found, and it needs live prices to pull.

    python scripts/odds_api.py --quotes     write config/odds_quotes.tsv
    python scripts/odds_api.py --board      quote every pending board fixture
    python scripts/odds_api.py --leagues    what maps to the API, what does not
    python scripts/odds_api.py --usage      credits left on the key

Needs ODDS_API_KEY in the environment. Without it every entry point returns
nothing rather than failing — a missing price is silence, not an error.

Costs are per LEAGUE, not per match: one call returns every upcoming fixture
in that competition. cost = markets x regions, so h2h+totals over the eu
region is 2 credits per league per sweep.

Writes config/odds_cache.json — derived, delete it and the next sweep rebuilds.
"""
from __future__ import annotations

import json
import math
import os
import re
import sys
import time
import unicodedata
import urllib.parse
import urllib.request
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import market_select

ROOT = Path(__file__).resolve().parents[2]
CACHE = ROOT / "config" / "odds_cache.json"
HOST = "https://api.the-odds-api.com/v4"
REGIONS = "eu"
FRESH_MIN = 90          # a cached quote older than this is refetched

# Board code -> the API's sport key. Only competitions the API carries; a
# code missing from here simply never receives a quote.
SPORT = {
    "ENG-PL": "soccer_epl", "ENG-CH": "soccer_efl_champ",
    "ENG-L1": "soccer_england_league1", "ENG-L2": "soccer_england_league2",
    "SCO-PL": "soccer_spl", "GER-BL": "soccer_germany_bundesliga",
    "GER-B2": "soccer_germany_bundesliga2", "GER-L3": "soccer_germany_liga3",
    "ITA-SA": "soccer_italy_serie_a", "ITA-SB": "soccer_italy_serie_b",
    "ESP-LL": "soccer_spain_la_liga", "ESP-L2": "soccer_spain_segunda_division",
    "FRA-L1": "soccer_france_ligue_one", "FRA-L2": "soccer_france_ligue_two",
    "NED-ED": "soccer_netherlands_eredivisie", "BEL-PL": "soccer_belgium_first_div",
    "POR-PL": "soccer_portugal_primeira_liga", "TUR-SL": "soccer_turkey_super_league",
    "GRE-SL": "soccer_greece_super_league", "DEN-SL": "soccer_denmark_superliga",
    "NOR-EL": "soccer_norway_eliteserien", "SWE-AL": "soccer_sweden_allsvenskan",
    "SWE-S2": "soccer_sweden_superettan", "SUI-SL": "soccer_switzerland_superleague",
    "POL-EK": "soccer_poland_ekstraklasa", "RUS-PL": "soccer_russia_premier_league",
    "AUT-BL": "soccer_austria_bundesliga", "IRL-PD": "soccer_league_of_ireland",
    "JPN-J1": "soccer_japan_j_league", "KOR-K1": "soccer_korea_kleague1",
    "CHN-SL": "soccer_china_superleague", "MLS": "soccer_usa_mls",
    "MEX-LMX": "soccer_mexico_ligamx", "BRA-SA": "soccer_brazil_campeonato",
    "BRA-SB": "soccer_brazil_serie_b", "ARG-PD": "soccer_argentina_primera_division",
    "CHI-PD": "soccer_chile_campeonato", "SAU-PL": "soccer_saudi_arabia_pro_league",
    "UCL": "soccer_uefa_champs_league", "UEL": "soccer_uefa_europa_league",
    "UECL": "soccer_uefa_europa_conference_league",
}

# Betting exchanges quote pre-commission and are not reachable from every
# jurisdiction, so their price is not comparable to a bookmaker's. They are
# kept but marked, and never allowed to set "best" on their own.
EXCHANGES = {"Betfair", "Matchbook", "Smarkets", "Betdaq"}

_DROP = {"fc", "cf", "sc", "ac", "afc", "cd", "ud", "sd", "rcd", "rc", "as",
         "ss", "us", "calcio", "de", "fk", "bk", "if", "sk", "club", "the"}


def _key() -> str | None:
    return os.environ.get("ODDS_API_KEY") or None


def _toks(s: str) -> set:
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower()
    return {w for w in re.sub(r"[^a-z0-9 ]", " ", s).split() if w not in _DROP}


def _get(path: str, **params) -> tuple[object, dict]:
    k = _key()
    if not k:
        return None, {}
    params["apiKey"] = k
    url = f"{HOST}{path}?{urllib.parse.urlencode(params)}"
    with urllib.request.urlopen(url, timeout=45) as r:
        return json.load(r), dict(r.headers)


def _cache() -> dict:
    try:
        return json.loads(CACHE.read_text())
    except Exception:
        return {}


def fetch_league(code: str, force: bool = False) -> list:
    """Every upcoming fixture in one competition, with all EU books.

    One call per league, cached. cost = 2 credits (h2h + totals, one region)
    regardless of how many fixtures come back.
    """
    sport = SPORT.get(code)
    if not sport:
        return []
    c = _cache()
    hit = c.get(sport)
    if hit and not force and (time.time() - hit["at"]) < FRESH_MIN * 60:
        return hit["events"]
    data, hdr = _get(f"/sports/{sport}/odds/", regions=REGIONS,
                     markets="h2h,totals", oddsFormat="decimal")
    if data is None:
        return hit["events"] if hit else []
    c[sport] = {"at": time.time(), "events": data,
                "left": hdr.get("x-requests-remaining")}
    CACHE.write_text(json.dumps(c))
    return data


def find(code: str, teams: str, day: str) -> dict | None:
    """The API's event for one board fixture, or None."""
    if " v " not in teams:
        return None
    hh, aa = (x.strip() for x in teams.split(" v ", 1))
    th, ta = _toks(hh), _toks(aa)
    for ev in fetch_league(code):
        if ev.get("commence_time", "")[:10] not in (day, _shift(day, 1),
                                                    _shift(day, -1)):
            continue
        if _toks(ev["home_team"]) & th and _toks(ev["away_team"]) & ta:
            return ev
    return None


def _shift(day: str, n: int) -> str:
    from datetime import date, timedelta
    y, m, d = (int(x) for x in day.split("-"))
    return str(date(y, m, d) + timedelta(days=n))


# --- deriving a lane's price from what the books actually quote ----------

def _fit_mu(pairs: list) -> float | None:
    """The market's goal expectation, from every total line it quotes.

    Each (line, over, under) gives a vig-free P(over) and therefore its own
    implied mu. With several lines quoted the spread between them is a real
    check on the Poisson assumption, which a single 2.5 line could never
    give — so the median is taken and the spread reported by callers.
    """
    mus = []
    for line, over, under in pairs:
        if not (over and under):
            continue
        o, u = 1 / over, 1 / under
        p = o / (o + u)
        lo, hi = 0.05, 8.0
        for _ in range(50):
            mid = (lo + hi) / 2
            if market_select.p_win(f"O{line}", mid) < p:
                lo = mid
            else:
                hi = mid
        mus.append((lo + hi) / 2)
    if not mus:
        return None
    mus.sort()
    return mus[len(mus) // 2]


def _book_totals(bk: dict) -> list:
    out = []
    for m in bk.get("markets", []):
        if m["key"] != "totals":
            continue
        by = {}
        for o in m["outcomes"]:
            by.setdefault(o.get("point"), {})[o["name"]] = o["price"]
        for line, side in by.items():
            if line is not None and "Over" in side and "Under" in side:
                out.append((line, side["Over"], side["Under"]))
    return out


def _book_h2h(bk: dict, home: str, away: str) -> tuple | None:
    for m in bk.get("markets", []):
        if m["key"] != "h2h":
            continue
        by = {o["name"]: o["price"] for o in m["outcomes"]}
        h, d, a = by.get(home), by.get("Draw"), by.get(away)
        if h and d and a:
            return h, d, a
    return None


def bought(rung: str) -> str:
    """The line a printed rung is actually STRUCK at.

    Athena publishes Asian rungs; a real slip is a whole or half line.
    U4.25 is bought as U4.5, U3.0 as U3.5, O1.5 as O1.0 — the same
    mapping every ROI table in docs/ settles at. The card must quote what
    the bettor will click, not the notation the engine prints: the two
    are different bets and, on 1 Sep, differed by about 1.5% of price.
    """
    side, v = rung[0], float(rung[1:])
    if v * 2 % 1:                      # quarter line: round to the safer half
        return f"{side}{(math.ceil(v*2)/2 if side == 'U' else math.floor(v*2)/2):.1f}"
    return f"{side}{v+0.5:.1f}" if side == "U" else f"{side}{v-0.5:.1f}"


def lane_price(ev: dict, lane: str) -> dict | None:
    """What each book offers on one lane, plus consensus and best.

    `lane` is a printed rung — "U4.25", "O1.5" — or a result lane —
    "DNB1", "1X", "12". Result lanes derive exactly from that book's own
    1X2 and carry its margin; total rungs the books do not quote directly
    are priced from that book's fitted goal expectation.
    """
    home, away = ev["home_team"], ev["away_team"]
    quotes = {}
    for bk in ev.get("bookmakers", []):
        name = bk.get("title", bk.get("key", "?"))
        if re.match(r"^(DNB[12]|1X|X2|12)$", lane):
            hda = _book_h2h(bk, home, away)
            if not hda:
                continue
            h, d, a = (1 / x for x in hda)
            price = {"DNB1": (h + a) / h, "DNB2": (h + a) / a,
                     "1X": 1 / (h + d), "X2": 1 / (a + d),
                     "12": 1 / (h + a)}[lane]
        else:
            pairs = _book_totals(bk)
            # float compare, not string: f"{3.0:g}" is "3" against a
            # lane of "3.0", so every WHOLE line silently missed its own
            # quoted price and fell through to the fit.
            want = float(lane[1:])
            exact = next((p for p in pairs if abs(p[0] - want) < 1e-9), None)
            if exact:
                price = exact[1] if lane[0] == "O" else exact[2]
            else:
                mu = _fit_mu(pairs)
                if mu is None:
                    continue
                # that book's own margin, from the line it does quote
                orr = sum(1 / x for x in (pairs[0][1], pairs[0][2]))
                p = market_select.p_win(lane, mu)
                if not p:
                    continue
                price = 1 / (p * orr)
        # A derived price under evens is not a quote anyone offers — it
        # means the market treats the lane as near-certain and the fit has
        # run past the edge of what a book would post. Report the lane as
        # unpriceable rather than inventing a number below 1.01.
        if price < 1.01:
            continue
        quotes[name] = round(price, 3)
    if not quotes:
        return None
    books = {k: v for k, v in quotes.items()
             if not any(x in k for x in EXCHANGES)}
    pool = books or quotes
    best = max(pool.items(), key=lambda kv: kv[1])
    vals = sorted(pool.values())
    exch = max(((k, v) for k, v in quotes.items() if k not in books),
               key=lambda kv: kv[1], default=None)
    return {"lane": lane, "n": len(pool),
            "consensus": round(vals[len(vals) // 2], 3),
            "best": best[1], "book": best[0],
            "exchange": exch[1] if exch else None,
            "unibet_nl": quotes.get("Unibet (NL)"),
            "quotes": quotes}


QUOTES = ROOT / "config" / "odds_quotes.tsv"


def write_quotes() -> int:
    """Derive config/odds_quotes.tsv — what the market offers on every
    pending lane. The renderer reads this file and never calls the API:
    rendering must stay offline, deterministic and free.
    """
    from scripts.board import load
    rows = []
    for f in load():
        if f.settled or f.status:
            continue
        ev = find(f.code, f.teams, f.kickoff.split(" ")[0])
        if not ev:
            continue
        for which, cell in ((1, f.tip1), (2, f.tip2), (3, f.tip3)):
            c = (cell or "").strip()
            if not c or c.startswith("—"):
                continue
            # A TEAM total is not a match total. No book in this feed
            # quotes team_totals, and pricing "Bolton U1.5" off the match
            # ladder returns a number for a different bet entirely — the
            # first render of this file showed 4.05 on it. No source, no
            # quote.
            if "(team)" in c:
                continue
            m = (re.search(r"(1X|X2|12|DNB[12])", c) if which == 3
                 else re.search(r"(?:^|[^A-Za-z])([OU]\d+(?:\.\d+)?)", c))
            if not m:
                continue
            # Quote the line the bettor will actually strike, not the
            # rung Athena prints. Result lanes are already real markets.
            rung = m.group(1)
            want = rung if which == 3 else bought(rung)
            q = lane_price(ev, want)
            if not q:
                continue
            rows.append((f.teams, str(which), q["lane"], f"{q['consensus']:.2f}",
                         f"{q['best']:.2f}", q["book"],
                         f"{q['unibet_nl']:.2f}" if q["unibet_nl"] else "",
                         str(q["n"])))
    head = ["# What the market is offering on each pending lane. Derived by",
            "# scripts/odds_api.py --quotes from live bookmaker prices; the",
            "# renderer reads it and never calls the API itself. Delete it and",
            "# the cards fall back to the engine's own buy>= bar.",
            "# fixture\twhich\tlane\tconsensus\tbest\tbook\tunibet_nl\tbooks"]
    QUOTES.write_text("\n".join(head + ["\t".join(r) for r in rows]) + "\n")
    return len(rows)


def usage() -> dict:
    _d, hdr = _get("/sports/")
    return {k: v for k, v in hdr.items() if k.lower().startswith("x-requests")}


def main() -> None:
    args = sys.argv[1:]
    if not _key():
        print("ODDS_API_KEY not set — nothing to do", file=sys.stderr)
        return
    if "--usage" in args:
        print(usage())
        return
    if "--leagues" in args:
        from scripts.board import load
        codes = sorted({f.code for f in load()})
        have = [c for c in codes if c in SPORT]
        miss = [c for c in codes if c not in SPORT]
        print(f"{len(have)} of {len(codes)} board competitions map to the API")
        print("  mapped :", ", ".join(have))
        print("  no feed:", ", ".join(miss))
        return
    if "--quotes" in args:
        n = write_quotes()
        print(f"{n} lanes quoted -> {QUOTES}")
        return
    if "--board" in args:
        from scripts.board import load
        pend = [f for f in load() if not f.settled and not f.status]
        seen = {}
        for f in pend:
            ev = find(f.code, f.teams, f.kickoff.split(" ")[0])
            if not ev:
                continue
            for which, cell in ((1, f.tip1), (3, f.tip3)):
                c = (cell or "").strip()
                if not c or c.startswith("—"):
                    continue
                m = (re.search(r"(1X|X2|12|DNB[12])", c) if which == 3
                     else re.search(r"(?:^|[^A-Za-z])([OU]\d+(?:\.\d+)?)", c))
                if not m:
                    continue
                q = lane_price(ev, m.group(1))
                if not q:
                    continue
                buy = re.search(r"buy≥\s*(\d+(?:\.\d+)?)", c)
                seen.setdefault(f.teams, []).append((which, q, buy and float(buy.group(1))))
        for teams, qs in seen.items():
            print(f"\n{teams}")
            for which, q, buy in qs:
                u = f"  Unibet(NL) {q['unibet_nl']}" if q["unibet_nl"] else ""
                b = f"  buy≥{buy}" if buy else ""
                print(f"  tip {which} {q['lane']:6} consensus {q['consensus']:5.2f}"
                      f"   best {q['best']:5.2f} ({q['book']}){u}{b}")
        return
    print(__doc__)


if __name__ == "__main__":
    main()
