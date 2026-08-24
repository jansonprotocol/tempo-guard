"""
Stamp each league's measured form into the fixture tables' League column.

    | — | LaLiga (80.5 −0.1) | Osasuna v Levante | ...

Two numbers, from `config/league_hitrates.tsv`: the league's Tip 1 hit rate
over its most recent 200 replayed matches, and the GAP between that and what
the engine claimed. The gap is the one to read before trusting a row — a
league at (70.7 −11.6) is telling you its probabilities are broken however
pretty the tip looks, and a league at (82.0 +5.3) under-claims.

The badge is DERIVED, same rule as every number on the page: the tsv is
written from a stored replay run, this script stamps it, and re-running both
refreshes every row. A league with no entry (cups, mostly) is left bare
rather than given a guess.

    python scripts/league_badges.py            stamp the tables from the tsv
    python scripts/league_badges.py --check    exit 1 if stale, change nothing

Run BEFORE scripts/playable.py — the block copies League cells verbatim from
the tables, so it inherits the badges on its next rebuild.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
RATES = ROOT / "config" / "league_hitrates.tsv"

# Display name in the tables -> store code in the tsv. Cups are absent on
# purpose: their path is under separate investigation and a badge would lend
# it a credibility it has not earned.
NAMES = {
    "Premier League": "ENG-PL", "Championship": "ENG-CH",
    "League One": "ENG-L1", "League Two": "ENG-L2", "National League": "ENG-NL",
    "LaLiga": "ESP-LL", "LaLiga 2": "ESP-L2",
    "Serie A": "ITA-SA", "Serie B": "ITA-SB",
    "Bundesliga": "GER-BL", "2. Bundesliga": "GER-B2",
    "Ligue 1": "FRA-L1", "Ligue 2": "FRA-L2",
    "Eredivisie": "NED-ED", "Liga Portugal": "POR-PL",
    "Belgian Pro League": "BEL-PL", "Trendyol Süper Lig": "TUR-SL",
    "Scottish Premiership": "SCO-PL", "Scottish Championship": "SCO-CH",
    "Danish Superliga": "DEN-SL", "Allsvenskan": "SWE-AL",
    "Eliteserien": "NOR-EL", "Ekstraklasa": "POL-EK",
    "J1 League": "JPN-J1", "Chinese Super League": "CHN-SL",
    "Brasileirão": "BRA-SA", "Brasileirão Série B": "BRA-SB",
    "Saudi Pro League": "SAU-PL", "MLS": "MLS",
    "Liga MX": "MEX-LMX", "Liga de Primera": "CHI-PD",
    "Categoría Primera A": "COL-PA", "Peruvian Liga 1": "PER-L1",
    "Argentine Primera": "ARG-PD", "Russian Premier League": "RUS-PL",
    "Ukrainian Premier League": "UKR-PL", "Swiss Super League": "SUI-SL",
    "Austrian Bundesliga": "AUT-BL", "Greek Super League": "GRE-SL",
    "Czech First League": "CZE-FL", "Croatian First League": "CRO-1L",
    "Eerste Divisie": "NED-D2",   # the SECOND tier — NED-ED is the Eredivisie
}

# An existing badge, so re-stamping replaces rather than stacks.
BADGE = re.compile(r" \(\d+\.\d [+\-−]\d+\.\d\)$")

HEADERS = ("| Live | League | Teams | Tip 1 | Tip 2 | Kickoff |",
           "| Result | League | Teams | Tip 1 | Tip 2 | Kickoff |")


def rates() -> dict[str, str]:
    out = {}
    for ln in RATES.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        code, _n, hit, gap = ln.split("\t")
        out[code] = f"({hit} {gap.replace('-', '−')})"
    return out


def stamp(text: str) -> str:
    by_code = rates()
    # Scope: from the first table header to the placed-bets heading, so the
    # playable block above (whose cells are COPIES) is never edited directly.
    start = text.index(HEADERS[0])
    end = text.index("### Actual placed bets", start)
    head, body, tail = text[:start], text[start:end], text[end:]

    # split("\n"), not splitlines(): the body ends mid-blank-line and the
    # round trip must be byte-exact or --check reports itself stale.
    lines = []
    for ln in body.split("\n"):
        if ln.startswith("|") and ln.count("|") == 7 and "---" not in ln:
            c = ln.split("|")
            league = BADGE.sub("", c[2].strip())
            code = NAMES.get(league)
            if code and code in by_code:
                c[2] = f" {league} {by_code[code]} "
                ln = "|".join(c)
        lines.append(ln)
    return head + "\n".join(lines) + tail


def main() -> None:
    text = README.read_text()
    new = stamp(text)
    if "--check" in sys.argv:
        if new != text:
            print("League badges are STALE. Run: python scripts/league_badges.py")
            sys.exit(1)
        print("league badges match the stored run")
        return
    if new == text:
        print("league badges already current")
        return
    README.write_text(new)
    print("league badges stamped; now run scripts/playable.py")


if __name__ == "__main__":
    main()
