"""
Parser for openfootball plain-text match datasets.

openfootball (github.com/openfootball) publishes football results and fixtures
as human-readable .txt files. Two layouts appear across the repos, and this
parser handles both:

  Format A — score in the middle (england, deutschland, espana, italy, france):
      Fri Aug 15 2025
        19:00   Liverpool  4-2 (1-0)  Bournemouth
                    (Hugo EKITIKE 37', Cody GAKPO 49')

  Format B — "v" separator, score at the end (europe repo, champions-league,
  and all upcoming-season fixture files):
        18:45  Athletic Club (ESP)     v Arsenal FC (ENG)         0-2 (0-0)
        20:00  Arsenal FC              v Coventry City FC            <- fixture

NOTES ON CORRECTNESS
====================
90-MINUTE SCORES. Over/under markets settle on regulation time, so knockout
lines carrying extra time or penalties must NOT be read at face value:

    Juventus v Galatasaray   3-2 a.e.t. (3-0, 1-0)
        -> 3-2 after extra time, 3-0 at 90', 1-0 at half time.  We take 3-0.

    PSG v Arsenal   4-3 pen. 1-1 a.e.t. (1-1, 0-1)
        -> 4-3 on penalties, 1-1 a.e.t., 1-1 at 90', 0-1 at HT.  We take 1-1.

When a.e.t. is present the first parenthesised pair is the 90-minute score and
the second is half time. Otherwise the single parenthesised pair is half time.

DATE / TIME INHERITANCE. Date headers omit the year after the first occurrence
("Sat Aug 16"), and fixtures listed under an earlier kick-off time repeat that
time implicitly. Both are carried forward while parsing.

Output is a DataFrame with the column names the ATHENA feature layer expects:
    date, home, away, hg, ag, hthg, htag, status, stage, country
`status` is "result" for played matches and "fixture" for scheduled ones.
"""
from __future__ import annotations

import re
from datetime import date, datetime
from pathlib import Path
from typing import Iterator, Optional

import pandas as pd

# ── Line classification ───────────────────────────────────────────────────────

# "= England | Premier League 2025/26"  — title line
_RE_TITLE = re.compile(r"^\s*=\s*(.+?)\s*$")

# "▪ Regular Season - 1" / "▪ League, Matchday 1" / "▪ Matchday 1"
_RE_STAGE = re.compile(r"^\s*▪\s*(.+?)\s*$")

# "Fri Aug 15 2025" or "Sat Aug 16"  (year optional — inherited when absent)
_RE_DATE = re.compile(
    r"^\s*(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+"
    r"(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+"
    r"(\d{1,2})(?:\s+(\d{4}))?\s*$",
    re.IGNORECASE,
)

# Goalscorer continuation lines start with "(" after indentation.
_RE_SCORERS = re.compile(r"^\s*\(")

# Comment / metadata lines
_RE_META = re.compile(r"^\s*#")

_MONTHS = {
    "jan": 1, "feb": 2, "mar": 3, "apr": 4, "may": 5, "jun": 6,
    "jul": 7, "aug": 8, "sep": 9, "oct": 10, "nov": 11, "dec": 12,
}

# Optional leading kick-off time.
_TIME = r"(?:(?P<time>\d{1,2}:\d{2})\s+)?"

# Score block. Captures the *reported* score plus any parenthesised pairs.
# Examples matched:
#   "4-2 (1-0)"                      -> 4-2, HT 1-0
#   "0-0"                            -> 0-0, no HT
#   "3-2 a.e.t. (3-0, 1-0)"          -> aet, 90' 3-0, HT 1-0
#   "4-3 pen. 1-1 a.e.t. (1-1, 0-1)" -> pens, 90' 1-1, HT 0-1
_SCORE = (
    r"(?P<pen>\d+-\d+\s*pen\.\s*)?"
    r"(?P<score>\d+-\d+)"
    r"(?P<aet>\s*a\.e\.t\.)?"
    r"(?:\s*\((?P<p1>\d+-\d+)(?:\s*,\s*(?P<p2>\d+-\d+))?\))?"
)

# Format B: "Home  v Away   [score]"
_RE_MATCH_V = re.compile(
    r"^\s*" + _TIME +
    r"(?P<home>.+?)\s+v(?:s)?\.?\s+(?P<away>.+?)"
    r"(?:\s{2,}" + _SCORE + r")?\s*$"
)

# Format A: "Home  score  Away"
_RE_MATCH_MID = re.compile(
    r"^\s*" + _TIME +
    r"(?P<home>.+?)\s{2,}" + _SCORE + r"\s{2,}(?P<away>.+?)\s*$"
)

# Trailing country code on cup team names: "Arsenal FC (ENG)"
_RE_COUNTRY = re.compile(r"\s*\(([A-Z]{3})\)\s*$")

# Status annotations that trail a fixture line, e.g.
#   "Rangers FC   v St. Johnstone FC   [cancelled]"
#   "Tottenham Hotspur (ENG) v Stade Rennais (FRA)   0-3   [awarded]"
# "awarded" matches are administrative forfeits (typically 0-3); their goals are
# not real football outcomes, so they are flagged and excluded from features by
# default rather than corrupting goal distributions.
_RE_ANNOTATION = re.compile(
    r"\s*\[(cancelled|abandoned|postponed|awarded|replay)\]\s*", re.IGNORECASE
)
# "3-0 awd." — the awarded-score spelling used in some datasets.
_RE_AWD = re.compile(r"\s*awd\.\s*", re.IGNORECASE)


def _split_score(text: Optional[str]) -> tuple[Optional[int], Optional[int]]:
    if not text:
        return None, None
    a, b = text.split("-", 1)
    return int(a), int(b)


def _clean_team(name: str) -> tuple[str, Optional[str]]:
    """Strip a trailing country code, returning (team_name, country_or_None)."""
    name = name.strip()
    m = _RE_COUNTRY.search(name)
    country = None
    if m:
        country = m.group(1)
        name = name[: m.start()].strip()
    return name, country


def _regulation_score(m: re.Match) -> tuple[Optional[int], Optional[int],
                                             Optional[int], Optional[int]]:
    """
    Resolve a match's 90-minute and half-time goals from a score match object.

    Returns (hg, ag, hthg, htag) where hg/ag are the REGULATION (90') goals —
    the figures over/under markets settle on.
    """
    aet = bool(m.group("aet"))
    p1 = m.group("p1")
    p2 = m.group("p2")

    if aet:
        # "3-2 a.e.t. (3-0, 1-0)" -> 90' is p1, HT is p2.
        # If the 90' pair is missing we cannot recover it; drop to None so the
        # match is excluded rather than silently scored on extra time.
        hg, ag = _split_score(p1)
        hthg, htag = _split_score(p2)
    else:
        hg, ag = _split_score(m.group("score"))
        hthg, htag = _split_score(p1)

    return hg, ag, hthg, htag


def _parse_date(line: str, current_year: Optional[int]) -> Optional[date]:
    m = _RE_DATE.match(line)
    if not m:
        return None
    month = _MONTHS[m.group(1).lower()[:3]]
    day = int(m.group(2))
    year = int(m.group(3)) if m.group(3) else current_year
    if year is None:
        return None
    try:
        return date(year, month, day)
    except ValueError:
        return None


def parse_text(content: str) -> pd.DataFrame:
    """Parse an openfootball .txt file body into a match DataFrame."""
    rows: list[dict] = []
    cur_date: Optional[date] = None
    cur_year: Optional[int] = None
    cur_stage: str = ""

    for raw in content.splitlines():
        line = raw.rstrip()
        if not line.strip() or _RE_META.match(line) or _RE_SCORERS.match(line):
            continue

        if _RE_TITLE.match(line) and "|" in line or line.strip().startswith("="):
            # Title may carry the season ("... 2025/26"); use it to seed the year
            season = re.search(r"(\d{4})/(\d{2,4})", line)
            if season:
                cur_year = int(season.group(1))
            continue

        stage = _RE_STAGE.match(line)
        if stage:
            cur_stage = stage.group(1)
            continue

        d = _parse_date(line, cur_year)
        if d is not None:
            cur_date = d
            cur_year = d.year
            continue

        # Strip any trailing status annotation before match parsing so it does
        # not leak into the away-team name.
        annotation = None
        ann = _RE_ANNOTATION.search(line)
        if ann:
            annotation = ann.group(1).lower()
            line = _RE_ANNOTATION.sub(" ", line).rstrip()
        if _RE_AWD.search(line):
            annotation = annotation or "awarded"
            line = _RE_AWD.sub(" ", line)

        # Match lines. Try the "v" layout first: it is unambiguous, whereas the
        # middle-score regex can mis-split names containing digits.
        m = _RE_MATCH_V.match(line)
        layout = "v"
        if not m:
            m = _RE_MATCH_MID.match(line)
            layout = "mid"
        if not m or cur_date is None:
            continue

        home, home_country = _clean_team(m.group("home"))
        away, away_country = _clean_team(m.group("away"))
        if not home or not away:
            continue

        has_score = bool(m.group("score"))
        if has_score:
            hg, ag, hthg, htag = _regulation_score(m)
        else:
            hg = ag = hthg = htag = None

        # Status: a played match is a "result"; an annotated one keeps its
        # annotation (cancelled/awarded/...) so callers can filter deliberately;
        # anything else with no score is an upcoming fixture.
        if annotation:
            status = annotation
        elif hg is not None:
            status = "result"
        else:
            status = "fixture"

        rows.append({
            "date": cur_date,
            "home": home,
            "away": away,
            "hg": hg,
            "ag": ag,
            "hthg": hthg,
            "htag": htag,
            "status": status,
            "stage": cur_stage,
            "country": home_country or away_country,
            "layout": layout,
        })

    df = pd.DataFrame(rows)
    if df.empty:
        return df
    df["date"] = pd.to_datetime(df["date"])
    return df.drop(columns=["layout"]).sort_values("date").reset_index(drop=True)


def parse_file(path: str | Path) -> pd.DataFrame:
    """Parse an openfootball .txt file from disk."""
    text = Path(path).read_text(encoding="utf-8", errors="replace")
    return parse_text(text)
