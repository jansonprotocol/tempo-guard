"""
ESPN scoreboard reader — the third source, for competitions the other two drop.

WHY THIS EXISTS
===============
Brazilian Série B is the case that forced it. openfootball publishes the
fixture list and stopped filling in results in May 2025, so its file trails off
into unplayed matches; football-data.co.uk covers Brazil but only Série A. Both
sources are dead ends for the same competition, and no amount of config fixes
that.

ESPN's public scoreboard covers it, needs no key, returns a whole season in one
request, and carries shots on target on every finished match back to 2019 —
which openfootball never provided for Brazil at all. So this is an upgrade for
the league rather than a patch: it restores the competition *and* feeds the
shot-conversion blend that was previously inert there.

ODDS ARE NOT READ
=================
The scoreboard payload contains an `odds` block. It is never parsed, and
`assert_no_odds` fails loudly if a bookmaker-derived column ever reaches a
frame, mirroring the guard on the football-data reader. The engine's premise is
that it prices matches from football alone; a market line leaking into the
feature set would quietly turn it into a bookmaker-follower and the hit rate
would look fine while it happened.

NAMING: REPLACE, DO NOT MERGE
=============================
A league sourced here is sourced here entirely, rather than being stitched onto
whatever openfootball left behind. ESPN says "Vila Nova" where openfootball says
"Grêmio Novorizontino SP", and merging two naming conventions by fuzzy match is
precisely what once turned England's 9,880 stored matches into 17,616. One
source per competition, no reconciliation, no duplicates.

The cost is the 2018 Série B season, which ESPN does not carry. Eight seasons
with shot data beat nine without.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import pandas as pd
import requests

SCOREBOARD = "https://site.api.espn.com/apis/site/v2/sports/soccer/{code}/scoreboard"

# Generous, since a season arrives in a single call.
TIMEOUT = 90
LIMIT = 1000

# Only these are read off a competitor. Anything else in the payload —
# including the odds block — is ignored by construction rather than by
# filtering afterwards.
_STAT_MAP = {
    "shotsOnTarget": "st",
    "totalShots": "s",
    "wonCorners": "c",
    "foulsCommitted": "f",
}

# Statuses that count as a completed 90 minutes.
#
# STATUS_FINAL_PEN has to be here, and missing it would have been a silent
# disaster. Japan's 2026 restructure sends drawn league matches to a shootout,
# so 51 of its 2026 fixtures carry that status — and they are *precisely the
# draws*. Accepting only STATUS_FULL_TIME would have dropped every drawn match
# from the league and skewed its goal distribution with nothing looking wrong.
# The score field holds the regulation result; the shootout lives in `notes`
# and is never read.
#
# STATUS_FINAL_AET is deliberately excluded. Its score includes extra-time
# goals, and totals markets settle on 90 minutes, so those rows would be
# graded against a number the bet never used.
_FINISHED = {"STATUS_FULL_TIME", "STATUS_FINAL_PEN"}

# Columns a frame from here may contain. Mirrors the football-data allowlist.
COLUMNS = [
    "date", "home", "away", "hg", "ag",
    "hs", "as_", "hst", "ast", "hc", "ac", "hf", "af",
    "season", "league_code", "country", "status",
]

_ODDS_HINTS = ("odd", "b365", "bw", "iw", "ps", "wh", "vc", "max", "avg",
               "ahh", "aha", "line", "price")


def assert_no_odds(df: pd.DataFrame) -> None:
    """
    Fail loudly if anything bookmaker-shaped reached the frame.

    Cheap to run and the failure it guards against is silent: odds are
    excellent predictors, so a leak would raise the hit rate and hide itself in
    the improvement.
    """
    bad = [c for c in df.columns
           if any(h in c.lower() for h in _ODDS_HINTS)]
    if bad:
        raise ValueError(f"odds-derived columns must never be loaded: {bad}")


def _stats(competitor: dict) -> dict:
    out: dict[str, Optional[float]] = {v: None for v in _STAT_MAP.values()}
    for s in competitor.get("statistics") or []:
        key = _STAT_MAP.get(s.get("name"))
        if key is None:
            continue
        try:
            out[key] = float(s.get("displayValue"))
        except (TypeError, ValueError):
            out[key] = None
    return out


def fetch_season(espn_code: str, year: int, league_code: str,
                 country: str = "", calendar_year: bool = True) -> pd.DataFrame:
    """
    One season of a competition, finished matches only.

    `calendar_year` decides what a season *is*. Brazil and MLS run January to
    December, so the year is the season. Europe runs August to May, and
    fetching those as a calendar year splices the back half of one season onto
    the front half of the next — the rows all exist and every season label is
    wrong, which is worse than missing data because nothing looks broken.

    Unplayed fixtures are dropped rather than stored with null scores: the
    store holds results, and a fixture row with no score is what made
    openfootball's Série B file look complete when it was not.
    """
    span = (f"{year}0101-{year}1231" if calendar_year
            else f"{year}0701-{year + 1}0630")
    r = requests.get(
        SCOREBOARD.format(code=espn_code),
        params={"dates": span, "limit": LIMIT},
        timeout=TIMEOUT,
    )
    r.raise_for_status()
    events = r.json().get("events") or []

    rows = []
    for ev in events:
        comps = ev.get("competitions") or []
        if not comps:
            continue
        c = comps[0]
        if c.get("status", {}).get("type", {}).get("name") not in _FINISHED:
            continue

        sides = {x.get("homeAway"): x for x in c.get("competitors") or []}
        h, a = sides.get("home"), sides.get("away")
        if not h or not a:
            continue
        try:
            hg, ag = int(h["score"]), int(a["score"])
        except (KeyError, TypeError, ValueError):
            continue

        hs, as_ = _stats(h), _stats(a)
        rows.append({
            "date": datetime.strptime(c["date"][:10], "%Y-%m-%d"),
            "home": h["team"]["displayName"],
            "away": a["team"]["displayName"],
            "hg": hg, "ag": ag,
            "hs": hs["s"], "as_": as_["s"],
            "hst": hs["st"], "ast": as_["st"],
            "hc": hs["c"], "ac": as_["c"],
            "hf": hs["f"], "af": as_["f"],
            "season": str(year) if calendar_year else f"{year}-{str(year + 1)[2:]}",
            "league_code": league_code,
            "country": country,
            "status": "FT",
        })

    df = pd.DataFrame(rows, columns=COLUMNS)
    assert_no_odds(df)
    return df.sort_values("date").reset_index(drop=True) if len(df) else df


def fetch_seasons(espn_code: str, years, league_code: str,
                  country: str = "", progress=None,
                  calendar_year: bool = True) -> pd.DataFrame:
    """Several seasons, concatenated. A failed year is skipped, not fatal."""
    say = progress or (lambda _m: None)
    frames = []
    for y in years:
        try:
            df = fetch_season(espn_code, y, league_code, country, calendar_year)
        except Exception as exc:
            say(f"  {league_code} {y}: failed ({exc})")
            continue
        if len(df):
            frames.append(df)
            say(f"  {league_code} {y}: {len(df)} matches")
        else:
            say(f"  {league_code} {y}: none")
    if not frames:
        return pd.DataFrame(columns=COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    assert_no_odds(out)
    return out.sort_values("date").reset_index(drop=True)
