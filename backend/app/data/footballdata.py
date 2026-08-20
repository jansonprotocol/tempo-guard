"""
football-data.co.uk provider — current results and real match statistics.

Complements the openfootball source, which is excellent for history and breadth
but auto-updates weekly and runs weeks behind on in-progress seasons. This
source publishes within hours of a match finishing, and carries measured shot
counts rather than the goal-derived estimates the engine falls back on.

NO BOOKMAKER DATA IS INGESTED
=============================
These files carry ~130 columns, of which roughly 108 are bookmaker prices. This
parser reads a strict allowlist of football columns and discards every odds
column at parse time — they are never stored, never reach a feature, and never
influence a prediction.

That is a deliberate design choice, not an oversight. Odds encode the market's
own forecast; feeding them in would make ATHENA partly a market-follower, and
any apparent "edge" would just be the bookmakers' opinion echoed back. The
engine forms its view from football alone.

(The trade-off is worth stating plainly: without prices, profitability can only
ever be *modelled* from goal distributions, not measured against what a bet
would actually have paid.)

TWO FILE LAYOUTS
================
Main leagues — one file per league-season, richest statistics:
    https://www.football-data.co.uk/mmz4281/2526/E0.csv
    Div,Date,Time,HomeTeam,AwayTeam,FTHG,FTAG,FTR,HTHG,HTAG,HTR,Referee,
    HS,AS,HST,AST,HF,AF,HC,AC,HY,AY,HR,AR,<108 odds columns>

Extra leagues — one file covering all seasons, goals only:
    https://www.football-data.co.uk/new/CHN.csv
    Country,League,Season,Date,Time,Home,Away,HG,AG,Res,<odds columns>

Both are normalised to the store's schema so downstream code cannot tell which
source a match came from.
"""
from __future__ import annotations

import csv
import io
from datetime import datetime
from typing import Iterable, Optional

import pandas as pd
import requests

BASE_URL = "https://www.football-data.co.uk"

# ── Column allowlists ─────────────────────────────────────────────────────────
# Everything not named here is dropped, which is what keeps odds out. Adding a
# column means deliberately choosing to ingest it.

# Main-league layout.
_MAIN_COLUMNS = {
    "Date": "date", "Time": "time",
    "HomeTeam": "home", "AwayTeam": "away",
    "FTHG": "hg", "FTAG": "ag",
    "HTHG": "hthg", "HTAG": "htag",
    "HS": "hs", "AS": "as_", "HST": "hst", "AST": "ast",
    "HC": "hc", "AC": "ac",
    "HF": "hf", "AF": "af",
    "HY": "hy", "AY": "ay", "HR": "hr", "AR": "ar",
    "Referee": "referee",
}

# Extra-league layout (goals only).
_EXTRA_COLUMNS = {
    "Date": "date", "Time": "time",
    "Home": "home", "Away": "away",
    "HG": "hg", "AG": "ag",
    "Season": "season_raw", "League": "league_raw", "Country": "country",
}

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "ATHENA-TempoGuard/3.0"
_TIMEOUT = 45


def _fetch(url: str) -> Optional[str]:
    try:
        resp = _SESSION.get(url, timeout=_TIMEOUT)
    except requests.RequestException:
        return None
    if resp.status_code != 200 or not resp.content:
        return None
    # These files are Windows-encoded and inconsistently so; be forgiving.
    return resp.content.decode("utf-8-sig", errors="replace")


def _parse_date(value: str) -> Optional[pd.Timestamp]:
    """Dates appear as DD/MM/YYYY, and as DD/MM/YY in older files."""
    value = (value or "").strip()
    for fmt in ("%d/%m/%Y", "%d/%m/%y"):
        try:
            return pd.Timestamp(datetime.strptime(value, fmt))
        except ValueError:
            continue
    return None


def _to_int(value: str) -> Optional[int]:
    try:
        return int(float(str(value).strip()))
    except (ValueError, TypeError):
        return None


def _rows_to_frame(rows: list[dict], mapping: dict[str, str]) -> pd.DataFrame:
    """Normalise raw CSV rows into the store schema, dropping unmapped columns."""
    out = []
    int_fields = {"hg", "ag", "hthg", "htag", "hs", "as_", "hst", "ast",
                  "hc", "ac", "hf", "af", "hy", "ay", "hr", "ar"}

    for raw in rows:
        rec: dict = {}
        for src_col, dest in mapping.items():
            if src_col not in raw:
                continue
            val = raw[src_col]
            if dest == "date":
                rec["date"] = _parse_date(val)
            elif dest in int_fields:
                rec[dest] = _to_int(val)
            else:
                rec[dest] = (val or "").strip() or None

        if rec.get("date") is None or not rec.get("home") or not rec.get("away"):
            continue
        # A row with no score is a fixture the file lists but has not played.
        rec["status"] = "result" if rec.get("hg") is not None and rec.get("ag") is not None else "fixture"
        rec.setdefault("stage", "")
        out.append(rec)

    df = pd.DataFrame(out)
    if df.empty:
        return df
    return df.sort_values("date").reset_index(drop=True)


def _read_csv(text: str) -> list[dict]:
    reader = csv.DictReader(io.StringIO(text))
    return [r for r in reader if r]


# ── Public API ────────────────────────────────────────────────────────────────

def fetch_main(div: str, season: str) -> pd.DataFrame:
    """
    Fetch one main-league season.

    div    football-data division code, e.g. "E0", "SP1", "D1", "I1", "F1".
    season four-digit season key, e.g. "2526" for 2025-26.
    """
    text = _fetch(f"{BASE_URL}/mmz4281/{season}/{div}.csv")
    if text is None:
        return pd.DataFrame()
    return _rows_to_frame(_read_csv(text), _MAIN_COLUMNS)


def fetch_extra(country_code: str, season: Optional[str] = None) -> pd.DataFrame:
    """
    Fetch an extra-league file (all seasons in one file), optionally filtered.

    country_code e.g. "CHN", "JPN", "BRA", "ARG", "MEX", "USA", "RUS".
    season       e.g. "2026" — matches the file's own Season column.
    """
    text = _fetch(f"{BASE_URL}/new/{country_code}.csv")
    if text is None:
        return pd.DataFrame()

    df = _rows_to_frame(_read_csv(text), _EXTRA_COLUMNS)
    if df.empty:
        return df
    if season is not None and "season_raw" in df.columns:
        df = df[df["season_raw"].astype(str) == str(season)].reset_index(drop=True)
    return df


def season_key(start_year: int) -> str:
    """2025 -> '2526', the form main-league URLs use."""
    return f"{str(start_year)[2:]}{str(start_year + 1)[2:]}"


def assert_no_odds(df: pd.DataFrame) -> None:
    """
    Guard: fail loudly if a bookmaker column ever reaches a stored frame.

    Cheap insurance for a property that is easy to break by accident and hard
    to notice once broken.
    """
    suspects = [
        c for c in df.columns
        if any(tag in c.upper() for tag in
               ("B365", "BW", "IW", "PS", "WH", "VC", "MAX", "AVG", "ODD",
                "BF", "BMGM", "BV", "1XB", "GB", "LB", "SB", "SJ", "SY"))
    ]
    if suspects:
        raise ValueError(f"Bookmaker columns must not be stored: {suspects}")
