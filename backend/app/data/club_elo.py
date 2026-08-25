"""
The cup lane's strength source: Club Elo, as-of, from a committed snapshot.

This is the first external data the engine predicts from, and the boundary
is drawn tightly on purpose:

  - results-derived only. Elo is computed from match results
    (clubelo.com); no odds, no market information — the founding
    constraint holds.
  - cups only. Domestic leagues never touch this module; their features
    come from the store alone, exactly as before.
  - committed, not fetched. Predictions read config/club_elo.parquet
    (860 clubs, daily 2023-03 → 2026-01, trimmed from the
    tonyelhabr/club-rankings mirror of clubelo.com) and
    config/club_elo_names.json (380 store-name → clubelo-name mappings).
    A refresh is a reviewed commit, never a network call at predict time.

The model is the one that passed every window cup_elo.py measured:

    mu = rolling_3y_competition_base
       + intercept                     tracked on the trailing 180 days
       + B1 * |elo_home − elo_away|    per 100 Elo
       + B2 * (elo_home + elo_away)

Slopes are FROZEN from the pooled Swiss-era fit (2024-07 → 2026-01,
1,563 fixtures). The intercept is the part that drifts between seasons
(−0.83 vs −0.36 measured), so it is refit monthly from trailing residuals
— the "walked" shape that went −1.8 / −2.4 on the two seasons and +4.2 on
the 202-fixture out-of-sample dress rehearsal (Jan–May 2026 knockouts on
Elo frozen at Jan 14).

Staleness: Elo lagged 60 days graded identically to fresh, and the dress
rehearsal ran on ratings up to seven months old. MAX_STALE_DAYS = 400
abstains when a club's newest rating is more than a season out of date —
at that point the number describes a different squad.
"""
from __future__ import annotations

import json
from datetime import date, datetime
from functools import lru_cache
from pathlib import Path
from typing import Optional

import pandas as pd

from app.data import store

CUPS = ("UCL", "UEL", "UECL", "UCL-Q", "UEL-Q", "UECL-Q")

# Pooled Swiss-era fit, scripts/cup_elo.py, 25 Aug 2026. Per 100 Elo.
B1 = 0.154
B2 = 0.015
B0_FALLBACK = -0.661          # pooled intercept, used until 100 trailing rows

# Cup OVER tips state ~3.5 points more than they deliver. Measured on the
# wired live path over both Swiss seasons (1,878 tips, 26 Aug): O1.5 ran
# -3.3 in 24-25 and -3.7 in 25-26, and the miss is FLAT across probability
# bands (-2.9 / -3.9 / -3.8 low to high) — a level bias, not a tail or a
# season. The under rungs are calibrated (U4.25 -0.6, U3.0 +2.2) and are
# left untouched; a mu-level fix would trade their calibration away, which
# is why this is a stated-probability debit on the over family only. A
# GLOBAL says-debit was considered earlier and dropped — across the five
# offline windows the overall gap wobbles +/-4 with no direction — but the
# rung-level cut is uniform in both seasons, which is the two-window bar.
OVER_SAYS_DEBIT = 0.035


def stated_p(league_code: str, market: str, p: float) -> float:
    """The probability to PUBLISH for a tip: cup over rungs read hot by a
    measured, stable 3.5 points, so their stated number carries the debit.
    Everything else — cup unders, every domestic market — passes through."""
    if league_code in CUPS and market.split()[-1].startswith("O"):
        return max(0.0, p - OVER_SAYS_DEBIT)
    return p

SCALE = 100.0
MAX_STALE_DAYS = 400
_TRAIL_DAYS = 180
_MIN_TRAIL_ROWS = 100

_CONFIG_DIR = Path(__file__).resolve().parents[3] / "config"


@lru_cache(maxsize=1)
def _names() -> dict:
    return json.loads((_CONFIG_DIR / "club_elo_names.json").read_text())


_TR = str.maketrans({"ø": "o", "Ø": "o", "ð": "d", "Ð": "d", "þ": "th",
                     "æ": "ae", "Æ": "ae", "ł": "l", "Ł": "l", "đ": "d",
                     "ı": "i", "ß": "ss"})
_STOP = {"fc", "fk", "bk", "if", "sk", "ks", "nk", "cf", "sc", "ac", "afc",
         "cfr", "pfc", "fci", "ksv", "kf", "club", "cp"}


def _norm(s: str) -> str:
    import unicodedata
    s = s.translate(_TR)
    s = unicodedata.normalize("NFKD", s).encode("ascii", "ignore").decode()
    for ch in ".-'/()":
        s = s.replace(ch, " ")
    return " ".join(t for t in s.lower().split() if t not in _STOP)


@lru_cache(maxsize=1)
def _norm_index() -> dict:
    """Normalized store-name -> clubelo name, so a fixture typed as
    "Bayern München" still finds the mapping keyed "FC Bayern München"."""
    return {_norm(k): v for k, v in _names().items()}


@lru_cache(maxsize=1)
def _series() -> dict:
    df = pd.read_parquet(_CONFIG_DIR / "club_elo.parquet")
    out = {}
    for club, g in df.groupby("Club", observed=True):
        out[str(club)] = (list(g["date"]), list(g["Elo"]))
    return out


def _cutoff(match_date: date) -> pd.Timestamp:
    return pd.Timestamp(datetime.combine(match_date, datetime.min.time()))


def elo_asof(store_name: str, when: pd.Timestamp) -> Optional[float]:
    """The club's Elo strictly before `when`, or None when unmapped,
    absent, or staler than MAX_STALE_DAYS."""
    club = _names().get(store_name) or _norm_index().get(_norm(store_name))
    if club is None:
        return None
    ser = _series().get(club)
    if ser is None:
        return None
    dates, elos = ser
    import bisect
    i = bisect.bisect_left(dates, when)
    if i == 0:
        return None
    if (when - dates[i - 1]).days > MAX_STALE_DAYS:
        return None
    return float(elos[i - 1])


@lru_cache(maxsize=32)
def _frame(code: str) -> Optional[pd.DataFrame]:
    df = store.load_results(code)
    if df is None or df.empty:
        return None
    return df.dropna(subset=["hg", "ag"]).sort_values("date")


def rolling_base(code: str, when: pd.Timestamp,
                 fallback: Optional[float]) -> Optional[float]:
    """Trailing three-year competition mean, the instruments' definition."""
    df = _frame(code)
    if df is None:
        return fallback
    w = df[(df.date < when) & (df.date >= when - pd.Timedelta(days=1095))]
    if len(w) < 40:
        w = df[df.date < when]
    return float((w.hg + w.ag).mean()) if len(w) >= 30 else fallback


@lru_cache(maxsize=64)
def _intercept(year: int, month: int) -> float:
    """Mean residual over the trailing 180 days of cup fixtures with Elo,
    refit at month boundaries — the walked shape cup_elo.py validated."""
    start = pd.Timestamp(year=year, month=month, day=1)
    resid = []
    for code in CUPS:
        df = _frame(code)
        if df is None:
            continue
        w = df[(df.date < start)
               & (df.date >= start - pd.Timedelta(days=_TRAIL_DAYS))]
        for r in w.itertuples():
            eh = elo_asof(str(r.home), r.date)
            ea = elo_asof(str(r.away), r.date)
            b = rolling_base(code, r.date, None)
            if eh is None or ea is None or b is None:
                continue
            eh, ea = eh / SCALE, ea / SCALE
            resid.append(int(r.hg) + int(r.ag) - b
                         - (B1 * abs(eh - ea) + B2 * (eh + ea)))
    if len(resid) < _MIN_TRAIL_ROWS:
        return B0_FALLBACK
    return float(sum(resid) / len(resid))


def cup_mu(league_code: str, home: str, away: str, match_date: date,
           fallback_base: Optional[float]) -> Optional[tuple[float, float]]:
    """(mu_total, competition_base) for a cup fixture, or None to abstain.

    Abstains — never guesses — when either club lacks a fresh-enough Elo
    or the competition lacks a baseline.
    """
    if league_code not in CUPS:
        return None
    when = _cutoff(match_date)
    eh = elo_asof(home, when)
    ea = elo_asof(away, when)
    if eh is None or ea is None:
        return None
    base = rolling_base(league_code, when, fallback_base)
    if base is None:
        return None
    b0 = _intercept(when.year, when.month)
    eh, ea = eh / SCALE, ea / SCALE
    mu = base + b0 + B1 * abs(eh - ea) + B2 * (eh + ea)
    return max(0.5, min(6.0, mu)), base
