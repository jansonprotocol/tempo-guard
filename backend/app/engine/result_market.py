"""
Tip 3 — the result lane the engine always refused to price.

The bettor's proposal (29 Aug): 1X when the away win is the least likely
outcome, X2 mirrored, 12 when the draw is, upgraded to DNB when one side
is significantly the stronger inside its double chance. The engine
already holds a goal expectation per side; two independent Poissons turn
that pair into P(home), P(draw), P(away), and every lane follows.

Measured before shipping (scripts/result_lanes.py, 15,048 fixtures
across 52 leagues, two half-windows):

    raw            H says 41.2 happens 44.4 (+3.2 BOTH halves),
                   A −3.2/−4.3 — home advantage under-split, stable
    draw           +0.1/+1.1 — the feared Poisson draw bias never
                   appears: TEAM_SHRINK compresses the sides and pumps
                   the draw up by roughly the amount Poisson drops it
    RESULT_TILT    1.10 on the home/away odds ratio closes all six
                   outcome gaps to within 1.4 in both halves
    strongest DC   0.70–0.80 band (three quarters of prints) +0.1/−0.5;
                   every higher band UNDERCLAIMS (+1.6 to +7.1)
    DNB            underclaims at every band (+1.3 to +8.5)

The lane never overclaims anywhere measured — its failure mode is
modesty, which the buy price simply inherits as a wider margin.

PROBATION: like the cup lane before it, Tip 3 starts outside the
playable tallies and the hero window. It earns its way in with live
results or it comes off.
"""
from __future__ import annotations

import math
from datetime import date, datetime, timedelta
from typing import Optional

from app.data import store

# The home tilt: multiply the home/away chance ratio by this before
# normalising. Fitted on half 1 (H gap +3.2 -> -0.3), holds on half 2
# (-0.3), leaves the draw honest (+0.3/+1.4).
RESULT_TILT = 1.10

# A double chance prints from this probability. The floor sits at the
# bottom of the measured honest band — below it the sample thins and the
# lane stops being a read.
DC_FLOOR = 0.72

# The DNB upgrade: inside the printed double chance, when the stronger
# side carries at least this share of the no-draw probability, the lane
# sharpens to Draw No Bet — "significantly stronger", as proposed. The
# 0.65+ bands measured +3.3 to +8.5 UNDER their claims in both halves.
DNB_FROM = 0.65

# Minimum edge over the league's as-of base rate, mirroring team_total:
# a lane that merely restates how often 12 lands anyway is not a read.
MIN_EDGE = 0.02

_WINDOW_DAYS = 365 * 3
_MIN_SAMPLE = 150
_MAX_G = 10
_CACHE: dict[tuple[str, date], Optional[dict]] = {}

LANES = ("1X", "X2", "12", "DNB1", "DNB2")


def _pois(mu: float) -> list[float]:
    return [math.exp(-mu) * mu ** k / math.factorial(k)
            for k in range(_MAX_G + 1)]


def result_probs(gf_h: float, gf_a: float) -> tuple[float, float, float]:
    """(p_home, p_draw, p_away), tilted and normalised."""
    ph, pa = _pois(gf_h), _pois(gf_a)
    home = draw = away = 0.0
    for i, x in enumerate(ph):
        for j, y in enumerate(pa):
            p = x * y
            if i > j:
                home += p
            elif i == j:
                draw += p
            else:
                away += p
    home *= RESULT_TILT
    away /= RESULT_TILT
    s = home + draw + away
    return home / s, draw / s, away / s


def base_rates(league_code: str, match_date: date) -> Optional[dict]:
    """{lane: how often it lands in this league}, strictly before the day."""
    key = (league_code, match_date)
    if key in _CACHE:
        return _CACHE[key]
    df = store.load_results(league_code)
    if df is None or df.empty:
        _CACHE[key] = None
        return None
    cutoff = datetime.combine(match_date, datetime.min.time())
    past = df[df["date"] < cutoff]
    if len(past) < _MIN_SAMPLE:
        _CACHE[key] = None
        return None
    window = past[past["date"] >= cutoff - timedelta(days=_WINDOW_DAYS)]
    if len(window) < _MIN_SAMPLE:
        window = past
    hg = window["hg"].fillna(0).astype(int)
    ag = window["ag"].fillna(0).astype(int)
    h = float((hg > ag).mean())
    d = float((hg == ag).mean())
    a = float((hg < ag).mean())
    nd = h + a
    out = {"1X": h + d, "X2": a + d, "12": h + a,
           "DNB1": h / nd if nd else 0.5, "DNB2": a / nd if nd else 0.5}
    _CACHE[key] = out
    return out


def choose(league_code: str, match_date: date,
           p_home_tt05: Optional[float],
           p_away_tt05: Optional[float]) -> Optional[tuple[str, float, float]]:
    """(lane, probability, edge) for Tip 3, or None to stay silent.

    The strongest double chance prints when it clears DC_FLOOR and beats
    the league's base rate by MIN_EDGE; it sharpens to DNB when the
    stronger side holds DNB_FROM of the no-draw probability AND the DNB
    itself clears the same edge bar. Silence is an answer, as always.
    """
    if not p_home_tt05 or not p_away_tt05:
        return None
    if not (0 < p_home_tt05 < 1 and 0 < p_away_tt05 < 1):
        return None
    rates = base_rates(league_code, match_date)
    if rates is None:
        return None
    gf_h = -math.log(1 - p_home_tt05)
    gf_a = -math.log(1 - p_away_tt05)
    h, d, a = result_probs(gf_h, gf_a)

    dcs = {"1X": h + d, "X2": a + d, "12": h + a}
    lane, p = max(dcs.items(), key=lambda kv: kv[1])
    if p < DC_FLOOR:
        return None

    # The DNB upgrade, only inside a side-carrying double chance.
    if lane in ("1X", "X2"):
        nd = h + a
        strong, dnb = (h / nd, "DNB1") if lane == "1X" else (a / nd, "DNB2")
        if strong >= DNB_FROM:
            edge = strong - rates[dnb]
            if edge >= MIN_EDGE:
                return dnb, strong, edge

    edge = p - rates[lane]
    if edge < MIN_EDGE:
        return None
    return lane, p, edge


def won(lane: str, hg: int, ag: int) -> Optional[bool]:
    """Settlement from the final score. None = push (DNB draw)."""
    if lane == "1X":
        return hg >= ag
    if lane == "X2":
        return ag >= hg
    if lane == "12":
        return hg != ag
    if lane in ("DNB1", "DNB2"):
        if hg == ag:
            return None
        return (hg > ag) if lane == "DNB1" else (ag > hg)
    raise ValueError(f"not a result lane: {lane!r}")
