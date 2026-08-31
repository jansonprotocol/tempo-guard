"""
Price ANY lane on a boarded fixture, not just the ones the card printed.

The Found bets table used to show a dash whenever a position sat on a
lane the engine had not published — a double chance where the card
printed a DNB, a rung the ladder never offered. The bettor's own reads
were therefore the one part of the book with no probability beside them,
which is exactly where a number is most useful.

Everything needed to price the whole lane space of a fixture is three
numbers: the match goal expectation and each side's chance of scoring.
This module computes them ONCE per fixture, caches them, and prices any
lane from the cache — so the Found bets table gains a probability for
every row without a per-bet engine call.

    match totals   p_win(rung, mu)                    — the ladder
    team totals    the same rung curves on one side's gf
    result lanes   the tilted Poisson pair behind Tip 3

The cache lives in config/lane_cache.json, keyed by league|teams|date,
and is derived: delete it and the next render rebuilds it. Nothing here
reads a bookmaker price, and nothing here is typed by hand.
"""
from __future__ import annotations

import json
import math
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import market_select, result_market, team_total

CACHE = Path(__file__).resolve().parents[2] / "config" / "lane_cache.json"
_MEM: dict | None = None
_DIRTY = False


def _load() -> dict:
    global _MEM
    if _MEM is None:
        try:
            _MEM = json.loads(CACHE.read_text())
        except Exception:
            _MEM = {}
    return _MEM


def flush() -> None:
    """Write the cache back if anything new was priced."""
    global _DIRTY
    if _DIRTY and _MEM is not None:
        CACHE.write_text(json.dumps(_MEM, indent=0, sort_keys=True))
        _DIRTY = False


def inputs(code: str, teams: str, day: str) -> dict | None:
    """{mu, ph, pa} for one fixture — the engine's own as-of numbers.

    ph/pa are each side's chance to score at least once, which is what
    the team and result lanes are built from. None when the engine
    abstains on the fixture, which is an answer, not a gap."""
    global _DIRTY
    key = f"{code}|{teams}|{day}"
    mem = _load()
    if key in mem:
        return mem[key]
    out = None
    if " v " in teams:
        h, a = (x.strip() for x in teams.split(" v ", 1))
        try:
            from app.predict import build_request
            y, m, d = (int(x) for x in day.split("-"))
            req = build_request(code, h, a, date(y, m, d))
            if req is not None:
                out = dict(mu=req.mu_total, ph=req.p_home_tt05,
                           pa=req.p_away_tt05)
        except Exception:
            out = None
    mem[key] = out
    _DIRTY = True
    return out


def price(rung: str, side: str, code: str, teams: str, day: str
          ) -> float | None:
    """The engine's probability for this exact lane, or None when the
    fixture is one it abstains on."""
    inp = inputs(code, teams, day)
    if not inp:
        return None
    mu, ph, pa = inp.get("mu"), inp.get("ph"), inp.get("pa")

    if rung in ("DNB", "1X", "X2", "12"):
        if not ph or not pa or not (0 < ph < 1 and 0 < pa < 1):
            return None
        h, d, a = result_market.result_probs(-math.log(1 - ph),
                                             -math.log(1 - pa))
        if rung == "1X":
            return h + d
        if rung == "X2":
            return a + d
        if rung == "12":
            return h + a
        # DNB is conditional on a decisive result, the same basis the
        # card prints it on.
        nd = h + a
        if nd <= 0:
            return None
        return (h if side == "H" else a) / nd

    if side in ("H", "A"):
        p_tt05 = ph if side == "H" else pa
        if not p_tt05 or not 0 < p_tt05 < 1:
            return None
        curve = team_total.RUNGS.get(rung)
        if curve is None:
            return None
        return curve[0](-math.log(1 - p_tt05))

    if mu is None:
        return None
    try:
        return market_select.p_win(rung, mu)
    except Exception:
        return None
