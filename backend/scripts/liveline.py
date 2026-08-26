"""
How a lane stands while the match is still running.

A pre-match probability priced ninety minutes; once the ball is rolling
the only honest statement is what the CURRENT score has done to the bet.
This says it in the shortest possible form — "✓ landed", "needs 1 more",
"room for 3" — and it says it from `pricing.settle_fraction`, the same
settlement the ledger pays out on, rather than a second opinion about
what a line means.

Over rungs can be WON in play and never given back; under rungs can only
be LOST in play, so they get headroom instead of a verdict. Quarter lines
carry their half states through unchanged.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing

# "**Al-Nassr O1.5** 84.0% …" or "U4.25 86.5% …" — the market, and the
# team it belongs to when the lane is a team total.
_MARKET = re.compile(r"\*\*\s*(?:(?P<team>.+?)\s+)?(?P<mk>[OU]\d+(?:\.\d+)?)"
                     r"\s*\*\*|(?P<mk2>[OU]\d+(?:\.\d+)?)")
_SCORE = re.compile(r"(\d+)\s*-\s*(\d+)")


def score_of(status: str) -> Optional[tuple[int, int]]:
    """(home, away) from a live status line, or None when it carries no
    score yet."""
    m = _SCORE.search(status or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _goals_for(cell: str, teams: str, hg: int, ag: int) -> Optional[tuple]:
    m = _MARKET.search(cell)
    if not m:
        return None
    market = m.group("mk") or m.group("mk2")
    team = (m.group("team") or "").strip()
    if not team or " v " not in teams:
        return market, hg + ag
    home, away = (x.strip() for x in teams.split(" v ", 1))
    low = team.lower()
    if low in home.lower() or home.lower() in low:
        return market, hg
    if low in away.lower() or away.lower() in low:
        return market, ag
    return market, hg + ag


def progress(cell: str, teams: str, status: str) -> str:
    """A short live state for one lane, or "" when it cannot be read."""
    sc = score_of(status)
    if sc is None:
        return ""
    got = _goals_for(cell, teams, *sc)
    if got is None:
        return ""
    market, goals = got
    try:
        now = pricing.settle_fraction(market, goals)
    except (ValueError, IndexError):
        return ""

    if market.startswith("O"):
        # An over, once cleared, cannot be taken back.
        if now >= 1.0:
            return "✓ landed"
        for k in range(1, 8):
            if pricing.settle_fraction(market, goals + k) >= 1.0:
                half = " (half in)" if now > 0 else ""
                return f"needs {k} more{half}"
        return ""
    # An under can only be lost from here. A quarter line settles in
    # halves and a whole line can push, so the state comes first and
    # headroom only when the lane is still whole.
    if now <= -1.0:
        return "✗ gone"
    if now < 0.0:
        return "half gone"
    if now == 0.0:
        return "push as it stands"
    if now < 1.0:
        return "half safe"
    for k in range(1, 8):
        if pricing.settle_fraction(market, goals + k) < 1.0:
            return "next goal hurts" if k == 1 else f"room for {k - 1}"
    return "room to spare"
