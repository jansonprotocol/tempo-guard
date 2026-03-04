# backend/app/util/asian_lines.py
"""
Asian line evaluation from the bettor's perspective.

Over lines — win threshold = ceil(line):
  O1.5  → win at 2+ goals
  O1.75 → win at 2+ goals
  O2    → win at 2+ goals
  O2.25 → win at 3+ goals
  O2.5  → win at 3+ goals
  O2.75 → win at 3+ goals
  O3    → win at 3+ goals
  O3.25 → win at 4+ goals
  ... and so on

Under lines — win threshold = total < ceil(line + 0.5):
  U4.25 → win at max 4 goals  (< 5)
  U4    → win at max 4 goals  (< 5)
  U3.75 → win at max 4 goals  (< 5)
  U3.5  → win at max 3 goals  (< 4)
  U3.25 → win at max 3 goals  (< 4)
  U3    → win at max 3 goals  (< 4)
  U2.75 → win at max 3 goals  (< 4)
  U2.5  → win at max 2 goals  (< 3)
  ... and so on

Compound/slash markets (e.g. "O2.5/3" or "U3.5/4") are evaluated
on the lower (more conservative) line only.
"""
from __future__ import annotations

import math
from typing import Optional


def evaluate_market(market: str, home_goals: int, away_goals: int) -> Optional[bool]:
    """
    Evaluate whether a bet WON for the given market and scoreline.

    Returns:
        True  — win
        False — loss
        None  — unrecognised market, can't evaluate
    """
    total = home_goals + away_goals
    m = market.strip().upper()

    # ── Handle compound/slash lines → take the lower (first) line ──
    if "/" in m:
        m = m.split("/")[0].strip()

    # ── BTTS / special markets ────────────────────────────────────
    if m == "BTTS":
        return home_goals > 0 and away_goals > 0
    if m in ("NO_BTTS", "NO BTTS"):
        return not (home_goals > 0 and away_goals > 0)

    # ── Over lines ────────────────────────────────────────────────
    if m.startswith("O"):
        try:
            line = float(m[1:])
            # Win threshold: ceil(line) — so O2 and O1.75 both require 2+
            return total >= math.ceil(line)
        except ValueError:
            return None

    # ── Under lines ──────────────────────────────────────────────
    if m.startswith("U"):
        try:
            line = float(m[1:])
            # Win threshold: total < ceil(line + 0.5)
            # U3.5 → ceil(4.0)=4 → total < 4 → max 3 goals  ✓
            # U3.75 → ceil(4.25)=5 → total < 5 → max 4 goals ✓
            # U4    → ceil(4.5)=5  → total < 5 → max 4 goals ✓
            return total < math.ceil(line + 0.5)
        except ValueError:
            return None

    return None


def market_description(market: str) -> str:
    """
    Human-readable description of a market for display/debugging.
    """
    m = market.strip().upper()
    if "/" in m:
        parts = m.split("/")
        return f"{market_description(parts[0])} / {market_description(parts[1])}"

    if m == "BTTS":
        return "Both teams to score"
    if m in ("NO_BTTS", "NO BTTS"):
        return "Not both teams to score"

    if m.startswith("O"):
        try:
            line = float(m[1:])
            threshold = math.ceil(line)
            return f"Over {line} — win at {threshold}+ goals"
        except ValueError:
            pass

    if m.startswith("U"):
        try:
            line = float(m[1:])
            max_goals = math.ceil(line + 0.5) - 1
            return f"Under {line} — win at max {max_goals} goals"
        except ValueError:
            pass

    return market
