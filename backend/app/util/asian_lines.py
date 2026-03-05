# backend/app/util/asian_lines.py
"""
Asian line evaluation from the bettor's perspective.

Quarter-ball lines are SPLIT BETS (half stake on each of two adjacent lines).
The split point produces either a half-win or half-loss depending on the line:

OVER lines:
  O2.25 = half O2.0 + half O2.5
    3+ goals → full win
    2  goals → O2.0 pushes, O2.5 loses  → HALF LOSS
    ≤1 goals → full loss

  O2.75 = half O2.5 + half O3.0
    4+ goals → full win
    3  goals → O2.5 wins, O3.0 pushes   → HALF WIN
    ≤2 goals → full loss

  O1.75 = half O1.5 + half O2.0
    2+ goals → full win  (O1.5 wins at 2, O2.0 pushes at 2 → half win at 2... )

  Wait — O1.75:
    2  goals → O1.5 wins (2>1.5), O2.0 pushes (2==2)  → HALF WIN
    1  goal  → O1.5 loses, O2.0 loses                  → full loss

UNDER lines:
  U3.75 = half U3.5 + half U4.0
    ≤3 goals → full win
    4  goals → U3.5 loses, U4.0 pushes  → HALF LOSS
    5+ goals → full loss

  U3.25 = half U3.0 + half U3.5
    ≤2 goals → full win
    3  goals → U3.0 pushes, U3.5 wins   → HALF WIN
    4+ goals → full loss

Pattern:
  .25 over  lines → HALF LOSS at split point
  .75 over  lines → HALF WIN  at split point
  .75 under lines → HALF LOSS at split point
  .25 under lines → HALF WIN  at split point

Return values:
  True        = full win
  False       = full loss
  "half_win"  = win half stake, refund half (net positive)
  "half_loss" = lose half stake, refund half (net negative)

For calibration:
  True / "half_win"  → counted as HIT  (half_win at reduced weight)
  False / "half_loss"→ counted as MISS (half_loss at reduced weight)
"""
from __future__ import annotations

import math
from typing import Union

# Return type: True, False, "half_win", or "half_loss"
MarketResult = Union[bool, str, None]


def evaluate_market(
    market: str,
    home_goals: int,
    away_goals: int,
) -> MarketResult:
    """
    Evaluate a market result.

    Returns:
        True        — full win
        False       — full loss
        "half_win"  — half win (win on one half, push on other)
        "half_loss" — half loss (lose on one half, push on other)
        None        — unrecognised market
    """
    total = home_goals + away_goals
    m = market.strip().upper()

    # Compound/slash → take lower (conservative) line
    if "/" in m:
        m = m.split("/")[0].strip()

    # ── BTTS ─────────────────────────────────────────────────────────
    if m == "BTTS":
        return home_goals > 0 and away_goals > 0
    if m in ("NO_BTTS", "NO BTTS"):
        return not (home_goals > 0 and away_goals > 0)

    # ── Over lines ────────────────────────────────────────────────────
    if m.startswith("O"):
        try:
            line = float(m[1:])
        except ValueError:
            return None

        frac = round(line % 1, 2)

        if frac == 0.25:
            # e.g. O2.25 = half O2.0 + half O2.5
            # Split point (floor): total == floor(line) → HALF LOSS
            floor_line = math.floor(line)   # 2
            if total >= floor_line + 1:     # 3+ → full win
                return True
            if total == floor_line:         # exactly 2 → half loss
                return "half_loss"
            return False                    # ≤1 → full loss

        if frac == 0.75:
            # e.g. O2.75 = half O2.5 + half O3.0
            # Split point (ceil): total == ceil(line) → HALF WIN
            ceil_line = math.ceil(line)     # 3
            if total > ceil_line:           # 4+ → full win
                return True
            if total == ceil_line:          # exactly 3 → half win
                return "half_win"
            return False                    # ≤2 → full loss

        if frac == 0.0:
            # Whole line: push at exactly line
            if total == int(line):
                return "half_win"   # push = refund = treated as half win
            return total > line

        # Half line (.5): no push
        return total > line

    # ── Under lines ───────────────────────────────────────────────────
    if m.startswith("U"):
        try:
            line = float(m[1:])
        except ValueError:
            return None

        frac = round(line % 1, 2)

        if frac == 0.75:
            # e.g. U3.75 = half U3.5 + half U4.0
            # Split point (ceil): total == ceil(line) → HALF LOSS
            ceil_line = math.ceil(line)     # 4
            if total < ceil_line - 1:       # ≤3 → actually total < 3.5 → ≤3 → full win
                return True
            # Recalculate properly:
            # U3.5 wins if total ≤ 3, U4.0 wins if total ≤ 3, pushes at 4
            lower_half = math.floor(line) + 0.5  # 3.5
            if total < lower_half:          # ≤3 → both win → full win
                return True
            if total == ceil_line:          # exactly 4 → U3.5 loses, U4.0 pushes → half loss
                return "half_loss"
            return False                    # 5+ → full loss

        if frac == 0.25:
            # e.g. U3.25 = half U3.0 + half U3.5
            # Split point (floor): total == floor(line) → HALF WIN
            floor_line = math.floor(line)   # 3
            if total < floor_line:          # ≤2 → both win → full win
                return True
            if total == floor_line:         # exactly 3 → U3.0 pushes, U3.5 wins → half win
                return "half_win"
            return False                    # 4+ → full loss

        if frac == 0.0:
            if total == int(line):
                return "half_win"   # push
            return total < line

        # Half line: no push
        return total < line

    return None


def is_hit(result: MarketResult) -> bool:
    """Returns True if the result is a win or half-win (no money lost)."""
    return result is True or result == "half_win"


def is_miss(result: MarketResult) -> bool:
    """Returns True if the result is a loss or half-loss."""
    return result is False or result == "half_loss"


def result_weight(result: MarketResult) -> float:
    """
    Calibration weight for a result:
      full win   → 1.0
      half win   → 0.5
      half loss  → -0.5  (counted as partial miss)
      full loss  → -1.0
    Not used directly but useful for EV calculations.
    """
    if result is True:       return  1.0
    if result == "half_win": return  0.5
    if result == "half_loss":return -0.5
    if result is False:      return -1.0
    return 0.0


def hit_weight(result: MarketResult) -> float:
    """
    Convert a market result to a numeric weight for calibration scoring.

    Returns:
        1.0  = full win
        0.5  = half win  (win half stake)
        0.25 = half loss (lose half stake) — still partial credit
        0.0  = full loss
        -1.0 = unrecognised market (skip in calibration)
    """
    if result is True:        return 1.0
    if result == "half_win":  return 0.5
    if result == "half_loss": return 0.25
    if result is False:       return 0.0
    return -1.0  # None = unrecognised
    """Human-readable description for display/debugging."""
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
            frac = round(line % 1, 2)
            if frac == 0.25:
                f = math.floor(line)
                return f"Over {line} — win at {f+1}+, half loss at {f} goals"
            if frac == 0.75:
                c = math.ceil(line)
                return f"Over {line} — win at {c+1}+, half win at {c} goals"
            win_at = math.ceil(line) + (1 if frac == 0.0 else 0)
            return f"Over {line} — win at {win_at}+ goals"
        except ValueError:
            pass
    if m.startswith("U"):
        try:
            line = float(m[1:])
            frac = round(line % 1, 2)
            if frac == 0.75:
                c = math.ceil(line)
                return f"Under {line} — win at ≤{c-1} goals, half loss at {c} goals"
            if frac == 0.25:
                f = math.floor(line)
                return f"Under {line} — win at ≤{f-1} goals, half win at {f} goals"
            max_g = int(line - 0.5) if frac == 0.5 else int(line) - 1
            return f"Under {line} — win at max {max_g} goals"
        except ValueError:
            pass
    return market
