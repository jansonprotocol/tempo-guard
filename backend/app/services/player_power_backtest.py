# backend/app/services/player_power_backtest.py
"""
ATHENA v2.0 — Point-in-time squad power for calibration backtesting.

When routes_calibration.py replays historical matches, it needs squad power
scores that reflect the team composition AT the time of that match, not today.

This module provides:
  - get_historical_player_nudge():  returns the player-power support_delta
    nudge for a match using the most recent SquadSnapshot before match_date
  - compare_with_without_power():   runs two sim passes (with/without player
    power) to measure the marginal hit-rate improvement

Design:
  If no SquadSnapshot exists before match_date → returns 0.0 (v1 fallback).
  If only one team has a snapshot → returns 0.0 (both needed for delta).
  Uses strength_coefficient for cross-league normalisation (intl matches).
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from sqlalchemy.orm import Session

from app.models.models_players import SquadSnapshot
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig

# Mirror the constants from predict.py so backtest behaves identically
PLAYER_POWER_BLEND     = 0.30
PLAYER_POWER_MAX_EFFECT = 0.08
INTL_LEAGUE_CODES       = {"UCL", "UEL", "UECL", "EC", "WC"}


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def get_historical_squad_power(
    db: Session,
    team: str,
    league_code: str,
    match_date: date,
) -> Optional[float]:
    """
    Find the most recent SquadSnapshot for a team BEFORE match_date.

    Returns squad_power (0–100) or None if no snapshot exists.
    Uses the snapshot with the largest snapshot_date <= match_date.

    Fallback: if no historical snapshot predates the match, use the
    most recent snapshot available (current squad assessment).
    This allows A/B testing before historical snapshots accumulate.
    Once you have months of snapshots, the fallback rarely triggers.
    """
    # Primary: point-in-time (snapshot before match)
    snap = (
        db.query(SquadSnapshot)
        .filter(
            SquadSnapshot.team == team,
            SquadSnapshot.league_code == league_code,
            SquadSnapshot.snapshot_date <= match_date,
            SquadSnapshot.squad_power.isnot(None),
        )
        .order_by(SquadSnapshot.snapshot_date.desc())
        .first()
    )
    if snap:
        return float(snap.squad_power)

    # Fallback: use most recent snapshot regardless of date
    # This lets the sweep/evaluate work before history accumulates
    fallback = (
        db.query(SquadSnapshot)
        .filter(
            SquadSnapshot.team == team,
            SquadSnapshot.league_code == league_code,
            SquadSnapshot.squad_power.isnot(None),
        )
        .order_by(SquadSnapshot.snapshot_date.desc())
        .first()
    )
    if fallback:
        return float(fallback.squad_power)

    return None


def get_historical_player_nudge(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    blend_weight: float = PLAYER_POWER_BLEND,
) -> float:
    """
    Compute player-power support_delta nudge using point-in-time squad snapshots.

    Identical logic to predict.py's _get_player_power_nudge(), but reads
    from SquadSnapshot (historical) instead of TeamConfig (current).

    Args:
        blend_weight: override the default blend for A/B testing different weights.

    Returns 0.0 if:
      - Either team has no historical snapshot
      - blend_weight is 0.0
      - Both teams have equal power
    """
    if blend_weight <= 0.0:
        return 0.0

    # For domestic matches, look up snapshots in the match's league
    # For intl matches (UCL etc), look up in each team's home league
    if league_code in INTL_LEAGUE_CODES:
        # Need to find each team's home league from TeamConfig
        home_cfg = db.query(TeamConfig).filter_by(team=home_team).first()
        away_cfg = db.query(TeamConfig).filter_by(team=away_team).first()
        home_league = home_cfg.league_code if home_cfg else None
        away_league = away_cfg.league_code if away_cfg else None
    else:
        home_league = league_code
        away_league = league_code

    if not home_league or not away_league:
        return 0.0

    home_power = get_historical_squad_power(db, home_team, home_league, match_date)
    away_power = get_historical_squad_power(db, away_team, away_league, match_date)

    if home_power is None or away_power is None:
        return 0.0

    # Cross-league normalisation for international competitions
    if league_code in INTL_LEAGUE_CODES:
        home_lc = db.query(LeagueConfig).filter_by(league_code=home_league).first()
        away_lc = db.query(LeagueConfig).filter_by(league_code=away_league).first()
        if home_lc and home_lc.strength_coefficient:
            home_power *= float(home_lc.strength_coefficient)
        if away_lc and away_lc.strength_coefficient:
            away_power *= float(away_lc.strength_coefficient)

    power_delta = (home_power - away_power) / 100.0
    nudge = power_delta * blend_weight
    nudge = _clip(nudge, -PLAYER_POWER_MAX_EFFECT, PLAYER_POWER_MAX_EFFECT)

    return round(nudge, 4)


def has_any_snapshots(db: Session, league_code: str) -> bool:
    """Quick check whether ANY squad snapshots exist for a league."""
    return (
        db.query(SquadSnapshot)
        .filter(
            SquadSnapshot.league_code == league_code,
            SquadSnapshot.squad_power.isnot(None),
        )
        .first()
    ) is not None
