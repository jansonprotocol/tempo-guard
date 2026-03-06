# backend/app/services/predict.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.engine.types import MatchRequest, Prediction
from app.engine.pipeline import evaluate_athena
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig


def _get_league_bias(db: Session, league_code: str) -> tuple[float, float, float]:
    """
    Look up league-level bias configuration from DB.
    Returns (over_bias, under_bias, tempo_factor).

    These values are applied as ADDITIVE adjustments inside evaluate_athena:
      - over_bias - under_bias  → shifts support_delta
      - tempo_factor            → scales raw tempo signal (1.0 = neutral)

    Falls back to conservative neutral defaults when league not found.
    """
    cfg = (
        db.query(LeagueConfig)
        .filter(LeagueConfig.league_code == league_code)
        .first()
    )
    if not cfg:
        return 0.05, 0.05, 0.50  # neutral defaults

    return (
        float(cfg.base_over_bias  or 0.05),
        float(cfg.base_under_bias or 0.05),
        float(cfg.tempo_factor    or 0.50),
    )


def _get_team_nudge(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
) -> float:
    """
    Look up per-team calibration nudges for both teams in the matchup.
    Returns combined nudge = (home_nudge + away_nudge) / 2.

    Each team's nudge reflects ATHENA's historical miss pattern for that team:
      positive → team matchups tend to produce more goals than signals suggest
      negative → team matchups tend to produce fewer goals than signals suggest

    Both home and away nudges are averaged because both teams contribute
    equally to the total goals context. A leaky home defense and a clinical
    away attack should both push the combined nudge positive.

    Returns 0.0 if no team configs exist yet (neutral — no effect).
    """
    home_cfg = (
        db.query(TeamConfig)
        .filter_by(league_code=league_code, team=home_team)
        .first()
    )
    away_cfg = (
        db.query(TeamConfig)
        .filter_by(league_code=league_code, team=away_team)
        .first()
    )

    home_nudge = float(home_cfg.over_nudge or 0.0) if home_cfg else 0.0
    away_nudge = float(away_cfg.over_nudge or 0.0) if away_cfg else 0.0

    combined = (home_nudge + away_nudge) / 2.0
    return combined


def predict_match(db: Session, req: MatchRequest) -> Prediction:
    """
    Main entry point for generating ATHENA predictions.
    Retrieves league calibration config + team nudges,
    applies the engine pipeline, and returns a Prediction object.
    """
    over_bias, under_bias, tempo_factor = _get_league_bias(db, req.league_code)
    team_nudge = _get_team_nudge(
        db, req.league_code, req.home_team, req.away_team
    )
    return evaluate_athena(req, over_bias, under_bias, tempo_factor, team_nudge)
