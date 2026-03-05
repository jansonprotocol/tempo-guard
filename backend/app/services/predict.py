# backend/app/services/predict.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.engine.types import MatchRequest, Prediction
from app.engine.pipeline import evaluate_athena
from app.models.league_config import LeagueConfig


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
        return 0.05, 0.05, 0.50  # neutral defaults: bias range 0-0.25, tempo 0.5=neutral

    return (
        float(cfg.base_over_bias  or 0.05),
        float(cfg.base_under_bias or 0.05),
        float(cfg.tempo_factor    or 0.50),
    )


def predict_match(db: Session, req: MatchRequest) -> Prediction:
    """
    Main entry point for generating ATHENA predictions.
    Retrieves league calibration config, applies the engine pipeline,
    and returns a Prediction object.
    """
    over_bias, under_bias, tempo_factor = _get_league_bias(db, req.league_code)
    return evaluate_athena(req, over_bias, under_bias, tempo_factor)
