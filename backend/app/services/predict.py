from __future__ import annotations

from sqlalchemy.orm import Session

from app.engine.types import MatchRequest, Prediction
from app.engine.pipeline import evaluate_athena
from app.models.league_config import LeagueConfig


def _get_league_bias(db: Session, league_code: str) -> tuple[float, float, float]:
    """
    Look up league-level bias configuration.
    Returns (over_bias, under_bias, tempo_factor).
    Falls back to conservative, safe defaults when not found.
    """
    cfg = (
        db.query(LeagueConfig)
        .filter(LeagueConfig.league_code == league_code)
        .first()
    )

    if not cfg:
        # Safe neutral fallback
        return 0.02, 0.02, 0.50

    return (
        float(cfg.base_over_bias or 0.02),
        float(cfg.base_under_bias or 0.02),
        float(cfg.tempo_factor or 0.50),
    )


def predict_match(db: Session, req: MatchRequest) -> Prediction:
    """
    Main entry for generating ATHENA predictions.
    It retrieves league biases, applies the engine pipeline,
    and returns a Prediction object.
    """
    over_bias, under_bias, tempo_factor = _get_league_bias(db, req.league_code)
    return evaluate_athena(req, over_bias, under_bias, tempo_factor)
