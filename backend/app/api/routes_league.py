from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session
from app.database.db import get_db
from app.models.league_config import LeagueConfig

router = APIRouter()

@router.get("/league-configs")
def get_league_configs(db: Session = Depends(get_db)):
    rows = db.query(LeagueConfig).all()
    return [
        {
            "league_code": r.league_code,
            "over_bias": r.base_over_bias,
            "under_bias": r.base_under_bias,
            "tempo_factor": r.tempo_factor,
            "safety_mode": r.safety_mode,
            "aggression_level": getattr(r, "aggression_level", 0.5),
            "volatility": getattr(r, "volatility", 0.5),
            "description": r.description or ""
        }
        for r in rows
    ]

@router.get("/league-list")
def league_list(db: Session = Depends(get_db)):
    """Minimal list for frontend dropdown."""
    rows = db.query(LeagueConfig).order_by(LeagueConfig.league_code).all()
    return [
        {
            "code": r.league_code,
            "name": (r.description or r.league_code).strip()
        }
        for r in rows
    ]
