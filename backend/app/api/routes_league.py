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
        }
        for r in rows
    ]
