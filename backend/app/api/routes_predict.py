from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
from app.database.db import SessionLocal
from sqlalchemy.orm import Session
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.resolve_team import resolve_team_name

router = APIRouter()

class PredictBody(BaseModel):
    league_code: str = Field(..., example="NLD1")
    home_team: str = Field(..., example="Ajax")
    away_team: str = Field(..., example="PSV")
    match_date: Optional[date] = Field(None, example="2026-03-01")
    sot_proj_total: Optional[float] = Field(None, example=11.2)
    support_idx_over_delta: Optional[float] = Field(None, example=0.09)
    p_two_plus: Optional[float] = Field(None, example=0.76)
    p_home_tt05: Optional[float] = Field(None, example=0.71)
    p_away_tt05: Optional[float] = Field(None, example=0.65)
    tempo_index: Optional[float] = Field(None, example=0.62)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

@router.post("/predict")
def post_predict(body: PredictBody, db: Session = Depends(get_db)):
    try:
        # Resolve team names through Team/Alias tables
        home_resolved = resolve_team_name(db, body.home_team, body.league_code)
        away_resolved = resolve_team_name(db, body.away_team, body.league_code)

        data = body.model_dump()
        data["home_team"] = home_resolved
        data["away_team"] = away_resolved
        req = MatchRequest(**data)
        pred = predict_match(db, req)

        # v2.0: Generate performance tags (graceful — returns empty if no player data)
        perf_tags = {}
        try:
            from app.services.performance_tags import generate_match_tags
            from app.services.performance_tags import _compute_league_zone_avgs
            league_avgs = _compute_league_zone_avgs(db, body.league_code)

            # Quick form delta lookup — use cached standings if available
            form_deltas = {}
            try:
                from app.services.form_delta import compute_form_delta
                delta_data = compute_form_delta(db, body.league_code)
                if delta_data and delta_data.get("teams"):
                    form_deltas = {t["team"]: t["form_delta"] for t in delta_data["teams"]}
            except Exception:
                pass

            perf_tags = generate_match_tags(
                db, body.league_code, home_resolved, away_resolved, form_deltas
            )
        except Exception:
            pass

        # Calibrated probability — converts raw confidence_score to a true
        # hit-rate estimate using isotonic regression on historical results.
        # Falls back to normalised raw score if no calibration data exists yet.
        calibrated_probability = None
        try:
            from app.services.confidence_calibrator import calibrate_confidence
            calibrated_probability = calibrate_confidence(
                db, pred.confidence_score, league_code=body.league_code
            )
        except Exception:
            pass

        return {
            "league_code": pred.league_code,
            "fixture": pred.fixture,
            "corridor": {
                "low": pred.corridor.low,
                "high": pred.corridor.high,
                "lean": pred.corridor.lean,
            },
            "translated_play": {
                "market": pred.translated_play.market,
                "confidence": pred.translated_play.confidence,
            },
            "confidence_score": pred.confidence_score,
            "calibrated_probability": calibrated_probability,
            "applied_modules": pred.applied_modules,
            "safety_flags": pred.safety_flags,
            "explanations": pred.explanations,
            "performance_tags": perf_tags,
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
