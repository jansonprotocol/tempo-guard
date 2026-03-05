# backend/app/api/routes_retrosim.py
from __future__ import annotations

from datetime import date

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field, validator
from typing import Optional
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.data_providers.fbref_base import asof_features, validate_match_existed

router = APIRouter()


class RetroBody(BaseModel):
    league_code: str  = Field(..., example="ENG-PL")
    home_team:   str  = Field(..., example="Arsenal")
    away_team:   str  = Field(..., example="Chelsea")
    match_date:  date = Field(..., example="2025-10-11")
    league_search_hint: Optional[str] = None

    @validator("match_date")
    def must_be_past(cls, v):
        if v >= date.today():
            raise ValueError(
                "match_date must be a past date for retrosim. "
                "Use /futurematch for upcoming fixtures."
            )
        return v


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/retrosim")
def post_retrosim(body: RetroBody, db: Session = Depends(get_db)):
    try:
        # ── Validate the match actually happened ─────────────────────
        valid, reason = validate_match_existed(
            body.league_code,
            body.home_team,
            body.away_team,
            body.match_date,
        )
        if not valid:
            raise HTTPException(status_code=422, detail=reason)

        # ── Compute features as-of match date ────────────────────────
        metrics = asof_features(
            body.league_code,
            body.home_team,
            body.away_team,
            body.match_date,
        )

        req = MatchRequest(
            league_code=body.league_code,
            home_team=body.home_team,
            away_team=body.away_team,
            match_date=body.match_date,
            sot_proj_total=metrics.get("sot_proj_total"),
            support_idx_over_delta=metrics.get("support_idx_over_delta"),
            p_two_plus=metrics.get("p_two_plus"),
            p_home_tt05=metrics.get("p_home_tt05"),
            p_away_tt05=metrics.get("p_away_tt05"),
            tempo_index=metrics.get("tempo_index"),
        )

        pred = predict_match(db, req)

        return {
            "mode":             "retrosim",
            "league_code":      pred.league_code,
            "fixture":          pred.fixture,
            "corridor":         {"low": pred.corridor.low, "high": pred.corridor.high, "lean": pred.corridor.lean},
            "translated_play":  {"market": pred.translated_play.market, "confidence": pred.translated_play.confidence},
            "confidence_score": pred.confidence_score,
            "applied_modules":  pred.applied_modules,
            "safety_flags":     pred.safety_flags,
            "explanations":     pred.explanations,
            "inputs_used":      metrics,
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
