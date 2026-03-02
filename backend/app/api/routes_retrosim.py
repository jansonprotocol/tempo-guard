# backend/app/api/routes_retrosim.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.data_providers.api_football_retro import find_fixture_and_asof_stats

router = APIRouter()

class RetroBody(BaseModel):
    league_code: str = Field(..., example="ENG-PL")
    home_team: str = Field(..., example="Arsenal")
    away_team: str = Field(..., example="Chelsea")
    match_date: date = Field(..., example="2025-10-11")
    # Optional: if you want to constrain AUTO lookup:
    league_search_hint: Optional[str] = None

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@router.post("/retrosim")
def post_retrosim(body: RetroBody, db: Session = Depends(get_db)):
    try:
        metrics, fixture_obj = find_fixture_and_asof_stats(
            league_code=body.league_code,
            home=body.home_team,
            away=body.away_team,
            d=body.match_date,
            league_search_hint=body.league_search_hint
        )

        # Build MatchRequest using retro metrics (or fall back to defaults if empty)
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
            tempo_index=metrics.get("tempo_index")
        )

        pred = predict_match(db, req)

        # If we found the actual fixture, surface the FT score to compare with corridor
        actual = None
        if fixture_obj:
            goals = fixture_obj.get("goals") or {}
            actual = {
                "home": goals.get("home"),
                "away": goals.get("away"),
                "total": (goals.get("home") or 0) + (goals.get("away") or 0),
                "league_id": (fixture_obj.get("league") or {}).get("id"),
                "fixture_id": (fixture_obj.get("fixture") or {}).get("id"),
                "status": (fixture_obj.get("fixture") or {}).get("status", {}).get("short"),
            }

        return {
            "mode": "retrosim",
            "league_code": pred.league_code,
            "fixture": pred.fixture,
            "corridor": {"low": pred.corridor.low, "high": pred.corridor.high, "lean": pred.corridor.lean},
            "translated_play": {"market": pred.translated_play.market, "confidence": pred.translated_play.confidence},
            "confidence_score": pred.confidence_score,
            "applied_modules": pred.applied_modules,
            "safety_flags": pred.safety_flags,
            "explanations": pred.explanations,
            "inputs_used": metrics or {},
            "actual_result": actual
        }
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
