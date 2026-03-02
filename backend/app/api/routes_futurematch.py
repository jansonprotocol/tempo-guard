# backend/app/api/routes_futurematch.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel, Field
from typing import Optional
from datetime import date
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.data_providers.api_football import enrich_from_live_data

router = APIRouter()

class FutureBody(BaseModel):
    league_code: str = Field(..., example="ENG-PL")
    home_team: str = Field(..., example="Arsenal")
    away_team: str = Field(..., example="Chelsea")
    match_date: date = Field(..., example="2026-03-05")
    # Optional manual overrides; if present we NEVER override them from live:
    sot_proj_total: Optional[float] = None
    support_idx_over_delta: Optional[float] = None
    p_two_plus: Optional[float] = None
    p_home_tt05: Optional[float] = None
    p_away_tt05: Optional[float] = None
    tempo_index: Optional[float] = None
    # Optional: league search hint (e.g., "UEFA Champions League")
    league_search_hint: Optional[str] = None

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

@router.post("/futurematch")
def post_futurematch(body: FutureBody, db: Session = Depends(get_db)):
    try:
        # REMOVE the unwanted field before creating MatchRequest
        bd = body.model_dump()
        league_search_hint = bd.pop("league_search_hint", None)

        req = MatchRequest(**bd)

        missing = any([
            req.sot_proj_total is None,
            req.support_idx_over_delta is None,
            req.p_two_plus is None,
            req.p_home_tt05 is None,
            req.p_away_tt05 is None,
            req.tempo_index is None,
        ])

        if missing:
            live = enrich_from_live_data(
                league_code=req.league_code,
                home=req.home_team,
                away=req.away_team,
                match_date=req.match_date,
                league_name_for_search=league_search_hint
            )
            if live:
                if req.sot_proj_total is None: req.sot_proj_total = live.get("sot_proj_total")
                if req.support_idx_over_delta is None: req.support_idx_over_delta = live.get("support_idx_over_delta")
                if req.p_two_plus is None: req.p_two_plus = live.get("p_two_plus")
                if req.p_home_tt05 is None: req.p_home_tt05 = live.get("p_home_tt05")
                if req.p_away_tt05 is None: req.p_away_tt05 = live.get("p_away_tt05")
                if req.tempo_index is None: req.tempo_index = live.get("tempo_index")

        pred = predict_match(db, req)

        return {
            "mode": "futurematch",
            "league_code": pred.league_code,
            "fixture": pred.fixture,
            "corridor": {
                "low": pred.corridor.low,
                "high": pred.corridor.high,
                "lean": pred.corridor.lean
            },
            "translated_play": {
                "market": pred.translated_play.market,
                "confidence": pred.translated_play.confidence
            },
            "confidence_score": pred.confidence_score,
            "applied_modules": pred.applied_modules,
            "safety_flags": pred.safety_flags,
            "explanations": pred.explanations,
            "inputs_used": {
                "sot_proj_total": req.sot_proj_total,
                "support_idx_over_delta": req.support_idx_over_delta,
                "p_two_plus": req.p_two_plus,
                "p_home_tt05": req.p_home_tt05,
                "p_away_tt05": req.p_away_tt05,
                "tempo_index": req.tempo_index
            }
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
