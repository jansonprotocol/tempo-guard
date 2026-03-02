# backend/app/api/routes_calibration.py
from __future__ import annotations
from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel
from typing import List, Optional
from datetime import date, timedelta
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.data_providers.api_football_retro import (
    find_fixture_and_asof_stats, _get, API_BASE
)

router = APIRouter()

class CalibResult(BaseModel):
    total: int
    evaluated: int
    hits: int
    misses: int
    skipped: int
    hit_rate: float
    sample: List[dict]

def get_db():
    db = SessionLocal()
    try: yield db
    finally: db.close()

def _is_under_hit(total_goals: int, low: float, high: float, lean: str) -> Optional[bool]:
    if lean == "under":
        # Corridor default is 1.5–4.5; treat <= 3 as under 3.5, etc.
        # Simple policy: if translated play contains "U" we check against 3.5 boundary
        return (total_goals <= 3)
    elif lean == "over":
        return (total_goals >= 2)
    return None

@router.get("/calibrate/league", response_model=CalibResult)
def calibrate_league(
    league_code: str,
    start: date,
    end: date,
    limit: int = Query(100, ge=1, le=1000),
    league_search_hint: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Batch retrosim across a date window for a given league.
    Returns hit/miss counts and a small sample of rows.
    """
    try:
        # Pull fixtures by date window; use API /fixtures?from=&to= and then filter by league if possible
        data = _get(f"{API_BASE}/fixtures", {"from": start.isoformat(), "to": end.isoformat()})
        fixtures = data.get("response") or []
        rows = []
        hits = misses = skipped = evaluated = 0

        for fx in fixtures[:limit]:
            lg = fx.get("league") or {}
            # If the league_code is not the same code space as provider, we still run by names via retrosim helper
            d = date.fromtimestamp((fx.get("fixture") or {}).get("timestamp", 0)) if (fx.get("fixture") or {}).get("timestamp") else None
            if not d:
                continue

            home = (fx.get("teams") or {}).get("home", {}).get("name")
            away = (fx.get("teams") or {}).get("away", {}).get("name")
            if not home or not away:
                continue

            # Retrosim metrics as-of that day
            metrics, _ = find_fixture_and_asof_stats(league_code, home, away, d, league_search_hint)
            req = MatchRequest(
                league_code=league_code, home_team=home, away_team=away, match_date=d,
                sot_proj_total=metrics.get("sot_proj_total"),
                support_idx_over_delta=metrics.get("support_idx_over_delta"),
                p_two_plus=metrics.get("p_two_plus"),
                p_home_tt05=metrics.get("p_home_tt05"),
                p_away_tt05=metrics.get("p_away_tt05"),
                tempo_index=metrics.get("tempo_index"),
            )
            pred = predict_match(db, req)

            # Actual FT result
            goals = fx.get("goals") or {}
            total = (goals.get("home") or 0) + (goals.get("away") or 0)
            evaluated += 1

            # Simple corridor success check (you can make this stricter later)
            hit_flag = _is_under_hit(total, pred.corridor.low, pred.corridor.high, pred.corridor.lean)
            if hit_flag is None:
                skipped += 1
            elif hit_flag:
                hits += 1
            else:
                misses += 1

            if len(rows) < 20:
                rows.append({
                    "date": d.isoformat(),
                    "home": home, "away": away, "total_goals": total,
                    "corridor": {"low": pred.corridor.low, "high": pred.corridor.high, "lean": pred.corridor.lean},
                    "translated_play": {"market": pred.translated_play.market, "confidence": pred.translated_play.confidence},
                    "confidence_score": pred.confidence_score,
                    "applied_modules": pred.applied_modules,
                    "inputs_used": metrics
                })

        total = len(fixtures[:limit])
        hit_rate = round((hits / max(1, (hits + misses))) * 100.0, 1)
        return CalibResult(
            total=total, evaluated=evaluated, hits=hits, misses=misses, skipped=skipped,
            hit_rate=hit_rate, sample=rows
        )
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
