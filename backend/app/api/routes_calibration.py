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

INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Matchup validation endpoint ───────────────────────────────────────────────

@router.get("/validate-matchup")
def validate_matchup(
    home_team:   str,
    away_team:   str,
    league_code: str,
    match_date:  Optional[str] = None,
    db: Session = Depends(get_db),
):
    """
    Pre-submit validation for the home page matchup form.

    Checks:
      1. Both teams exist in the given league (alias-resolved).
         Skipped for international competitions (UCL/UEL/UECL/EC/WC).
      2. For past dates: the fixture actually existed in the snapshot
         on that specific date (uses validate_match_existed from fbref_base).
         Skipped for future dates — we can't validate what hasn't happened.

    Returns:
      { valid: true }  — all checks passed, safe to submit
      { valid: false, reason: "..." }  — clear user-facing message
    """
    from app.models.team import Team
    from app.services.data_providers.fbref_base import validate_match_existed
    from datetime import date as date_type

    home_r = resolve_team_name(db, home_team, league_code)
    away_r = resolve_team_name(db, away_team, league_code)

    # ── 1. League membership check ────────────────────────────────────
    # Skip for international competitions — teams come from domestic leagues.
    if league_code not in INTL_LEAGUE_CODES:
        home_in_league = db.query(Team).filter_by(
            team_key=home_r, league_code=league_code
        ).first()
        away_in_league = db.query(Team).filter_by(
            team_key=away_r, league_code=league_code
        ).first()

        if not home_in_league:
            return {
                "valid": False,
                "reason": f"\'{home_team}\' is not registered in {league_code}. "
                          f"Check the team name or select the correct league.",
            }
        if not away_in_league:
            return {
                "valid": False,
                "reason": f"\'{away_team}\' is not registered in {league_code}. "
                          f"Check the team name or select the correct league.",
            }

    # ── 2. Past-date fixture existence check ──────────────────────────
    if match_date:
        try:
            parsed_date = date_type.fromisoformat(match_date)
        except ValueError:
            return {"valid": False, "reason": "Invalid date format."}

        today = date_type.today()
        if parsed_date < today:
            exists, reason = validate_match_existed(
                league_code, home_r, away_r, parsed_date
            )
            if not exists:
                return {"valid": False, "reason": reason}

    return {"valid": True}


class PredictBody(BaseModel):
    # ── Required ──────────────────────────────────────────────────────
    league_code: str            = Field(...,  example="ENG-PL")
    home_team:   str            = Field(...,  example="Arsenal")
    away_team:   str            = Field(...,  example="Chelsea")
    match_date:  Optional[date] = Field(None, example="2026-04-05")

    # ── Core rolling features (computed by asof_features if omitted) ──
    sot_proj_total:         Optional[float] = Field(None, example=11.2)
    support_idx_over_delta: Optional[float] = Field(None, example=0.09)
    p_two_plus:             Optional[float] = Field(None, example=0.76)
    p_home_tt05:            Optional[float] = Field(None, example=0.71)
    p_away_tt05:            Optional[float] = Field(None, example=0.65)
    tempo_index:            Optional[float] = Field(None, example=0.62)

    # ── Module features (v2.2 — passable directly) ───────────────────
    # Computed by fbref_base.asof_features from rolling data.
    # Passing them here overrides pipeline defaults — useful for
    # manual predictions or retrosim with known match conditions.
    deg_pressure:  Optional[float] = Field(None, example=0.15)
    det_boost:     Optional[float] = Field(None, example=0.35)
    home_det:      Optional[float] = Field(None, example=0.40)
    away_det:      Optional[float] = Field(None, example=0.30)
    eps_stability: Optional[float] = Field(None, example=0.72)


@router.post("/predict")
def post_predict(body: PredictBody, db: Session = Depends(get_db)):
    try:
        # ── Resolve team names ────────────────────────────────────────
        home_resolved = resolve_team_name(db, body.home_team, body.league_code)
        away_resolved = resolve_team_name(db, body.away_team, body.league_code)

        # Build MatchRequest from pipeline fields only (exclude odds fields)
        req = MatchRequest(
            league_code            = body.league_code,
            home_team              = home_resolved,
            away_team              = away_resolved,
            match_date             = body.match_date,
            sot_proj_total         = body.sot_proj_total,
            support_idx_over_delta = body.support_idx_over_delta,
            p_two_plus             = body.p_two_plus,
            p_home_tt05            = body.p_home_tt05,
            p_away_tt05            = body.p_away_tt05,
            tempo_index            = body.tempo_index,
            deg_pressure           = body.deg_pressure,
            det_boost              = body.det_boost,
            home_det               = body.home_det,
            away_det               = body.away_det,
            eps_stability          = body.eps_stability,
        )
        pred = predict_match(db, req)

        # ── Performance tags ──────────────────────────────────────────
        perf_tags = {}
        try:
            from app.services.performance_tags import generate_match_tags
            from app.services.performance_tags import _compute_league_zone_avgs
            _compute_league_zone_avgs(db, body.league_code)
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

        # ── Calibrated probability ────────────────────────────────────
        calibrated_probability = None
        try:
            from app.services.confidence_calibrator import calibrate_confidence
            calibrated_probability = calibrate_confidence(
                db, pred.confidence_score, league_code=body.league_code
            )
        except Exception:
            pass

        return {
            "league_code":            pred.league_code,
            "fixture":                pred.fixture,
            "corridor": {
                "low":  pred.corridor.low,
                "high": pred.corridor.high,
                "lean": pred.corridor.lean,
            },
            "translated_play": {
                "market":     pred.translated_play.market,
                "confidence": pred.translated_play.confidence,
            },
            "confidence_score":       pred.confidence_score,
            "calibrated_probability": calibrated_probability,
            "applied_modules":        pred.applied_modules,
            "safety_flags":           pred.safety_flags,
            "explanations":           pred.explanations,
            "performance_tags":       perf_tags,
        }

    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))
