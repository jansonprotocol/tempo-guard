# backend/app/api/routes_batch.py
"""
Batch prediction and validation endpoints.

POST /api/batch-predict
    Loops over all FBrefFixtures in the next FIXTURE_DAYS days,
    runs ATHENA predict_match on each, stores in PredictionLog.
    Skip fixtures that already have a pending/hit/miss prediction.

POST /api/batch-validate
    Loops over all PredictionLog entries with status=pending
    whose match_date has passed.
    Looks up actual score from FBrefSnapshot, evaluates hit/miss.
    Run this after your daily scrape.

GET /api/predictions
    Returns PredictionLog entries for the frontend Predictions page.
    Grouped by date, sorted recent → older.
"""
from __future__ import annotations

import io
import json
from datetime import date, datetime, timedelta
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.database.models_predictions import FBrefFixture, PredictionLog, Base
from app.database.db import engine
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.data_providers.fbref_base import asof_features, _parse_score_column, _resolve_columns
from app.util.asian_lines import evaluate_market, hit_weight

# Auto-create tables if needed
Base.metadata.create_all(bind=engine)

router = APIRouter()

FIXTURE_DAYS = 5


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── POST /api/batch-predict ────────────────────────────────────────────────────

@router.post("/batch-predict")
def batch_predict(
    days_ahead: int = Query(FIXTURE_DAYS, ge=1, le=14,
                            description="How many days ahead to predict"),
    dry_run: bool = Query(False, description="If true, preview without saving"),
    db: Session = Depends(get_db),
):
    """
    Generate ATHENA predictions for all upcoming fixtures.
    Skips fixtures already predicted (pending/hit/miss).
    """
    today  = date.today()
    cutoff = today + timedelta(days=days_ahead)

    fixtures = db.query(FBrefFixture).filter(
        FBrefFixture.match_date >= today,
        FBrefFixture.match_date <= cutoff,
    ).order_by(FBrefFixture.match_date).all()

    if not fixtures:
        return {"message": "No fixtures found in window.", "days_ahead": days_ahead, "predicted": 0}

    predicted = skipped = errors = 0
    results = []

    for fix in fixtures:
        # Skip if already predicted
        existing = db.query(PredictionLog).filter(
            PredictionLog.league_code == fix.league_code,
            PredictionLog.home_team   == fix.home_team,
            PredictionLog.away_team   == fix.away_team,
            PredictionLog.match_date  == fix.match_date,
        ).first()

        if existing:
            skipped += 1
            continue

        try:
            metrics = asof_features(
                fix.league_code,
                fix.home_team,
                fix.away_team,
                fix.match_date,
            )

            req = MatchRequest(
                league_code=fix.league_code,
                home_team=fix.home_team,
                away_team=fix.away_team,
                match_date=fix.match_date,
                sot_proj_total=metrics.get("sot_proj_total"),
                support_idx_over_delta=metrics.get("support_idx_over_delta"),
                p_two_plus=metrics.get("p_two_plus"),
                p_home_tt05=metrics.get("p_home_tt05"),
                p_away_tt05=metrics.get("p_away_tt05"),
                tempo_index=metrics.get("tempo_index"),
            )

            pred = predict_match(db, req)

            entry = {
                "league_code":    fix.league_code,
                "home_team":      fix.home_team,
                "away_team":      fix.away_team,
                "match_date":     fix.match_date.isoformat(),
                "market":         pred.translated_play.market,
                "confidence":     pred.translated_play.confidence,
                "corridor_low":   pred.corridor.low,
                "corridor_high":  pred.corridor.high,
                "lean":           pred.corridor.lean,
                "confidence_score": pred.confidence_score,
            }
            results.append(entry)

            if not dry_run:
                log = PredictionLog(
                    league_code=fix.league_code,
                    home_team=fix.home_team,
                    away_team=fix.away_team,
                    match_date=fix.match_date,
                    market=pred.translated_play.market,
                    confidence=pred.translated_play.confidence,
                    corridor_low=pred.corridor.low,
                    corridor_high=pred.corridor.high,
                    lean=pred.corridor.lean,
                    confidence_score=pred.confidence_score,
                    applied_modules=json.dumps(pred.applied_modules),
                    explanations=json.dumps(pred.explanations),
                    p_two_plus=metrics.get("p_two_plus"),
                    tempo_index=metrics.get("tempo_index"),
                    sot_proj_total=metrics.get("sot_proj_total"),
                    support_delta=metrics.get("support_idx_over_delta"),
                    status="pending",
                    predicted_at=datetime.utcnow(),
                )
                db.add(log)
            predicted += 1

        except Exception as e:
            errors += 1
            print(f"[batch-predict] Error {fix.home_team} vs {fix.away_team}: {e}")
            continue

    if not dry_run:
        db.commit()

    return {
        "dry_run":   dry_run,
        "fixtures_found": len(fixtures),
        "predicted": predicted,
        "skipped":   skipped,
        "errors":    errors,
        "results":   results if dry_run else [],
    }


# ── POST /api/batch-validate ───────────────────────────────────────────────────

@router.post("/batch-validate")
def batch_validate(
    dry_run: bool = Query(False, description="If true, preview without saving"),
    db: Session = Depends(get_db),
):
    """
    Validate pending predictions whose match date has passed.
    Looks up actual score from FBrefSnapshot, marks hit/miss/void.
    Run this after your daily scrape.
    """
    today = date.today()

    pending = db.query(PredictionLog).filter(
        PredictionLog.status.in_(["pending", "void"]),
        PredictionLog.match_date <  today,
    ).order_by(PredictionLog.match_date).all()

    if not pending:
        return {"message": "No pending predictions to validate.", "evaluated": 0}

    hits = misses = voids = 0
    results = []

    for pred in pending:
        actual_score, total = _lookup_actual_score(
            pred.league_code, pred.home_team, pred.away_team, pred.match_date, db
        )

        if actual_score is None:
            status = "void"
            voids += 1
        else:
            hg, ag = map(int, actual_score.split("-"))
            result = evaluate_market(pred.market, hg, ag)
            hw = hit_weight(result)
            if hw < 0:
                status = "void"
                voids += 1
            elif hw >= 0.5:
                status = "hit"
                hits += 1
            else:
                status = "miss"
                misses += 1

        results.append({
            "league_code":  pred.league_code,
            "home":         pred.home_team,
            "away":         pred.away_team,
            "date":         pred.match_date.isoformat(),
            "market":       pred.market,
            "actual_score": actual_score,
            "total_goals":  total,
            "status":       status,
        })

        if not dry_run:
            pred.status       = status
            pred.actual_score = actual_score
            pred.actual_total = total
            pred.evaluated_at = datetime.utcnow()

    if not dry_run:
        db.commit()

    total_evaluated = hits + misses + voids
    hit_rate = round(hits / max(1, hits + misses) * 100, 1)

    return {
        "dry_run":   dry_run,
        "evaluated": total_evaluated,
        "hits":      hits,
        "misses":    misses,
        "voids":     voids,
        "hit_rate":  hit_rate,
        "results":   results,
    }


def _lookup_actual_score(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    db: Session,
) -> tuple[Optional[str], Optional[int]]:
    """
    Look up actual score from FBrefSnapshot.
    Returns ("2-1", 3) or (None, None) if not found.
    """
    from app.database.models_fbref import FBrefSnapshot
    from app.services.data_providers.fbref_base import _match_team, _norm

    snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not snap:
        return None, None

    try:
        df = pd.read_parquet(io.BytesIO(snap.data))
    except Exception:
        return None, None

    c = _resolve_columns(df)
    if not all([c["date"], c["ht"], c["at"]]):
        return None, None

    # Parse score if needed
    if not c["hg"] and c["score"]:
        df = _parse_score_column(df, c["score"])
        c  = _resolve_columns(df)

    if not c["hg"] or not c["ag"]:
        return None, None

    df[c["date"]] = pd.to_datetime(df[c["date"]], errors="coerce")
    day_df = df[df[c["date"]].dt.date == match_date]

    if day_df.empty:
        return None, None

    all_teams = list(set(
        day_df[c["ht"]].astype(str).tolist() +
        day_df[c["at"]].astype(str).tolist()
    ))

    matched_home = _match_team(home_team, all_teams)
    matched_away = _match_team(away_team, all_teams)

    if not matched_home or not matched_away:
        return None, None

    h_norm = _norm(matched_home)
    a_norm = _norm(matched_away)

    for _, row in day_df.iterrows():
        if (_norm(str(row[c["ht"]])) == h_norm and
                _norm(str(row[c["at"]])) == a_norm):
            hg = int(row[c["hg"]])
            ag = int(row[c["ag"]])
            return f"{hg}-{ag}", hg + ag

    return None, None


# ── GET /api/predictions ───────────────────────────────────────────────────────

@router.get("/predictions")
def get_predictions(
    status: Optional[str] = Query(None, description="Filter: pending / hit / miss / void"),
    league_code: Optional[str] = Query(None),
    days: int = Query(30, ge=1, le=365, description="How many days back to include"),
    db: Session = Depends(get_db),
):
    """
    Returns predictions for the frontend Predictions page.
    Sorted by date descending (most recent first).
    """
    from app.database.models_predictions import PredictionLog as PL

    cutoff = date.today() - timedelta(days=days)
    q = db.query(PL).filter(PL.match_date >= cutoff)

    if status:
        q = q.filter(PL.status == status)
    if league_code:
        q = q.filter(PL.league_code == league_code)

    rows = q.order_by(PL.match_date.asc(), PL.id.asc()).all()

    # Group by date
    grouped: dict = {}
    for r in rows:
        d = r.match_date.isoformat()
        if d not in grouped:
            grouped[d] = []
        grouped[d].append({
            "id":             r.id,
            "league_code":    r.league_code,
            "home_team":      r.home_team,
            "away_team":      r.away_team,
            "market":         r.market,
            "confidence":     r.confidence,
            "corridor":       f"{r.corridor_low}–{r.corridor_high}",
            "lean":           r.lean,
            "confidence_score": r.confidence_score,
            "status":         r.status,
            "actual_score":   r.actual_score,
            "actual_total":   r.actual_total,
            "p_two_plus":     r.p_two_plus,
            "tempo_index":    r.tempo_index,
            "predicted_at":   r.predicted_at.isoformat() if r.predicted_at else None,
            "evaluated_at":   r.evaluated_at.isoformat() if r.evaluated_at else None,
        })

    return {
        "total":    len(rows),
        "by_date":  grouped,
    }
