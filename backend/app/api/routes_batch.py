# backend/app/api/routes_batch.py
"""
Batch prediction and validation endpoints.

POST /api/batch-predict
    Loops over all FBrefFixtures in the next FIXTURE_DAYS days,
    runs ATHENA predict_match on each, stores in PredictionLog.
    Deduplicates fixtures by normalised team name before predicting.

POST /api/batch-validate
    Loops over all PredictionLog entries with status=pending
    whose match_date has passed.
    Looks up actual score from FBrefSnapshot, evaluates hit/miss.

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
from app.database.models_predictions import FBrefFixture, PredictionLog, CalibrationLog, Base
from app.database.db import engine
from app.engine.types import MatchRequest
from app.services.predict import predict_match
from app.services.data_providers.fbref_base import asof_features, _parse_score_column, _resolve_columns, _match_team, _norm
from app.util.asian_lines import evaluate_market, hit_weight
from app.services.resolve_team import resolve_team_name, clear_resolve_cache

# Auto-create tables if needed (optional, handled in main.py)
Base.metadata.create_all(bind=engine)

router = APIRouter()

FIXTURE_DAYS = 5


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def _get_variance_flag(league_code: str, db) -> str | None:
    """Return variance flag based on latest calibration hit rate for a league."""
    latest = (
        db.query(CalibrationLog)
        .filter(CalibrationLog.league_code == league_code)
        .order_by(CalibrationLog.run_at.desc())
        .first()
    )
    if not latest:
        return None
    if latest.hit_rate >= 80:
        return "green"
    elif latest.hit_rate >= 70:
        return "orange"
    else:
        return "red"


def _dedup_key(league_code: str, home: str, away: str, match_date) -> str:
    """
    Build a normalised dedup key for a fixture.
    Strips accents, lowercases, and trims — so "FC Fredericia" and
    "Fredericia" produce the same key.
    """
    h = _norm(home)
    a = _norm(away)
    return f"{league_code}|{match_date}|{h}|{a}"


def _has_existing_prediction(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
    match_date,
) -> bool:
    """
    Check if a prediction already exists for this fixture, using
    normalised name matching to prevent duplicates from name variants.

    Checks both exact match AND normalised match against existing predictions.
    """
    # Fast path: exact match
    exact = db.query(PredictionLog).filter(
        PredictionLog.league_code == league_code,
        PredictionLog.home_team == home_team,
        PredictionLog.away_team == away_team,
        PredictionLog.match_date == match_date,
        PredictionLog.status.in_(["pending", "hit", "miss"]),
    ).first()

    if exact:
        return True

    # Slow path: check all predictions on this date+league with normalised names
    h_norm = _norm(home_team)
    a_norm = _norm(away_team)

    day_preds = db.query(PredictionLog).filter(
        PredictionLog.league_code == league_code,
        PredictionLog.match_date == match_date,
        PredictionLog.status.in_(["pending", "hit", "miss"]),
    ).all()

    for pred in day_preds:
        if _norm(pred.home_team) == h_norm and _norm(pred.away_team) == a_norm:
            return True

    return False


# ── POST /api/batch-predict ────────────────────────────────────────────────────

@router.post("/batch-predict")
def batch_predict(
    days_ahead: int = Query(FIXTURE_DAYS, ge=1, le=60,
                            description="How many days ahead to predict"),
    dry_run: bool = Query(False, description="If true, preview without saving"),
    force: bool = Query(False, description="If true, delete existing pending predictions and re-predict"),
    league_code: Optional[str] = Query(None, description="Limit to a single league"),
    db: Session = Depends(get_db),
):
    """
    Generate ATHENA predictions for all upcoming fixtures.
    Deduplicates fixtures by normalised team name before predicting.
    Use ?force=true to wipe pending entries and re-predict.
    """
    today  = date.today()
    cutoff = today + timedelta(days=days_ahead)

    q = db.query(FBrefFixture).filter(
        FBrefFixture.match_date >= today,
        FBrefFixture.match_date <= cutoff,
    )
    if league_code:
        q = q.filter(FBrefFixture.league_code == league_code)

    raw_fixtures = q.order_by(FBrefFixture.match_date).all()

    if not raw_fixtures:
        return {"message": "No fixtures found in window.", "days_ahead": days_ahead, "predicted": 0}

    # ── Resolve team names through Team/Alias tables ─────────────────
    # "FC Fredericia" → looks up TeamAlias → finds "Fredericia" → uses that.
    # This happens BEFORE dedup so all variants collapse to the same name.
    clear_resolve_cache()

    resolved_fixtures = []
    for fix in raw_fixtures:
        r_home = resolve_team_name(db, fix.home_team, fix.league_code)
        r_away = resolve_team_name(db, fix.away_team, fix.league_code)
        resolved_fixtures.append({
            "fix": fix,
            "home": r_home,
            "away": r_away,
        })

    # ── Deduplicate by resolved name ─────────────────────────────────
    seen_keys: set = set()
    fixtures = []
    deduped = 0

    for rf in resolved_fixtures:
        key = _dedup_key(rf["fix"].league_code, rf["home"], rf["away"], rf["fix"].match_date)
        if key in seen_keys:
            deduped += 1
            continue
        seen_keys.add(key)
        fixtures.append(rf)

    if deduped:
        print(f"[batch-predict] Deduped {deduped} fixture variants from {len(raw_fixtures)} raw fixtures")

    # force=true: wipe pending entries so they get re-predicted fresh
    if force and not dry_run:
        deleted = 0
        for rf in fixtures:
            fix = rf["fix"]
            h_norm = _norm(rf["home"])
            a_norm = _norm(rf["away"])
            day_pending = db.query(PredictionLog).filter(
                PredictionLog.league_code == fix.league_code,
                PredictionLog.match_date == fix.match_date,
                PredictionLog.status == "pending",
            ).all()
            for pred in day_pending:
                if _norm(pred.home_team) == h_norm and _norm(pred.away_team) == a_norm:
                    db.delete(pred)
                    deleted += 1

        db.commit()
        if deleted:
            print(f"[batch-predict] Force-deleted {deleted} pending predictions")

    predicted = skipped = errors = 0
    results = []
    skipped_by_league: dict = {}

    for rf in fixtures:
        fix = rf["fix"]
        home_resolved = rf["home"]
        away_resolved = rf["away"]

        # Skip if already has a prediction (uses resolved + normalised matching)
        if _has_existing_prediction(db, fix.league_code, home_resolved, away_resolved, fix.match_date):
            skipped += 1
            skipped_by_league[fix.league_code] = skipped_by_league.get(fix.league_code, 0) + 1
            continue

        try:
            # Use ORIGINAL fixture names for asof_features (they match FBref snapshot data)
            metrics = asof_features(
                fix.league_code,
                fix.home_team,
                fix.away_team,
                fix.match_date,
            )

            # Use RESOLVED names for the prediction request + storage
            req = MatchRequest(
                league_code=fix.league_code,
                home_team=home_resolved,
                away_team=away_resolved,
                match_date=fix.match_date,
                sot_proj_total=metrics.get("sot_proj_total"),
                support_idx_over_delta=metrics.get("support_idx_over_delta"),
                p_two_plus=metrics.get("p_two_plus"),
                p_home_tt05=metrics.get("p_home_tt05"),
                p_away_tt05=metrics.get("p_away_tt05"),
                tempo_index=metrics.get("tempo_index"),
                deg_pressure=metrics.get("deg_pressure"),
                det_boost=metrics.get("det_boost"),
                home_det=metrics.get("home_det"),
                away_det=metrics.get("away_det"),
                eps_stability=metrics.get("eps_stability"),
            )

            pred = predict_match(db, req)

            entry = {
                "league_code":    fix.league_code,
                "home_team":      home_resolved,
                "away_team":      away_resolved,
                "match_date":     fix.match_date.isoformat(),
                "market":         pred.translated_play.market,
                "confidence":     pred.translated_play.confidence,
                "corridor_low":   pred.corridor.low,
                "corridor_high":  pred.corridor.high,
                "lean":           pred.corridor.lean,
                "confidence_score": pred.confidence_score,
                "variance_flag": _get_variance_flag(fix.league_code, db),
            }
            results.append(entry)

            if not dry_run:
                log = PredictionLog(
                    league_code=fix.league_code,
                    home_team=home_resolved,
                    away_team=away_resolved,
                    match_date=fix.match_date,
                    match_time=getattr(fix, "match_time", None),
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
                    variance_flag=_get_variance_flag(fix.league_code, db),
                    predicted_at=datetime.utcnow(),
                )
                db.add(log)
            predicted += 1

        except Exception as e:
            errors += 1
            print(f"[batch-predict] Error {fix.league_code} {fix.home_team} vs {fix.away_team}: {e}")
            continue

    if not dry_run:
        db.commit()

    return {
        "dry_run":          dry_run,
        "fixtures_found":   len(raw_fixtures),
        "fixtures_deduped": deduped,
        "predicted":        predicted,
        "skipped":          skipped,
        "skipped_by_league": skipped_by_league,
        "errors":           errors,
        "tip": "Use ?force=true to wipe pending and re-predict." if skipped == len(fixtures) and skipped > 0 else None,
        "results":          results if dry_run else [],
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
    Uses fuzzy matching to handle team name variants.
    """
    from app.database.models_fbref import FBrefSnapshot

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
    Now includes v2.0 performance tags per match.
    """
    from app.database.models_predictions import PredictionLog as PL

    cutoff = date.today() - timedelta(days=days)
    q = db.query(PL).filter(PL.match_date >= cutoff)

    if status:
        q = q.filter(PL.status == status)
    if league_code:
        q = q.filter(PL.league_code == league_code)

    rows = q.order_by(PL.match_date.asc(), PL.id.asc()).all()

    # v2.0: Performance tags cached per league
    _league_tag_cache: dict = {}

    def _get_league_cache(lc: str) -> dict:
        if lc in _league_tag_cache:
            return _league_tag_cache[lc]
        cache = {"form_deltas": {}, "league_avgs": {"atk": 50.0, "mid": 50.0, "def": 50.0, "gk": 50.0}}
        try:
            from app.services.performance_tags import _compute_league_zone_avgs
            cache["league_avgs"] = _compute_league_zone_avgs(db, lc)
            from app.services.form_delta import compute_form_delta
            delta_data = compute_form_delta(db, lc)
            if delta_data and delta_data.get("teams"):
                cache["form_deltas"] = {t["team"]: t["form_delta"] for t in delta_data["teams"]}
        except Exception:
            pass
        _league_tag_cache[lc] = cache
        return cache

    def _match_tags_safe(lc: str, home: str, away: str) -> dict:
        try:
            from app.services.performance_tags import generate_match_tags
            cache = _get_league_cache(lc)
            return generate_match_tags(db, lc, home, away, cache["form_deltas"])
        except Exception:
            return {}

    grouped: dict = {}
    for r in rows:
        d = r.match_date.isoformat()
        if d not in grouped:
            grouped[d] = []

        tags = _match_tags_safe(r.league_code, r.home_team, r.away_team)

        grouped[d].append({
            "id":             r.id,
            "league_code":    r.league_code,
            "home_team":      r.home_team,
            "away_team":      r.away_team,
            "match_time":     getattr(r, "match_time", None),
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
            "variance_flag":  getattr(r, "variance_flag", None),
            "performance_tags": tags,
        })

    return {
        "total":    len(rows),
        "by_date":  grouped,
    }


# ── POST /api/cleanup-duplicate-predictions ───────────────────────────────────

@router.post("/cleanup-duplicate-predictions")
def cleanup_duplicate_predictions(
    dry_run: bool = Query(True, description="Preview only — set false to actually delete"),
    db: Session = Depends(get_db),
):
    """
    Find and remove duplicate predictions caused by team name variants.
    Keeps the oldest prediction per normalised (league, date, home, away) group.
    """
    all_preds = db.query(PredictionLog).order_by(PredictionLog.id.asc()).all()

    seen: dict = {}  # dedup_key → first PredictionLog.id
    duplicates = []

    for pred in all_preds:
        key = _dedup_key(pred.league_code, pred.home_team, pred.away_team, pred.match_date)
        if key in seen:
            duplicates.append({
                "id": pred.id,
                "match": f"{pred.home_team} vs {pred.away_team}",
                "date": pred.match_date.isoformat(),
                "league": pred.league_code,
                "status": pred.status,
                "kept_id": seen[key],
            })
            if not dry_run:
                db.delete(pred)
        else:
            seen[key] = pred.id

    if not dry_run and duplicates:
        db.commit()

    return {
        "dry_run": dry_run,
        "total_predictions": len(all_preds),
        "duplicates_found": len(duplicates),
        "duplicates": duplicates[:50],  # cap output
    }


# ── GET /api/fixtures-debug ────────────────────────────────────────────────────

@router.get("/fixtures-debug")
def fixtures_debug(
    days_ahead: int = Query(60, ge=1, le=365),
    db: Session = Depends(get_db),
):
    from datetime import date, timedelta

    today  = date.today()
    cutoff = today + timedelta(days=days_ahead)

    all_fixtures = db.query(FBrefFixture).order_by(
        FBrefFixture.league_code, FBrefFixture.match_date
    ).all()

    window_fixtures = [f for f in all_fixtures
                       if today <= f.match_date <= cutoff]

    by_league: dict = {}
    for f in all_fixtures:
        lc = f.league_code
        if lc not in by_league:
            by_league[lc] = []
        by_league[lc].append(f.match_date.isoformat())

    window_by_league: dict = {}
    for f in window_fixtures:
        lc = f.league_code
        if lc not in window_by_league:
            window_by_league[lc] = []
        window_by_league[lc].append(f.match_date.isoformat())

    skipped_by_league: dict = {}
    for f in window_fixtures:
        if _has_existing_prediction(db, f.league_code, f.home_team, f.away_team, f.match_date):
            lc = f.league_code
            skipped_by_league[lc] = skipped_by_league.get(lc, 0) + 1

    return {
        "today":              today.isoformat(),
        "window":             f"{today} → {cutoff}",
        "total_in_db":        len(all_fixtures),
        "total_in_window":    len(window_fixtures),
        "all_leagues_in_db":  {lc: len(dates) for lc, dates in by_league.items()},
        "leagues_in_window":  {lc: dates for lc, dates in window_by_league.items()},
        "already_predicted":  skipped_by_league,
    }


# ── Migration endpoints (kept from original) ─────────────────────────────────

@router.post("/migrate/add-variance-flag")
def migrate_add_variance_flag(db: Session = Depends(get_db)):
    from sqlalchemy import text, inspect
    results = {}
    inspector = inspect(db.bind)
    existing_cols = [c["name"] for c in inspector.get_columns("prediction_log")]
    if "variance_flag" not in existing_cols:
        db.execute(text("ALTER TABLE prediction_log ADD COLUMN variance_flag VARCHAR"))
        db.commit()
        results["variance_flag"] = "added"
    else:
        results["variance_flag"] = "already exists"
    if "match_time" not in existing_cols:
        db.execute(text("ALTER TABLE prediction_log ADD COLUMN match_time VARCHAR"))
        db.commit()
        results["match_time"] = "added"
    else:
        results["match_time"] = "already exists"
    existing_tables = inspector.get_table_names()
    if "calibration_log" not in existing_tables:
        db.execute(text("""
            CREATE TABLE calibration_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                league_code VARCHAR NOT NULL,
                hit_rate FLOAT NOT NULL,
                sample_size INTEGER,
                applied BOOLEAN DEFAULT 0,
                run_at TIMESTAMP
            )
        """))
        db.commit()
        results["calibration_log"] = "created"
    else:
        results["calibration_log"] = "already exists"
    return {"status": "ok", "migrations": results}


@router.post("/migrate/add-module-columns")
def migrate_add_module_columns(db: Session = Depends(get_db)):
    from sqlalchemy import text, inspect
    results = {}
    inspector = inspect(db.bind)
    league_cols = [c["name"] for c in inspector.get_columns("league_configs")]
    for col, default in [("deg_sensitivity", 1.0), ("det_sensitivity", 1.0), ("eps_sensitivity", 1.0)]:
        if col not in league_cols:
            db.execute(text(f"ALTER TABLE league_configs ADD COLUMN {col} FLOAT DEFAULT {default}"))
            db.commit()
            results[f"league_configs.{col}"] = "added"
        else:
            results[f"league_configs.{col}"] = "already exists"
    team_cols = [c["name"] for c in inspector.get_columns("team_configs")]
    for col, default in [("det_nudge", 0.0), ("deg_nudge", 0.0)]:
        if col not in team_cols:
            db.execute(text(f"ALTER TABLE team_configs ADD COLUMN {col} FLOAT DEFAULT {default}"))
            db.commit()
            results[f"team_configs.{col}"] = "added"
        else:
            results[f"team_configs.{col}"] = "already exists"
    for col in ["avg_det", "avg_deg"]:
        if col not in team_cols:
            db.execute(text(f"ALTER TABLE team_configs ADD COLUMN {col} FLOAT"))
            db.commit()
            results[f"team_configs.{col}"] = "added"
        else:
            results[f"team_configs.{col}"] = "already exists"
    return {"status": "ok", "migrations": results}


@router.post("/migrate/backfill-variance-flags")
def backfill_variance_flags(db: Session = Depends(get_db)):
    from sqlalchemy import text
    rows = db.execute(text("""
        SELECT league_code, hit_rate FROM calibration_log
        WHERE id IN (SELECT MAX(id) FROM calibration_log GROUP BY league_code)
    """)).fetchall()
    if not rows:
        return {"status": "no_calibration_data"}
    league_flags = {}
    for league_code, hit_rate in rows:
        league_flags[league_code] = "green" if hit_rate >= 80 else "orange" if hit_rate >= 70 else "red"
    updated = 0
    for league_code, flag in league_flags.items():
        result = db.execute(text("""
            UPDATE prediction_log SET variance_flag = :flag
            WHERE league_code = :lc AND (variance_flag IS NULL OR variance_flag = '')
        """), {"flag": flag, "lc": league_code})
        updated += result.rowcount
    db.commit()
    return {"status": "ok", "rows_updated": updated, "flags": league_flags}


@router.post("/patch-prediction-metadata")
def patch_prediction_metadata(
    league_code: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    q = db.query(PredictionLog)
    if league_code:
        q = q.filter(PredictionLog.league_code == league_code)
    predictions = q.all()
    time_updated = flag_updated = 0
    for pred in predictions:
        if not getattr(pred, "match_time", None):
            fix = db.query(FBrefFixture).filter(
                FBrefFixture.league_code == pred.league_code,
                FBrefFixture.home_team == pred.home_team,
                FBrefFixture.away_team == pred.away_team,
                FBrefFixture.match_date == pred.match_date,
            ).first()
            if fix and getattr(fix, "match_time", None):
                pred.match_time = fix.match_time
                time_updated += 1
        if not getattr(pred, "variance_flag", None):
            flag = _get_variance_flag(pred.league_code, db)
            if flag:
                pred.variance_flag = flag
                flag_updated += 1
    db.commit()
    return {"status": "ok", "time_updated": time_updated, "flag_updated": flag_updated}
