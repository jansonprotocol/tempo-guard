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
from app.services.weather_service import get_match_weather, get_stadium_coords, match_hour_utc
from app.services.confidence_calibrator import calibrate_confidence
from app.services.feature_cache import warm_snapshot_cache
from app.models.league_config import LeagueConfig

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


def _compute_alt_market(
    variance_flag: str,
    market: str,
    confidence: str,
    confidence_score: float,
    p_home_tt05: float,
    p_away_tt05: float,
    alt_flip_threshold: float = 0.62,
    tt_home_bias: float = 0.0,
    use_alt_market: bool = True,
    tt_home_weak: bool = False,
    tt_away_weak: bool = False,
    tt_confidence_min: float = 0.62,
) -> tuple[str, str] | tuple[None, None]:
    """
    Apply alt market substitution (TT/flip) when use_alt_market is True.

    Rules (confidence_score based):
      score < alt_flip_threshold → flip to opposite main (Over→U3.5, Under→O1.75)
      score >= alt_flip_threshold → strongest TT side (Home or Away O0.5)

    use_alt_market: if False, returns (None, None) — original market served.
    tt_home_weak/tt_away_weak: if True, that side falls back to original market.
    alt_flip_threshold: per-league, calibration-tunable (default 0.62)
    tt_home_bias: positive = prefer home TT, negative = prefer away TT
    """
    original = market

    # Calibration may suppress alt substitution for this league
    if not use_alt_market:
        return None, None

    score = confidence_score or {"HIGH": 0.85, "MEDIUM": 0.65, "LOW": 0.40}.get(confidence, 0.65)

    if score < alt_flip_threshold:
        alt = "U3.5" if market.startswith("O") else "O1.75"
        return alt, original

    # TT confidence gate — below this score serve original, not TT
    if score < tt_confidence_min:
        return None, None

    if p_home_tt05 is not None or p_away_tt05 is not None:
        h = (p_home_tt05 or 0.0) + tt_home_bias
        a = p_away_tt05 or 0.0
        if h >= a:
            alt = original if tt_home_weak else "TT Home O0.5"
        else:
            alt = original if tt_away_weak else "TT Away O0.5"
        if alt == original:
            return None, None  # weak side — serve original
        return alt, original

    return None, None


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

    # ── Pre-build existing prediction keys ───────────────────────────
    # Avoids a DB query per fixture inside the loop. One query up front
    # loads all relevant predictions into a set for O(1) lookup.
    all_dates  = {rf["fix"].match_date for rf in fixtures}
    all_leagues = {rf["fix"].league_code for rf in fixtures}
    existing_preds = set()
    if all_dates and all_leagues:
        window_preds = db.query(
            PredictionLog.league_code,
            PredictionLog.home_team,
            PredictionLog.away_team,
            PredictionLog.match_date,
        ).filter(
            PredictionLog.league_code.in_(all_leagues),
            PredictionLog.match_date.in_(all_dates),
            PredictionLog.status.in_(["pending", "hit", "miss"]),
        ).all()
        for p in window_preds:
            existing_preds.add(
                f"{p.league_code}|{p.match_date}|{_norm(p.home_team)}|{_norm(p.away_team)}"
            )

    # ── Pre-warm feature cache per league ────────────────────────────
    # Loads snapshot DataFrames into memory once per league so
    # asof_features skips its internal DB read on every fixture.
    warmed_leagues: set = set()
    for rf in fixtures:
        lc = rf["fix"].league_code
        if lc not in warmed_leagues:
            try:
                warm_snapshot_cache(db, lc)
                warmed_leagues.add(lc)
            except Exception:
                pass

    # ── Pre-parse score columns in warmed snapshots ───────────────────
    # The warmed DataFrames may still have a raw Score column.
    # Pre-parse once per league so asof_features never triggers the
    # per-fixture score parsing path.
    try:
        from app.services.data_providers.fbref_base import (
            _SNAPSHOT_OVERRIDE, _parse_score_column as _fbref_parse_score
        )
        for lc in warmed_leagues:
            if lc in _SNAPSHOT_OVERRIDE:
                _df = _SNAPSHOT_OVERRIDE[lc]
                _cols = [str(c).lower() for c in _df.columns]
                _sc = next(
                    (c for c in _df.columns if str(c).lower() in ("score", "scores")),
                    None
                )
                if _sc and "hg" not in _cols:
                    _SNAPSHOT_OVERRIDE[lc] = _fbref_parse_score(_df, _sc)
    except Exception:
        pass

    # ── Pre-load squad power per league ──────────────────────────────
    # predict_match → _player_power_nudge reads TeamConfig.squad_power
    # for every fixture. Pre-load into a dict so predict_match can use
    # it directly without DB queries per fixture.
    # Injected into predict module's config cache via a per-request
    # override dict attached to the db session.
    try:
        from app.models.team_config import TeamConfig as _TC
        if not hasattr(db, "_squad_power_cache"):
            db._squad_power_cache = {}
        for lc in warmed_leagues:
            if lc not in db._squad_power_cache:
                rows = (
                    db.query(_TC.team, _TC.squad_power)
                    .filter(
                        _TC.league_code == lc,
                        _TC.squad_power.isnot(None),
                    )
                    .all()
                )
                db._squad_power_cache[lc] = {r.team: float(r.squad_power) for r in rows}
    except Exception:
        pass

    # ── Pre-cache per-league data ────────────────────────────────────
    # _get_variance_flag, LeagueConfig, and calibrate_confidence are called
    # once per fixture = 3 DB queries × 200 fixtures = 600 queries.
    # Pre-load all of these into dicts keyed by league_code.
    _variance_cache: dict = {}
    _cfg_cache: dict = {}
    _cal_cache: dict = {}  # (league_code, rounded_score) → cal_prob
    for lc in all_leagues:
        _variance_cache[lc] = _get_variance_flag(lc, db)
        _cfg_cache[lc] = db.query(LeagueConfig).filter_by(league_code=lc).first()

    def _get_cal_prob(lc: str, score: float) -> float | None:
        """Calibrated probability with per-league+score memoization."""
        if score is None:
            return None
        key = (lc, round(score, 3))
        if key not in _cal_cache:
            try:
                _cal_cache[key] = calibrate_confidence(db, score, league_code=lc)
            except Exception:
                _cal_cache[key] = None
        return _cal_cache[key]

    # Pre-fetch weather per unique (home_team, date) pair.
    # get_match_weather makes an HTTP call — doing it 200× is the biggest
    # single bottleneck. One call per unique home+date combination instead.
    _weather_cache: dict = {}  # (home_resolved, date) → {tag, impact} | None
    for rf in fixtures:
        key = (rf["home"], rf["fix"].match_date)
        if key not in _weather_cache:
            try:
                coords = get_stadium_coords(rf["home"])
                if coords:
                    hour = match_hour_utc(getattr(rf["fix"], "match_time", None))
                    _weather_cache[key] = get_match_weather(
                        coords[0], coords[1], rf["fix"].match_date, hour_utc=hour
                    )
                else:
                    _weather_cache[key] = None
            except Exception:
                _weather_cache[key] = None

    for rf in fixtures:
        fix = rf["fix"]
        home_resolved = rf["home"]
        away_resolved = rf["away"]

        # Skip if already has a prediction — O(1) set lookup
        pred_key = f"{fix.league_code}|{fix.match_date}|{_norm(home_resolved)}|{_norm(away_resolved)}"
        if pred_key in existing_preds:
            skipped += 1
            skipped_by_league[fix.league_code] = skipped_by_league.get(fix.league_code, 0) + 1
            continue

        # Skip if below league minimum confidence gate
        # (confidence_score not yet known here — check after predict_match)
        try:
            # Use ORIGINAL fixture names for asof_features (they match FBref snapshot data)
            metrics = asof_features(
                fix.league_code,
                fix.home_team,
                fix.away_team,
                fix.match_date,
            )

            # ── Weather adjustment (pre-fetched) ───────────────────────
            weather_tag    = None
            weather_impact = 0.0
            w = _weather_cache.get((home_resolved, fix.match_date))
            if w:
                weather_tag    = w["weather_tag"]
                weather_impact = w["weather_impact"]
                if weather_impact != 0.0:
                    current_deg = metrics.get("deg_pressure") or 0.0
                    metrics["deg_pressure"] = round(
                        min(1.0, max(0.0, current_deg + weather_impact)), 3
                    )
                    print(
                        f"[batch-predict] Weather {fix.league_code} "
                        f"{home_resolved}: {weather_tag} "
                        f"→ deg {current_deg:.3f}→{metrics['deg_pressure']:.3f}"
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

            # Skip if below league minimum confidence gate
            if _min_conf > 0.0 and (pred.confidence_score or 0.0) < _min_conf:
                skipped += 1
                skipped_by_league[fix.league_code] = skipped_by_league.get(fix.league_code, 0) + 1
                continue

            # ── Alt market substitution for red variance leagues ─────
            variance_flag = _variance_cache.get(fix.league_code)
            p_home_tt = metrics.get("p_home_tt05")
            p_away_tt = metrics.get("p_away_tt05")
            _cfg = _cfg_cache.get(fix.league_code)
            _flip_thresh  = float(_cfg.alt_flip_threshold or 0.62)   if _cfg and hasattr(_cfg, "alt_flip_threshold")    and _cfg.alt_flip_threshold    is not None else 0.62
            _tt_bias      = float(_cfg.tt_home_bias or 0.0)           if _cfg and hasattr(_cfg, "tt_home_bias")          and _cfg.tt_home_bias          is not None else 0.0
            _use_alt      = bool(getattr(_cfg, "use_alt_market", True)) if _cfg else True
            _tt_home_weak  = bool(getattr(_cfg,  "tt_home_weak",       False)) if _cfg else False
            _tt_away_weak  = bool(getattr(_cfg,  "tt_away_weak",       False)) if _cfg else False
            _tt_conf_min   = float(getattr(_cfg, "tt_confidence_min",  None) or 0.62) if _cfg else 0.62
            _min_conf      = float(getattr(_cfg, "min_confidence",     None) or 0.0)  if _cfg else 0.0
            alt_market, original_market = _compute_alt_market(
                variance_flag,
                pred.translated_play.market,
                pred.translated_play.confidence,
                pred.confidence_score,
                p_home_tt,
                p_away_tt,
                alt_flip_threshold=_flip_thresh,
                tt_home_bias=_tt_bias,
                use_alt_market=_use_alt,
                tt_home_weak=_tt_home_weak,
                tt_away_weak=_tt_away_weak,
                tt_confidence_min=_tt_conf_min,
            )
            final_market    = alt_market if alt_market else pred.translated_play.market
            alt_suppressed  = (not _use_alt)  # calibration blocked TT/flip for this league

            # ── Calibrated probability (memoized per league+score) ───
            cal_prob = _get_cal_prob(fix.league_code, pred.confidence_score)

            entry = {
                "league_code":           fix.league_code,
                "home_team":             home_resolved,
                "away_team":             away_resolved,
                "match_date":            fix.match_date.isoformat(),
                "market":                final_market,
                "original_market":       original_market,
                "alt_suppressed":        alt_suppressed,
                "confidence":            pred.translated_play.confidence,
                "corridor_low":          pred.corridor.low,
                "corridor_high":         pred.corridor.high,
                "lean":                  pred.corridor.lean,
                "confidence_score":      pred.confidence_score,
                "calibrated_probability": cal_prob,
                "variance_flag":         variance_flag,
                "weather_tag":           weather_tag,
                "weather_impact":        weather_impact if weather_impact != 0.0 else None,
            }
            results.append(entry)

            if not dry_run:
                log = PredictionLog(
                    league_code=fix.league_code,
                    home_team=home_resolved,
                    away_team=away_resolved,
                    match_date=fix.match_date,
                    match_time=getattr(fix, "match_time", None),
                    market=final_market,
                    confidence=pred.translated_play.confidence,
                    corridor_low=pred.corridor.low,
                    corridor_high=pred.corridor.high,
                    lean=pred.corridor.lean,
                    confidence_score=pred.confidence_score,
                    applied_modules=json.dumps(pred.applied_modules),
                    explanations=json.dumps({
                        "modules": pred.explanations,
                        "p_home_tt05": p_home_tt,
                        "p_away_tt05": p_away_tt,
                        "original_market": original_market,
                        "alt_suppressed": alt_suppressed,
                    }),
                    p_two_plus=metrics.get("p_two_plus"),
                    tempo_index=metrics.get("tempo_index"),
                    sot_proj_total=metrics.get("sot_proj_total"),
                    support_delta=metrics.get("support_idx_over_delta"),
                    status="pending",
                    variance_flag=variance_flag,
                    predicted_at=datetime.utcnow(),
                    weather_tag=weather_tag,
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

    # ── Per-league caches (populated once, reused per row) ──────────
    # Avoids repeated DB queries for calibration breakpoints,
    # performance tag data, and form deltas inside the per-row loop.
    _league_tag_cache:   dict = {}
    _calib_cache:        dict = {}   # league_code → breakpoints list or None

    def _get_calib_breakpoints(lc: str):
        """Load calibration breakpoints once per league, cache in memory."""
        if lc in _calib_cache:
            return _calib_cache[lc]
        import json
        from app.services.confidence_calibrator import ConfidenceCalibration
        # Try league-specific first, then global
        row = (
            db.query(ConfidenceCalibration)
            .filter_by(league_code=lc)
            .first()
        ) or (
            db.query(ConfidenceCalibration)
            .filter(ConfidenceCalibration.league_code.is_(None))
            .first()
        )
        bp = json.loads(row.breakpoints_json) if row and row.breakpoints_json else None
        _calib_cache[lc] = bp
        return bp

    def _apply_calib(lc: str, raw_score: float) -> float | None:
        """Apply pre-loaded breakpoints without hitting the DB."""
        if raw_score is None:
            return None
        bp = _get_calib_breakpoints(lc)
        if not bp:
            return None
        from app.services.confidence_calibrator import _apply_breakpoints
        return round(_apply_breakpoints(bp, raw_score), 4)

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

    def _get_stored_tt(row, key: str):
        """Pull TT probability from stored explanations JSON."""
        import json
        try:
            if row.explanations:
                data = json.loads(row.explanations)
                if isinstance(data, dict) and key in data:
                    return data[key]
        except Exception:
            pass
        return None

    grouped: dict = {}
    for r in rows:
        d = r.match_date.isoformat()
        if d not in grouped:
            grouped[d] = []

        tags = _match_tags_safe(r.league_code, r.home_team, r.away_team)

        # Calibrated probability — uses pre-loaded breakpoints, no DB query
        cal_prob = _apply_calib(r.league_code, r.confidence_score)

        grouped[d].append({
            "id":                    r.id,
            "league_code":           r.league_code,
            "home_team":             r.home_team,
            "away_team":             r.away_team,
            "match_time":            getattr(r, "match_time", None),
            "market":                r.market,
            "confidence":            r.confidence,
            "corridor":              f"{r.corridor_low}–{r.corridor_high}",
            "lean":                  r.lean,
            "confidence_score":      r.confidence_score,
            "calibrated_probability": cal_prob,
            "status":                r.status,
            "actual_score":          r.actual_score,
            "actual_total":          r.actual_total,
            "p_two_plus":            r.p_two_plus,
            "tempo_index":           r.tempo_index,
            # TT values for alt lane — pulled from stored inputs JSON if available
            "p_home_tt05":           _get_stored_tt(r, "p_home_tt05"),
            "p_away_tt05":           _get_stored_tt(r, "p_away_tt05"),
            "original_market":       _get_stored_tt(r, "original_market"),
            "alt_suppressed":        _get_stored_tt(r, "alt_suppressed"),
            "predicted_at":          r.predicted_at.isoformat() if r.predicted_at else None,
            "evaluated_at":          r.evaluated_at.isoformat() if r.evaluated_at else None,
            "variance_flag":         getattr(r, "variance_flag", None),
            "weather_tag":           getattr(r, "weather_tag", None),
            "performance_tags":      tags,
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
                run_at DATETIME
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


@router.post("/migrate/fix-null-league-config-defaults")
def migrate_fix_null_league_config_defaults(db: Session = Depends(get_db)):
    """Set NULL values to defaults for columns added without backfilling existing rows."""
    from sqlalchemy import text
    fixes = [
        ("tt_home_weak",          "false"),
        ("tt_away_weak",          "false"),
        ("min_confidence",        "0.0"),
        ("orig_ahead_runs",       "0"),
        ("confidence_scale",      "1.0"),
        ("confidence_floor",      "0.60"),
        ("tt_confidence_min",     "0.62"),
        ("use_alt_market",        "true"),
        ("alt_min_original_win_rate", "0.70"),
        ("alt_flip_threshold",    "0.62"),
        ("tt_home_bias",          "0.0"),
    ]
    results = {}
    for col, default in fixes:
        try:
            r = db.execute(text(
                f"UPDATE league_configs SET {col} = {default} WHERE {col} IS NULL"
            ))
            db.commit()
            results[col] = f"{r.rowcount} rows updated"
        except Exception as e:
            db.rollback()
            results[col] = f"skipped: {e}"
    return {"status": "ok", "fixes": results}


@router.post("/migrate/add-player-shooting-columns")
def migrate_add_player_shooting_columns(db: Session = Depends(get_db)):
    """Add shots_per90 and goals_minus_xg to player_season_stats."""
    from sqlalchemy import text, inspect
    results = {}
    inspector = inspect(db.bind)
    try:
        cols = [c["name"] for c in inspector.get_columns("player_season_stats")]
    except Exception:
        return {"status": "error", "message": "player_season_stats table not found"}
    for col, col_type, default in [
        ("shots_per90",    "FLOAT", "0.0"),
        ("goals_minus_xg", "FLOAT", "0.0"),
    ]:
        if col not in cols:
            db.execute(text(f"ALTER TABLE player_season_stats ADD COLUMN {col} {col_type} DEFAULT {default}"))
            db.commit()
            results[f"player_season_stats.{col}"] = "added"
        else:
            results[f"player_season_stats.{col}"] = "already exists"
    return {"status": "ok", "migrations": results}


@router.post("/migrate/add-alt-suppression-columns")
def migrate_add_alt_suppression_columns(db: Session = Depends(get_db)):
    """Add use_alt_market and alt_min_original_win_rate columns to league_configs."""
    from sqlalchemy import text, inspect
    results = {}
    inspector = inspect(db.bind)
    league_cols = [c["name"] for c in inspector.get_columns("league_configs")]
    for col, col_type, default in [
        ("use_alt_market",            "BOOLEAN", "true"),
        ("alt_min_original_win_rate",  "FLOAT",   "0.70"),
    ]:
        if col not in league_cols:
            db.execute(text(f"ALTER TABLE league_configs ADD COLUMN {col} {col_type} DEFAULT {default}"))
            db.commit()
            results[f"league_configs.{col}"] = "added"
        else:
            results[f"league_configs.{col}"] = "already exists"
    return {"status": "ok", "migrations": results}


@router.post("/migrate/add-tt-threshold-columns")
def migrate_add_tt_threshold_columns(db: Session = Depends(get_db)):
    """Add alt_flip_threshold and tt_home_bias columns to league_configs."""
    from sqlalchemy import text, inspect
    results = {}
    inspector = inspect(db.bind)
    league_cols = [c["name"] for c in inspector.get_columns("league_configs")]
    for col, default in [("alt_flip_threshold", 0.62), ("tt_home_bias", 0.0)]:
        if col not in league_cols:
            db.execute(text(f"ALTER TABLE league_configs ADD COLUMN {col} FLOAT DEFAULT {default}"))
            db.commit()
            results[f"league_configs.{col}"] = "added"
        else:
            results[f"league_configs.{col}"] = "already exists"
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


# ── PATCH /api/predictions/{id}/odds ──────────────────────────────────────────

@router.patch("/predictions/{prediction_id}/odds")
def log_closing_odds(
    prediction_id: int,
    closing_odds:  float = Query(..., description="Decimal odds for ATHENA's side"),
    opposing_odds: Optional[float] = Query(None, description="Decimal odds for opposite side (enables vig removal)"),
    db: Session = Depends(get_db),
):
    """
    Log closing bookmaker odds against a stored prediction and compute edge.

    Call this once the market has closed (ideally kick-off minus 1h) to
    record how much edge ATHENA had vs the closing line.

    Updates prediction_log.closing_odds, market_prob, and edge.
    Requires the prediction to have a calibrated_probability stored; if not
    yet fitted, falls back to normalising the raw confidence_score.

    Returns the full odds context including kelly_fraction and value_rating.
    """
    pred = db.query(PredictionLog).filter(PredictionLog.id == prediction_id).first()
    if not pred:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content={"detail": f"Prediction {prediction_id} not found."})

    from app.services.odds_service import parse_decimal_odds, compute_odds_context
    from app.services.confidence_calibrator import calibrate_confidence

    dec_odds = parse_decimal_odds(closing_odds)
    opp_odds = parse_decimal_odds(opposing_odds) if opposing_odds else None

    if not dec_odds:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=422, content={"detail": f"Invalid odds value: {closing_odds}"})

    # Get calibrated probability — live lookup so it always uses latest fit
    cal_prob = None
    try:
        cal_prob = calibrate_confidence(db, pred.confidence_score, league_code=pred.league_code)
    except Exception:
        pass

    if cal_prob is None:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=422, content={
            "detail": "No calibrated probability available. Run POST /calibrate/confidence first."
        })

    ctx = compute_odds_context(
        market                 = pred.market,
        calibrated_probability = cal_prob,
        closing_odds           = dec_odds,
        opposing_odds          = opp_odds,
    )

    # Persist to prediction_log
    try:
        pred.closing_odds = ctx.decimal_odds
        pred.market_prob  = ctx.market_prob
        pred.edge         = ctx.edge
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"[batch] Warning: could not persist odds to prediction {prediction_id}: {e}")

    return {
        "prediction_id":      prediction_id,
        "league_code":        pred.league_code,
        "fixture":            f"{pred.home_team} vs {pred.away_team}",
        "match_date":         pred.match_date.isoformat(),
        "market":             pred.market,
        "calibrated_prob":    cal_prob,
        "decimal_odds":       ctx.decimal_odds,
        "raw_market_prob":    ctx.raw_market_prob,
        "market_prob":        ctx.market_prob,
        "vig_pct":            ctx.vig_pct,
        "edge":               ctx.edge,
        "kelly_fraction":     ctx.kelly_fraction,
        "value_rating":       ctx.value_rating,
        "status":             pred.status,
    }


# ── Bulk delete predictions ────────────────────────────────────────────────────

@router.delete("/predictions/bulk")
def bulk_delete_predictions(
    ids: str = Query(None, description="Comma-separated prediction IDs to delete"),
    league_code: str = Query(None, description="Delete all pending predictions for a league"),
    status: str = Query(None, description="Filter by status: pending, hit, miss, void"),
    db: Session = Depends(get_db),
):
    """
    Delete predictions by ID list or by league + status filter.

    Examples:
      DELETE /api/predictions/bulk?ids=1,2,3,4
      DELETE /api/predictions/bulk?league_code=FRA-L2&status=pending
      DELETE /api/predictions/bulk?league_code=MLS  (all statuses)
    """
    deleted = 0

    if ids:
        id_list = [int(i.strip()) for i in ids.split(",") if i.strip().isdigit()]
        if id_list:
            deleted = (
                db.query(PredictionLog)
                .filter(PredictionLog.id.in_(id_list))
                .delete(synchronize_session=False)
            )
            db.commit()
        return {"deleted": deleted, "mode": "by_ids", "ids": id_list}

    if league_code:
        q = db.query(PredictionLog).filter(PredictionLog.league_code == league_code)
        if status:
            q = q.filter(PredictionLog.status == status)
        deleted = q.delete(synchronize_session=False)
        db.commit()
        return {"deleted": deleted, "mode": "by_league", "league_code": league_code, "status": status or "all"}

    return {"deleted": 0, "error": "Provide either ids= or league_code="}
