# backend/app/api/routes_calibration.py
"""
ATHENA Calibration Engine

Reads directly from FBref snapshots — no external API needed.

Workflow:
  1. Load the parquet snapshot for a league
  2. For each completed match, run asof_features AS OF that match date
     (only uses matches before that date — no lookahead)
  3. Run predict_match to get ATHENA's call
  4. Compare against actual score
  5. Report hit rate per market type
  6. Optionally write bias adjustments back to league_configs

Hit detection per market:
  O1.5  → total goals >= 2
  O2.5  → total goals >= 3
  O3.5  → total goals >= 4
  O4.5  → total goals >= 5
  U1.5  → total goals <= 1
  U2.5  → total goals <= 2
  U3.5  → total goals <= 3
  BTTS  → both teams scored (hg > 0 and ag > 0)
"""
from __future__ import annotations

import io
from datetime import date, datetime
from typing import List, Optional

import pandas as pd
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.engine.types import MatchRequest
from app.models.league_config import LeagueConfig
from app.services.data_providers.fbref_base import asof_features, _parse_score_column
from app.services.predict import predict_match
from app.util.asian_lines import evaluate_market

router = APIRouter()


# ── Response models ────────────────────────────────────────────────
class MarketStats(BaseModel):
    market:   str
    hits:     int
    misses:   int
    skipped:  int
    hit_rate: float


class CalibResult(BaseModel):
    league_code:    str
    total_matches:  int
    evaluated:      int
    skipped:        int
    overall_hit_rate: float
    by_market:      List[MarketStats]
    bias_suggestion: dict
    applied:        bool
    sample:         List[dict]


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Bias suggestion ───────────────────────────────────────────────
def _suggest_bias(
    over_hits: int, over_total: int,
    under_hits: int, under_total: int,
    current_over: float, current_under: float,
    current_tempo: float,
) -> dict:
    """
    Suggest adjustments to league_config based on observed hit rates.
    Conservative nudges only — never more than ±0.02 per calibration run.
    """
    suggestions = {
        "base_over_bias":  current_over,
        "base_under_bias": current_under,
        "tempo_factor":    current_tempo,
        "notes":           [],
    }

    if over_total >= 20:
        over_rate = over_hits / over_total
        if over_rate > 0.65:
            new = round(min(current_over + 0.01, 0.08), 3)
            suggestions["base_over_bias"] = new
            suggestions["notes"].append(
                f"Over hit rate {over_rate:.1%} → nudge over_bias up to {new}"
            )
        elif over_rate < 0.40:
            new = round(max(current_over - 0.01, 0.0), 3)
            suggestions["base_over_bias"] = new
            suggestions["notes"].append(
                f"Over hit rate {over_rate:.1%} → nudge over_bias down to {new}"
            )

    if under_total >= 20:
        under_rate = under_hits / under_total
        if under_rate > 0.65:
            new = round(min(current_under + 0.01, 0.08), 3)
            suggestions["base_under_bias"] = new
            suggestions["notes"].append(
                f"Under hit rate {under_rate:.1%} → nudge under_bias up to {new}"
            )
        elif under_rate < 0.40:
            new = round(max(current_under - 0.01, 0.0), 3)
            suggestions["base_under_bias"] = new
            suggestions["notes"].append(
                f"Under hit rate {under_rate:.1%} → nudge under_bias down to {new}"
            )

    if not suggestions["notes"]:
        suggestions["notes"].append("Hit rates within expected range — no adjustment needed.")

    return suggestions


# ── Main calibration endpoint ──────────────────────────────────────
@router.get("/calibrate/league", response_model=CalibResult)
def calibrate_league(
    league_code: str,
    limit: int = Query(100, ge=10, le=500,
                       description="Max matches to evaluate (most recent first)"),
    min_matches_before: int = Query(3, ge=2, le=20,
                                    description="Min prior matches needed per team to evaluate"),
    apply: bool = Query(False,
                        description="If true, write bias adjustments back to league_configs"),
    db: Session = Depends(get_db),
):
    """
    Calibrate ATHENA against historical FBref data for a given league.

    - Reads the stored parquet snapshot
    - Runs retrosim for each completed match using only prior data
    - Evaluates hit/miss per translated play market
    - Optionally applies bias corrections to league_configs
    """

    # ── Load snapshot ────────────────────────────────────────────────
    row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not row:
        return JSONResponse(
            status_code=404,
            content={"detail": f"No snapshot found for league_code={league_code}. Run the scraper first."}
        )

    try:
        df = pd.read_parquet(io.BytesIO(row.data))
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Could not read snapshot: {e}"})

    # ── Parse score column into hg/ag ────────────────────────────────
    score_col = next(
        (c for c in df.columns if c.lower() in ("score", "scores")), None
    )
    if score_col and "hg" not in df.columns:
        df = _parse_score_column(df, score_col)

    if "hg" not in df.columns or "ag" not in df.columns:
        return JSONResponse(
            status_code=422,
            content={"detail": "Snapshot has no parseable score column."}
        )

    # ── Identify column names ────────────────────────────────────────
    col_map = {c.lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")

    if not all([date_col, home_col, away_col]):
        return JSONResponse(
            status_code=422,
            content={"detail": "Snapshot missing Date/Home/Away columns."}
        )

    # ── Filter completed matches ─────────────────────────────────────
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, "hg", "ag"])
    df = df.sort_values(date_col, ascending=False)  # most recent first

    completed = df.head(limit).copy()
    total_matches = len(completed)

    if total_matches == 0:
        return JSONResponse(
            status_code=422,
            content={"detail": "No completed matches found in snapshot."}
        )

    # ── Load current league config ───────────────────────────────────
    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    current_over  = float(cfg.base_over_bias  or 0.02) if cfg else 0.02
    current_under = float(cfg.base_under_bias or 0.02) if cfg else 0.02
    current_tempo = float(cfg.tempo_factor    or 0.55) if cfg else 0.55

    # ── Run retrosim for each match ──────────────────────────────────
    market_tracker: dict[str, dict] = {}  # market → {hits, misses, skipped}
    overall_hits = overall_misses = skipped = 0
    sample_rows: list[dict] = []

    for _, match_row in completed.iterrows():
        match_date = match_row[date_col].date()
        home_team  = str(match_row[home_col])
        away_team  = str(match_row[away_col])
        hg = int(match_row["hg"])
        ag = int(match_row["ag"])

        # Get features AS OF this match date (excludes this match itself)
        try:
            metrics = asof_features(
                league_code, home_team, away_team, match_date,
                min_matches=min_matches_before,
            )
        except Exception:
            skipped += 1
            continue

        if not metrics:
            skipped += 1
            continue

        # Build request and predict
        try:
            req = MatchRequest(
                league_code=league_code,
                home_team=home_team,
                away_team=away_team,
                match_date=match_date,
                sot_proj_total=metrics.get("sot_proj_total"),
                support_idx_over_delta=metrics.get("support_idx_over_delta"),
                p_two_plus=metrics.get("p_two_plus"),
                p_home_tt05=metrics.get("p_home_tt05"),
                p_away_tt05=metrics.get("p_away_tt05"),
                tempo_index=metrics.get("tempo_index"),
            )
            pred = predict_match(db, req)
        except Exception:
            skipped += 1
            continue

        market = pred.translated_play.market
        hit = evaluate_market(market, hg, ag)

        # Track per-market
        if market not in market_tracker:
            market_tracker[market] = {"hits": 0, "misses": 0, "skipped": 0}

        if hit is None:
            market_tracker[market]["skipped"] += 1
            skipped += 1
        elif hit:
            market_tracker[market]["hits"] += 1
            overall_hits += 1
        else:
            market_tracker[market]["misses"] += 1
            overall_misses += 1

        # Sample (first 20)
        if len(sample_rows) < 20:
            sample_rows.append({
                "date":       match_date.isoformat(),
                "home":       home_team,
                "away":       away_team,
                "actual":     f"{hg}-{ag}",
                "total_goals": hg + ag,
                "market":     market,
                "hit":        hit,
                "confidence": pred.translated_play.confidence,
                "corridor":   f"{pred.corridor.low}–{pred.corridor.high}",
                "lean":       pred.corridor.lean,
                "inputs":     metrics,
            })

    evaluated = overall_hits + overall_misses
    overall_hit_rate = round(overall_hits / max(1, evaluated) * 100, 1)

    # ── Build per-market stats ────────────────────────────────────────
    by_market = []
    over_hits = over_total = under_hits = under_total = 0

    for market, stats in sorted(market_tracker.items()):
        h = stats["hits"]
        m = stats["misses"]
        s = stats["skipped"]
        rate = round(h / max(1, h + m) * 100, 1)
        by_market.append(MarketStats(
            market=market, hits=h, misses=m, skipped=s, hit_rate=rate
        ))
        if market.startswith("O"):
            over_hits  += h
            over_total += h + m
        elif market.startswith("U"):
            under_hits  += h
            under_total += h + m

    # ── Bias suggestion ───────────────────────────────────────────────
    suggestion = _suggest_bias(
        over_hits, over_total,
        under_hits, under_total,
        current_over, current_under, current_tempo,
    )

    # ── Apply if requested ────────────────────────────────────────────
    applied = False
    if apply and cfg:
        cfg.base_over_bias  = suggestion["base_over_bias"]
        cfg.base_under_bias = suggestion["base_under_bias"]
        cfg.tempo_factor    = suggestion["tempo_factor"]
        db.commit()
        applied = True

    return CalibResult(
        league_code=league_code,
        total_matches=total_matches,
        evaluated=evaluated,
        skipped=skipped,
        overall_hit_rate=overall_hit_rate,
        by_market=by_market,
        bias_suggestion=suggestion,
        applied=applied,
        sample=sample_rows,
    )
