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
from app.util.asian_lines import evaluate_market, hit_weight

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
TARGET_HIT_RATE   = 0.86   # minimum acceptable weighted hit rate
NUDGE_STEP        = 0.01   # max change per calibration run
MAX_BIAS          = 0.10
MIN_BIAS          = 0.00


def _suggest_bias(
    over_hits: int, over_total: int,
    under_hits: int, under_total: int,
    current_over: float, current_under: float,
    current_tempo: float,
    overall_hit_rate: float,
) -> dict:
    """
    Suggest adjustments to league_config based on observed hit rates.
    Target: >= 86% weighted hit rate.
    Conservative nudges — max ±0.01 per run.
    Run apply=true multiple times to converge on target.
    """
    suggestions = {
        "base_over_bias":  current_over,
        "base_under_bias": current_under,
        "tempo_factor":    current_tempo,
        "notes":           [],
        "target_hit_rate": TARGET_HIT_RATE,
        "current_hit_rate": round(overall_hit_rate / 100, 3),
        "gap_to_target":   round(TARGET_HIT_RATE - overall_hit_rate / 100, 3),
    }

    if overall_hit_rate >= TARGET_HIT_RATE * 100:
        suggestions["notes"].append(
            f"Hit rate {overall_hit_rate:.1f}% meets target {TARGET_HIT_RATE*100:.0f}% "
            f"— no adjustment needed."
        )
        return suggestions

    # Below target — work out which side is dragging the rate down
    over_rate  = over_hits  / max(1, over_total)
    under_rate = under_hits / max(1, under_total)

    suggestions["notes"].append(
        f"Hit rate {overall_hit_rate:.1f}% is below target {TARGET_HIT_RATE*100:.0f}% "
        f"(gap: {(TARGET_HIT_RATE - overall_hit_rate/100)*100:.1f}pp) — adjustments suggested."
    )

    # Over side underperforming
    if over_total >= 20:
        if over_rate < TARGET_HIT_RATE:
            gap = TARGET_HIT_RATE - over_rate
            if gap > 0.20:
                # Far below target — nudge both bias and tempo
                new_over = round(min(current_over + NUDGE_STEP, MAX_BIAS), 3)
                new_tempo = round(min(current_tempo + NUDGE_STEP, 1.0), 3)
                suggestions["base_over_bias"] = new_over
                suggestions["tempo_factor"]   = new_tempo
                suggestions["notes"].append(
                    f"Over rate {over_rate:.1%} is {gap*100:.1f}pp below target "
                    f"→ nudge over_bias {current_over}→{new_over}, "
                    f"tempo_factor {current_tempo}→{new_tempo}"
                )
            else:
                new_over = round(min(current_over + NUDGE_STEP, MAX_BIAS), 3)
                suggestions["base_over_bias"] = new_over
                suggestions["notes"].append(
                    f"Over rate {over_rate:.1%} is {gap*100:.1f}pp below target "
                    f"→ nudge over_bias {current_over}→{new_over}"
                )
        elif over_rate > 0.93:
            # Overfit — dial back slightly
            new_over = round(max(current_over - NUDGE_STEP, MIN_BIAS), 3)
            suggestions["base_over_bias"] = new_over
            suggestions["notes"].append(
                f"Over rate {over_rate:.1%} very high — possible overfit "
                f"→ nudge over_bias down {current_over}→{new_over}"
            )

    # Under side underperforming
    if under_total >= 20:
        if under_rate < TARGET_HIT_RATE:
            gap = TARGET_HIT_RATE - under_rate
            new_under = round(min(current_under + NUDGE_STEP, MAX_BIAS), 3)
            suggestions["base_under_bias"] = new_under
            suggestions["notes"].append(
                f"Under rate {under_rate:.1%} is {gap*100:.1f}pp below target "
                f"→ nudge under_bias {current_under}→{new_under}"
            )
        elif under_rate > 0.93:
            new_under = round(max(current_under - NUDGE_STEP, MIN_BIAS), 3)
            suggestions["base_under_bias"] = new_under
            suggestions["notes"].append(
                f"Under rate {under_rate:.1%} very high — possible overfit "
                f"→ nudge under_bias down {current_under}→{new_under}"
            )

    if len(suggestions["notes"]) == 1:
        suggestions["notes"].append(
            "Insufficient data on one or both sides (< 20 matches) — "
            "run with a higher limit for better suggestions."
        )

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

    # ── Check data depth ─────────────────────────────────────────────
    # If all matches are within a 14-day window, season just started
    date_range = (completed[date_col].max() - completed[date_col].min()).days
    if date_range < 14 and total_matches < 50:
        return JSONResponse(
            status_code=422,
            content={
                "detail": f"Insufficient history for {league_code} — only {total_matches} matches "
                          f"all within {date_range} days. Rescrape once the season has 6+ rounds played."
            }
        )

    # ── Load current league config ───────────────────────────────────
    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    current_over  = float(cfg.base_over_bias  or 0.02) if cfg else 0.02
    current_under = float(cfg.base_under_bias or 0.02) if cfg else 0.02
    current_tempo = float(cfg.tempo_factor    or 0.55) if cfg else 0.55

    # ── Run retrosim for each match ──────────────────────────────────
    # Matches sorted most-recent-first (head(limit) on desc-sorted df).
    # Recency weights:
    #   positions  1–10  → 1.0  (recent misses MUST be addressed)
    #   positions 11–30  → 0.5  (notable but not critical)
    #   positions 31+    → 0.2  (historical context only)

    def _weight(position: int) -> float:
        if position <= 10: return 1.0
        if position <= 30: return 0.5
        return 0.2

    market_tracker: dict[str, dict] = {}
    w_hits = w_misses = 0.0
    skipped = 0
    sample_rows: list[dict] = []

    for pos, (_, match_row) in enumerate(completed.iterrows(), start=1):
        match_date = match_row[date_col].date()
        home_team  = str(match_row[home_col])
        away_team  = str(match_row[away_col])
        hg = int(match_row["hg"])
        ag = int(match_row["ag"])
        w  = _weight(pos)

        try:
            metrics = asof_features(
                league_code, home_team, away_team, match_date,
                min_matches=min_matches_before,
            )
        except Exception as e:
            skipped += 1
            sample_rows.append({"position": pos, "skipped_reason": f"asof_features: {e}"}) if len(sample_rows) < 5 else None
            continue

        if not metrics:
            skipped += 1
            sample_rows.append({"position": pos, "skipped_reason": "metrics empty"}) if len(sample_rows) < 5 else None
            continue

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
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            skipped += 1
            if len(sample_rows) < 5:
                sample_rows.append({"position": pos, "skipped_reason": f"predict_match: {e}", "traceback": tb})
            continue

        market = pred.translated_play.market
        result = evaluate_market(market, hg, ag)
        hw     = hit_weight(result)

        if market not in market_tracker:
            market_tracker[market] = {
                "w_hits": 0.0, "w_misses": 0.0, "skipped": 0,
                "raw_hits": 0, "raw_misses": 0,
            }

        if hw < 0:
            # Unrecognised market — skip
            market_tracker[market]["skipped"] += 1
            skipped += 1
        else:
            # hw: 1.0=win, 0.75=half_win, 0.5=push, 0.25=half_loss, 0.0=loss
            hit_contrib  = hw * w
            miss_contrib = (1.0 - hw) * w
            market_tracker[market]["w_hits"]   += hit_contrib
            market_tracker[market]["w_misses"] += miss_contrib
            w_hits   += hit_contrib
            w_misses += miss_contrib
            if hw >= 0.5:
                market_tracker[market]["raw_hits"] += 1
            else:
                market_tracker[market]["raw_misses"] += 1

        if len(sample_rows) < 20:
            sample_rows.append({
                "position":    pos,
                "weight":      w,
                "date":        match_date.isoformat(),
                "home":        home_team,
                "away":        away_team,
                "actual":      f"{hg}-{ag}",
                "total_goals": hg + ag,
                "market":      market,
                "result":      result,
                "hit":         hw >= 0.5,
                "hit_weight":  hw,
                "confidence":  pred.translated_play.confidence,
                "corridor":    f"{pred.corridor.low}–{pred.corridor.high}",
                "lean":        pred.corridor.lean,
                "inputs":      metrics,
            })

    evaluated = sum(
        s["raw_hits"] + s["raw_misses"] for s in market_tracker.values()
    )
    overall_hit_rate = round(w_hits / max(0.001, w_hits + w_misses) * 100, 1)

    # ── Build per-market stats ────────────────────────────────────────
    by_market = []
    over_w_hits = over_w_total = under_w_hits = under_w_total = 0.0

    for market, stats in sorted(market_tracker.items()):
        wh = stats["w_hits"]
        wm = stats["w_misses"]
        s  = stats["skipped"]
        rh = stats["raw_hits"]
        rm = stats["raw_misses"]
        # Hit rate reported as weighted %
        rate = round(wh / max(0.001, wh + wm) * 100, 1)
        by_market.append(MarketStats(
            market=market,
            hits=rh,
            misses=rm,
            skipped=s,
            hit_rate=rate,
        ))
        if market.startswith("O"):
            over_w_hits  += wh
            over_w_total += wh + wm
        elif market.startswith("U"):
            under_w_hits  += wh
            under_w_total += wh + wm

    # ── Bias suggestion ───────────────────────────────────────────────
    suggestion = _suggest_bias(
        over_w_hits, over_w_total,
        under_w_hits, under_w_total,
        current_over, current_under, current_tempo,
        overall_hit_rate,
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
