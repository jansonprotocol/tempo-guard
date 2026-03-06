# backend/app/api/routes_calibration.py
"""
ATHENA Calibration Engine

Workflow:
  1. Load the parquet snapshot for a league
  2. For each completed match, run asof_features AS OF that match date
  3. Run predict_match to get ATHENA's call
  4. Compare against actual score
  5. Compute lean_gap (over_score - under_score) for each match
  6. Find optimal bias shift: flip max misses, protect wins
  7. Optionally write adjustments back to league_configs

Lean gap analysis:
  lean_gap = over_score - under_score (at time of prediction)
  For over miss:  need to reduce lean_gap below 0 → negative bias shift
  For under miss: need to raise lean_gap above 0  → positive bias shift
  Optimal shift = largest shift that flips most misses without flipping wins
"""
from __future__ import annotations

import io
from typing import List

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

# ── Constants ──────────────────────────────────────────────────────
TARGET_HIT_RATE = 0.86
NUDGE_STEP      = 0.02
MAX_BIAS        = 0.13
MIN_BIAS        = 0.00


# ── hit_weight helper ──────────────────────────────────────────────
def hit_weight(result) -> float:
    # Half-wins = full wins, half-losses = full losses
    # because bettor always offsets the line before placing.
    if result is True:        return 1.0
    if result == "half_win":  return 1.0
    if result == "half_loss": return 0.0
    if result is False:       return 0.0
    return -1.0  # None or unrecognised = skip


# ── Response models ────────────────────────────────────────────────
class MarketStats(BaseModel):
    market:   str
    hits:     int
    misses:   int
    skipped:  int
    hit_rate: float


class CalibResult(BaseModel):
    league_code:      str
    total_matches:    int
    evaluated:        int
    skipped:          int
    overall_hit_rate: float
    by_market:        List[MarketStats]
    bias_suggestion:  dict
    applied:          bool
    sample:           List[dict]


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ── Lean gap optimal nudge finder ─────────────────────────────────
def _find_optimal_bias_shift(lean_records: list) -> dict:
    """
    For each miss, compute the minimum bias shift needed to flip it.
    Find the largest shift that flips the most misses without flipping wins.

    lean_gap = over_score - under_score
      Positive → over lean was chosen
      Negative → under lean was chosen

    Over miss:  lean_gap > 0, outcome needed under → shift negatively
    Under miss: lean_gap < 0, outcome needed over  → shift positively
    """
    over_misses  = [r for r in lean_records if r["is_miss"] and r["is_over"]]
    under_misses = [r for r in lean_records if r["is_miss"] and not r["is_over"]]
    over_wins    = [r for r in lean_records if not r["is_miss"] and r["is_over"]]
    under_wins   = [r for r in lean_records if not r["is_miss"] and not r["is_over"]]

    result = {
        "optimal_bias_shift":     0.0,
        "optimal_tempo_shift":    0.0,
        "over_misses_flippable":  0,
        "under_misses_flippable": 0,
        "wins_at_risk":           0,
        "analysis":               [],
    }

    if not lean_records:
        return result

    over_miss_w  = sum(r["weight"] for r in over_misses)
    under_miss_w = sum(r["weight"] for r in under_misses)

    if over_miss_w >= under_miss_w and over_misses:
        # Shift NEGATIVE — reduce over pressure
        # Each miss needs a shift of at least its lean_gap to flip
        # Each win has a margin of its lean_gap before it flips
        flip_thresholds = sorted([r["lean_gap"] for r in over_misses])
        win_margins     = [r["lean_gap"] for r in over_wins]
        safest_max      = min(win_margins) if win_margins else MAX_BIAS

        best_shift = best_flipped = 0
        for threshold in flip_thresholds:
            needed = threshold + 0.001
            if needed > 0 and needed <= safest_max:
                flipped = sum(1 for r in over_misses if r["lean_gap"] <= needed)
                if flipped > best_flipped:
                    best_flipped = flipped
                    best_shift   = needed

        wins_at_risk = sum(1 for r in over_wins if r["lean_gap"] <= best_shift)
        result["optimal_bias_shift"]    = round(-best_shift, 4)
        result["over_misses_flippable"] = best_flipped
        result["wins_at_risk"]          = wins_at_risk
        result["analysis"].append(
            f"Over misses dominate (w={over_miss_w:.1f} vs {under_miss_w:.1f}). "
            f"Optimal shift: -{round(best_shift, 3)} → "
            f"flips {best_flipped}/{len(over_misses)} misses, "
            f"{wins_at_risk}/{len(over_wins)} wins at risk."
        )

    elif under_misses:
        # Shift POSITIVE — reduce under pressure
        flip_thresholds = sorted([abs(r["lean_gap"]) for r in under_misses])
        win_margins     = [abs(r["lean_gap"]) for r in under_wins]
        safest_max      = min(win_margins) if win_margins else MAX_BIAS

        best_shift = best_flipped = 0
        for threshold in flip_thresholds:
            needed = threshold + 0.001
            if needed > 0 and needed <= safest_max:
                flipped = sum(1 for r in under_misses if abs(r["lean_gap"]) <= needed)
                if flipped > best_flipped:
                    best_flipped = flipped
                    best_shift   = needed

        wins_at_risk = sum(1 for r in under_wins if abs(r["lean_gap"]) <= best_shift)
        result["optimal_bias_shift"]      = round(best_shift, 4)
        result["under_misses_flippable"]  = best_flipped
        result["wins_at_risk"]            = wins_at_risk
        result["analysis"].append(
            f"Under misses dominate (w={under_miss_w:.1f} vs {over_miss_w:.1f}). "
            f"Optimal shift: +{round(best_shift, 3)} → "
            f"flips {best_flipped}/{len(under_misses)} misses, "
            f"{wins_at_risk}/{len(under_wins)} wins at risk."
        )

    # ── Tempo shift (independent of bias) ────────────────────────────
    tempo_over_misses = [r for r in over_misses if r["raw_tempo"] > 0.75]
    if tempo_over_misses:
        avg_contrib = sum((r["raw_tempo"] - 0.5) * 0.30 for r in tempo_over_misses) / len(tempo_over_misses)
        result["optimal_tempo_shift"] = round(-avg_contrib * 0.5, 4)
        result["analysis"].append(
            f"{len(tempo_over_misses)} high-tempo over misses — "
            f"avg tempo lean contribution: {round(avg_contrib, 3)}. "
            f"Suggested tempo_factor shift: {round(result['optimal_tempo_shift'], 3)}"
        )

    return result


# ── Bias suggestion ────────────────────────────────────────────────
def _suggest_bias(
    over_hits: float, over_total: float,
    under_hits: float, under_total: float,
    current_over: float, current_under: float,
    current_tempo: float,
    overall_hit_rate: float,
    miss_patterns: dict,
    lean_records: list,
) -> dict:
    new_over  = current_over
    new_under = current_under
    new_tempo = current_tempo
    notes     = []
    optimal   = _find_optimal_bias_shift(lean_records)

    suggestions = {
        "base_over_bias":   current_over,
        "base_under_bias":  current_under,
        "tempo_factor":     current_tempo,
        "notes":            notes,
        "target_hit_rate":  TARGET_HIT_RATE,
        "current_hit_rate": round(overall_hit_rate / 100, 3),
        "gap_to_target":    round(TARGET_HIT_RATE - overall_hit_rate / 100, 3),
        "miss_patterns":    miss_patterns,
        "lean_analysis":    optimal,
        "applied_changes":  {},
    }

    if overall_hit_rate >= TARGET_HIT_RATE * 100:
        notes.append(
            f"Hit rate {overall_hit_rate:.1f}% meets target "
            f"{TARGET_HIT_RATE * 100:.0f}% — no adjustment needed."
        )
        return suggestions

    notes.append(
        f"Hit rate {overall_hit_rate:.1f}% below target {TARGET_HIT_RATE * 100:.0f}% "
        f"(gap: {(TARGET_HIT_RATE - overall_hit_rate / 100) * 100:.1f}pp)."
    )

    # ── Bias shift — capped to NUDGE_STEP ────────────────────────────
    bias_shift = max(-NUDGE_STEP, min(NUDGE_STEP, optimal["optimal_bias_shift"]))

    if bias_shift < 0:
        new_over  = round(max(current_over  + bias_shift, MIN_BIAS), 3)
        new_under = round(min(current_under - bias_shift, MAX_BIAS), 3)
        notes.append(
            f"Reduce over pressure by {abs(bias_shift):.3f}: "
            f"over_bias {current_over}→{new_over}, "
            f"under_bias {current_under}→{new_under} "
            f"(flips {optimal['over_misses_flippable']} misses, "
            f"{optimal['wins_at_risk']} wins at risk)"
        )
    elif bias_shift > 0:
        new_over  = round(min(current_over  + bias_shift, MAX_BIAS), 3)
        new_under = round(max(current_under - bias_shift, MIN_BIAS), 3)
        notes.append(
            f"Increase over pressure by {bias_shift:.3f}: "
            f"over_bias {current_over}→{new_over}, "
            f"under_bias {current_under}→{new_under} "
            f"(flips {optimal['under_misses_flippable']} misses, "
            f"{optimal['wins_at_risk']} wins at risk)"
        )
    else:
        notes.append(
            "No safe bias shift found — all flippable misses would also flip wins. "
            "Try resetting biases to neutral (0.05/0.05/0.50) and recalibrating."
        )

    # ── Tempo shift ───────────────────────────────────────────────────
    tempo_shift = max(-NUDGE_STEP, min(NUDGE_STEP, optimal["optimal_tempo_shift"]))
    if abs(tempo_shift) > 0.005:
        new_tempo = round(max(0.40, min(0.80, current_tempo + tempo_shift)), 3)
        notes.append(
            f"Dampen tempo influence: tempo_factor {current_tempo}→{new_tempo}"
        )

    if miss_patterns.get("half_loss_count", 0) > 0:
        notes.append(
            f"Note: {miss_patterns['half_loss_count']} half-losses — "
            f"acceptable, bettor recovers half stake."
        )

    suggestions["base_over_bias"]  = new_over
    suggestions["base_under_bias"] = new_under
    suggestions["tempo_factor"]    = new_tempo
    return suggestions


# ── Main calibration endpoint ──────────────────────────────────────
@router.get("/calibrate/league", response_model=CalibResult)
def calibrate_league(
    league_code: str,
    limit: int = Query(100, ge=10, le=500),
    min_matches_before: int = Query(3, ge=2, le=20),
    apply: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Calibrate ATHENA against historical FBref data.
    Uses lean gap analysis to find the minimum bias shift that flips
    misses to wins without affecting winning predictions.
    """
    row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not row:
        return JSONResponse(status_code=404, content={
            "detail": f"No snapshot for {league_code}. Run the scraper first."
        })

    try:
        df = pd.read_parquet(io.BytesIO(row.data))
    except Exception as e:
        return JSONResponse(status_code=500, content={"detail": f"Could not read snapshot: {e}"})

    score_col = next((c for c in df.columns if c.lower() in ("score", "scores")), None)
    if score_col and "hg" not in df.columns:
        df = _parse_score_column(df, score_col)

    if "hg" not in df.columns or "ag" not in df.columns:
        return JSONResponse(status_code=422, content={"detail": "No parseable score column."})

    col_map  = {c.lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")

    if not all([date_col, home_col, away_col]):
        return JSONResponse(status_code=422, content={"detail": "Missing Date/Home/Away columns."})

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, "hg", "ag"])
    df = df.sort_values(date_col, ascending=False)
    completed     = df.head(limit).copy()
    total_matches = len(completed)

    if total_matches == 0:
        return JSONResponse(status_code=422, content={"detail": "No completed matches."})

    date_range = (completed[date_col].max() - completed[date_col].min()).days
    if date_range < 14 and total_matches < 50:
        return JSONResponse(status_code=422, content={
            "detail": f"Insufficient history — {total_matches} matches over {date_range} days."
        })

    cfg           = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    current_over  = float(cfg.base_over_bias  or 0.05) if cfg else 0.05
    current_under = float(cfg.base_under_bias or 0.05) if cfg else 0.05
    current_tempo = float(cfg.tempo_factor    or 0.50) if cfg else 0.50

    def _weight(pos: int) -> float:
        if pos <= 10: return 1.0
        if pos <= 30: return 0.5
        return 0.2

    market_tracker: dict = {}
    w_hits = w_misses = 0.0
    skipped = 0
    sample_rows: list = []
    lean_records: list = []

    miss_patterns = {
        "over_miss_low_goals":   0,
        "over_miss_neg_delta":   0,
        "over_miss_high_tempo":  0,
        "under_miss_high_goals": 0,
        "under_miss_high_p2p":   0,
        "half_loss_count":       0,
        "total_over_misses":     0,
        "total_under_misses":    0,
    }

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
            if len(sample_rows) < 5:
                sample_rows.append({"position": pos, "skipped_reason": f"asof_features: {e}"})
            continue

        if not metrics:
            skipped += 1
            if len(sample_rows) < 5:
                sample_rows.append({"position": pos, "skipped_reason": "metrics empty"})
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
            skipped += 1
            if len(sample_rows) < 5:
                sample_rows.append({
                    "position": pos,
                    "skipped_reason": f"predict_match: {e}",
                    "traceback": traceback.format_exc(),
                })
            continue

        market = pred.translated_play.market
        result = evaluate_market(market, hg, ag)
        hw     = hit_weight(result)

        # ── Lean gap ──────────────────────────────────────────────────
        raw_sd    = metrics.get("support_idx_over_delta") or 0.0
        raw_tempo = metrics.get("tempo_index") or 0.55
        raw_p2p   = metrics.get("p_two_plus") or 0.68
        adj_tempo = max(0.0, min(0.95, raw_tempo * current_tempo * 2.0))
        adj_sd    = raw_sd + (current_over - current_under)
        _over_s   = adj_sd + (adj_tempo - 0.5) * 0.30
        _under_s  = (0.72 - raw_p2p) * 0.50 + (0.5 - adj_tempo) * 0.30
        lean_gap  = round(_over_s - _under_s, 4)

        is_over_market = market.startswith("O")
        is_full_miss   = hw == 0.0

        lean_records.append({
            "lean_gap":  lean_gap,
            "is_miss":   is_full_miss,
            "is_over":   is_over_market,
            "weight":    w,
            "raw_sd":    raw_sd,
            "raw_tempo": raw_tempo,
            "raw_p2p":   raw_p2p,
        })

        if market not in market_tracker:
            market_tracker[market] = {
                "w_hits": 0.0, "w_misses": 0.0, "skipped": 0,
                "raw_hits": 0, "raw_misses": 0,
            }

        if hw < 0:
            market_tracker[market]["skipped"] += 1
            skipped += 1
        else:
            market_tracker[market]["w_hits"]   += hw * w
            market_tracker[market]["w_misses"] += (1.0 - hw) * w
            w_hits   += hw * w
            w_misses += (1.0 - hw) * w
            if hw >= 0.5:
                market_tracker[market]["raw_hits"] += 1
            else:
                market_tracker[market]["raw_misses"] += 1

        total_goals  = hg + ag
        is_half_loss = hw == 0.25

        if is_half_loss:
            miss_patterns["half_loss_count"] += 1
        if is_over_market and is_full_miss:
            miss_patterns["total_over_misses"] += 1
            if total_goals <= 1:  miss_patterns["over_miss_low_goals"] += 1
            if raw_sd < 0:        miss_patterns["over_miss_neg_delta"] += 1
            if raw_tempo > 0.80 and total_goals <= 2:
                miss_patterns["over_miss_high_tempo"] += 1
        if not is_over_market and is_full_miss:
            miss_patterns["total_under_misses"] += 1
            if total_goals >= 4:  miss_patterns["under_miss_high_goals"] += 1
            if raw_p2p > 0.72:    miss_patterns["under_miss_high_p2p"] += 1

        if len(sample_rows) < 20:
            sample_rows.append({
                "position":    pos,
                "weight":      w,
                "date":        match_date.isoformat(),
                "home":        home_team,
                "away":        away_team,
                "actual":      f"{hg}-{ag}",
                "total_goals": total_goals,
                "market":      market,
                "result":      result,
                "hit":         hw >= 0.5,
                "hit_weight":  hw,
                "lean_gap":    lean_gap,
                "confidence":  pred.translated_play.confidence,
                "corridor":    f"{pred.corridor.low}–{pred.corridor.high}",
                "lean":        pred.corridor.lean,
                "inputs":      metrics,
            })

    evaluated        = sum(s["raw_hits"] + s["raw_misses"] for s in market_tracker.values())
    overall_hit_rate = round(w_hits / max(0.001, w_hits + w_misses) * 100, 1)

    by_market = []
    over_w_hits = over_w_total = under_w_hits = under_w_total = 0.0

    for market, stats in sorted(market_tracker.items()):
        wh   = stats["w_hits"]
        wm   = stats["w_misses"]
        rate = round(wh / max(0.001, wh + wm) * 100, 1)
        by_market.append(MarketStats(
            market=market, hits=stats["raw_hits"],
            misses=stats["raw_misses"], skipped=stats["skipped"],
            hit_rate=rate,
        ))
        if market.startswith("O"):
            over_w_hits  += wh; over_w_total += wh + wm
        elif market.startswith("U"):
            under_w_hits += wh; under_w_total += wh + wm

    suggestion = _suggest_bias(
        over_w_hits, over_w_total,
        under_w_hits, under_w_total,
        current_over, current_under, current_tempo,
        overall_hit_rate, miss_patterns, lean_records,
    )

    applied = False
    applied_changes = {}

    if apply and cfg:
        before = {
            "base_over_bias":  current_over,
            "base_under_bias": current_under,
            "tempo_factor":    current_tempo,
        }
        after = {
            "base_over_bias":  suggestion["base_over_bias"],
            "base_under_bias": suggestion["base_under_bias"],
            "tempo_factor":    suggestion["tempo_factor"],
        }
        changed = {
            k: {"before": before[k], "after": after[k]}
            for k in before if round(before[k], 4) != round(after[k], 4)
        }
        if changed:
            cfg.base_over_bias  = after["base_over_bias"]
            cfg.base_under_bias = after["base_under_bias"]
            cfg.tempo_factor    = after["tempo_factor"]
            db.commit()
            applied = True
            applied_changes = changed
            suggestion["notes"].append(
                "Applied: " + ", ".join(
                    f"{k} {v['before']}→{v['after']}" for k, v in changed.items()
                )
            )
        else:
            suggestion["notes"].append(
                "apply=true but nothing changed — already at suggested values."
            )

    suggestion["applied_changes"] = applied_changes

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
