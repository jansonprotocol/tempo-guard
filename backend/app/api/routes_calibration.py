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
import json
import os
from typing import List

import pandas as pd
from fastapi import APIRouter, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import CalibrationLog
from app.engine.pipeline import evaluate_athena
from app.engine.types import MatchRequest
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig
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
        # Optimize for NET GAIN: misses flipped - wins flipped (weighted)
        # A shift is worthwhile if it flips more misses than wins overall.
        all_thresholds = sorted(set(
            [r["lean_gap"] for r in over_misses] +
            [r["lean_gap"] for r in over_wins]
        ))

        best_shift = best_net = 0
        best_flipped = best_wins_lost = 0

        for threshold in all_thresholds:
            needed       = threshold + 0.001
            if needed <= 0 or needed > MAX_BIAS:
                continue
            misses_flipped = sum(r["weight"] for r in over_misses if r["lean_gap"] <= needed)
            wins_lost      = sum(r["weight"] for r in over_wins   if r["lean_gap"] <= needed)
            net            = misses_flipped - wins_lost
            if net > best_net:
                best_net         = net
                best_shift       = needed
                best_flipped     = int(sum(1 for r in over_misses if r["lean_gap"] <= needed))
                best_wins_lost   = int(sum(1 for r in over_wins   if r["lean_gap"] <= needed))

        wins_at_risk = best_wins_lost
        result["optimal_bias_shift"]    = round(-best_shift, 4)
        result["over_misses_flippable"] = best_flipped
        result["wins_at_risk"]          = wins_at_risk
        result["analysis"].append(
            f"Over misses dominate (w={over_miss_w:.1f} vs {under_miss_w:.1f}). "
            f"Optimal shift: -{round(best_shift, 3)} → "
            f"flips {best_flipped}/{len(over_misses)} misses, "
            f"{wins_at_risk}/{len(over_wins)} wins at risk "
            f"(net gain: {round(best_net, 2)})."
        )

    elif under_misses:
        # Shift POSITIVE — reduce under pressure
        all_thresholds = sorted(set(
            [abs(r["lean_gap"]) for r in under_misses] +
            [abs(r["lean_gap"]) for r in under_wins]
        ))

        best_shift = best_net = 0
        best_flipped = best_wins_lost = 0

        for threshold in all_thresholds:
            needed         = threshold + 0.001
            if needed <= 0 or needed > MAX_BIAS:
                continue
            misses_flipped = sum(r["weight"] for r in under_misses if abs(r["lean_gap"]) <= needed)
            wins_lost      = sum(r["weight"] for r in under_wins   if abs(r["lean_gap"]) <= needed)
            net            = misses_flipped - wins_lost
            if net > best_net:
                best_net       = net
                best_shift     = needed
                best_flipped   = int(sum(1 for r in under_misses if abs(r["lean_gap"]) <= needed))
                best_wins_lost = int(sum(1 for r in under_wins   if abs(r["lean_gap"]) <= needed))

        wins_at_risk = best_wins_lost
        result["optimal_bias_shift"]      = round(best_shift, 4)
        result["under_misses_flippable"]  = best_flipped
        result["wins_at_risk"]            = wins_at_risk
        result["analysis"].append(
            f"Under misses dominate (w={under_miss_w:.1f} vs {over_miss_w:.1f}). "
            f"Optimal shift: +{round(best_shift, 3)} → "
            f"flips {best_flipped}/{len(under_misses)} misses, "
            f"{wins_at_risk}/{len(under_wins)} wins at risk "
            f"(net gain: {round(best_net, 2)})."
        )

    # ── Tempo shift (independent of bias) ────────────────────────────
    tempo_over_misses = [r for r in over_misses if r["raw_tempo"] > 0.75]
    if tempo_over_misses:
        avg_contrib  = sum((r["raw_tempo"] - 0.5) * 0.30 for r in tempo_over_misses) / len(tempo_over_misses)
        avg_lean_gap = sum(abs(r["lean_gap"]) for r in tempo_over_misses) / len(tempo_over_misses)
        suggested_shift = round(-avg_contrib * 0.5, 4)

        # Only suggest if the average lean_gap is actually closeable by tempo dampening.
        # Max realistic tempo contribution change ≈ 0.05 per 0.1 tempo_factor shift.
        # If avg_lean_gap > 0.10, tempo dampening won't flip these misses.
        if avg_lean_gap <= 0.10:
            result["optimal_tempo_shift"] = suggested_shift
            result["analysis"].append(
                f"{len(tempo_over_misses)} high-tempo over misses — "
                f"avg tempo lean contribution: {round(avg_contrib, 3)}, "
                f"avg lean_gap: {round(avg_lean_gap, 3)} (closeable). "
                f"Suggested tempo_factor shift: {round(suggested_shift, 3)}"
            )
        else:
            result["analysis"].append(
                f"{len(tempo_over_misses)} high-tempo over misses — "
                f"avg lean_gap {round(avg_lean_gap, 3)} too large for tempo dampening to close. "
                f"These are irreducible variance — no tempo change suggested."
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
            "No net-positive bias shift found — flipping any misses would flip equal or more wins. "
            "Current calibration is already near optimal for this window."
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


# ── Shared calibration core ────────────────────────────────────────
def _run_calibration(
    league_code: str,
    limit: int,
    min_matches_before: int,
    apply: bool,
    db: Session,
) -> CalibResult | JSONResponse:
    """
    Core calibration logic shared by single-league and bulk endpoints.
    Returns CalibResult on success, JSONResponse on error.
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

    score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
    if score_col and "hg" not in df.columns:
        df = _parse_score_column(df, score_col)

    if "hg" not in df.columns or "ag" not in df.columns:
        return JSONResponse(status_code=422, content={"detail": "No parseable score column."})

    # Flatten tuple columns (some FBref leagues use grouped headers)
    if len(df.columns) > 0 and isinstance(df.columns[0], tuple):
        df.columns = [
            " ".join(str(p) for p in col if not str(p).startswith("Unnamed")).strip() or str(col[-1])
            for col in df.columns
        ]
    col_map  = {str(c).lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")

    if not all([date_col, home_col, away_col]):
        return JSONResponse(status_code=422, content={"detail": "Missing Date/Home/Away columns."})

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, "hg", "ag"])

    # Deduplicate — scraper merges current + previous season and may produce
    # duplicate rows for matches that appear in both. Drop any (date, home, away)
    # duplicates before processing to prevent double-counting.
    before_dedup = len(df)
    df = df.drop_duplicates(subset=[date_col, home_col, away_col])
    dupes_removed = before_dedup - len(df)
    if dupes_removed:
        print(f"[calibration] Removed {dupes_removed} duplicate rows from snapshot before calibration.")

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
    team_tracker:   dict = {}   # team → {over_hits, over_total, under_hits, under_total}
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
            # Call evaluate_athena directly with team_nudge=0.0 — bypassing any
            # previously-written team nudges. Calibration must evaluate on raw
            # league-level signals only, otherwise team nudges (which are derived
            # FROM calibration data) feed back into the very loop that generates them,
            # causing different results between runs and circular drift.
            pred = evaluate_athena(
                req,
                current_over,
                current_under,
                current_tempo,
                team_nudge=0.0,
            )
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

            # ── Per-team stat accumulation ────────────────────────────
            # Track each team's hit/miss split separately for over and under calls.
            # Uses raw counts (not weighted) — we want enough sample to trust the nudge.
            for team in [home_team, away_team]:
                if team not in team_tracker:
                    team_tracker[team] = {
                        "over_hits": 0, "over_total": 0,
                        "under_hits": 0, "under_total": 0,
                    }
                if is_over_market:
                    team_tracker[team]["over_total"] += 1
                    if hw >= 0.5:
                        team_tracker[team]["over_hits"] += 1
                else:
                    team_tracker[team]["under_total"] += 1
                    if hw >= 0.5:
                        team_tracker[team]["under_hits"] += 1

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
                "date":        match_date.strftime("%d/%m/%Y"),
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

        # ── Team-level nudge calibration ──────────────────────────────
        # For each team with enough samples, compute how their matches
        # deviate from league average hit rate and write a nudge.
        #
        # Nudge formula:
        #   gap = team_hit_rate - league_hit_rate
        #   nudge = gap * 0.3, clipped to [-0.05, +0.05]
        #
        # Interpretation:
        #   Team hits 60% on over calls, league avg is 77% → gap=-0.17
        #   nudge = -0.17 * 0.3 = -0.051 → clipped to -0.05
        #   → support_delta reduced by 0.05 when this team appears → fewer over calls
        #
        MIN_TEAM_SAMPLES = 6   # need at least 6 matches to trust the nudge
        NUDGE_SCALE      = 0.3
        NUDGE_MAX        = 0.05

        team_nudges_applied = {}

        for team, stats in team_tracker.items():
            over_n  = stats["over_total"]
            under_n = stats["under_total"]

            over_nudge  = 0.0
            under_nudge = 0.0

            if over_n >= MIN_TEAM_SAMPLES:
                team_over_rate  = stats["over_hits"] / over_n
                gap = team_over_rate - (overall_hit_rate / 100.0)
                over_nudge = round(
                    max(-NUDGE_MAX, min(NUDGE_MAX, gap * NUDGE_SCALE)), 4
                )

            if under_n >= MIN_TEAM_SAMPLES:
                team_under_rate = stats["under_hits"] / under_n
                gap = team_under_rate - (overall_hit_rate / 100.0)
                under_nudge = round(
                    max(-NUDGE_MAX, min(NUDGE_MAX, gap * NUDGE_SCALE)), 4
                )

            if over_n < MIN_TEAM_SAMPLES and under_n < MIN_TEAM_SAMPLES:
                continue  # not enough data — leave neutral

            # Upsert TeamConfig
            from datetime import datetime as _dt
            existing = (
                db.query(TeamConfig)
                .filter_by(league_code=league_code, team=team)
                .first()
            )
            if existing:
                existing.over_nudge     = over_nudge
                existing.under_nudge    = under_nudge
                existing.over_hit_rate  = round(stats["over_hits"] / over_n, 3) if over_n else None
                existing.under_hit_rate = round(stats["under_hits"] / under_n, 3) if under_n else None
                existing.over_matches   = over_n
                existing.under_matches  = under_n
                existing.last_calibrated = _dt.utcnow()
            else:
                db.add(TeamConfig(
                    league_code=league_code,
                    team=team,
                    over_nudge=over_nudge,
                    under_nudge=under_nudge,
                    over_hit_rate=round(stats["over_hits"] / over_n, 3) if over_n else None,
                    under_hit_rate=round(stats["under_hits"] / under_n, 3) if under_n else None,
                    over_matches=over_n,
                    under_matches=under_n,
                    last_calibrated=_dt.utcnow(),
                ))

            if abs(over_nudge) > 0.005 or abs(under_nudge) > 0.005:
                team_nudges_applied[team] = {
                    "over_nudge":  over_nudge,
                    "under_nudge": under_nudge,
                    "over_rate":   round(stats["over_hits"] / over_n, 3) if over_n else None,
                    "over_n":      over_n,
                }

        db.commit()

        if team_nudges_applied:
            suggestion["notes"].append(
                f"Team nudges applied: {len(team_nudges_applied)} teams adjusted. "
                f"Notable: " + ", ".join(
                    f"{t} ({v['over_nudge']:+.3f})"
                    for t, v in sorted(
                        team_nudges_applied.items(),
                        key=lambda x: abs(x[1]["over_nudge"]),
                        reverse=True
                    )[:5]
                )
            )
            applied_changes["team_nudges"] = team_nudges_applied
            applied = True

    suggestion["applied_changes"] = applied_changes

    # ── Log this calibration run ──────────────────────────────────────────────
    try:
        calib_entry = CalibrationLog(
            league_code = league_code,
            hit_rate    = overall_hit_rate,
            sample_size = evaluated,
            applied     = applied,
            run_at      = __import__("datetime").datetime.utcnow(),
        )
        db.add(calib_entry)
        db.commit()
    except Exception as _log_err:
        print(f"[calibration] Warning: could not save CalibrationLog: {_log_err}")

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
    Calibrate ATHENA for a single league.
    Use apply=true to write the suggested bias adjustments to the DB.
    """
    return _run_calibration(league_code, limit, min_matches_before, apply, db)


# ── Bulk calibration endpoint ────────────────────────────────────────────────
@router.post("/calibrate/all")
def calibrate_all_leagues(
    limit: int = Query(100, ge=10, le=500, description="Max matches per league"),
    min_matches_before: int = Query(3, ge=2, le=20),
    apply: bool = Query(False, description="Write adjustments to DB for all leagues"),
    db: Session = Depends(get_db),
):
    """
    Run calibration across ALL leagues with a snapshot in the DB.
    Same logic as /calibrate/league but iterates every league automatically.

    Returns a summary per league — hit rate, variance flag, and whether
    adjustments were applied. Leagues with insufficient data are skipped
    and reported under 'skipped'.
    """
    from datetime import datetime as dt

    snapshots = db.query(FBrefSnapshot).all()
    if not snapshots:
        return {"message": "No snapshots found. Run the scraper first.", "results": []}

    results   = []
    skipped   = []
    applied_n = 0

    for snap in snapshots:
        lc = snap.league_code
        result = _run_calibration(lc, limit, min_matches_before, apply, db)

        # JSONResponse means an error (no data / insufficient history)
        if isinstance(result, JSONResponse):
            import json as _json
            body = _json.loads(result.body if isinstance(result.body, str) else result.body.decode("utf-8"))
            skipped.append({"league_code": lc, "reason": body.get("detail", "unknown error")})
            continue

        if result.overall_hit_rate >= 80:
            vflag = "green"
        elif result.overall_hit_rate >= 70:
            vflag = "orange"
        else:
            vflag = "red"

        if result.applied:
            applied_n += 1

        results.append({
            "league_code":    lc,
            "hit_rate":       result.overall_hit_rate,
            "variance_flag":  vflag,
            "evaluated":      result.evaluated,
            "total_matches":  result.total_matches,
            "applied":        result.applied,
            "bias_suggestion": result.bias_suggestion,
        })

    results.sort(key=lambda x: x["hit_rate"])  # worst first so easy to scan

    return {
        "run_at":        dt.utcnow().isoformat(),
        "apply":         apply,
        "leagues_run":   len(results),
        "leagues_skipped": len(skipped),
        "applied_count": applied_n,
        "summary": {
            "green":  sum(1 for r in results if r["variance_flag"] == "green"),
            "orange": sum(1 for r in results if r["variance_flag"] == "orange"),
            "red":    sum(1 for r in results if r["variance_flag"] == "red"),
        },
        "results": results,
        "skipped": skipped,
    }


# ── League reset endpoint ──────────────────────────────────────────────────────
@router.delete("/calibrate/reset")
def reset_league_calibration(
    league_code: str,
    wipe_snapshot: bool = Query(False),
    db: Session = Depends(get_db),
):
    """
    Full calibration reset for a league.

    Resets:
      - LeagueConfig biases back to league_configs.json defaults
      - All TeamConfig nudges for this league (deleted entirely)
      - Optionally wipes the FBrefSnapshot (?wipe_snapshot=true)

    Use this when a league has been over-calibrated during testing,
    or when snapshot data was corrupted/duplicated before fixes were deployed.

    After reset:
      1. Run scrape_fbref to get a fresh clean snapshot
      2. Run /calibrate/league?apply=false to review
      3. Run /calibrate/league?apply=true to seed clean nudges
    """
    result = {
        "league_code":       league_code,
        "league_config":     None,
        "team_nudges_wiped": 0,
        "snapshot_wiped":    False,
        "notes":             [],
    }

    # ── 1. Reset LeagueConfig to defaults from league_configs.json ────────────
    # Try multiple path locations — structure differs between local and Render
    possible_paths = [
        os.path.join(os.path.dirname(__file__), "..", "seed", "league_configs.json"),
        "/app/app/seed/league_configs.json",
    ]
    defaults_path = next(
        (p for p in possible_paths if os.path.exists(os.path.normpath(p))), None
    )
    default_over  = 0.03
    default_under = 0.01
    default_tempo = 0.50

    try:
        if defaults_path:
            with open(os.path.normpath(defaults_path)) as f:
                configs = json.load(f)
            match = next((c for c in configs if c["league_code"] == league_code), None)
            if match:
                default_over  = match.get("base_over_bias",  default_over)
                default_under = match.get("base_under_bias", default_under)
                default_tempo = match.get("tempo_factor",    default_tempo)
        else:
            result["notes"].append("league_configs.json not found in any expected location — using hardcoded defaults.")
    except Exception as e:
        result["notes"].append(f"Could not load league_configs.json ({e}) — using hardcoded defaults.")

    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    if cfg:
        cfg.base_over_bias  = default_over
        cfg.base_under_bias = default_under
        cfg.tempo_factor    = default_tempo
        db.commit()
        result["league_config"] = {
            "base_over_bias":  default_over,
            "base_under_bias": default_under,
            "tempo_factor":    default_tempo,
        }
        result["notes"].append(
            f"LeagueConfig reset to defaults: over={default_over} under={default_under} tempo={default_tempo}"
        )
    else:
        result["notes"].append(f"No LeagueConfig found for {league_code} — nothing to reset.")

    # ── 2. Wipe all TeamConfig nudges for this league ─────────────────────────
    deleted = (
        db.query(TeamConfig)
        .filter_by(league_code=league_code)
        .delete(synchronize_session=False)
    )
    db.commit()
    result["team_nudges_wiped"] = deleted
    result["notes"].append(f"Wiped {deleted} team nudge(s) for {league_code}.")

    # ── 3. Optionally wipe the FBrefSnapshot ──────────────────────────────────
    if wipe_snapshot:
        snap_deleted = (
            db.query(FBrefSnapshot)
            .filter_by(league_code=league_code)
            .delete(synchronize_session=False)
        )
        db.commit()
        result["snapshot_wiped"] = snap_deleted > 0
        if snap_deleted:
            result["notes"].append(
                f"Snapshot wiped — run scrape_fbref before next calibration."
            )
        else:
            result["notes"].append("No snapshot found to wipe.")

    return result
