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
import threading
import uuid

from datetime import datetime as dt
from typing import List
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, Query
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.database.models_predictions import CalibrationLog
from app.services.resolve_team import resolve_team_name
from app.engine.pipeline import evaluate_athena
from app.engine.types import MatchRequest
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig
from app.services.data_providers.fbref_base import asof_features, _parse_score_column
from app.services.predict import predict_match
from app.services.player_power_backtest import get_historical_player_nudge
from app.services.form_delta_history import get_historical_form_delta
from app.util.asian_lines import evaluate_market
# v2.2: feature cache (eliminates repeated parquet reads during calibration loops)
from app.services.feature_cache import warm_snapshot_cache, cached_asof_features, clear_feature_cache, cache_stats
# v2.2: confidence calibrator (isotonic regression on historical hit/miss data)
from app.services.confidence_calibrator import fit_calibration, calibration_status

router = APIRouter()

# ── Constants ──────────────────────────────────────────────────────
TARGET_HIT_RATE = 0.86
NUDGE_STEP      = 0.02   # per calibration run — with 0.0–1.0 scale and neutral at 0.5, gives 25 steps to floor
MAX_BIAS        = 1.00
MIN_BIAS        = 0.00

# ── Background job store (in-memory, survives within a single dyno) ──
# Key: job_id → { status, progress, total, results, skipped, error, ... }
_calibration_jobs: dict = {}
_jobs_lock = threading.Lock()
_running_leagues: set = set()    # leagues currently being calibrated
_running_lock = threading.Lock() # protects _running_leagues


def _league_is_running(league_code: str) -> bool:
    """Return True if a calibration is already in progress for this league."""
    with _running_lock:
        return league_code in _running_leagues


def _league_set_running(league_code: str) -> bool:
    """Mark league as running. Returns False if already running."""
    with _running_lock:
        if league_code in _running_leagues:
            return False
        _running_leagues.add(league_code)
        return True


def _league_clear_running(league_code: str) -> None:
    """Mark league as no longer running."""
    with _running_lock:
        _running_leagues.discard(league_code)


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
    Scan a grid of candidate bias shifts and return the one that maximises
    weighted flipped-misses while minimising collateral flipped-wins.

    Each record in lean_records has:
        lean_gap  – over_score minus under_score at prediction time
        is_miss   – True if the prediction was a full miss
        is_over   – True if the market was an Over
        weight    – recency weight (1.0 / 0.5 / 0.2)

    Logic:
      * An over-miss flips to a win when we shift lean_gap downward enough
        that it crosses zero → we need shift <= -lean_gap
      * An under-miss flips when we shift upward → shift >= -lean_gap
      * A win flips (bad) in the opposite direction
      Optimal shift = the step that maximises Σ(flipped_miss_weight) with
      the smallest Σ(flipped_win_weight).
    """
    if not lean_records:
        return {"optimal_shift": 0.0, "analysis": "No lean records available"}

    SHIFT_RANGE = [-0.30, -0.25, -0.20, -0.15, -0.10, -0.05, 0.0,
                    0.05,  0.10,  0.15,  0.20,  0.25,  0.30]

    best_shift   = 0.0
    best_score   = -999.0
    shift_scores = []

    for shift in SHIFT_RANGE:
        flipped_miss_w = 0.0
        flipped_win_w  = 0.0

        for r in lean_records:
            gap    = r["lean_gap"]
            miss   = r["is_miss"]
            is_over = r["is_over"]
            w      = r["weight"]
            shifted = gap + shift

            if miss:
                # Miss flips to win if the shifted gap crosses to the right side
                if is_over  and shifted <= 0:
                    flipped_miss_w += w
                if not is_over and shifted >= 0:
                    flipped_miss_w += w
            else:
                # Win flips to miss — penalise this
                if is_over  and shifted <= 0:
                    flipped_win_w += w
                if not is_over and shifted >= 0:
                    flipped_win_w += w

        # Net score: reward flipped misses, penalise collateral flipped wins
        score = flipped_miss_w - 1.5 * flipped_win_w
        shift_scores.append({
            "shift":           shift,
            "flipped_misses":  round(flipped_miss_w, 3),
            "flipped_wins":    round(flipped_win_w, 3),
            "score":           round(score, 3),
        })
        if score > best_score:
            best_score = score
            best_shift = shift

    return {
        "optimal_shift": best_shift,
        "best_score":    round(best_score, 3),
        "analysis":      shift_scores,
    }


# ── Sensitivity  ─────────────────────────────────────────
def _suggest_sensitivities(
    deg_det_records: list,
    current_deg_sens: float,
    current_det_sens: float,
    current_eps_sens: float,
) -> dict:
    """
    Analyse miss patterns against deg_pressure, det_boost and eps_stability.
    Suggests multiplier adjustments (range 0.5–2.0, step 0.1).
    """
    MIN_RECORDS    = 15
    MIN_SIGNAL     = 6
    STEP           = 0.10
    SENS_MIN       = 0.50
    SENS_MAX       = 2.00
    SCALE          = 1.5

    result = {
        "deg_sensitivity": current_deg_sens,
        "det_sensitivity": current_det_sens,
        "eps_sensitivity": current_eps_sens,
    }

    if len(deg_det_records) < MIN_RECORDS:
        result["insufficient_data"] = True
        result["note"] = f"Only {len(deg_det_records)} records (need {MIN_RECORDS})"
        return result

    baseline_miss = sum(1 for r in deg_det_records if r["is_miss"]) / len(deg_det_records)

    def _suggest_one(field: str, threshold_high: float, current: float) -> float:
        high = [r for r in deg_det_records if r[field] >= threshold_high]
        low  = [r for r in deg_det_records if r[field] <  threshold_high]
        if len(high) < MIN_SIGNAL or len(low) < MIN_SIGNAL:
            return current
        miss_high = sum(1 for r in high if r["is_miss"]) / len(high)
        miss_low  = sum(1 for r in low  if r["is_miss"]) / len(low)
        lift = miss_high - miss_low
        # Positive lift → high values correlate with misses → increase sensitivity
        raw = 1.0 + lift * SCALE
        capped = max(SENS_MIN, min(SENS_MAX, raw))
        stepped = round(max(current - STEP, min(current + STEP, capped)), 2)
        return stepped

    result["deg_sensitivity"] = _suggest_one("deg_pressure",  0.12, current_deg_sens)
    result["det_sensitivity"] = _suggest_one("det_boost",     0.55, current_det_sens)
    result["eps_sensitivity"] = _suggest_one("eps_stability", 0.75, current_eps_sens)

    result["baseline_miss_rate"] = round(baseline_miss, 3)
    result["records_analysed"]   = len(deg_det_records)
    return result


# ── Form delta sensitivity  ───────────────────────────────
def _suggest_form_delta(
    deg_det_records: list,
    current_form_sens: float,
) -> dict:
    """
    Analyse miss patterns based on form delta of home and away teams.
    Suggests a form_delta_sensitivity multiplier.
    """
    MIN_RECORDS = 10
    MIN_SIGNAL_RECS = 5
    SCALE = 2.0
    SENS_CAP_LOW = 0.0
    SENS_CAP_HIGH = 2.0
    SENS_STEP = 0.1

    result = {
        "form_delta_sensitivity": current_form_sens,
        "form_delta_analysis": "Insufficient data",
    }

    # Filter records that have both form deltas
    valid = [r for r in deg_det_records if r.get("home_form_delta") is not None and r.get("away_form_delta") is not None]
    if len(valid) < MIN_RECORDS:
        result["form_delta_analysis"] = f"Insufficient records with form delta ({len(valid)} < {MIN_RECORDS})"
        return result

    # Define "extreme form delta" – e.g., abs(delta) >= 3
    extreme_threshold = 3
    high_form_matches = [r for r in valid if abs(r["home_form_delta"]) >= extreme_threshold or abs(r["away_form_delta"]) >= extreme_threshold]

    if len(high_form_matches) < MIN_SIGNAL_RECS:
        result["form_delta_analysis"] = f"Not enough matches with extreme form delta ({len(high_form_matches)} < {MIN_SIGNAL_RECS})"
        return result

    # Compute miss rate for extreme form matches vs baseline
    miss_rate_high = sum(1 for r in high_form_matches if r["is_miss"]) / len(high_form_matches)
    miss_rate_base = sum(1 for r in valid if r["is_miss"]) / len(valid)
    lift = miss_rate_high - miss_rate_base

    # Suggest sensitivity: if lift positive (more misses when form extreme), increase sensitivity
    raw_suggested = 1.0 + lift * SCALE
    capped = max(SENS_CAP_LOW, min(SENS_CAP_HIGH, raw_suggested))
    stepped = round(max(
        current_form_sens - SENS_STEP,
        min(current_form_sens + SENS_STEP, capped)
    ), 2)

    result["form_delta_sensitivity"] = stepped
    result["form_delta_analysis"] = (
        f"{len(high_form_matches)} matches with extreme form delta (≥{extreme_threshold}): "
        f"miss_rate={round(miss_rate_high,3)} vs baseline={round(miss_rate_base,3)} "
        f"(lift={round(lift,3)}) → suggested sensitivity={stepped}"
    )
    return result


# ── TT threshold tuner ───────────────────────────────────────────────────
def _suggest_tt_thresholds(
    calib_records: list,
    current_flip_threshold: float,
    current_tt_home_bias: float,
    current_tt_confidence_min: float = 0.62,
    current_min_conf: float = 0.0,
) -> dict:
    """
    Analyse per-confidence-bucket hit rates to suggest a better
    alt_flip_threshold, tt_home_bias, and tt_confidence_min for this league.

    tt_confidence_min: the lower gate on TT picks. Picks between
    alt_flip_threshold and tt_confidence_min route to original market.
    Calibration moves this ±0.05 per run based on per-bucket TT performance.
    """
    MIN_BUCKET = 4
    STEP       = 0.05
    BIAS_STEP  = 0.05
    BIAS_MAX   = 0.50  # raised to allow stronger home/away TT routing correction

    result = {
        "alt_flip_threshold":  current_flip_threshold,
        "tt_home_bias":        current_tt_home_bias,
        "tt_confidence_min":   current_tt_confidence_min,
        "analysis":            "Insufficient data",
    }

    if len(calib_records) < 20:
        return result

    # ── Per-bucket flip vs TT comparison ─────────────────────────────
    buckets: dict = {}
    for r in calib_records:
        b = round(round(r["confidence_score"] / STEP) * STEP, 2)
        if b not in buckets:
            buckets[b] = {"flip_hits": 0.0, "flip_total": 0.0,
                           "tt_hits":   0.0, "tt_total":   0.0}
        mkt = r["market"]
        hw  = r["hw"]
        w   = r["weight"]
        if hw < 0:
            continue
        is_tt   = mkt.startswith("TT")
        is_flip = mkt in ("U3.5", "O1.75")
        if is_tt:
            buckets[b]["tt_total"] += w
            buckets[b]["tt_hits"]  += hw * w
        elif is_flip:
            buckets[b]["flip_total"] += w
            buckets[b]["flip_hits"]  += hw * w

    # Find lowest bucket where TT beats flip by at least 3pp
    bucket_analysis = []
    best_tt_threshold = current_flip_threshold
    for b in sorted(buckets):
        bkt = buckets[b]
        tt_rate   = bkt["tt_hits"]   / max(0.001, bkt["tt_total"])   if bkt["tt_total"]   >= MIN_BUCKET else None
        flip_rate = bkt["flip_hits"] / max(0.001, bkt["flip_total"]) if bkt["flip_total"] >= MIN_BUCKET else None
        bucket_analysis.append({
            "bucket":    b,
            "tt_rate":   round(tt_rate   * 100, 1) if tt_rate   is not None else None,
            "flip_rate": round(flip_rate * 100, 1) if flip_rate is not None else None,
        })
        if tt_rate is not None and flip_rate is not None:
            if tt_rate > flip_rate + 0.03:
                best_tt_threshold = b  # TT wins here → threshold could come down to here

    # ── Aggregate flip vs TT fallback ────────────────────────────────────
    # If no bucket had enough flip picks to compare per-bucket,
    # use aggregate flip hit rate vs aggregate TT hit rate instead.
    # This catches leagues (e.g. ESP-LL2) where flip picks are rare but
    # consistently outperform TT — threshold should rise to send more to flip.
    agg_flip_hits = agg_flip_total = 0.0
    agg_tt_hits   = agg_tt_total   = 0.0
    for b_data in buckets.values():
        agg_flip_hits  += b_data["flip_hits"]
        agg_flip_total += b_data["flip_total"]
        agg_tt_hits    += b_data["tt_hits"]
        agg_tt_total   += b_data["tt_total"]

    no_bucket_comparison = best_tt_threshold == current_flip_threshold
    # Require at least 3 flip picks before acting on aggregate signal.
    # 3 picks at consistent 80%+ is meaningful enough to shift threshold by 0.05.
    AGG_FLIP_MIN = 3
    if no_bucket_comparison and agg_flip_total >= AGG_FLIP_MIN and agg_tt_total >= MIN_BUCKET:
        agg_flip_rate = agg_flip_hits / agg_flip_total
        agg_tt_rate   = agg_tt_hits   / agg_tt_total
        if agg_flip_rate > agg_tt_rate + 0.05:
            best_tt_threshold = current_flip_threshold + STEP
        elif agg_tt_rate > agg_flip_rate + 0.05:
            best_tt_threshold = current_flip_threshold - STEP
    elif no_bucket_comparison and agg_flip_total >= AGG_FLIP_MIN and agg_tt_total < MIN_BUCKET:
        # Suppressed league — no TT baseline. Compare flip vs overall hit rate instead.
        all_hits = all_total = 0.0
        for r in calib_records:
            if r["hw"] >= 0:
                all_total += r["weight"]
                all_hits  += r["hw"] * r["weight"]
        if all_total >= AGG_FLIP_MIN:
            agg_flip_rate = agg_flip_hits / agg_flip_total
            overall_rate  = all_hits / all_total
            if agg_flip_rate > overall_rate + 0.05:
                # Flip clearly beats overall → raise threshold
                best_tt_threshold = current_flip_threshold + STEP
            elif agg_flip_rate < overall_rate - 0.05:
                # Flip underperforms → lower threshold
                best_tt_threshold = current_flip_threshold - STEP

    # Step toward suggested threshold conservatively
    if best_tt_threshold < current_flip_threshold - STEP:
        new_threshold = round(current_flip_threshold - STEP, 2)
    elif best_tt_threshold >= current_flip_threshold + STEP:
        new_threshold = round(current_flip_threshold + STEP, 2)
    else:
        new_threshold = current_flip_threshold

    new_threshold = round(max(0.40, min(0.80, new_threshold)), 2)

    # ── TT Home vs Away bias ──────────────────────────────────────────
    tt_home_hits = tt_home_total = 0.0
    tt_away_hits = tt_away_total = 0.0
    for r in calib_records:
        if r["hw"] < 0:
            continue
        w = r["weight"]
        if r["market"] == "TT Home O0.5":
            tt_home_total += w
            tt_home_hits  += r["hw"] * w
        elif r["market"] == "TT Away O0.5":
            tt_away_total += w
            tt_away_hits  += r["hw"] * w

    new_tt_bias = current_tt_home_bias
    if tt_home_total >= MIN_BUCKET and tt_away_total >= MIN_BUCKET:
        home_rate = tt_home_hits / tt_home_total
        away_rate = tt_away_hits / tt_away_total
        gap = home_rate - away_rate
        # Nudge bias toward the stronger side, step BIAS_STEP per run
        # Accelerate bias step when the gap is large (>10pp) — slow 0.05
        # steps take too many runs to converge on strong signals.
        effective_step = BIAS_STEP * 2 if abs(gap) > 0.10 else BIAS_STEP
        if gap > 0.05:
            new_tt_bias = round(min(BIAS_MAX,  current_tt_home_bias + effective_step), 2)
        elif gap < -0.05:
            new_tt_bias = round(max(-BIAS_MAX, current_tt_home_bias - effective_step), 2)
        result["tt_home_rate"] = round(home_rate * 100, 1)
        result["tt_away_rate"] = round(away_rate * 100, 1)
        result["tt_gap"]       = round(gap * 100, 1)

    result["alt_flip_threshold"] = new_threshold
    result["tt_home_bias"]       = new_tt_bias
    result["bucket_analysis"]    = bucket_analysis

    # ── TT confidence gate tuning ─────────────────────────────────────
    # For each bucket, compare TT hit rate vs overall TT rate.
    # Find the lowest bucket where TT consistently beats the overall rate.
    # Set tt_confidence_min = that boundary.
    # This sheds low-confidence TT picks that drag the average down.
    TT_GATE_MIN_BUCKET = 5
    overall_tt_hits = overall_tt_total = 0.0
    for r in calib_records:
        if r["hw"] >= 0 and r["market"].startswith("TT"):
            overall_tt_hits  += r["hw"] * r["weight"]
            overall_tt_total += r["weight"]

    new_tt_conf_min = current_tt_confidence_min
    if overall_tt_total >= TT_GATE_MIN_BUCKET * 2:
        overall_tt_rate = overall_tt_hits / overall_tt_total
        # Build per-bucket TT rates
        tt_buckets: dict = {}
        for r in calib_records:
            if r["hw"] >= 0 and r["market"].startswith("TT"):
                b = round(round(r["confidence_score"] / STEP) * STEP, 2)
                if b not in tt_buckets:
                    tt_buckets[b] = {"hits": 0.0, "total": 0.0}
                tt_buckets[b]["total"] += r["weight"]
                tt_buckets[b]["hits"]  += r["hw"] * r["weight"]

        # Find lowest bucket where TT beats overall rate
        best_gate = current_tt_confidence_min
        for b in sorted(tt_buckets):
            bkt = tt_buckets[b]
            if bkt["total"] < TT_GATE_MIN_BUCKET:
                continue
            bkt_rate = bkt["hits"] / bkt["total"]
            if bkt_rate >= overall_tt_rate - 0.02:  # within 2pp of overall
                best_gate = b  # this bucket is good enough → gate can come down to here
                break           # stop at first good bucket from bottom up

        # Step toward best_gate ±0.05 per run
        if best_gate < current_tt_confidence_min - STEP:
            new_tt_conf_min = round(current_tt_confidence_min - STEP, 2)
        elif best_gate > current_tt_confidence_min + STEP:
            new_tt_conf_min = round(current_tt_confidence_min + STEP, 2)

        # Clamp between alt_flip_threshold and 0.85
        new_tt_conf_min = round(max(new_threshold, min(0.85, new_tt_conf_min)), 2)

    result["tt_confidence_min"] = new_tt_conf_min

    # ── Min confidence gate tuning ───────────────────────────────────
    # If picks below tt_confidence_min are hitting <45% across all markets,
    # suggest raising min_confidence to skip them entirely.
    # If they recover above 55%, lower it back.
    MIN_CONF_BUCKET = 5
    low_conf_hits = low_conf_total = 0.0
    for r in calib_records:
        if r["hw"] >= 0 and r["confidence_score"] < new_tt_conf_min:
            low_conf_total += r["weight"]
            low_conf_hits  += r["hw"] * r["weight"]

    new_min_conf = current_min_conf
    if low_conf_total >= MIN_CONF_BUCKET:
        low_conf_rate = low_conf_hits / low_conf_total
        if low_conf_rate < 0.58 and new_tt_conf_min > current_min_conf:
            # Low-confidence band underperforming — jump directly to tt_confidence_min
            # if the rate is very bad (<50%), otherwise step conservatively.
            if low_conf_rate < 0.50:
                new_min_conf = new_tt_conf_min  # jump to gate immediately
            else:
                new_min_conf = round(min(new_tt_conf_min, current_min_conf + STEP), 2)
        elif low_conf_rate > 0.65 and current_min_conf > 0.0:
            # Recovering strongly — lower the gate
            new_min_conf = round(max(0.0, current_min_conf - STEP), 2)

    result["min_confidence"] = new_min_conf
    # Flag weak TT sides — use lower minimum (3 picks) since the bias may
    # have already reduced one side to very few picks
    TT_WEAK_MIN = 3
    if tt_home_total >= TT_WEAK_MIN or tt_away_total >= TT_WEAK_MIN:
        home_rate = tt_home_hits / tt_home_total if tt_home_total >= TT_WEAK_MIN else None
        away_rate = tt_away_hits / tt_away_total if tt_away_total >= TT_WEAK_MIN else None
        result["tt_home_weak"] = (home_rate < 0.65) if home_rate is not None else False
        result["tt_away_weak"] = (away_rate < 0.65) if away_rate is not None else False
    result["analysis"] = (
        f"Flip threshold: {current_flip_threshold} → {new_threshold} | "
        f"TT home bias: {current_tt_home_bias} → {new_tt_bias}"
    )
    return result


# ── Alt market suppression analyser ──────────────────────────────────────
def _suggest_alt_market_use(
    alt_vs_original: list,
    current_use_alt: bool,
    current_min_win_rate: float,
) -> dict:
    """
    Analyse whether the alt market (TT/flip) is actively hurting vs the
    original ATHENA market on missed predictions.

    For each match where alt market missed, checks if the original market
    would have won. If that "original saves the miss" rate exceeds
    alt_min_original_win_rate, suggest disabling alt substitution.

    Re-enables if suppressed and the alt is now outperforming.
    """
    MIN_RECORDS = 15

    result = {
        "use_alt_market":            current_use_alt,
        "alt_min_original_win_rate": current_min_win_rate,
        "analysis":                  "Insufficient data",
        "alt_miss_count":            0,
        "original_wins_on_alt_miss": 0,
        "original_win_rate_on_miss": None,
        "alt_overall_rate":          None,
        "original_overall_rate":     None,
    }

    if len(alt_vs_original) < MIN_RECORDS:
        return result

    # Overall hit rates (weighted)
    alt_hits = alt_total = orig_hits = orig_total = 0.0
    for r in alt_vs_original:
        if r["alt_hw"] < 0:
            continue
        w = r["weight"]
        alt_total += w
        alt_hits  += r["alt_hw"] * w
        if r["original_hw"] >= 0:
            orig_total += w
            orig_hits  += r["original_hw"] * w

    if alt_total < MIN_RECORDS:
        return result

    alt_rate  = alt_hits  / alt_total
    orig_rate = orig_hits / orig_total if orig_total > 0 else 0.0

    result["alt_overall_rate"]      = round(alt_rate  * 100, 1)
    result["original_overall_rate"] = round(orig_rate * 100, 1)

    # Miss analysis: when alt loses, does original win?
    miss_w = orig_win_on_miss_w = 0.0
    for r in alt_vs_original:
        if r["alt_hw"] < 0:
            continue
        if r["alt_hw"] < 0.5:   # alt missed
            w = r["weight"]
            miss_w += w
            if r["original_hw"] >= 0.5:   # original would have won
                orig_win_on_miss_w += w

    result["alt_miss_count"]            = round(miss_w, 1)
    result["original_wins_on_alt_miss"] = round(orig_win_on_miss_w, 1)

    if miss_w < 4:
        result["analysis"] = f"Too few alt misses ({round(miss_w,1)}) to analyse suppression"
        return result

    orig_win_rate_on_miss = orig_win_on_miss_w / miss_w
    result["original_win_rate_on_miss"] = round(orig_win_rate_on_miss * 100, 1)

    if current_use_alt:
        # Track consecutive runs where original beats alt
        new_orig_ahead = current_orig_ahead_runs
        if alt_rate is not None and orig_rate is not None:
            if orig_rate > alt_rate + 0.01:
                new_orig_ahead = current_orig_ahead_runs + 1
            else:
                new_orig_ahead = 0
        result["orig_ahead_runs"] = new_orig_ahead

        suppress = (
            (orig_win_rate_on_miss >= current_min_win_rate and alt_rate < orig_rate - 0.02)
            or (orig_win_rate_on_miss >= 0.50 and alt_rate < orig_rate - 0.05)
            or (alt_rate < orig_rate - 0.05 and miss_w >= 8)
            or (new_orig_ahead >= 3)  # original leading 3+ consecutive runs
        )
        if suppress:
            result["use_alt_market"] = False
            result["analysis"] = (
                f"SUPPRESSING alt market: on {round(miss_w,1)} alt misses, "
                f"original wins {round(orig_win_rate_on_miss*100,1)}% "
                f"(threshold {round(current_min_win_rate*100,1)}%). "
                f"Alt {round(alt_rate*100,1)}% vs original {round(orig_rate*100,1)}."
            )
        else:
            result["analysis"] = (
                f"Alt market retained: original wins {round(orig_win_rate_on_miss*100,1)}% "
                f"of alt misses (threshold {round(current_min_win_rate*100,1)}%). "
                f"Alt {round(alt_rate*100,1)}% vs original {round(orig_rate*100,1)}."
            )
    else:
        if alt_rate > orig_rate + 0.05:
            result["use_alt_market"] = True
            result["analysis"] = (
                f"RE-ENABLING alt market: alt {round(alt_rate*100,1)}% "
                f"now outperforms original {round(orig_rate*100,1)}% by >5pp."
            )
        else:
            result["analysis"] = (
                f"Alt market remains suppressed: alt {round(alt_rate*100,1)}% "
                f"vs original {round(orig_rate*100,1)}% — gap insufficient to re-enable."
            )

    return result


# ── Bias  ────────────────────────────────────────────────
def _suggest_bias(
    over_hits: float, over_total: float,
    under_hits: float, under_total: float,
    current_over: float, current_under: float,
    current_tempo: float,
    overall_hit_rate: float,
    miss_patterns: dict,
    lean_records: list,
) -> dict:
    """
    Suggest bias adjustments toward TARGET_HIT_RATE.

    Strategy:
      1. Run lean-gap optimal shift analysis
      2. Derive per-side (over/under) hit rates
      3. Nudge the weaker side's bias by NUDGE_STEP toward neutral
      4. Adjust tempo_factor based on over_miss_high_tempo pattern
    """
    over_rate  = round(over_hits  / max(0.001, over_total)  * 100, 1)
    under_rate = round(under_hits / max(0.001, under_total) * 100, 1)

    notes: list[str] = []
    notes.append(
        f"Hit rates — Over: {over_rate}% ({int(over_total)} picks), "
        f"Under: {under_rate}% ({int(under_total)} picks), "
        f"Overall: {overall_hit_rate}%"
    )

    # ── Lean-gap analysis ─────────────────────────────────────────────
    lean_result = _find_optimal_bias_shift(lean_records)
    optimal_shift = lean_result.get("optimal_shift", 0.0)
    if optimal_shift != 0.0:
        notes.append(
            f"Lean-gap optimal shift: {optimal_shift:+.2f} "
            f"(score={lean_result.get('best_score', 0.0):.3f})"
        )

    # ── Bias nudge logic ──────────────────────────────────────────────
    new_over  = current_over
    new_under = current_under

    gap_to_target = TARGET_HIT_RATE - overall_hit_rate / 100.0

    if overall_hit_rate < TARGET_HIT_RATE * 100:
        # Below target — nudge the worse side toward neutral (0.5)
        if over_total >= 5 and under_total >= 5:
            # Require 8pp gap before nudging to prevent oscillation
            # when over and under rates are close
            if over_rate < under_rate - 0.08:
                # Over side clearly weaker
                direction = 1.0 if current_over < 0.5 else -1.0
                new_over = round(
                    max(MIN_BIAS, min(MAX_BIAS, current_over + direction * NUDGE_STEP)), 4
                )
                notes.append(f"Nudging over bias {current_over} → {new_over} (over side weaker)")
            elif under_rate < over_rate - 0.08:
                # Under side clearly weaker
                direction = 1.0 if current_under < 0.5 else -1.0
                new_under = round(
                    max(MIN_BIAS, min(MAX_BIAS, current_under + direction * NUDGE_STEP)), 4
                )
                notes.append(f"Nudging under bias {current_under} → {new_under} (under side weaker)")
            else:
                notes.append(
                    f"Over/Under gap {abs(round(over_rate-under_rate,3)*100):.1f}pp "
                    f"< 8pp threshold — biases stable (over={round(over_rate*100,1)}% "
                    f"under={round(under_rate*100,1)}%)"
                )
        elif over_total >= 5:
            if over_rate < TARGET_HIT_RATE * 100:
                direction = 1.0 if current_over < 0.5 else -1.0
                new_over = round(
                    max(MIN_BIAS, min(MAX_BIAS, current_over + direction * NUDGE_STEP)), 4
                )
                notes.append(f"Nudging over bias {current_over} → {new_over}")
        elif under_total >= 5:
            if under_rate < TARGET_HIT_RATE * 100:
                direction = 1.0 if current_under < 0.5 else -1.0
                new_under = round(
                    max(MIN_BIAS, min(MAX_BIAS, current_under + direction * NUDGE_STEP)), 4
                )
                notes.append(f"Nudging under bias {current_under} → {new_under}")
        else:
            notes.append("Insufficient picks per side to suggest bias change.")
    else:
        notes.append(
            f"Hit rate {overall_hit_rate}% meets target {round(TARGET_HIT_RATE*100,1)}% — "
            "biases unchanged."
        )

    # ── Tempo nudge ───────────────────────────────────────────────────
    new_tempo = current_tempo
    total_over_misses = miss_patterns.get("total_over_misses", 0)
    high_tempo_misses = miss_patterns.get("over_miss_high_tempo", 0)

    if total_over_misses > 0:
        high_tempo_ratio = high_tempo_misses / total_over_misses
        if high_tempo_ratio >= 0.4 and current_tempo > 0.40:
            new_tempo = round(max(0.30, current_tempo - 0.02), 4)
            notes.append(
                f"High-tempo over-misses ratio={round(high_tempo_ratio,2)} — "
                f"reducing tempo {current_tempo} → {new_tempo}"
            )
        elif high_tempo_ratio < 0.15 and current_tempo < 0.65:
            new_tempo = round(min(0.70, current_tempo + 0.02), 4)
            notes.append(
                f"Low high-tempo miss ratio={round(high_tempo_ratio,2)} — "
                f"raising tempo {current_tempo} → {new_tempo}"
            )
        else:
            notes.append(f"Tempo factor unchanged at {current_tempo}.")
    else:
        notes.append(f"No over misses to evaluate tempo. Tempo unchanged at {current_tempo}.")

    return {
        "base_over_bias":  new_over,
        "base_under_bias": new_under,
        "tempo_factor":    new_tempo,
        "notes":           notes,
        "target_hit_rate": TARGET_HIT_RATE,
        "current_hit_rate": round(overall_hit_rate / 100, 3),
        "gap_to_target":   round(gap_to_target, 3),
        "over_hit_rate":   over_rate,
        "under_hit_rate":  under_rate,
        "miss_patterns":   miss_patterns,
        "lean_analysis":   lean_result,
        "applied_changes": {},
    }


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
    if not _league_set_running(league_code):
        return JSONResponse(status_code=409, content={
            "detail": f"Calibration for {league_code} is already running. Try again shortly."
        })
    try:
        return _run_calibration_inner(league_code, limit, min_matches_before, apply, db)
    finally:
        _league_clear_running(league_code)


def _run_calibration_inner(
    league_code: str,
    limit: int,
    min_matches_before: int,
    apply: bool,
    db: Session,
) -> CalibResult | JSONResponse:
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

    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    current_over  = float(cfg.base_over_bias  or 0.5) if cfg else 0.5
    current_under = float(cfg.base_under_bias or 0.5) if cfg else 0.5
    current_tempo = float(cfg.tempo_factor    or 0.50) if cfg else 0.50
    current_deg_sens = float(cfg.deg_sensitivity or 1.0) if cfg else 1.0
    current_det_sens = float(cfg.det_sensitivity or 1.0) if cfg else 1.0
    current_eps_sens = float(cfg.eps_sensitivity or 1.0) if cfg else 1.0
    current_form_sens      = float(cfg.form_delta_sensitivity or 0.0)  if cfg else 0.0
    current_flip_threshold    = float(getattr(cfg, "alt_flip_threshold",    None) or 0.62)  if cfg else 0.62
    current_tt_home_bias      = float(getattr(cfg, "tt_home_bias",          None) or 0.0)   if cfg else 0.0
    current_use_alt_market    = bool(getattr(cfg,  "use_alt_market",        True))           if cfg else True
    current_min_original_rate = float(getattr(cfg, "alt_min_original_win_rate", None) or 0.70) if cfg else 0.70
    current_orig_ahead_runs   = int(getattr(cfg,   "orig_ahead_runs",        0) or 0)         if cfg else 0
    # Weak TT side flags — when a side consistently underperforms (<65%),
    # skip that side entirely and fall back to original market
    _tt_home_weak       = bool(getattr(cfg, "tt_home_weak",       False)) if cfg else False
    _tt_away_weak       = bool(getattr(cfg, "tt_away_weak",       False)) if cfg else False
    current_tt_conf_min  = float(getattr(cfg, "tt_confidence_min", None) or 0.62) if cfg else 0.62
    current_min_conf     = float(getattr(cfg, "min_confidence",    None) or 0.0)  if cfg else 0.0

    # ── Determine current variance for this league ──────────────────
    # For red variance leagues, alt market becomes the primary evaluated market.
    latest_calib = (
        db.query(CalibrationLog)
        .filter(CalibrationLog.league_code == league_code)
        .order_by(CalibrationLog.run_at.desc())
        .first()
    )
    current_variance = None
    if latest_calib:
        if latest_calib.hit_rate >= 80:
            current_variance = "green"
        elif latest_calib.hit_rate >= 70:
            current_variance = "orange"
        else:
            current_variance = "red"
    # Respect the suppression flag — if calibration has blocked alt substitution
    # for this league, evaluate original markets so the hit rate reflects reality.
    is_alt_variance = current_use_alt_market

    # ── Pre-warm feature cache for this league ───────────────────────
    # Loads the snapshot DataFrame into memory once so asof_features,
    # get_historical_form_delta, and player nudge all skip DB reads.
    warm_snapshot_cache(db, league_code)

    # ── Pre-parse the warmed snapshot ────────────────────────────────
    # The warmed DataFrame may still have a raw Score column. Pre-parse
    # it once here so cached_asof_features never triggers the per-call
    # "[fbref_base] Parsing Score column" path.
    try:
        _snap_ov = None
        for _mp3 in ["app.services.feature_cache", "app.services.data_providers.fbref_base"]:
            try:
                import importlib as _il3
                _snap_ov = getattr(_il3.import_module(_mp3), "_SNAPSHOT_OVERRIDE", None)
                if _snap_ov is not None:
                    break
            except Exception:
                continue
        from app.services.data_providers.fbref_base import _parse_score_column as _fbref_parse_score
        if _snap_ov and league_code in _snap_ov:
            _snap_df = _snap_ov[league_code]
            _cols_lower = [str(c).lower() for c in _snap_df.columns]
            _score_col = next(
                (c for c in _snap_df.columns if str(c).lower() in ("score", "scores")),
                None
            )
            if _score_col and "hg" not in _cols_lower:
                _snap_ov[league_code] = _fbref_parse_score(_snap_df, _score_col)
    except Exception:
        pass

    # ── Pre-load squad power for all teams in this league ────────────
    # get_historical_player_nudge calls get_historical_squad_power twice
    # per match (home + away). Each call issues 2-3 DB queries to find
    # SquadSnapshot, fails to find a historical one, and falls back to
    # the most recent snapshot — querying the same rows every match.
    # Pre-loading all team power values once into a dict gives O(1)
    # lookup with zero DB queries inside the loop.
    from app.models.models_players import SquadSnapshot
    _squad_power_map: dict[str, float] = {}
    try:
        # Load the most recent snapshot per team for this league
        from sqlalchemy import func as _sa_func
        latest_snaps = (
            db.query(SquadSnapshot)
            .filter(
                SquadSnapshot.league_code == league_code,
                SquadSnapshot.squad_power.isnot(None),
            )
            .order_by(SquadSnapshot.team, SquadSnapshot.snapshot_date.desc())
            .all()
        )
        seen_teams: set = set()
        for snap in latest_snaps:
            if snap.team not in seen_teams:
                _squad_power_map[snap.team] = float(snap.squad_power)
                seen_teams.add(snap.team)
        print(f"[calibration] Pre-loaded squad power for "
              f"{len(_squad_power_map)} teams in {league_code}")
    except Exception as _sp_err:
        print(f"[calibration] Squad power pre-load skipped: {_sp_err}")

    # ── Per-match memoization caches ─────────────────────────────────
    _form_delta_cache: dict = {}

    # Pre-build a standings-by-date cache from the already-warmed snapshot.
    # get_historical_form_delta re-loads the parquet from DB every call —
    # 400 loads for 200 matches. Instead, slice the warmed DataFrame once
    # per unique date and cache the standings result.
    _standings_by_date: dict = {}  # match_date → standings list

    def _standings_asof(mdate) -> list:
        """Return standings computed from warmed snapshot up to mdate."""
        if mdate not in _standings_by_date:
            try:
                # _SNAPSHOT_OVERRIDE may live in feature_cache or fbref_base
                # depending on version — try both
                _snap_override = None
                for _mod_path in [
                    "app.services.feature_cache",
                    "app.services.data_providers.fbref_base",
                ]:
                    try:
                        import importlib
                        _mod = importlib.import_module(_mod_path)
                        _snap_override = getattr(_mod, "_SNAPSHOT_OVERRIDE", None)
                        if _snap_override is not None:
                            break
                    except Exception:
                        continue
                from app.services.form_delta import _compute_standings, _season_cutoff
                snap_df = _snap_override.get(league_code) if _snap_override else None
                if snap_df is None:
                    _standings_by_date[mdate] = []
                    return []
                col_map = {str(c).lower(): c for c in snap_df.columns}
                date_col = col_map.get("date")
                home_col = col_map.get("home") or col_map.get("home_team")
                away_col = col_map.get("away") or col_map.get("away_team")
                if not all([date_col, home_col, away_col]):
                    _standings_by_date[mdate] = []
                    return []
                sliced = snap_df[snap_df[date_col] <= import_pd_timestamp(mdate)]
                _standings_by_date[mdate] = _compute_standings(db, sliced, home_col, away_col)
            except Exception:
                _standings_by_date[mdate] = []
        return _standings_by_date[mdate]

    # Import pandas Timestamp once
    try:
        import pandas as _pd
        import_pd_timestamp = _pd.Timestamp
    except Exception:
        import_pd_timestamp = lambda d: d

    def _cached_form_delta(team: str, lc: str, mdate) -> Optional[int]:
        key = (team, mdate)
        if key not in _form_delta_cache:
            try:
                from app.services.form_delta import _season_cutoff
                standings = _standings_asof(mdate)
                actual_pos = next(
                    (e["pos"] for e in standings if e.get("team_key") == team), None
                )
                if actual_pos is None:
                    _form_delta_cache[key] = None
                    return None
                # Expected position from squad power ranking (fast path)
                sorted_teams = sorted(
                    _squad_power_map.items(), key=lambda x: x[1], reverse=True
                )
                expected_pos = next(
                    (i + 1 for i, (t, _) in enumerate(sorted_teams) if t == team), None
                )
                if expected_pos is None:
                    _form_delta_cache[key] = None
                else:
                    _form_delta_cache[key] = expected_pos - actual_pos
            except Exception:
                _form_delta_cache[key] = get_historical_form_delta(db, team, lc, mdate)
        return _form_delta_cache[key]

    # Player nudge using pre-loaded squad power — zero DB queries
    def _cached_player_nudge(lc: str, home: str, away: str, mdate) -> float:
        if not _squad_power_map:
            # Fall back to original if pre-load failed
            return get_historical_player_nudge(db, lc, home, away, mdate)
        home_power = _squad_power_map.get(home)
        away_power = _squad_power_map.get(away)
        if home_power is None or away_power is None:
            return 0.0
        from app.services.predict import PLAYER_POWER_BLEND, PLAYER_POWER_MAX_EFFECT
        power_delta = (home_power - away_power) / 100.0
        nudge = power_delta * PLAYER_POWER_BLEND
        return round(max(-PLAYER_POWER_MAX_EFFECT, min(PLAYER_POWER_MAX_EFFECT, nudge)), 4)

    def _weight(pos: int) -> float:
        if pos <= 10: return 1.0
        if pos <= 30: return 0.5
        return 0.2

    market_tracker: dict = {}
    team_tracker:   dict = {}
    w_hits = w_misses = 0.0
    skipped = 0
    skipped_matches: list = []
    sample_rows: list = []
    lean_records: list = []
    deg_det_records: list = []
    calib_records: list    = []   # per-match records for TT threshold tuning
    alt_vs_original: list = []   # per-match alt vs original market comparison

    # ── Shadow alt-lane trackers ──────────────────────────────────────
    # Tracks what hit rate would be if we played the alternative lane
    # instead of the main market on qualifying picks.
    # Flip trigger: confidence_score < alt_flip_threshold (per-league, calibration-tunable)
    # TT trigger:   alt_flip_threshold <= confidence_score < 0.75 (mid MEDIUM)
    ALT_FLIP_THRESHOLD = current_flip_threshold  # per-league, default 0.62
    ALT_TT_THRESHOLD   = 0.75
    alt_flip_hits = alt_flip_misses = 0.0
    alt_flip_count = 0
    alt_tt_hits = alt_tt_misses = 0.0
    alt_tt_count = 0

    def _derive_flip_market(main_market):
        if main_market.startswith("O"):
            return "U3.5"
        return "O1.75"

    def _tt_stronger_side(p_home, p_away):
        if p_home is None and p_away is None:
            return None
        if p_home is None:
            return "away"
        if p_away is None:
            return "home"
        return "home" if p_home >= p_away else "away"

    def _tt_hit(p_home, p_away, hg, ag):
        side = _tt_stronger_side(p_home, p_away)
        if side is None:
            return -1.0
        if side == "away":
            return 1.0 if ag >= 1 else 0.0
        return 1.0 if hg >= 1 else 0.0

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

    # ── Dedup helpers ────────────────────────────────────────────────────────
    # Two-level deduplication:
    #
    # Level 1 (BEFORE feature computation): score-based key (date, hg, ag,
    #   home_prefix, away_prefix). "Las Palmas" and "Palmas" on the same date
    #   with the same score are the same game — skip the expensive asof_features
    #   call entirely. Using 4-char prefix of the raw name avoids full-string
    #   false positives while still collapsing name variants.
    #
    # Level 2 (AFTER resolve): resolved-name key (date, home_team, away_team,
    #   hg, ag). Catches cases where prefixes differ but resolution converges.
    _seen_matches: set = set()
    _seen_scores:  set = set()   # (date, hg, ag, home4, away4) — pre-resolve fast skip

    def _score_key(mdate, h_raw, a_raw, hg, ag):
        h4 = h_raw.strip().lower()[:4]
        a4 = a_raw.strip().lower()[:4]
        return (mdate, hg, ag, h4, a4)

    for pos, (_, match_row) in enumerate(completed.iterrows(), start=1):
        match_date = match_row[date_col].date()
        home_team_raw = str(match_row[home_col])
        away_team_raw = str(match_row[away_col])
        hg = int(match_row["hg"])
        ag = int(match_row["ag"])
        w  = _weight(pos)

        # Level 1: fast score-based dedup — skip before any DB/feature work
        sk = _score_key(match_date, home_team_raw, away_team_raw, hg, ag)
        if sk in _seen_scores:
            continue  # dedup — not counted as a skip
        _seen_scores.add(sk)

        # Resolve team names to canonical keys (used for tracking/nudge/delta)
        home_team = resolve_team_name(db, home_team_raw, league_code)
        away_team = resolve_team_name(db, away_team_raw, league_code)

        # Level 2: resolved-name dedup — catches cases not caught by level 1
        _match_key = (match_date, home_team, away_team, hg, ag)
        if _match_key in _seen_matches:
            continue  # dedup — not counted as a skip
        _seen_matches.add(_match_key)

        try:
            # Pass RAW snapshot names to asof_features — fbref_base uses its
            # own fuzzy matcher (_match_team) to find rows in the snapshot.
            # Passing resolved canonical keys like "inter milan" fails because
            # the snapshot has "Inter" and the extra word tanks the match score.
            metrics = cached_asof_features(
                league_code, home_team_raw, away_team_raw, match_date,
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
                deg_pressure=metrics.get("deg_pressure"),
                det_boost=metrics.get("det_boost"),
                home_det=metrics.get("home_det"),
                away_det=metrics.get("away_det"),
                eps_stability=metrics.get("eps_stability"),
            )

            player_nudge = _cached_player_nudge(
                league_code, home_team, away_team, match_date,
            )
            pred = evaluate_athena(
                req,
                current_over,
                current_under,
                current_tempo,
                team_nudge=player_nudge,
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

        original_market = pred.translated_play.market
        conf_score = pred.confidence_score or {"HIGH": 0.85, "MEDIUM": 0.65, "LOW": 0.40}.get(
            pred.translated_play.confidence, 0.65
        )

        # Skip entirely if below league minimum confidence gate
        if current_min_conf > 0.0 and conf_score < current_min_conf:
            skipped_matches.append({"position": pos, "skipped_reason": f"below_min_confidence ({conf_score:.2f} < {current_min_conf})", "conf_score": round(conf_score, 3)})
            skipped += 1
            continue
        p_home_tt = metrics.get("p_home_tt05")
        p_away_tt = metrics.get("p_away_tt05")

        # For red variance leagues, substitute alt market as primary
        if is_alt_variance:
            if conf_score < ALT_FLIP_THRESHOLD:
                market = "U3.5" if original_market.startswith("O") else "O1.75"
            elif conf_score < current_tt_conf_min:
                # Below TT confidence gate — serve original market
                market = original_market
            # (min_confidence skip is handled before this block in outer loop)
            else:
                if p_home_tt is not None or p_away_tt is not None:
                    h = (p_home_tt or 0.0) + current_tt_home_bias
                    a = p_away_tt or 0.0
                    if h >= a:
                        market = original_market if _tt_home_weak else "TT Home O0.5"
                    else:
                        market = original_market if _tt_away_weak else "TT Away O0.5"
                else:
                    market = original_market
        else:
            market = original_market

        result = evaluate_market(market, hg, ag)
        hw = hit_weight(result)

        # Also evaluate the original market on this same match
        # so we can compare alt vs original performance directly.
        original_result = evaluate_market(original_market, hg, ag)
        original_hw     = hit_weight(original_result)

        # Record for TT threshold tuning
        calib_records.append({
            "confidence_score": conf_score,
            "market":           market,
            "hw":               hw,
            "weight":           w,
        })

        # Record for alt-vs-original suppression analysis
        if market != original_market:   # only when substitution actually happened
            alt_vs_original.append({
                "alt_hw":      hw,
                "original_hw": original_hw,
                "weight":      w,
                "market":      market,
                "original":    original_market,
            })

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

        # Initialize here so sample_rows.append can reference them regardless of hw
        home_form_delta = None
        away_form_delta = None

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

            # Compute historical form delta — uses memoization cache
            home_form_delta = _cached_form_delta(home_team, league_code, match_date)
            away_form_delta = _cached_form_delta(away_team, league_code, match_date)

            # Initialize team_tracker entries if needed
            for team in [home_team, away_team]:
                if team not in team_tracker:
                    team_tracker[team] = {
                        "over_hits": 0, "over_total": 0,
                        "under_hits": 0, "under_total": 0,
                        "det_values": [],
                        "deg_values": [],
                        "over_miss_det": [],
                        "under_miss_det": [],
                        # Actual scoring reality — market-agnostic
                        "goals_scored_home": [],   # goals scored when home
                        "goals_scored_away": [],   # goals scored when away
                        "goals_conceded_home": [], # goals conceded when home
                        "goals_conceded_away": [], # goals conceded when away
                        "tt_home_hits": 0, "tt_home_total": 0,  # TT Home hit rate
                        "tt_away_hits": 0, "tt_away_total": 0,  # TT Away hit rate
                    }

                # Update market‑specific stats
                if is_over_market:
                    team_tracker[team]["over_total"] += 1
                    if hw >= 0.5:
                        team_tracker[team]["over_hits"] += 1
                else:
                    team_tracker[team]["under_total"] += 1
                    if hw >= 0.5:
                        team_tracker[team]["under_hits"] += 1

                raw_det = metrics.get("det_boost") or 0.30
                raw_deg = metrics.get("deg_pressure") or 0.0
                team_tracker[team]["det_values"].append(raw_det)
                team_tracker[team]["deg_values"].append(raw_deg)

                if is_over_market and is_full_miss:
                    team_tracker[team]["over_miss_det"].append(raw_det)
                if not is_over_market and is_full_miss:
                    team_tracker[team]["under_miss_det"].append(raw_det)

                # Record actual goals — market-agnostic scoring reality
                if team == home_team:
                    team_tracker[team]["goals_scored_home"].append(hg)
                    team_tracker[team]["goals_conceded_home"].append(ag)
                    # TT Home tracking: did home team score?
                    team_tracker[team]["tt_home_total"] += 1
                    if hg >= 1:
                        team_tracker[team]["tt_home_hits"] += 1
                else:  # away team
                    team_tracker[team]["goals_scored_away"].append(ag)
                    team_tracker[team]["goals_conceded_away"].append(hg)
                    # TT Away tracking: did away team score?
                    team_tracker[team]["tt_away_total"] += 1
                    if ag >= 1:
                        team_tracker[team]["tt_away_hits"] += 1

            deg_det_records.append({
                "deg_pressure":  metrics.get("deg_pressure")  or 0.0,
                "det_boost":     metrics.get("det_boost")     or 0.30,
                "eps_stability": metrics.get("eps_stability") or 0.65,
                "is_over":       is_over_market,
                "is_miss":       is_full_miss,
                "total_goals":   hg + ag,
                "home_form_delta": home_form_delta,
                "away_form_delta": away_form_delta,
            })

        # ── Shadow alt-lane tracking ─────────────────────────────────
        raw_conf_score = pred.confidence_score if hasattr(pred, "confidence_score") else None
        if raw_conf_score is None:
            raw_conf_score = {"HIGH": 0.85, "MEDIUM": 0.65, "LOW": 0.40}.get(
                pred.translated_play.confidence, 0.65
            )
        p_home = metrics.get("p_home_tt05")
        p_away = metrics.get("p_away_tt05")

        if raw_conf_score < ALT_FLIP_THRESHOLD and hw >= 0:
            # Flip: evaluate opposite market
            flip_mkt = _derive_flip_market(market)
            flip_result = evaluate_market(flip_mkt, hg, ag)
            flip_hw = hit_weight(flip_result)
            if flip_hw >= 0:
                alt_flip_hits   += (flip_hw >= 0.5) * w
                alt_flip_misses += (flip_hw < 0.5)  * w
                alt_flip_count  += 1
                # Feed flip shadow into calib_records so the threshold
                # tuner can act on it even when suppression is on
                calib_records.append({
                    "confidence_score": raw_conf_score,
                    "market":           flip_mkt,
                    "hw":               flip_hw,
                    "weight":           w,
                })
        elif raw_conf_score < ALT_TT_THRESHOLD and hw >= 0:
            # TT shadow
            tt_hw = _tt_hit(p_home, p_away, hg, ag)
            if tt_hw >= 0:
                alt_tt_hits   += (tt_hw >= 0.5) * w
                alt_tt_misses += (tt_hw < 0.5)  * w
                alt_tt_count  += 1

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
                "original_market": original_market if market != original_market else None,
                "result":      result,
                "hit":         hw >= 0.5,
                "hit_weight":  hw,
                "lean_gap":    lean_gap,
                "confidence":  pred.translated_play.confidence,
                "corridor":    f"{pred.corridor.low}–{pred.corridor.high}",
                "lean":        pred.corridor.lean,
                "inputs":      metrics,
                "player_nudge":  player_nudge,
                "home_form_delta": home_form_delta,
                "away_form_delta": away_form_delta,
            })

    evaluated = sum(s["raw_hits"] + s["raw_misses"] for s in market_tracker.values())
    overall_hit_rate = round(w_hits / max(0.001, w_hits + w_misses) * 100, 1)

    by_market = []
    over_w_hits = over_w_total = under_w_hits = under_w_total = 0.0
    for market, stats in sorted(market_tracker.items()):
        wh = stats["w_hits"]
        wm = stats["w_misses"]
        rate = round(wh / max(0.001, wh + wm) * 100, 1)
        by_market.append(MarketStats(
            market=market, hits=stats["raw_hits"],
            misses=stats["raw_misses"], skipped=stats["skipped"],
            hit_rate=rate,
        ))
        if market.startswith("O"):
            over_w_hits += wh
            over_w_total += wh + wm
        elif market.startswith("U"):
            under_w_hits += wh
            under_w_total += wh + wm

    # ---- Build suggestions ----
    suggestion = _suggest_bias(
        over_w_hits, over_w_total,
        under_w_hits, under_w_total,
        current_over, current_under, current_tempo,
        overall_hit_rate, miss_patterns, lean_records,
    )
    if suggestion is None:
        print("[calibration] ERROR: _suggest_bias returned None!")
        suggestion = {
            "base_over_bias": current_over,
            "base_under_bias": current_under,
            "tempo_factor": current_tempo,
            "notes": ["Fallback due to None return"],
            "target_hit_rate": TARGET_HIT_RATE,
            "current_hit_rate": round(overall_hit_rate / 100, 3),
            "gap_to_target": round(TARGET_HIT_RATE - overall_hit_rate / 100, 3),
            "miss_patterns": miss_patterns,
            "lean_analysis": {"analysis": []},
            "applied_changes": {},
        }

    # Sensitivity suggestion
    sensitivity_suggestion = _suggest_sensitivities(
        deg_det_records,
        current_deg_sens, current_det_sens, current_eps_sens,
    )

    # ---------- FALLBACK FOR None ----------
    if sensitivity_suggestion is None:
        sensitivity_suggestion = {
            "deg_sensitivity": current_deg_sens,
            "det_sensitivity": current_det_sens,
            "eps_sensitivity": current_eps_sens,
            "insufficient_data": True,
            "note": "No sensitivity suggestion returned (likely insufficient data)"
        }
    # ---------------------------------------

    form_delta_suggestion = _suggest_form_delta(
        deg_det_records,
        current_form_sens,
    )

    suggestion["sensitivity"] = sensitivity_suggestion
    suggestion["form_delta"] = form_delta_suggestion

    # TT threshold suggestion
    tt_threshold_suggestion = _suggest_tt_thresholds(
        calib_records,
        current_flip_threshold,
        current_tt_home_bias,
        current_tt_conf_min,
        current_min_conf,
    )
    suggestion["tt_thresholds"] = tt_threshold_suggestion

    # Alt market suppression analysis
    alt_market_suggestion = _suggest_alt_market_use(
        alt_vs_original,
        current_use_alt_market,
        current_min_original_rate,
    )
    suggestion["alt_market_suppression"] = alt_market_suggestion

    # ── Shadow alt-lane hit rates ─────────────────────────────────────
    alt_flip_hr = round(alt_flip_hits / max(0.001, alt_flip_hits + alt_flip_misses) * 100, 1)         if alt_flip_count > 0 else None
    alt_tt_hr   = round(alt_tt_hits / max(0.001, alt_tt_hits + alt_tt_misses) * 100, 1)         if alt_tt_count > 0 else None
    suggestion["alt_lane_shadow"] = {
        "flip_hit_rate":    alt_flip_hr,
        "flip_evaluated":   alt_flip_count,
        "flip_description": "Opposite main market (U3.5/O1.75) on LOW or bottom-MEDIUM confidence picks",
        "tt_hit_rate":      alt_tt_hr,
        "tt_evaluated":     alt_tt_count,
        "tt_description":   "Strongest TT side on mid-MEDIUM confidence picks",
        "vs_main":          overall_hit_rate,
        "flip_gain":        round(alt_flip_hr - overall_hit_rate, 1) if alt_flip_hr else None,
        "tt_gain":          round(alt_tt_hr - overall_hit_rate, 1) if alt_tt_hr else None,
    }

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
                "Biases updated: " + ", ".join(
                    f"{k} {v['before']}→{v['after']}" for k, v in changed.items()
                )
            )
        # Note: "nothing changed" is deferred until after all checks complete

        # Update sensitivity multipliers
        sens = sensitivity_suggestion
        sens_changed = {}
        SENS_MIN_CHANGE = 0.05
        for field, current, key in [
            ("deg_sensitivity", current_deg_sens, "deg_sensitivity"),
            ("det_sensitivity", current_det_sens, "det_sensitivity"),
            ("eps_sensitivity", current_eps_sens, "eps_sensitivity"),
        ]:
            suggested = sens.get(key)
            if suggested is not None and abs(suggested - current) >= SENS_MIN_CHANGE:
                setattr(cfg, field, suggested)
                sens_changed[field] = {"before": current, "after": suggested}

        form_sens_suggested = form_delta_suggestion.get("form_delta_sensitivity")
        if form_sens_suggested is not None and abs(form_sens_suggested - current_form_sens) >= SENS_MIN_CHANGE:
            cfg.form_delta_sensitivity = form_sens_suggested
            sens_changed["form_delta_sensitivity"] = {"before": current_form_sens, "after": form_sens_suggested}

        # Apply TT threshold suggestions
        tt_flip_suggested = tt_threshold_suggestion.get("alt_flip_threshold")
        tt_bias_suggested = tt_threshold_suggestion.get("tt_home_bias")
        if tt_flip_suggested is not None and abs(tt_flip_suggested - current_flip_threshold) >= 0.01:
            if hasattr(cfg, "alt_flip_threshold"):
                cfg.alt_flip_threshold = tt_flip_suggested
                sens_changed["alt_flip_threshold"] = {"before": current_flip_threshold, "after": tt_flip_suggested}
        if tt_bias_suggested is not None and abs(tt_bias_suggested - current_tt_home_bias) >= 0.01:
            if hasattr(cfg, "tt_home_bias"):
                cfg.tt_home_bias = tt_bias_suggested
                sens_changed["tt_home_bias"] = {"before": current_tt_home_bias, "after": tt_bias_suggested}

        # Write weak TT side flags
        if hasattr(cfg, "tt_home_weak"):
            cfg.tt_home_weak = bool(tt_threshold_suggestion.get("tt_home_weak", False))
        if hasattr(cfg, "tt_away_weak"):
            cfg.tt_away_weak = bool(tt_threshold_suggestion.get("tt_away_weak", False))
        # Write consecutive suppression counter
        new_orig_ahead = alt_market_suggestion.get("orig_ahead_runs")
        if new_orig_ahead is not None and hasattr(cfg, "orig_ahead_runs"):
            cfg.orig_ahead_runs = new_orig_ahead
            if new_orig_ahead > 0:
                sens_changed["orig_ahead_runs"] = {"before": current_orig_ahead_runs, "after": new_orig_ahead}

        # Write min confidence gate
        min_conf_suggested = tt_threshold_suggestion.get("min_confidence")
        if min_conf_suggested is not None and abs(min_conf_suggested - current_min_conf) >= 0.01:
            if hasattr(cfg, "min_confidence"):
                cfg.min_confidence = min_conf_suggested
                sens_changed["min_confidence"] = {"before": current_min_conf, "after": min_conf_suggested}

        # Write TT confidence gate
        tt_conf_suggested = tt_threshold_suggestion.get("tt_confidence_min")
        if tt_conf_suggested is not None and abs(tt_conf_suggested - current_tt_conf_min) >= 0.01:
            if hasattr(cfg, "tt_confidence_min"):
                cfg.tt_confidence_min = tt_conf_suggested
                sens_changed["tt_confidence_min"] = {"before": current_tt_conf_min, "after": tt_conf_suggested}

        # Apply alt market suppression / re-enable
        suggested_use_alt = alt_market_suggestion.get("use_alt_market")
        if suggested_use_alt is not None and suggested_use_alt != current_use_alt_market:
            if hasattr(cfg, "use_alt_market"):
                cfg.use_alt_market = suggested_use_alt
                sens_changed["use_alt_market"] = {
                    "before": current_use_alt_market,
                    "after":  suggested_use_alt,
                }
                action = "SUPPRESSED" if not suggested_use_alt else "RE-ENABLED"
                suggestion["notes"].append(
                    f"Alt market {action}: {alt_market_suggestion.get('analysis','')}"
                )

        if sens_changed:
            db.commit()
            applied = True
            applied_changes.update(sens_changed)
            suggestion["notes"].append(
                "Sensitivities updated: " + ", ".join(
                    f"{k} {v['before']}→{v['after']}" for k, v in sens_changed.items()
                )
            )
        elif sens.get("insufficient_data"):
            suggestion["notes"].append(
                f"Sensitivity analysis: insufficient data ({sens.get('note', '')})."
            )
        else:
            suggestion["notes"].append(
                "Sensitivities: already at suggested values — no change."
            )

        # Update team‑level nudges (only standard fields)
        MIN_TEAM_SAMPLES = 6
        NUDGE_SCALE      = 0.3
        NUDGE_MAX        = 0.05
        team_nudges_applied = {}

        # Pre-load all TeamConfig rows for this league — one query
        # instead of one per team in the loop below.
        _existing_configs: dict = {
            tc.team: tc
            for tc in db.query(TeamConfig).filter_by(league_code=league_code).all()
        }

        # Compute league-wide scoring averages for comparison
        all_home_goals = [g for s in team_tracker.values() for g in s["goals_scored_home"]]
        all_away_goals = [g for s in team_tracker.values() for g in s["goals_scored_away"]]
        league_avg_home_scored = sum(all_home_goals) / max(1, len(all_home_goals))
        league_avg_away_scored = sum(all_away_goals) / max(1, len(all_away_goals))
        league_avg_scored = (league_avg_home_scored + league_avg_away_scored) / 2

        for team, stats in team_tracker.items():
            over_n  = stats["over_total"]
            under_n = stats["under_total"]
            over_nudge  = 0.0
            under_nudge = 0.0

            # ── Scoring-reality nudge ─────────────────────────────────
            # Compare team actual avg goals to league avg.
            # A team scoring 0.5 goals/game above avg gets +over_nudge.
            # This works for TT and Over leagues — market-agnostic.
            home_goals = stats["goals_scored_home"]
            away_goals = stats["goals_scored_away"]
            all_scored = home_goals + away_goals

            if len(all_scored) >= MIN_TEAM_SAMPLES:
                team_avg_scored = sum(all_scored) / len(all_scored)
                scoring_gap = team_avg_scored - league_avg_scored
                # Scale: 0.5 goals above avg → full NUDGE_MAX nudge
                over_nudge = round(
                    max(-NUDGE_MAX, min(NUDGE_MAX, scoring_gap * NUDGE_SCALE * 2.0)), 4
                )

            # ── TT-specific nudge for TT-heavy leagues ───────────────
            # Track how often this team scores at least 1 goal at home/away.
            # Supplements over_nudge for TT market routing.
            tt_home_n = stats["tt_home_total"]
            tt_away_n = stats["tt_away_total"]
            if tt_home_n >= MIN_TEAM_SAMPLES:
                tt_home_rate = stats["tt_home_hits"] / tt_home_n
                # League TT home avg: fraction of home teams that score
                league_tt_home = sum(
                    s["tt_home_hits"] for s in team_tracker.values()
                ) / max(1, sum(s["tt_home_total"] for s in team_tracker.values()))
                tt_gap = tt_home_rate - league_tt_home
                # Blend: over_nudge gets 60% scoring reality, 40% TT reality
                tt_nudge = round(
                    max(-NUDGE_MAX, min(NUDGE_MAX, tt_gap * NUDGE_SCALE * 1.5)), 4
                )
                over_nudge = round(
                    max(-NUDGE_MAX, min(NUDGE_MAX,
                        over_nudge * 0.6 + tt_nudge * 0.4)), 4
                )

            # ── Legacy market-based nudge (fallback when no scoring data) ─
            # Still useful for suppressed leagues serving Over/Under markets
            if len(all_scored) < MIN_TEAM_SAMPLES:
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

            # DET nudge
            DET_NUDGE_MAX   = 0.15
            DET_NUDGE_SCALE = 0.50
            det_values = stats["det_values"]
            det_nudge  = 0.0
            team_avg_det = None
            if len(det_values) >= MIN_TEAM_SAMPLES:
                league_avg_det = (
                    sum(
                        v for s in team_tracker.values()
                        for v in s["det_values"]
                    ) / max(1, sum(len(s["det_values"]) for s in team_tracker.values()))
                )
                team_avg_det = round(sum(det_values) / len(det_values), 3)
                det_deviation = team_avg_det - league_avg_det
                det_nudge = round(
                    max(-DET_NUDGE_MAX, min(DET_NUDGE_MAX, det_deviation * DET_NUDGE_SCALE)), 4
                )

            # DEG nudge
            DEG_NUDGE_MAX   = 0.10
            DEG_NUDGE_SCALE = 0.40
            from app.engine.pipeline import DEG_TRIGGER as _DEG_TRIGGER
            deg_nudge    = 0.0
            team_avg_deg = None
            deg_values   = stats["deg_values"]
            if len(deg_values) >= MIN_TEAM_SAMPLES:
                team_avg_deg = round(sum(deg_values) / len(deg_values), 3)

            over_miss_det  = stats["over_miss_det"]
            if over_n >= MIN_TEAM_SAMPLES and over_miss_det:
                low_deg_misses = sum(1 for d in over_miss_det if d < _DEG_TRIGGER)
                all_low_deg_over = sum(
                    1 for d in (stats["det_values"])
                    if d < _DEG_TRIGGER
                )
                if all_low_deg_over >= 4:
                    team_low_deg_miss_rate = low_deg_misses / all_low_deg_over
                    baseline = 1.0 - (overall_hit_rate / 100.0)
                    excess_miss = team_low_deg_miss_rate - baseline
                    deg_nudge = round(
                        max(-DEG_NUDGE_MAX, min(DEG_NUDGE_MAX, excess_miss * DEG_NUDGE_SCALE)), 4
                    )

            # Update or create TeamConfig (dict lookup — no DB query per team)
            from datetime import datetime as _dt
            existing = _existing_configs.get(team)

            if existing:
                existing.over_nudge      = over_nudge
                existing.under_nudge     = under_nudge
                existing.det_nudge       = det_nudge
                existing.deg_nudge       = deg_nudge
                existing.avg_det         = team_avg_det
                existing.avg_deg         = team_avg_deg
                existing.over_hit_rate   = round(stats["over_hits"] / over_n, 3) if over_n else None
                existing.under_hit_rate  = round(stats["under_hits"] / under_n, 3) if under_n else None
                existing.over_matches    = over_n
                existing.under_matches   = under_n
                existing.last_calibrated = _dt.utcnow()
            else:
                try:
                    db.add(TeamConfig(
                        league_code=league_code,
                        team=team,
                        over_nudge=over_nudge,
                        under_nudge=under_nudge,
                        det_nudge=det_nudge,
                        deg_nudge=deg_nudge,
                        avg_det=team_avg_det,
                        avg_deg=team_avg_deg,
                        over_hit_rate=round(stats["over_hits"] / over_n, 3) if over_n else None,
                        under_hit_rate=round(stats["under_hits"] / under_n, 3) if under_n else None,
                        over_matches=over_n,
                        under_matches=under_n,
                        last_calibrated=_dt.utcnow(),
                    ))
                    db.flush()  # catch constraint errors early
                except Exception:
                    # Row was inserted by a concurrent run — fall back to update
                    db.rollback()
                    _existing = db.query(TeamConfig).filter_by(
                        league_code=league_code, team=team
                    ).first()
                    if _existing:
                        _existing.over_nudge   = over_nudge
                        _existing.under_nudge  = under_nudge
                        _existing.det_nudge    = det_nudge
                        _existing.deg_nudge    = deg_nudge
                        _existing.last_calibrated = _dt.utcnow()

            # Track if any significant nudge was applied (for summary)
            if abs(over_nudge) > 0.005 or abs(under_nudge) > 0.005 \
               or abs(det_nudge) > 0.01 or abs(deg_nudge) > 0.01:
                team_nudges_applied[team] = {
                    "over_nudge":  over_nudge,
                    "under_nudge": under_nudge,
                    "det_nudge":   det_nudge,
                    "deg_nudge":   deg_nudge,
                    "over_rate":   round(stats["over_hits"] / over_n, 3) if over_n else None,
                    "over_n":      over_n,
                    "avg_det":     team_avg_det,
                }

        db.commit()

        if team_nudges_applied:
            suggestion["notes"].append(
                f"Team nudges applied: {len(team_nudges_applied)} teams adjusted. "
                f"Notable: " + ", ".join(
                    f"{t} (over={v['over_nudge']:+.3f})"
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

    # ── Deferred summary note ─────────────────────────────────────────
    # Now that all change blocks have run we can give an honest summary.
    if apply and cfg:
        total_changes = len(applied_changes)
        if total_changes == 0:
            suggestion["notes"].append(
                "apply=true — no changes made. All values already at suggested levels."
            )
        else:
            change_summary = []
            if "base_over_bias" in applied_changes or "base_under_bias" in applied_changes or "tempo_factor" in applied_changes:
                bias_parts = [k for k in ("base_over_bias", "base_under_bias", "tempo_factor") if k in applied_changes]
                change_summary.append(f"Biases: {', '.join(bias_parts)}")
            sens_keys = [k for k in applied_changes if k.endswith("_sensitivity")]
            if sens_keys:
                change_summary.append(f"Sensitivities: {', '.join(sens_keys)}")
            if "team_nudges" in applied_changes:
                change_summary.append(f"Team nudges: {len(applied_changes['team_nudges'])} teams")
            suggestion["notes"].append(
                f"Summary: {len(total_changes if isinstance(total_changes, dict) else applied_changes)} parameter group(s) updated — "
                + " | ".join(change_summary)
            )

    # ── Write CalibrationLog ──────────────────────────────────────────
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

    # ── Backfill variance flags on pending predictions ────────────────
    # When apply=true, the hit rate has been recorded. Update the
    # variance_flag on all pending predictions for this league so the
    # frontend reflects the current calibration quality immediately.
    if apply:
        try:
            from app.database.models_predictions import PredictionLog
            new_flag = (
                "green"  if overall_hit_rate >= 80 else
                "orange" if overall_hit_rate >= 70 else
                "red"
            )
            updated_flags = (
                db.query(PredictionLog)
                .filter(
                    PredictionLog.league_code == league_code,
                    PredictionLog.status == "pending",
                )
                .all()
            )
            for pred in updated_flags:
                pred.variance_flag = new_flag
            if updated_flags:
                db.commit()
                print(f"[calibration] Updated variance_flag to '{new_flag}' "
                      f"for {len(updated_flags)} pending predictions in {league_code}")
        except Exception as _flag_err:
            print(f"[calibration] Warning: could not update variance flags: {_flag_err}")

    # Release the snapshot DataFrame from memory now that this league is done
    clear_feature_cache(league_code)

    # ── Top-level summary (readable at a glance) ─────────────────────
    # Condenses the most important findings so you don't have to read
    # through the full bias_suggestion dict to understand what happened.
    top_summary = {
        "hit_rate":        overall_hit_rate,
        "target":          round(TARGET_HIT_RATE * 100, 1),
        "gap_to_target":   round(TARGET_HIT_RATE * 100 - overall_hit_rate, 1),
        "evaluated":       evaluated,
        "skipped":         skipped,
        "applied":         applied,
        "changes_made":    list(applied_changes.keys()) if applied_changes else [],
        "best_market":     max(by_market, key=lambda m: m.hit_rate).market if len(by_market) > 1 else None,
        "worst_market":    min(by_market, key=lambda m: m.hit_rate).market if len(by_market) > 1 else None,
        "min_conf_skips":  len([s for s in skipped_matches if "below_min_confidence" in s.get("skipped_reason", "")]),
        "conf_distribution": {
            "below_flip":    sum(1 for s in skipped_matches + sample_rows if s.get("conf_score", s.get("confidence_score_raw", 0)) < current_flip_threshold),
            "flip_to_tt":    sum(1 for s in skipped_matches if current_flip_threshold <= s.get("conf_score", 0) < current_min_conf),
            "above_gate":    evaluated,
        },
        "over_misses":     miss_patterns["total_over_misses"],
        "under_misses":    miss_patterns["total_under_misses"],
        "variance_flag":   (
            "green"  if overall_hit_rate >= 80 else
            "orange" if overall_hit_rate >= 70 else
            "red"
        ),
    }
    # Inject at top of suggestion so it appears first in the JSON
    suggestion = {"summary": top_summary, **suggestion}

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
# ── Main calibration endpoint (single league) ──────────────────────
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


# ══════════════════════════════════════════════════════════════════════════════
# ASYNC BULK CALIBRATION — background job pattern to prevent 502 timeouts
# ══════════════════════════════════════════════════════════════════════════════

def _calibrate_all_background(
    job_id: str,
    limit: int,
    min_matches_before: int,
    apply: bool,
):
    """
    Background task: calibrate all leagues that have snapshots.
    Updates _calibration_jobs[job_id] with progress.
    """
    from app.database.db import SessionLocal
    from app.database.models_fbref import FBrefSnapshot

    db = SessionLocal()
    try:
        # Get all unique league codes that have snapshots
        leagues = db.query(FBrefSnapshot.league_code).distinct().all()
        total = len(leagues)

        with _jobs_lock:
            _calibration_jobs[job_id]["status"] = "running"
            _calibration_jobs[job_id]["total"] = total

        results = []
        for idx, (league_code,) in enumerate(leagues, start=1):
            with _jobs_lock:
                _calibration_jobs[job_id]["current_league"] = league_code
                _calibration_jobs[job_id]["progress"] = idx

            try:
                # Use the existing calibration core for each league
                calib_result = _run_calibration(
                    league_code,
                    limit,
                    min_matches_before,
                    apply,
                    db,
                )
                # calib_result is either CalibResult or JSONResponse.
                # Convert to serializable dict.
                if hasattr(calib_result, "dict"):
                    result_data = calib_result.dict()
                else:
                    # If it's a JSONResponse, extract content.
                    result_data = {"error": calib_result.body.decode() if hasattr(calib_result, "body") else "Unknown error"}
                results.append({
                    "league_code": league_code,
                    "result": result_data,
                })
            except Exception as e:
                results.append({
                    "league_code": league_code,
                    "error": str(e),
                })

        with _jobs_lock:
            _calibration_jobs[job_id]["status"] = "done"
            _calibration_jobs[job_id]["result"] = results
            _calibration_jobs[job_id]["current_league"] = None
    except Exception as e:
        import traceback
        with _jobs_lock:
            _calibration_jobs[job_id]["status"] = "error"
            _calibration_jobs[job_id]["error"] = str(e)
            _calibration_jobs[job_id]["traceback"] = traceback.format_exc()
    finally:
        db.close()

@router.post("/calibrate/all")
def calibrate_all_leagues(
    background_tasks: BackgroundTasks,
    limit: int = Query(100, ge=10, le=500, description="Max matches per league"),
    min_matches_before: int = Query(3, ge=2, le=20),
    apply: bool = Query(False, description="Write adjustments to DB for all leagues"),
    db: Session = Depends(get_db),
):
    """
    Kick off bulk calibration as a background job.
    """
    count = db.query(FBrefSnapshot).count()
    if count == 0:
        return {"message": "No snapshots found. Run the scraper first.", "results": []}

    job_id = str(uuid.uuid4())[:8]

    with _jobs_lock:
        _calibration_jobs[job_id] = {
            "status":         "queued",
            "progress":       0,
            "total":          count,
            "current_league": None,
            "result":         None,
            "error":          None,
            "started_at":     dt.utcnow().isoformat(),
            "apply":          apply,
        }

    background_tasks.add_task(
        _calibrate_all_background,
        job_id, limit, min_matches_before, apply,
    )

    return {
        "job_id":   job_id,
        "status":   "queued",
        "total":    count,
        "message":  f"Calibration started for {count} leagues. "
                    f"Poll GET /calibrate/all/status?job_id={job_id} for progress.",
    }

@router.get("/calibrate/all/status")
def calibrate_all_status(
    job_id: str = Query(..., description="Job ID from POST /calibrate/all"),
):
    with _jobs_lock:
        job = _calibration_jobs.get(job_id)

    if not job:
        return JSONResponse(status_code=404, content={
            "detail": f"No job found with id '{job_id}'. "
                      "Jobs are stored in-memory and lost on dyno restart."
        })

    response = {
        "job_id":         job_id,
        "status":         job["status"],
        "progress":       job["progress"],
        "total":          job["total"],
        "current_league": job["current_league"],
        "started_at":     job["started_at"],
        "apply":          job["apply"],
    }

    if job["status"] == "done":
        response["result"] = job["result"]
    elif job["status"] == "error":
        response["error"]     = job["error"]
        response["traceback"] = job.get("traceback")

    return response


@router.get("/calibrate/all/jobs")
def list_calibration_jobs():
    with _jobs_lock:
        return {
            "jobs": [
                {
                    "job_id":     jid,
                    "status":     j["status"],
                    "progress":   j["progress"],
                    "total":      j["total"],
                    "started_at": j["started_at"],
                    "apply":      j["apply"],
                }
                for jid, j in _calibration_jobs.items()
            ]
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
    """
    result = {
        "league_code":       league_code,
        "league_config":     None,
        "team_nudges_wiped": 0,
        "snapshot_wiped":    False,
        "notes":             [],
    }
    NEUTRAL_OVER        = 0.5
    NEUTRAL_UNDER       = 0.5
    NEUTRAL_TEMPO       = 0.50
    NEUTRAL_SENSITIVITY = 1.0

    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    if cfg:
        cfg.base_over_bias  = NEUTRAL_OVER
        cfg.base_under_bias = NEUTRAL_UNDER
        cfg.tempo_factor    = NEUTRAL_TEMPO
        cfg.deg_sensitivity = NEUTRAL_SENSITIVITY
        cfg.det_sensitivity = NEUTRAL_SENSITIVITY
        cfg.eps_sensitivity = NEUTRAL_SENSITIVITY
        cfg.form_delta_sensitivity = 0.0
        db.commit()
        result["league_config"] = {
            "base_over_bias":  NEUTRAL_OVER,
            "base_under_bias": NEUTRAL_UNDER,
            "tempo_factor":    NEUTRAL_TEMPO,
            "deg_sensitivity": NEUTRAL_SENSITIVITY,
            "det_sensitivity": NEUTRAL_SENSITIVITY,
            "eps_sensitivity": NEUTRAL_SENSITIVITY,
            "form_delta_sensitivity": 0.0,
        }
        result["notes"].append(
            f"LeagueConfig reset to neutral midpoints: "
            f"over={NEUTRAL_OVER} under={NEUTRAL_UNDER} tempo={NEUTRAL_TEMPO} "
            f"(sensitivities reset to 1.0, form_delta_sensitivity to 0.0). "
            f"Run calibrate?apply=true to let data decide."
        )
    else:
        result["notes"].append(f"No LeagueConfig found for {league_code} — nothing to reset.")

    deleted = (
        db.query(TeamConfig)
        .filter_by(league_code=league_code)
        .delete(synchronize_session=False)
    )
    db.commit()
    result["team_nudges_wiped"] = deleted
    result["notes"].append(f"Wiped {deleted} team nudge(s) for {league_code}.")

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


# ── Confidence calibration endpoints ───────────────────────────────────────────

@router.post("/calibrate/confidence")
def calibrate_confidence_scores(
    league_code: str = Query(None, description="Single league, or omit for global fit"),
    min_samples: int = Query(30, ge=10, le=500,
                             description="Minimum hit+miss predictions required"),
    fit_global: bool = Query(True,
                             description="Also fit a global calibration across all leagues"),
    db: Session = Depends(get_db),
):
    """
    Fit isotonic regression calibration on historical prediction outcomes.

    Reads (confidence_score, hit/miss) pairs from PredictionLog and fits
    a monotone calibration curve so that confidence_score values correspond
    to actual historical hit rates.

    The calibrated_probability field in /predict responses uses this mapping.

    Recommended workflow:
      1. Run calibration for individual leagues once you have 30+ predictions each.
      2. Run with fit_global=true to also build a cross-league global calibration
         (used as fallback for leagues with insufficient data).
      3. Re-run weekly or after a significant number of new results are in.
    """
    results = []

    # League-specific fit
    if league_code:
        result = fit_calibration(db, league_code=league_code, min_samples=min_samples)
        results.append(result)
    else:
        # Fit per-league for all leagues that have predictions
        from app.database.models_predictions import PredictionLog
        from sqlalchemy import func as sa_func
        league_counts = (
            db.query(PredictionLog.league_code, sa_func.count(PredictionLog.id))
            .filter(
                PredictionLog.status.in_(["hit", "miss"]),
                PredictionLog.confidence_score.isnot(None),
            )
            .group_by(PredictionLog.league_code)
            .all()
        )
        for lc, count in sorted(league_counts):
            if count >= min_samples:
                result = fit_calibration(db, league_code=lc, min_samples=min_samples)
                results.append(result)
            else:
                results.append({
                    "success": False,
                    "league_code": lc,
                    "reason": f"Only {count} samples (need {min_samples})",
                    "n_samples": count,
                })

    # Global fit (uses all leagues combined)
    global_result = None
    if fit_global:
        global_result = fit_calibration(db, league_code=None, min_samples=min_samples)

    successful = [r for r in results if r.get("success")]
    failed     = [r for r in results if not r.get("success")]

    return {
        "leagues_fitted":   len(successful),
        "leagues_skipped":  len(failed),
        "global":           global_result,
        "results":          results,
        "tip": (
            "calibrated_probability now appears in /predict responses. "
            "Re-run this endpoint weekly or after batch-validate adds new results."
        ),
    }


@router.get("/calibrate/confidence/status")
def confidence_calibration_status(
    db: Session = Depends(get_db),
):
    """
    Show current state of all stored confidence calibrations.
    Reports sample count, Brier score, and improvement vs uncalibrated baseline.
    """
    from app.services.feature_cache import cache_stats as feature_cache_stats
    status = calibration_status(db)
    return {
        "calibrations":  status,
        "total":         len(status),
        "feature_cache": feature_cache_stats(),
        "tip": (
            "Run POST /calibrate/confidence to fit or refresh calibrations. "
            "Brier score: lower is better. improvement = raw_brier - calibrated_brier."
        ),
    }
