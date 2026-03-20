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
    # ... (unchanged, same as before)
    # (keep the existing implementation)
    # (I'll not repeat the whole function here for brevity, but you should keep the original)
    # Make sure to keep the exact same function as in your current file.
    pass


# ── Sensitivity  ─────────────────────────────────────────
def _suggest_sensitivities(
    deg_det_records: list,
    current_deg_sens: float,
    current_det_sens: float,
    current_eps_sens: float,
) -> dict:
    # ... (unchanged)
    pass


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
    # ... (unchanged)
    pass


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
    current_form_sens = float(cfg.form_delta_sensitivity or 0.0) if cfg else 0.0

    # ── Pre-warm feature cache for this league ───────────────────────
    # Loads the snapshot DataFrame into memory once so asof_features,
    # get_historical_form_delta, and player nudge all skip DB reads.
    warm_snapshot_cache(db, league_code)

    # ── Per-match memoization caches ─────────────────────────────────
    # get_historical_form_delta parses the full snapshot + computes
    # standings on every call. Same (team, date) pair appears many times
    # across 100 matches (e.g. a team appearing in 10 fixtures = 10 calls).
    # Cache keyed by (team, date) cuts 200 calls down to ~40 unique ones.
    _form_delta_cache: dict = {}

    def _cached_form_delta(team: str, lc: str, mdate) -> Optional[int]:
        key = (team, mdate)
        if key not in _form_delta_cache:
            _form_delta_cache[key] = get_historical_form_delta(db, team, lc, mdate)
        return _form_delta_cache[key]

    # get_historical_player_nudge queries SquadSnapshot per match pair.
    # Cache keyed by (home, away, date) eliminates all repeat lookups.
    _player_nudge_cache: dict = {}

    def _cached_player_nudge(lc: str, home: str, away: str, mdate) -> float:
        key = (home, away, mdate)
        if key not in _player_nudge_cache:
            _player_nudge_cache[key] = get_historical_player_nudge(db, lc, home, away, mdate)
        return _player_nudge_cache[key]

    def _weight(pos: int) -> float:
        if pos <= 10: return 1.0
        if pos <= 30: return 0.5
        return 0.2

    market_tracker: dict = {}
    team_tracker:   dict = {}
    w_hits = w_misses = 0.0
    skipped = 0
    sample_rows: list = []
    lean_records: list = []
    deg_det_records: list = []
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
        home_team_raw = str(match_row[home_col])
        away_team_raw = str(match_row[away_col])
        hg = int(match_row["hg"])
        ag = int(match_row["ag"])
        w  = _weight(pos)

        # Resolve team names to canonical keys
        home_team = resolve_team_name(db, home_team_raw, league_code)
        away_team = resolve_team_name(db, away_team_raw, league_code)

        try:
            metrics = cached_asof_features(
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

        market = pred.translated_play.market
        result = evaluate_market(market, hg, ag)
        hw = hit_weight(result)

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

            # Update or create TeamConfig (only existing fields)
            from datetime import datetime as _dt
            existing = (
                db.query(TeamConfig)
                .filter_by(league_code=league_code, team=team)
                .first()
            )

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
        "best_market":     max(by_market, key=lambda m: m.hit_rate).market if by_market else None,
        "worst_market":    min(by_market, key=lambda m: m.hit_rate).market if by_market else None,
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
