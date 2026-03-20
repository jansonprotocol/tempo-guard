# backend/app/services/confidence_calibrator.py
"""
ATHENA Confidence Calibrator — isotonic regression on PredictionLog history.

WHAT THIS DOES
==============
ATHENA's raw confidence_score is derived from signal strengths in the
prediction pipeline. There is no guarantee it is well-calibrated — i.e.
that a raw score of 80 actually corresponds to an 80% hit rate.

This module:
  1. Reads historical (confidence_score, hit/miss) pairs from PredictionLog.
  2. Bins scores into equal-frequency buckets.
  3. Applies PAVA (Pool Adjacent Violators Algorithm) — the gold-standard
     isotonic regression — to ensure the calibration is monotonically
     non-decreasing (higher raw score must → higher calibrated probability).
  4. Stores the resulting breakpoint mapping as JSON in the DB.
  5. At prediction time, interpolates the calibrated probability from those
     breakpoints given any raw score.

OUTPUTS
=======
  calibrated_probability  — float in [0.0, 1.0]
      The true estimated probability of this prediction being a hit.
      Used for Kelly-criterion staking, edge detection vs market odds,
      and displaying honest confidence levels in the frontend.

  brier_score             — float (lower = better)
      Measures calibration quality. Stored per fit for monitoring.
      Raw Brier is also stored so you can track improvement over time.

STORAGE
=======
  confidence_calibration table (created in main.py _safe_migrate):
    - league_code  VARCHAR  (NULL = global calibration across all leagues)
    - n_samples    INTEGER
    - brier_score  FLOAT
    - raw_brier    FLOAT
    - breakpoints_json  TEXT  ([[raw_score, calibrated_prob], ...])
    - fitted_at    DATETIME

USAGE
=====
    # Fit (run after calibration, or as a scheduled job)
    from app.services.confidence_calibrator import fit_calibration, calibrate_confidence

    result = fit_calibration(db)                      # global
    result = fit_calibration(db, league_code="ENG-PL")  # per-league

    # Apply at prediction time
    cal_prob = calibrate_confidence(db, raw_score=78.5, league_code="ENG-PL")
    # → e.g. 0.71  (meaning "this prediction hits about 71% of the time")
"""
from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from sqlalchemy import Column, Integer, String, Float, DateTime, Text
from sqlalchemy.orm import Session

from app.database.base import Base

# ── ORM Model ─────────────────────────────────────────────────────────────────

class ConfidenceCalibration(Base):
    __tablename__ = "confidence_calibration"

    id                = Column(Integer,  primary_key=True, autoincrement=True)
    league_code       = Column(String,   nullable=True, index=True)  # NULL = global
    n_samples         = Column(Integer,  default=0)
    brier_score       = Column(Float,    default=None)   # calibrated
    raw_brier         = Column(Float,    default=None)   # uncalibrated baseline
    breakpoints_json  = Column(Text,     default=None)   # [[raw, calibrated], ...]
    fitted_at         = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<ConfidenceCalibration "
            f"league={self.league_code or 'global'} "
            f"n={self.n_samples} "
            f"brier={self.brier_score}>"
        )


# ── Constants ─────────────────────────────────────────────────────────────────

MIN_SAMPLES      = 30   # minimum predictions to attempt calibration
MIN_BIN_SIZE     = 5    # minimum predictions per bin
MAX_BINS         = 12   # never more than this many breakpoints


# ── Core algorithm — PAVA ─────────────────────────────────────────────────────

def _isotonic_regression_1d(values: list[float]) -> list[float]:
    """
    Pool Adjacent Violators Algorithm (PAVA) for non-decreasing isotonic
    regression.  Runs in O(n) time with no external dependencies.

    Args:
        values: List of target values (actual hit rates per bin), sorted
                by the corresponding x values (raw confidence scores).

    Returns:
        List of the same length with non-decreasing values.
        Any violations are resolved by pooling adjacent blocks and
        replacing them with their weighted mean.
    """
    if not values:
        return []

    # Each block: [mean, count]
    blocks: list[list] = [[v, 1] for v in values]

    i = 0
    while i < len(blocks) - 1:
        if blocks[i][0] > blocks[i + 1][0]:
            # Violation — merge blocks i and i+1
            n1, n2 = blocks[i][1], blocks[i + 1][1]
            merged_mean = (blocks[i][0] * n1 + blocks[i + 1][0] * n2) / (n1 + n2)
            blocks[i] = [merged_mean, n1 + n2]
            blocks.pop(i + 1)
            # Step back to check if the merged block now violates the one before it
            if i > 0:
                i -= 1
        else:
            i += 1

    # Expand blocks back to per-bin values
    result: list[float] = []
    for mean, count in blocks:
        result.extend([mean] * count)

    return result


# ── Calibration fitting ───────────────────────────────────────────────────────

def fit_calibration(
    db: Session,
    league_code: Optional[str] = None,
    min_samples: int = MIN_SAMPLES,
) -> dict:
    """
    Fit isotonic calibration from historical PredictionLog data.

    Tries league-specific data first. If league_code is None, fits a global
    calibration across all leagues — useful when individual leagues don't yet
    have enough predictions.

    Args:
        db:           SQLAlchemy session.
        league_code:  If provided, fit for this league only.
                      If None, fit globally across all leagues.
        min_samples:  Minimum hit+miss predictions required. Returns an
                      error dict if insufficient data.

    Returns:
        Dict with:
          success (bool), n_samples, n_bins, brier_score, raw_brier,
          improvement (raw_brier - calibrated_brier), breakpoints, fitted_at.
    """
    from app.database.models_predictions import PredictionLog

    query = db.query(
        PredictionLog.confidence_score,
        PredictionLog.status,
    ).filter(
        PredictionLog.status.in_(["hit", "miss"]),
        PredictionLog.confidence_score.isnot(None),
    )
    if league_code:
        query = query.filter(PredictionLog.league_code == league_code)

    rows = query.all()

    if len(rows) < min_samples:
        return {
            "success":    False,
            "reason":     f"Insufficient data: {len(rows)} samples (need {min_samples})",
            "n_samples":  len(rows),
            "league_code": league_code or "global",
        }

    raw_scores  = [r.confidence_score for r in rows]
    is_hit      = [1.0 if r.status == "hit" else 0.0 for r in rows]

    # Sort by score ascending
    pairs       = sorted(zip(raw_scores, is_hit), key=lambda x: x[0])
    xs          = [p[0] for p in pairs]
    ys          = [p[1] for p in pairs]

    # Build equal-frequency bins
    n = len(xs)
    n_bins = min(MAX_BINS, max(3, n // MIN_BIN_SIZE))
    bin_size = n / n_bins

    bin_centers: list[float] = []
    bin_hit_rates: list[float] = []

    for i in range(n_bins):
        start = int(i * bin_size)
        end   = int((i + 1) * bin_size) if i < n_bins - 1 else n
        chunk_x = xs[start:end]
        chunk_y = ys[start:end]
        if not chunk_x:
            continue
        bin_centers.append(sum(chunk_x) / len(chunk_x))
        bin_hit_rates.append(sum(chunk_y) / len(chunk_y))

    # Apply PAVA — enforce monotonicity
    calibrated_rates = _isotonic_regression_1d(bin_hit_rates)
    breakpoints      = [
        [round(bin_centers[i], 4), round(calibrated_rates[i], 4)]
        for i in range(len(bin_centers))
    ]

    # Brier score — calibrated
    cal_brier = sum(
        (_apply_breakpoints(breakpoints, x) - y) ** 2
        for x, y in zip(xs, ys)
    ) / n

    # Brier score — raw (uncalibrated baseline)
    # Normalize raw score to [0,1] for a fair comparison
    score_max = max(xs) if max(xs) > 0 else 1.0
    raw_brier = sum(
        (x / score_max - y) ** 2
        for x, y in zip(xs, ys)
    ) / n

    # Persist to DB
    now = datetime.utcnow()
    bp_json = json.dumps(breakpoints)

    existing = (
        db.query(ConfidenceCalibration)
        .filter_by(league_code=league_code)
        .first()
    )
    if existing:
        existing.n_samples        = n
        existing.brier_score      = round(cal_brier, 6)
        existing.raw_brier        = round(raw_brier, 6)
        existing.breakpoints_json = bp_json
        existing.fitted_at        = now
    else:
        db.add(ConfidenceCalibration(
            league_code       = league_code,
            n_samples         = n,
            brier_score       = round(cal_brier, 6),
            raw_brier         = round(raw_brier, 6),
            breakpoints_json  = bp_json,
            fitted_at         = now,
        ))
    db.commit()

    print(
        f"[confidence_calibrator] Fitted {league_code or 'global'}: "
        f"n={n}, bins={len(breakpoints)}, "
        f"brier {round(raw_brier,4)} → {round(cal_brier,4)} "
        f"(Δ={round(raw_brier - cal_brier, 4):+.4f})"
    )

    return {
        "success":      True,
        "league_code":  league_code or "global",
        "n_samples":    n,
        "n_bins":       len(breakpoints),
        "brier_score":  round(cal_brier, 6),
        "raw_brier":    round(raw_brier, 6),
        "improvement":  round(raw_brier - cal_brier, 6),
        "breakpoints":  breakpoints,
        "fitted_at":    now.isoformat(),
    }


# ── Calibration application ───────────────────────────────────────────────────

def _apply_breakpoints(breakpoints: list[list], raw_score: float) -> float:
    """
    Interpolate calibrated probability from breakpoints.

    Uses linear interpolation between the two surrounding breakpoints.
    Clamps to the first/last breakpoint value outside the range.
    """
    if not breakpoints:
        return 0.5  # no data — neutral

    if raw_score <= breakpoints[0][0]:
        return breakpoints[0][1]

    if raw_score >= breakpoints[-1][0]:
        return breakpoints[-1][1]

    for i in range(len(breakpoints) - 1):
        x0, y0 = breakpoints[i]
        x1, y1 = breakpoints[i + 1]
        if x0 <= raw_score <= x1:
            t = (raw_score - x0) / (x1 - x0) if x1 != x0 else 0.0
            return y0 + t * (y1 - y0)

    return breakpoints[-1][1]


def calibrate_confidence(
    db: Session,
    raw_score: float,
    league_code: Optional[str] = None,
) -> float:
    """
    Convert a raw ATHENA confidence_score to a calibrated probability.

    Lookup order:
      1. League-specific calibration (if league_code is provided)
      2. Global calibration (across all leagues)
      3. Fallback: raw_score normalised to [0, 1] (no calibration available)

    Args:
        db:           SQLAlchemy session.
        raw_score:    Raw confidence_score from evaluate_athena.
        league_code:  League for lookup priority.

    Returns:
        Calibrated probability in [0.0, 1.0].
        Represents the estimated true hit rate for predictions at this score.
    """
    # 1. League-specific
    if league_code:
        cal = (
            db.query(ConfidenceCalibration)
            .filter_by(league_code=league_code)
            .first()
        )
        if cal and cal.breakpoints_json:
            try:
                bp = json.loads(cal.breakpoints_json)
                return round(_apply_breakpoints(bp, raw_score), 4)
            except Exception:
                pass

    # 2. Global
    cal = (
        db.query(ConfidenceCalibration)
        .filter(ConfidenceCalibration.league_code.is_(None))
        .first()
    )
    if cal and cal.breakpoints_json:
        try:
            bp = json.loads(cal.breakpoints_json)
            return round(_apply_breakpoints(bp, raw_score), 4)
        except Exception:
            pass

    # 3. Fallback — normalise raw score (safe for any scale)
    # If max possible score is ~100, divide by 100; if ~1.0, use as-is.
    normalized = raw_score / 100.0 if raw_score > 1.0 else raw_score
    return round(min(1.0, max(0.0, normalized)), 4)


def calibration_status(db: Session) -> list[dict]:
    """
    Return a summary of all stored calibrations — useful for the admin panel
    or a monitoring endpoint.
    """
    rows = (
        db.query(ConfidenceCalibration)
        .order_by(ConfidenceCalibration.fitted_at.desc())
        .all()
    )
    return [
        {
            "league_code":  r.league_code or "global",
            "n_samples":    r.n_samples,
            "brier_score":  r.brier_score,
            "raw_brier":    r.raw_brier,
            "improvement":  round((r.raw_brier or 0) - (r.brier_score or 0), 6),
            "fitted_at":    r.fitted_at.isoformat() if r.fitted_at else None,
        }
        for r in rows
    ]
