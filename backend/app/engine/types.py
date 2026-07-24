# backend/app/engine/types.py
from __future__ import annotations

from datetime import date
from typing import List, Optional

from pydantic import BaseModel


class MatchRequest(BaseModel):
    # ── Identity ──────────────────────────────────────────────────────
    league_code: str
    home_team:   str
    away_team:   str
    match_date:  date

    # ── Core features (from fbref_base.asof_features) ─────────────────
    sot_proj_total:         Optional[float] = None
    support_idx_over_delta: Optional[float] = None
    p_two_plus:             Optional[float] = None
    p_home_tt05:            Optional[float] = None
    p_away_tt05:            Optional[float] = None
    tempo_index:            Optional[float] = None

    # ── DEG/DET/EPS features (new) ────────────────────────────────────
    # deg_pressure  : structural decline signal [0.0, 1.0]
    #   High → both teams' recent form shows scoring drop / defensive erosion
    #   Feeds into DEG module → applies negative pressure on Over projections
    deg_pressure:   Optional[float] = None

    # det_boost  : combined volatility signal [0.0, 1.0]
    #   High → both teams show high variance, high-scoring rate, btts rate
    #   Feeds into DET module → expands Over corridors
    det_boost:      Optional[float] = None

    # home_det / away_det : per-team DET scores for bilateral chaos check
    #   Both required by BILATERAL_CHAOS_ESCALATOR
    home_det:       Optional[float] = None
    away_det:       Optional[float] = None

    # eps_stability : league-level phase consistency [0.0, 1.0]
    #   High → stable phases, low variance in goal totals
    #   Low  → erratic phases → EPS tapers over ceiling
    eps_stability:  Optional[float] = None


class Corridor(BaseModel):
    low:  float
    high: float
    lean: str


class TranslatedPlay(BaseModel):
    market:     str
    confidence: str


class Prediction(BaseModel):
    """
    ATHENA prediction result.

    Confidence is reported two complementary ways:
      - ``confidence_score``  : float in [0, 1] — the engine's raw internal
                                signal strength. NOT a probability.
      - ``translated_play.confidence`` : coarse LOW/MEDIUM/HIGH band tied to the
                                selected Asian line.
    For a true, calibrated hit probability, callers use
    ``confidence_calibrator.calibrate_confidence`` (surfaced as
    ``calibrated_probability`` at the API layer).

    ``rationale`` is a plain-language, user-facing summary derived from
    ``applied_modules`` — see ``app.engine.rationale.humanize``. ``explanations``
    remains the raw developer-facing signal trace.
    """
    # Bump when the response shape changes in a backward-incompatible way.
    schema_version:   str = "2.3"

    league_code:      str
    fixture:          str
    corridor:         Corridor
    translated_play:  TranslatedPlay
    confidence_score: float
    applied_modules:  List[str]
    safety_flags:     List[str]
    explanations:     List[str]

    # User-facing plain-language rationale (O2). Populated by the engine.
    rationale:        List[str] = []

    # Weather context (D7). Populated by predict_match when weather is applied;
    # None when weather was not evaluated (e.g. unknown stadium or historical sim).
    weather_tag:      Optional[str]   = None
    weather_impact:   Optional[float] = None
