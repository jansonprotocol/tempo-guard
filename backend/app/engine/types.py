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


class ModuleFlags(BaseModel):
    """
    Per-module on/off switches.

    Two purposes:
      1. Ablation — disable one module at a time and measure what it is
         actually worth, rather than assuming every module earns its place.
      2. Calibration — these are far higher-leverage dials than the bias and
         tempo factors, because a module toggle changes the market selection
         outright rather than nudging a score by a few hundredths.

    DEFAULTS REFLECT MEASURED CONTRIBUTION
    ======================================
    Every module was ablated across nine leagues and ~2,900 replayed matches
    (`app.ablate`). Modules that cost accuracy, or that never changed a single
    prediction, are off by default. Figures are mean contribution to hit rate,
    where positive means "disabling this makes results worse":

        ulr              +0.24%   fires on 59 predictions, helps 2 leagues
        deg              +0.03%   marginal but positive
        mfr              +0.03%   marginal but positive
        under_guard       0.00%   net neutral, but shapes 596 predictions and
                                  is the engine's only route to Under markets
        gate_b            0.00%   INERT — changed 0 predictions
        eps               0.00%   INERT — only moves the corridor ceiling,
                                  never the selected market
        bilateral         0.00%   INERT — same, ceiling only
        det              -0.45%   costs accuracy in 4 leagues
        burst_sentinel   -1.91%   costs accuracy in ALL NINE leagues

    Disabling the five non-earners together is worth +2.4% in-sample and
    +1.5% on a chronological holdout (7 of 9 leagues improve).

    The code behind the disabled modules is kept rather than deleted: their
    inputs (volatility, phase stability) are exactly the signals that richer
    data such as xG would make meaningful, and a per-league config can switch
    any of them back on. NED-ED, for instance, measurably prefers
    burst_sentinel enabled.
    """
    burst_sentinel:   bool = False   # -1.91%: hurt every league tested
    gate_b:           bool = False   # inert: never changed a prediction
    ulr:              bool = True    # +0.24%
    under_guard:      bool = True    # neutral, but the only Under pathway
    deg:              bool = True    # +0.03%
    det:              bool = False   # -0.45%
    eps:              bool = False   # inert: corridor-only, never the market
    mfr:              bool = True    # +0.03%
    bilateral:        bool = False   # inert: corridor-only, never the market

    def disabled(self) -> list[str]:
        return [k for k, v in self.model_dump().items() if not v]

    @classmethod
    def all_on(cls) -> "ModuleFlags":
        """The pre-prune engine — used by ablation to establish a baseline."""
        return cls(**{k: True for k in cls.model_fields})


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
