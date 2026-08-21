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

    # Expected total goals for this fixture, and the league's own recent
    # mean. tempo_index is a clipped rescaling of mu_total and loses the
    # extremes; the market selector needs the unclipped value and something
    # to compare it against.
    mu_total:               Optional[float] = None
    league_mu:              Optional[float] = None

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
    # Pick the market from the Poisson model by edge over a typical fixture,
    # instead of the threshold flowchart in translate_play. On by measurement:
    # 80.57% strike and +1.15% edge against the flowchart's 79.66% and +0.72%,
    # with 21 of 32 leagues clearing 80% rather than 14.
    prob_select: bool = True
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


class Lanes(BaseModel):
    """
    Two plays for the same fixture, at different sharpness.

    `safe` is the conservative call — a cushioned line that wins on most
    plausible scorelines. `sharp` is the same read taken further up the Asian
    ladder, and is only offered when the fixture sits far enough from its own
    league's scoring norm to justify it.

    Sharpness is league-relative on purpose. A 2.4-goal expectation is an
    ordinary afternoon in Italy or Argentina and a notably quiet one in Germany
    or the Netherlands, so the trigger compares a fixture against its own
    league's distribution rather than a global constant.

    Under the full-win grading convention several ladder rungs are equivalent —
    O2.25/O2.5/O2.75 all require three goals, U3.25/U3.5/U3.75 all require the
    total to stay under four. `sharp_tier` names that win condition so the
    choice of rung, which only changes the price, stays with the bettor.
    """
    safe:        TranslatedPlay
    sharp:       Optional[TranslatedPlay] = None
    sharp_tier:  Optional[str] = None      # e.g. "3+ goals", "under 3 goals"
    sharp_reason: Optional[str] = None     # why the sharper line was offered
    league_z:    Optional[float] = None    # fixture vs league norm, in sigma


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

    # Why the market was picked, when the probability selector chose it.
    # Carried as numbers rather than left in a formatted note so the
    # explanation layer states the real reason instead of reconstructing it.
    pick_win_prob:    Optional[float] = None
    pick_edge:        Optional[float] = None

    # Safe and sharp plays for this fixture. `translated_play` remains the safe
    # call so every existing caller keeps its meaning.
    lanes:            Optional[Lanes] = None

    # Weather context (D7). Populated by predict_match when weather is applied;
    # None when weather was not evaluated (e.g. unknown stadium or historical sim).
    weather_tag:      Optional[str]   = None
    weather_impact:   Optional[float] = None

    # Readable description of who is playing, computed as of match day. See
    # app.data.tags: these explain a tip, they do not produce one. Populated
    # only on single-match paths — a replay of thousands of fixtures does not
    # pay to build them, and nothing downstream reads them.
    home_tags:        List[str] = []
    away_tags:        List[str] = []
