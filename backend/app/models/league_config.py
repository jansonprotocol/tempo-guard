# backend/app/models/league_config.py
from sqlalchemy import Column, Integer, String, Float, Boolean
from app.database.base import Base


class LeagueConfig(Base):
    __tablename__ = "league_configs"

    id              = Column(Integer, primary_key=True, index=True)
    league_code     = Column(String,  unique=True, nullable=False)
    base_over_bias  = Column(Float,   default=0.0)
    base_under_bias = Column(Float,   default=0.0)
    tempo_factor    = Column(Float,   default=1.0)
    safety_mode     = Column(Boolean, default=True)
    aggression_level= Column(Float,   default=0.5)
    volatility      = Column(Float,   default=0.5)
    description     = Column(String,  default="")

    # Display / UI
    display_name    = Column(String,  default="")
    country_code    = Column(String,  default="")

    # ── DEG / DET / EPS sensitivity multipliers ──────────────────────
    # Applied to raw feature values before they enter the pipeline.
    # 1.0 = neutral (no amplification/dampening).
    # > 1.0 = amplify the signal for this league.
    # < 1.0 = dampen the signal for this league.
    #
    # deg_sensitivity: how strongly structural decline pressure is felt
    #   e.g. ITA-SA = 1.4 → DEG pressure 40% stronger than baseline
    #
    # det_sensitivity: how strongly volatility/burst signals are amplified
    #   e.g. BRA-SA = 1.6 → DET chaos escalation amplified for this league
    #
    # eps_sensitivity: how aggressively the EPS ceiling taper is applied
    #   e.g. TUR-SL = 1.3 → unstable phases cause a larger ceiling reduction
    #
    # Calibration writes these from miss-pattern analysis automatically.
    # Manual overrides are also valid — use admin panel or direct DB edit.
    deg_sensitivity = Column(Float, default=1.0)
    det_sensitivity = Column(Float, default=1.0)
    eps_sensitivity = Column(Float, default=1.0)

    # ── v2.x: Form delta sensitivity multiplier ──────────────────────
    # Adjusts how strongly the form delta (over/underperformance vs expected)
    # influences the prediction. 0.0 = no effect, >0.0 amplifies the signal.
    # Calibration will suggest updates based on miss patterns.
    form_delta_sensitivity = Column(Float, default=0.0)

    # ── v2.0: Cross-league strength coefficient ──────────────────────
    # Multiplier applied to player power scores when comparing teams
    # across different leagues (UCL/UEL/UECL predictions).
    #
    # Seeded from UEFA country coefficients, normalised to 0.70–1.30 range.
    # 1.0 = neutral (default for all leagues until manually seeded).
    #
    # Example usage:
    #   ENG-PL  = 1.25 → PL player with power 75 treated as 93.75 globally
    #   NED-ERE = 0.90 → Eredivisie player with power 75 treated as 67.5
    #
    # Only used when both teams come from different leagues (intl matches).
    # For domestic predictions, this field has no effect.
    strength_coefficient = Column(Float, default=1.0)

    # ── v2.2: Alt-lane TT threshold tuning ───────────────────────────
    alt_flip_threshold = Column(Float, default=0.62)
    tt_home_bias       = Column(Float, default=0.0)

    # ── v2.2: Alt-lane suppression ───────────────────────────────────
    # use_alt_market: if False, bypass TT/flip substitution entirely.
    # Calibration sets this when original market consistently outperforms
    # the alt on missed predictions (substitution actively hurts).
    #
    # alt_min_original_win_rate: if (alt_miss AND original_hit) / total_alt_misses
    # exceeds this, calibration flags the league for suppression. Default 0.70.
    use_alt_market            = Column(Boolean, default=True)
    alt_min_original_win_rate = Column(Float,   default=0.70)

    # ── v2.2: Per-side TT weakness flags ─────────────────────────────
    # Set by calibration when a TT side consistently hits <65%.
    # When True, that side falls back to original market instead.
    tt_home_weak = Column(Boolean, default=False)
    tt_away_weak = Column(Boolean, default=False)

    # ── v2.2: TT confidence gate ─────────────────────────────────────
    tt_confidence_min = Column(Float, default=0.62)

    # ── v2.3: Per-league confidence shaping ─────────────────────────
    # confidence_scale: multiplier on delta contribution to confidence score.
    #   Default 1.0 = neutral. Calibration raises this for leagues where
    #   signals are naturally compressed (e.g. FRA-L1) so genuine picks
    #   clear the tt_confidence_min gate without lowering the gate itself.
    #   Range: 0.5–2.5. A scale of 1.4 lifts a delta=0.30 pick from
    #   confidence 0.675 to 0.705 — enough to clear a 0.70 gate.
    #
    # confidence_floor: per-league baseline before delta is added.
    #   Default 0.60. Raising this for proven leagues lifts all picks
    #   uniformly. Works alongside confidence_scale.
    confidence_scale = Column(Float, default=1.0)
    confidence_floor = Column(Float, default=0.60)

    # ── v2.2: Consecutive suppression counter ────────────────────────
    # Increments each run where original beats alt by >1pp.
    # Resets to 0 when alt catches up. Suppression fires at 3.
    orig_ahead_runs = Column(Integer, default=0)

    # ── v2.2: Minimum confidence to serve ANY prediction ─────────────
    # Picks below this score are skipped entirely — no TT, flip, or original.
    # Default 0.0 = no gate (all picks served). Calibration tunes this up
    # when the low-confidence band consistently underperforms across all markets.
    min_confidence = Column(Float, default=0.0)
