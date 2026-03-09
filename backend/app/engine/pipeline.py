# backend/app/engine/pipeline.py
from __future__ import annotations

from math import isfinite
from typing import List, Tuple

from app.engine.types import MatchRequest, Prediction, Corridor, TranslatedPlay

# ── Core constants ────────────────────────────────────────────────────────────
ROUNDING   = 0.01
HYSTERESIS = 1.0

# BurstSentinel — raised thresholds so it doesn't fire on every high-tempo match
BURST_MIN_SUPPORT = 0.12   # was 0.10 — raised to prevent borderline triggers after rounding
BURST_MIN_P2P     = 0.80   # was 0.74
BURST_MIN_TEMPO   = 0.60   # was 0.55

# O2.5 add-on gates (unchanged — strict is correct here)
ADDON_O25_SUPPORT_DELTA   = 0.07
ADDON_MIN_SOT             = 10.5
ADDON_MIN_CORRIDOR_WIDTH  = 2.1
ADDON_MIN_TT05            = 0.60

# Under lean triggers (new)
UNDER_P2P_HARD   = 0.62   # p_two_plus below this → strong under signal
UNDER_P2P_SOFT   = 0.70   # p_two_plus below this → weak under signal
UNDER_DELTA_HARD = -0.04  # support_delta below this → under signal

# DEG thresholds — structural decline must be meaningful before applying pressure
DEG_TRIGGER      = 0.15   # deg_pressure below this → no effect
DEG_MAX_PRESSURE = 0.08   # max negative shift on over_score (≈ 1.8pp in doc terms)

# DET thresholds — volatility must be above baseline noise to expand corridors
DET_TRIGGER      = 0.30   # det_boost below this → no effect
DET_MAX_BOOST    = 0.06   # max positive shift on over_score

# EPS thresholds — instability must be clear before tapering ceiling
EPS_STABLE       = 0.65   # eps above this → no adjustment
EPS_MAX_TAPER    = 0.50   # max ceiling reduction (goals)

# MFR thresholds
MFR_SOFT_SD_LOW  = 0.02   # support_delta lower bound for soft trigger
MFR_SOFT_SD_HIGH = 0.07   # support_delta upper bound for soft trigger
MFR_SOFT_T_LOW   = 0.40   # tempo lower bound for soft trigger
MFR_SOFT_T_HIGH  = 0.65   # tempo upper bound for soft trigger
MFR_LIFT_SD_MIN  = 0.08   # support_delta minimum for TO_LIFT trigger

# BILATERAL thresholds — both teams must show meaningful DET to escalate
BILATERAL_MIN_DET = 0.45  # per-team det threshold
BILATERAL_MAX_EXP = 0.50  # max ceiling expansion (goals)


def _r(x: float) -> float:
    if x is None or not isfinite(x):
        return 0.0
    return round(x / ROUNDING) * ROUNDING


# ── Modules ───────────────────────────────────────────────────────────────────

def inline_veto(qlty_ok: bool, notes: List[str], modules: List[str]) -> bool:
    if not qlty_ok:
        modules.append("InlineVeto")
        notes.append("Inline Veto: data incomplete → default to Under corridor.")
        return True
    return False


def burst_sentinel(
    support_delta: float, p2p: float, tempo: float,
    notes: List[str], modules: List[str],
) -> bool:
    """
    Chaos/warm profile — forces Over floor.
    Raised thresholds vs v1 to prevent firing on every high-tempo match.
    ALL three conditions must be strong, not just present.
    """
    sd, p2p_r, t = _r(support_delta), _r(p2p), _r(tempo)
    cond = (sd >= BURST_MIN_SUPPORT) and (p2p_r >= BURST_MIN_P2P) and (t >= BURST_MIN_TEMPO)
    if cond:
        modules.append("BurstSentinel_FORCED_OVER")
        notes.append(
            f"BurstSentinel: support_delta={sd}, p2p={p2p_r}, tempo={t} "
            f"→ chaos profile → Over floor unlocked."
        )
    return cond


def gate_b(
    tempo: float, support_delta: float,
    notes: List[str], modules: List[str],
) -> bool:
    """Hard block: very low tempo + weak support → no over exposure."""
    t, sd = _r(tempo), _r(support_delta)
    hard_block = (t <= 0.35) and (sd <= 0.02)
    if hard_block:
        modules.append("GateB_HardBlock")
        notes.append("Gate-B: Over blocked (very low tempo & weak support).")
    return hard_block


def ulr_low_tempo(tempo: float, notes: List[str], modules: List[str]) -> bool:
    """Under lean rider: low tempo → ceiling favored."""
    t = _r(tempo)
    trigger = t <= 0.40
    if trigger:
        modules.append("ULR_v1.3.1_LT")
        notes.append("ULR: Low tempo → Under ceiling favored.")
    return trigger


def under_p2p_guard(
    p2p: float, support_delta: float,
    notes: List[str], modules: List[str],
) -> str:
    """
    NEW — p_two_plus-based under trigger.
    Fills the gap where tempo is high but goal expectation is actually low.

    Returns: "hard", "soft", or "none"
    """
    p2p_r = _r(p2p)
    sd    = _r(support_delta)

    if p2p_r <= UNDER_P2P_HARD:
        modules.append("UnderGuard_HARD")
        notes.append(
            f"UnderGuard: p_two_plus={p2p_r} ≤ {UNDER_P2P_HARD} "
            f"→ strong under signal regardless of tempo."
        )
        return "hard"

    if p2p_r <= UNDER_P2P_SOFT and sd <= UNDER_DELTA_HARD:
        modules.append("UnderGuard_SOFT")
        notes.append(
            f"UnderGuard: p_two_plus={p2p_r} ≤ {UNDER_P2P_SOFT} "
            f"and support_delta={sd} ≤ {UNDER_DELTA_HARD} → soft under signal."
        )
        return "soft"

    return "none"


def ceiling_cushion(apply: bool, notes: List[str], modules: List[str]) -> None:
    if apply:
        modules.append("CeilingCushion_ON")
        notes.append("Ceiling Cushion: prefer U3.5/4.5 over naked U3.5.")


def s_lock(
    prev_lean: str, new_lean: str, delta: float,
    notes: List[str], modules: List[str],
) -> str:
    if prev_lean and prev_lean != new_lean and delta < HYSTERESIS:
        modules.append("S-LOCK_Hysteresis")
        notes.append(
            f"S-LOCK: prevented flip {prev_lean}→{new_lean} "
            f"(Δ={delta:.2f}<{HYSTERESIS})."
        )
        return prev_lean
    return new_lean


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


# ── DEG/DET/EPS/MFR/BILATERAL modules ────────────────────────────────────────

def deg_degradation(
    deg_pressure: float,
    notes: List[str], modules: List[str],
) -> float:
    """
    DEG — Structural Degradation Model.

    Detects scoring decline + defensive erosion across both teams' recent form.
    Applies negative pressure on over_score to suppress Over projections.

    Returns: negative adjustment to over_score (0.0 to -DEG_MAX_PRESSURE).
    Cannot override Inline Veto or BurstSentinel.
    Adjustment window: up to ±1.8pp per spec (implemented as ±0.08 score units).
    """
    dp = _r(deg_pressure)
    if dp <= DEG_TRIGGER:
        return 0.0

    # Scale: DEG_TRIGGER (0.15) → 0 pressure, 1.0 → full -DEG_MAX_PRESSURE
    raw      = (dp - DEG_TRIGGER) / (1.0 - DEG_TRIGGER)
    pressure = round(_clip(raw * DEG_MAX_PRESSURE, 0.0, DEG_MAX_PRESSURE), 4)

    modules.append("DEG_Degradation")
    notes.append(
        f"DEG: structural decline pressure={dp} "
        f"→ over_score -{pressure} (defensive erosion/scoring drop detected)."
    )
    return -pressure


def det_detonation(
    det_boost: float,
    burst_on: bool,
    notes: List[str], modules: List[str],
) -> float:
    """
    DET — Detonation Model.

    Detects high variance, transition speed, and chaos probability.
    Expands Over corridors by boosting over_score.

    BurstSentinel supersedes DET — if burst already fired, DET adds nothing
    (BurstSentinel has already handled the Over path fully).

    Returns: positive adjustment to over_score (0.0 to +DET_MAX_BOOST).
    """
    if burst_on:
        return 0.0  # BurstSentinel already owns the Over path

    db = _r(det_boost)
    if db <= DET_TRIGGER:
        return 0.0

    raw   = (db - DET_TRIGGER) / (1.0 - DET_TRIGGER)
    boost = round(_clip(raw * DET_MAX_BOOST, 0.0, DET_MAX_BOOST), 4)

    modules.append("DET_Detonation")
    notes.append(
        f"DET: volatility burst det={db} "
        f"→ over_score +{boost} (high variance/BTTS/3+ goal rate detected)."
    )
    return boost


def eps_phase_stability(
    eps_stability: float,
    burst_on: bool,
    notes: List[str], modules: List[str],
) -> float:
    """
    EPS — Expected Phase Stability.

    Low EPS (erratic phase distribution) → taper the corridor ceiling.
    Supports Under 4.5 by preventing overconfident high-ceiling projections
    when goal distribution across the league is inconsistent.

    BurstSentinel_FORCED_OVER blocks EPS ceiling reduction — when chaos
    is already forced-over, tapering the ceiling is contradictory.

    Returns: ceiling delta (negative or 0.0, max reduction = -EPS_MAX_TAPER).
    """
    if burst_on:
        return 0.0  # BurstSentinel controls ceiling in chaos mode

    eps = _r(eps_stability)
    if eps >= EPS_STABLE:
        return 0.0  # stable phases — no adjustment needed

    # Scale: EPS_STABLE → 0 taper, 0.0 → full -EPS_MAX_TAPER
    raw   = (EPS_STABLE - eps) / EPS_STABLE
    taper = round(_clip(raw * EPS_MAX_TAPER, 0.0, EPS_MAX_TAPER), 2)

    modules.append("EPS_PhaseStability")
    notes.append(
        f"EPS: phase instability (stability={eps}, threshold={EPS_STABLE}) "
        f"→ ceiling -{taper} (late drift / staggered tempo detected)."
    )
    return -taper


def mfr_soft(
    support_delta: float, tempo: float,
    gateb_block: bool,
    notes: List[str], modules: List[str],
) -> float:
    """
    MFR v1.0 Soft — Momentum Flow Regulator.

    Detects moderate momentum imbalance (not chaos).
    Applies gentle +0.03 to +0.05 boost on over_score.
    Supports Over 1.5 and can lift from Under-lean to Over floor.

    Cannot override Gate-B hard blocks.
    Does NOT activate when support_delta is in strong territory (≥ MFR_LIFT_SD_MIN)
    — that zone belongs to MFR_TO_LIFT.

    Returns: positive adjustment to over_score (0.0 to +0.05).
    """
    if gateb_block:
        return 0.0

    sd = _r(support_delta)
    t  = _r(tempo)

    in_mfr_zone = (MFR_SOFT_SD_LOW <= sd < MFR_LIFT_SD_MIN) and \
                  (MFR_SOFT_T_LOW  <= t <= MFR_SOFT_T_HIGH)

    if not in_mfr_zone:
        return 0.0

    # Scale within zone: +0.03 at lower bound, +0.05 at upper bound
    sd_progress = (sd - MFR_SOFT_SD_LOW) / (MFR_LIFT_SD_MIN - MFR_SOFT_SD_LOW)
    boost       = round(_clip(0.03 + sd_progress * 0.02, 0.03, 0.05), 4)

    modules.append("MFR_Soft")
    notes.append(
        f"MFR Soft: moderate momentum sd={sd} tempo={t} "
        f"→ over_score +{boost} (momentum flow supports Over floor)."
    )
    return boost


def mfr_to_lift(
    support_delta: float,
    gateb_block: bool,
    notes: List[str], modules: List[str],
) -> float:
    """
    MFR_TO_LIFT — Tempo Override Lift (advanced MFR variant).

    Activates when SupportIdxΔ_Over is strong (≥ MFR_LIFT_SD_MIN = 0.08).
    Provides a stronger over_score lift (+0.06 to +0.08).
    Can elevate Over 2.5 path if Gate-B is relaxed.

    Cannot override Gate-B hard blocks.

    Returns: positive adjustment to over_score (0.0 to +0.08).
    """
    if gateb_block:
        return 0.0

    sd = _r(support_delta)
    if sd < MFR_LIFT_SD_MIN:
        return 0.0

    # Scale: 0.08 → +0.06, 0.15 → +0.08
    raw   = (sd - MFR_LIFT_SD_MIN) / (0.15 - MFR_LIFT_SD_MIN)
    boost = round(_clip(0.06 + raw * 0.02, 0.06, 0.08), 4)

    modules.append("MFR_TO_LIFT")
    notes.append(
        f"MFR_TO_LIFT: strong momentum sd={sd} "
        f"→ over_score +{boost} (SupportIdxΔ strong — Over 2.5 pathway open)."
    )
    return boost


def bilateral_chaos_escalator(
    home_det: float, away_det: float,
    burst_on: bool,
    notes: List[str], modules: List[str],
) -> float:
    """
    BILATERAL_CHAOS_ESCALATOR v1.0

    Detects mutual volatility escalation — both teams must be independently volatile.
    Amplifies corridor width by expanding the ceiling.
    Promotes U2.5–U5.0 and expands Over 1.5 → O1.5–O3.5 corridor range.

    Requires BOTH home_det and away_det ≥ BILATERAL_MIN_DET (0.45).
    Strength is determined by min(home_det, away_det) — weakest link governs.

    BurstSentinel supersedes: if burst already fired, corridor is already maxed.

    Returns: positive ceiling delta (0.0 to +BILATERAL_MAX_EXP).
    """
    if burst_on:
        return 0.0  # BurstSentinel already handles burst-prone fixtures

    hd = _r(home_det)
    ad = _r(away_det)

    if hd < BILATERAL_MIN_DET or ad < BILATERAL_MIN_DET:
        return 0.0  # one or both teams not volatile enough

    bilateral_strength = min(hd, ad)
    raw      = (bilateral_strength - BILATERAL_MIN_DET) / (1.0 - BILATERAL_MIN_DET)
    expansion = round(_clip(raw * BILATERAL_MAX_EXP, 0.0, BILATERAL_MAX_EXP), 2)

    modules.append("BILATERAL_CHAOS_ESCALATOR")
    notes.append(
        f"BilateralChaos: home_det={hd} away_det={ad} (both ≥ {BILATERAL_MIN_DET}) "
        f"→ corridor ceiling +{expansion} (mutual volatility escalation)."
    )
    return expansion

def build_corridor(
    lean: str,
    tempo: float,
    p2p: float,
    ulr_on: bool,
    burst_on: bool,
    under_guard: str,
    eps_taper: float = 0.0,
    bilateral_expansion: float = 0.0,
) -> Tuple[float, float]:
    """
    Build low/high corridor bounds.

    Low bound:
      - Under lean or hard under guard → raise floor (1.5 stays safe baseline)
      - Over burst → keep 1.5

    High bound:
      - Driven by tempo and p2p together
      - High tempo + high p2p → wider  (up to 5.0)
      - Low p2p even with high tempo → tighten ceiling
      - Under guard hard → 3.5 ceiling
      - Under guard soft → 4.0 ceiling

    Post-module adjustments (applied after base corridor):
      eps_taper           : negative float, tapers ceiling (EPS instability)
      bilateral_expansion : positive float, expands ceiling (BILATERAL_CHAOS)
    """
    low = 1.5  # always safe floor

    # High bound — tempo sets the base
    if tempo >= 0.70:
        high = 5.0
    elif tempo >= 0.55:
        high = 4.5
    elif tempo <= 0.35:
        high = 4.0
    else:
        high = 4.5

    # p2p tightens the ceiling when goal expectation is low
    if p2p <= 0.65:
        high = min(high, 3.5)
    elif p2p <= 0.72:
        high = min(high, 4.0)

    # Under guard overrides
    if under_guard == "hard":
        high = min(high, 3.5)
    elif under_guard == "soft":
        high = min(high, 4.0)

    # Burst forces wide ceiling
    if burst_on:
        high = max(high, 5.0)

    # EPS taper (negative) — cannot reduce below 3.0
    high = max(3.0, high + eps_taper)

    # Bilateral expansion — cannot push above 5.5
    high = min(5.5, high + bilateral_expansion)

    return (low, round(high, 1))


# ── O2.5 gate ────────────────────────────────────────────────────────────────

def _o25_addon_allowed(
    support_delta: float, sot_proj_total: float, width: float,
    p_home_tt05: float, p_away_tt05: float,
) -> bool:
    """O2.5 only if ALL gates pass — strict by design."""
    return all([
        _r(support_delta)   >= ADDON_O25_SUPPORT_DELTA,
        _r(sot_proj_total)  >= ADDON_MIN_SOT,
        _r(width)           >= ADDON_MIN_CORRIDOR_WIDTH,
        _r(p_home_tt05)     >= ADDON_MIN_TT05,
        _r(p_away_tt05)     >= ADDON_MIN_TT05,
    ])


# ── Translate play ────────────────────────────────────────────────────────────

def translate_play(
    lean: str,
    corridor: Tuple[float, float],
    burst_on: bool,
    under_guard: str,
    support_delta: float,
    sot_proj_total: float,
    p_home_tt05: float,
    p_away_tt05: float,
    p2p: float,
    confidence_score: float,
    notes: List[str],
    flags: List[str],
    modules: List[str],
) -> TranslatedPlay:
    """
    Translates lean + signal strength into a specific Asian line market.

    Over lines:
      Weak   (conf < 0.65)  → O1.75  (safe, wins at 2+)
      Medium (conf < 0.78)  → O2.25  (wins at 3+, push at 2)
      Strong (conf ≥ 0.78)  → O2.5   (wins at 3+)

    Under lines:
      Hard guard / very low p2p → U3.5   (confident: max 3 goals)
      Soft guard / medium under → U3.75  (cushion: wins at max 4)
      Weak under / balanced     → U4.25  (wide cushion: wins at max 4)

    BurstSentinel always forces Over (line depends on strength).
    """
    low, high  = corridor
    width      = high - low
    p2p_r      = _r(p2p)
    conf       = confidence_score

    # ── BurstSentinel path ───────────────────────────────────────────
    if burst_on:
        if _o25_addon_allowed(support_delta, sot_proj_total, width,
                               p_home_tt05, p_away_tt05):
            notes.append("BurstSentinel + O2.5 gate → O2.5.")
            return TranslatedPlay(market="O2.5", confidence="LOW")
        notes.append("BurstSentinel → O1.5 floor.")
        return TranslatedPlay(market="O1.5", confidence="HIGH")

    # ── Hard under guard ─────────────────────────────────────────────
    if under_guard == "hard":
        ceiling_cushion(True, notes, modules)
        notes.append("UnderGuard hard + low p2p → U3.5 (tight ceiling).")
        return TranslatedPlay(market="U3.5", confidence="HIGH")

    # ── Under / balanced lean ────────────────────────────────────────
    if lean in ("under", "balanced"):
        ceiling_cushion(True, notes, modules)
        if under_guard == "soft" or p2p_r <= 0.70:
            notes.append("Under lean + soft guard → U3.75 (cushion to 4 goals).")
            return TranslatedPlay(market="U3.75", confidence="HIGH")
        notes.append("Under/balanced lean → U4.25 (wide cushion).")
        return TranslatedPlay(market="U4.25", confidence="MEDIUM")

    # ── Over lean ────────────────────────────────────────────────────
    if lean == "over":
        # Negative or zero support delta means under pressure is equal or stronger —
        # downgrade to O1.75 unless confidence is very high
        # Note: _r() rounds to 1dp so -0.007 becomes 0.0 — use <= 0 not < 0
        if _r(support_delta) <= 0 and conf < 0.72:
            notes.append(f"Support delta {_r(support_delta)} ≤ 0 → downgrade to O1.75.")
            return TranslatedPlay(market="O1.75", confidence="MEDIUM")
        # O2.5: only on very strong signal — high conf AND all addon gates
        addon_ok = _o25_addon_allowed(support_delta, sot_proj_total,
                                      width, p_home_tt05, p_away_tt05)
        notes.append(
            f"O2.5 gate: conf={round(conf,2)} (need 0.82) "
            f"addon_ok={addon_ok} sd={_r(support_delta)} sot={_r(sot_proj_total)}"
        )
        if conf >= 0.82 and addon_ok:
            notes.append("Strong over signal + all gates → O2.5.")
            return TranslatedPlay(market="O2.5", confidence="LOW")
        # O2.25: requires conf ≥ 0.78 AND p2p ≥ 0.80 AND positive support_delta
        # All three must be true — negative delta means FBref signal opposes the over call,
        # those matches belong at O1.75 regardless of p2p/confidence.
        if conf >= 0.78 and p2p_r >= 0.80 and support_delta > 0:
            notes.append("Strong over signal + positive delta → O2.25.")
            return TranslatedPlay(market="O2.25", confidence="MEDIUM")
        # O1.75: safe floor for everything else — wins at 2+
        notes.append("Over signal → O1.75 (safe floor, wins at 2+).")
        return TranslatedPlay(market="O1.75", confidence="MEDIUM")

    # ── Fallback ─────────────────────────────────────────────────────
    flags.append("SinglesOnly")
    notes.append("Fallback → U4.25.")
    return TranslatedPlay(market="U4.25", confidence="MEDIUM")


# ── Main entry ────────────────────────────────────────────────────────────────

def evaluate_athena(
    req: MatchRequest,
    league_bias_over: float,
    league_bias_under: float,
    tempo_factor: float,
    team_nudge: float = 0.0,   # combined home+away team-level calibration nudge
) -> Prediction:
    notes:   List[str] = []
    modules: List[str] = []
    flags:   List[str] = ["SinglesOnly"]

    # ── Safe input fallbacks + league bias adjustments ──────────────
    # League biases are ADDITIVE on top of computed features.
    # This means calibration apply actually shifts predictions.
    # tempo_factor scales the raw tempo signal (1.0 = neutral).
    raw_tempo     = req.tempo_index            if req.tempo_index            is not None else 0.55
    raw_support   = req.support_idx_over_delta if req.support_idx_over_delta is not None else 0.0
    sot           = req.sot_proj_total         if req.sot_proj_total         is not None else 9.0
    p2p           = req.p_two_plus             if req.p_two_plus             is not None else 0.68
    p_home_tt05   = req.p_home_tt05            if req.p_home_tt05            is not None else 0.62
    p_away_tt05   = req.p_away_tt05            if req.p_away_tt05            is not None else 0.58

    # DEG/DET/EPS/Bilateral inputs — safe fallbacks to neutral values
    # so that old predictions without these fields behave identically
    # to the pre-DEG/DET engine (no regressions on existing data).
    deg_p    = req.deg_pressure   if req.deg_pressure   is not None else 0.0
    det_b    = req.det_boost      if req.det_boost       is not None else 0.30
    h_det    = req.home_det       if req.home_det        is not None else 0.30
    a_det    = req.away_det       if req.away_det        is not None else 0.30
    eps      = req.eps_stability  if req.eps_stability   is not None else 0.65

    # Apply league calibration adjustments
    # over_bias/under_bias range: 0.00–0.13 per side, neutral = 0.05 each
    # Net shift = (over_bias - under_bias), max ±0.13
    # Typical raw_support range is ±0.15 — bias should nudge, not dominate
    # tempo_factor: 0.5 = neutral (multiplier=1.0), <0.5 dampens, >0.5 amplifies
    # Capped at 0.95 to prevent artificial BurstSentinel triggers
    tempo         = max(0.0, min(0.95, raw_tempo * tempo_factor * 2.0))
    support_delta = raw_support + (league_bias_over - league_bias_under) + team_nudge

    # BurstSentinel uses RAW values to prevent bias inflation from
    # pushing borderline matches into forced-over territory
    burst_support = raw_support
    burst_p2p     = req.p_two_plus if req.p_two_plus is not None else 0.68
    burst_tempo   = raw_tempo

    # ── Pre-lean protections ─────────────────────────────────────────
    quality_ok = True
    veto       = inline_veto(quality_ok, notes, modules)

    # ── Modules ──────────────────────────────────────────────────────
    # BurstSentinel uses raw unbiased values — prevents calibration
    # nudges from artificially triggering forced-over mode
    burst_on    = burst_sentinel(burst_support, burst_p2p, burst_tempo, notes, modules)
    gateb_block = gate_b(tempo, support_delta, notes, modules)
    ulr_on      = ulr_low_tempo(tempo, notes, modules)
    under_guard = under_p2p_guard(p2p, support_delta, notes, modules)

    # DEG/DET/EPS/MFR/BILATERAL — run after core protections
    # DEG: fail-safe respected (cannot override Inline Veto — veto zeros it implicitly
    # by forcing under lean, but we still let DEG log for transparency)
    deg_adj  = deg_degradation(deg_p, notes, modules)
    det_adj  = det_detonation(det_b, burst_on, notes, modules)
    eps_tap  = eps_phase_stability(eps, burst_on, notes, modules)
    mfr_adj  = mfr_soft(support_delta, tempo, gateb_block, notes, modules)
    lift_adj = mfr_to_lift(support_delta, gateb_block, notes, modules)
    # MFR Soft and MFR_TO_LIFT are mutually exclusive — only the stronger fires
    # mfr_to_lift threshold is higher so it replaces soft when both would trigger
    mfr_total = lift_adj if lift_adj > 0.0 else mfr_adj
    bilateral_exp = bilateral_chaos_escalator(h_det, a_det, burst_on, notes, modules)

    # ── Lean scoring ─────────────────────────────────────────────────
    # Balanced formula — tempo contribution is halved to stop BRA-SA
    # from always swamping the under signal.
    #
    # Over score:  support_delta + tempo/2 contribution
    # Under score: p2p inversion + (1-tempo) contribution
    # Both are symmetric around 0.35 neutral tempo.

    over_score  = _r(support_delta + (tempo - 0.5) * 0.30)
    under_score = _r((0.72 - p2p) * 0.50 + (0.5 - tempo) * 0.30)

    # Core module adjustments
    if burst_on:
        over_score  += 0.10
    if gateb_block or veto:
        under_score += 0.08
    if ulr_on:
        under_score += 0.05
    if under_guard == "hard":
        under_score += 0.15
    elif under_guard == "soft":
        under_score += 0.07

    # DEG/DET/MFR adjustments to lean scores
    # DEG reduces over_score (structural decline → suppress over)
    # DET increases over_score (volatility → expand over outlook)
    # MFR increases over_score (momentum → support over floor)
    # Note: DEG applies even when veto fired (for transparency), but veto's
    # under_score boost ensures under wins regardless.
    over_score += deg_adj   # negative
    over_score += det_adj   # positive
    over_score += mfr_total # positive

    over_score  = _r(over_score)
    under_score = _r(under_score)
    delta       = abs(over_score - under_score)

    notes.append(
        f"Lean scores: over={over_score} under={under_score} "
        f"delta={round(delta,2)} tempo={round(tempo,2)} "
        f"p2p={round(p2p,3)} sd={_r(support_delta)} "
        f"deg={round(deg_adj,4)} det={round(det_adj,4)} mfr={round(mfr_total,4)}"
    )

    # ── Lean decision ────────────────────────────────────────────────
    if burst_on:
        new_lean = "over"
    elif gateb_block or veto or ulr_on or under_guard == "hard":
        new_lean = "under"
    elif under_guard == "soft" and under_score >= over_score:
        new_lean = "under"
    else:
        if over_score > under_score:
            new_lean = "over"
        elif under_score > over_score:
            new_lean = "under"
        else:
            new_lean = "balanced"

    final_lean = s_lock(
        prev_lean=new_lean, new_lean=new_lean,
        delta=delta, notes=notes, modules=modules,
    )

    # ── Corridor ──────────────────────────────────────────────────────
    low, high = build_corridor(
        final_lean, tempo, p2p, ulr_on, burst_on, under_guard,
        eps_taper=eps_tap,
        bilateral_expansion=bilateral_exp,
    )

    # ── Confidence score (needed by translate_play for line selection) 
    confidence_score = min(0.95, max(0.55,
        0.60
        + (delta * 0.25)
        + (0.05 if final_lean in ("under",) else 0.0)
    ))

    # ── Translation ───────────────────────────────────────────────────
    translated = translate_play(
        final_lean, (low, high), burst_on, under_guard,
        support_delta, sot, p_home_tt05, p_away_tt05,
        p2p, confidence_score,
        notes, flags, modules,
    )

    return Prediction(
        league_code=req.league_code,
        fixture=f"{req.home_team} vs {req.away_team}",
        corridor=Corridor(low=low, high=high, lean=final_lean),
        translated_play=translated,
        confidence_score=round(confidence_score, 2),
        applied_modules=modules,
        safety_flags=flags,
        explanations=notes,
    )
