# backend/app/engine/pipeline.py
from __future__ import annotations

from math import isfinite
from typing import List, Tuple

from app.engine.types import MatchRequest, Prediction, Corridor, TranslatedPlay

# ── Core constants ────────────────────────────────────────────────────────────
ROUNDING   = 0.1
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


# ── Corridor builder ──────────────────────────────────────────────────────────

def build_corridor(
    lean: str,
    tempo: float,
    p2p: float,
    ulr_on: bool,
    burst_on: bool,
    under_guard: str,
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
        # O2.25: medium signal — conf ≥ 0.68 or p2p ≥ 0.75
        if conf >= 0.68 or p2p_r >= 0.75:
            notes.append("Medium over signal → O2.25 (half win at 2 goals).")
            return TranslatedPlay(market="O2.25", confidence="MEDIUM")
        # O1.75: weak signal — safe floor, wins at 2+
        notes.append("Weak over signal → O1.75 (safe floor, wins at 2+).")
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

    # Apply league calibration adjustments
    # over_bias/under_bias range: 0.00–0.25, neutral = 0.05 each
    # tempo_factor: 0.5 = neutral, >0.5 amplifies, <0.5 dampens
    # Capped at 0.95 to prevent artificial BurstSentinel triggers
    tempo         = max(0.0, min(0.95, raw_tempo * tempo_factor * 2.0))
    support_delta = raw_support + (league_bias_over - league_bias_under)

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

    # ── Lean scoring ─────────────────────────────────────────────────
    # Balanced formula — tempo contribution is halved to stop BRA-SA
    # from always swamping the under signal.
    #
    # Over score:  support_delta + tempo/2 contribution
    # Under score: p2p inversion + (1-tempo) contribution
    # Both are symmetric around 0.35 neutral tempo.

    over_score  = _r(support_delta + (tempo - 0.5) * 0.30)
    under_score = _r((0.72 - p2p) * 0.50 + (0.5 - tempo) * 0.30)

    # Module adjustments
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

    over_score  = _r(over_score)
    under_score = _r(under_score)
    delta       = abs(over_score - under_score)

    notes.append(
        f"Lean scores: over={over_score} under={under_score} "
        f"delta={round(delta,2)} tempo={round(tempo,2)} "
        f"p2p={round(p2p,3)} sd={_r(support_delta)}"
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
        final_lean, tempo, p2p, ulr_on, burst_on, under_guard
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
