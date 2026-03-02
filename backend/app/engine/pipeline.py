from __future__ import annotations
from typing import List, Tuple
from math import isfinite
from app.engine.types import MatchRequest, Prediction, Corridor, TranslatedPlay

# ATHENA core constants (conservative defaults) [1](https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC1iMmQ2LWU3YmUtMDACLTAwCgBGAAADt%2bmUE3rpw0ejjqWOUoV9NAcALXSofLnuY06Hm4FKMynP7AAAAgEMAAAALXSofLnuY06Hm4FKMynP7AAIc9qaGwAAAA%3d%3d&exvsurl=1&viewmodel=ReadMessageItem)
ROUNDING = 0.1
HYSTERESIS = 1.0
ADDON_O25_SUPPORT_DELTA = 0.07
ADDON_MIN_SOT = 10.5
ADDON_MIN_CORRIDOR_WIDTH = 2.1
ADDON_MIN_TT05 = 0.60

def _r(x: float) -> float:
    if x is None or not isfinite(x):
        return 0.0
    return round(x / ROUNDING) * ROUNDING

def inline_veto(qlty_ok: bool, notes: List[str], modules: List[str]) -> bool:
    if not qlty_ok:
        modules.append("InlineVeto")
        notes.append("Inline Veto: data incomplete → default to Under corridor.")
        return True
    return False

def burst_sentinel(support_delta: float, p2p: float, tempo: float, notes: List[str], modules: List[str]) -> bool:
    support_delta, p2p, tempo = _r(support_delta), _r(p2p), _r(tempo)
    cond = (support_delta >= 0.08) and (p2p >= 0.74) and (tempo >= 0.55)  # [1](https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC1iMmQ2LWU3YmUtMDACLTAwCgBGAAADt%2bmUE3rpw0ejjqWOUoV9NAcALXSofLnuY06Hm4FKMynP7AAAAgEMAAAALXSofLnuY06Hm4FKMynP7AAIc9qaGwAAAA%3d%3d&exvsurl=1&viewmodel=ReadMessageItem)
    if cond:
        modules.append("BurstSentinel_FORCED_OVER")
        notes.append("BurstSentinel: chaos/warm profile → Over floor unlocked.")
    return cond

def gate_b(tempo: float, support_delta: float, notes: List[str], modules: List[str]) -> bool:
    tempo, support_delta = _r(tempo), _r(support_delta)
    hard_block = (tempo <= 0.35 and support_delta <= 0.02)
    if hard_block:
        modules.append("GateB_HardBlock")
        notes.append("Gate‑B: Over exposure blocked (low tempo & weak support).")
    return hard_block

def ulr_low_tempo(tempo: float, notes: List[str], modules: List[str]) -> bool:
    tempo = _r(tempo)
    trigger = tempo <= 0.40
    if trigger:
        modules.append("ULR_v1.3.1_LT")
        notes.append("ULR: Low tempo detected → Under ceiling favored.")
    return trigger

def ceiling_cushion(apply: bool, notes: List[str], modules: List[str]) -> None:
    if apply:
        modules.append("CeilingCushion_ON")
        notes.append("Ceiling Cushion: prefer U3.5/4.5 over naked U3.5.")  # [1](https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC1iMmQ2LWU3YmUtMDACLTAwCgBGAAADt%2bmUE3rpw0ejjqWOUoV9NAcALXSofLnuY06Hm4FKMynP7AAAAgEMAAAALXSofLnuY06Hm4FKMynP7AAIc9qaGwAAAA%3d%3d&exvsurl=1&viewmodel=ReadMessageItem)

def s_lock(prev_lean: str, new_lean: str, delta: float, notes: List[str], modules: List[str]) -> str:
    if prev_lean and prev_lean != new_lean and delta < HYSTERESIS:
        modules.append("S‑LOCK_Hysteresis")
        notes.append(f"S‑LOCK: prevented flip {prev_lean}→{new_lean} (Δ={delta:.2f}<{HYSTERESIS}).")
        return prev_lean
    return new_lean

def build_corridor(lean: str, tempo: float, ulr_on: bool, burst_on: bool) -> Tuple[float, float]:
    low, high = 1.5, 4.5  # safe baseline
    if tempo >= 0.65:
        high = 5.0
    elif tempo <= 0.35:
        high = 4.0
    return (low, high)

def _o25_addon_allowed(support_delta: float, sot_proj_total: float, width: float,
                       p_home_tt05: float, p_away_tt05: float) -> bool:
    # O2.5 only if ALL gates pass (strict) [1](https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC1iMmQ2LWU3YmUtMDACLTAwCgBGAAADt%2bmUE3rpw0ejjqWOUoV9NAcALXSofLnuY06Hm4FKMynP7AAAAgEMAAAALXSofLnuY06Hm4FKMynP7AAIc9qaGwAAAA%3d%3d&exvsurl=1&viewmodel=ReadMessageItem)
    conds = [
        (_r(support_delta) >= ADDON_O25_SUPPORT_DELTA),
        (_r(sot_proj_total) >= ADDON_MIN_SOT),
        (_r(width) >= ADDON_MIN_CORRIDOR_WIDTH),
        (_r(p_home_tt05) >= ADDON_MIN_TT05),
        (_r(p_away_tt05) >= ADDON_MIN_TT05),
        True  # defensive‑strength block placeholder
    ]
    return all(conds)

def translate_play(lean: str, corridor: Tuple[float, float], burst_on: bool,
                   support_delta: float, sot_proj_total: float,
                   p_home_tt05: float, p_away_tt05: float,
                   notes: List[str], flags: List[str], modules: List[str]) -> TranslatedPlay:
    low, high = corridor
    width = high - low

    if burst_on:
        if _o25_addon_allowed(support_delta, sot_proj_total, width, p_home_tt05, p_away_tt05):
            notes.append("O2.5 add‑on gate passed → O2.5 (LOW_CONF).")
            return TranslatedPlay(market="O2.5", confidence="LOW")
        return TranslatedPlay(market="O1.5", confidence="HIGH")  # forced Over floors

    if lean in ("under", "balanced"):
        ceiling_cushion(True, notes, modules)             # singles‑only, ceiling‑first on Unders [1](https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC1iMmQ2LWU3YmUtMDACLTAwCgBGAAADt%2bmUE3rpw0ejjqWOUoV9NAcALXSofLnuY06Hm4FKMynP7AAAAgEMAAAALXSofLnuY06Hm4FKMynP7AAIc9qaGwAAAA%3d%3d&exvsurl=1&viewmodel=ReadMessageItem)
        return TranslatedPlay(market="U3.5/4.5", confidence="HIGH")

    if lean == "over":
        if _o25_addon_allowed(support_delta, sot_proj_total, width, p_home_tt05, p_away_tt05):
            notes.append("O2.5 add‑on gate passed → O2.5 (LOW_CONF).")
            return TranslatedPlay(market="O2.5", confidence="LOW")
        return TranslatedPlay(market="O1.5", confidence="MEDIUM")

    flags.append("SinglesOnly")
    return TranslatedPlay(market="U3.5/4.5", confidence="MEDIUM")

def evaluate_athena(req: MatchRequest,
                    league_bias_over: float,
                    league_bias_under: float,
                    tempo_factor: float) -> Prediction:
    notes: List[str] = []
    modules: List[str] = []
    flags: List[str] = ["SinglesOnly"]  # invariant [1](https://outlook.live.com/owa/?ItemID=AQMkADAwATNiZmYAZC1iMmQ2LWU3YmUtMDACLTAwCgBGAAADt%2bmUE3rpw0ejjqWOUoV9NAcALXSofLnuY06Hm4FKMynP7AAAAgEMAAAALXSofLnuY06Hm4FKMynP7AAIc9qaGwAAAA%3d%3d&exvsurl=1&viewmodel=ReadMessageItem)

    # Inputs with safe fallbacks
    tempo = req.tempo_index if req.tempo_index is not None else max(0.0, min(1.0, tempo_factor))
    sot = req.sot_proj_total if req.sot_proj_total is not None else 9.0
    support_delta = req.support_idx_over_delta if req.support_idx_over_delta is not None else (league_bias_over - league_bias_under)
    p2p = req.p_two_plus if req.p_two_plus is not None else 0.68
    p_home_tt05 = req.p_home_tt05 if req.p_home_tt05 is not None else 0.62
    p_away_tt05 = req.p_away_tt05 if req.p_away_tt05 is not None else 0.58

    # Pre‑lean protections
    quality_ok = True
    veto = inline_veto(quality_ok, notes, modules)

    # Chaos & safety modules
    burst_on = burst_sentinel(support_delta, p2p, tempo, notes, modules)
    gateb_block = gate_b(tempo, support_delta, notes, modules)
    ulr_on = ulr_low_tempo(tempo, notes, modules)

    # Scores and lean
    over_score = _r(support_delta + (tempo * 0.4) + (0.05 if burst_on else 0.0))
    under_score = _r((1.0 - tempo) * 0.35 + (0.05 if ulr_on else 0.0) + (0.02 if gateb_block else 0.0))
    delta = abs(over_score - under_score)

    if burst_on:
        new_lean = "over"
    elif gateb_block or veto or ulr_on:
        new_lean = "under"
    else:
        new_lean = "over" if over_score > under_score else ("under" if under_score > over_score else "balanced")

    final_lean = s_lock(prev_lean=new_lean, new_lean=new_lean, delta=delta, notes=notes, modules=modules)

    low, high = build_corridor(final_lean, tempo, ulr_on, burst_on)
    translated = translate_play(final_lean, (low, high), burst_on, support_delta, sot, p_home_tt05, p_away_tt05, notes, flags, modules)

    confidence_score = min(0.95, max(0.55, 0.60 + (delta * 0.25) + (0.05 if translated.market in ("U3.5/4.5", "O1.5") else 0.0)))

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
