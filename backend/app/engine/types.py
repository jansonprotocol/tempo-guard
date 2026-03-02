from __future__ import annotations
from dataclasses import dataclass, field
from typing import List, Optional, Literal
from datetime import date

Lean = Literal["over", "under", "balanced"]
Market = Literal["O0.5", "O1.5", "O2.5", "U3.5", "U3.5/4.5", "SKIP"]
Confidence = Literal["LOW", "MEDIUM", "HIGH"]

@dataclass
class MatchRequest:
    league_code: str
    home_team: str
    away_team: str
    match_date: Optional[date] = None
    # optional inputs (can be None; safe defaults will be used)
    sot_proj_total: Optional[float] = None
    support_idx_over_delta: Optional[float] = None
    p_two_plus: Optional[float] = None
    p_home_tt05: Optional[float] = None
    p_away_tt05: Optional[float] = None
    tempo_index: Optional[float] = None

@dataclass
class Corridor:
    low: float
    high: float
    lean: Lean

@dataclass
class TranslatedPlay:
    market: Market
    confidence: Confidence

@dataclass
class Prediction:
    league_code: str
    fixture: str
    corridor: Corridor
    translated_play: TranslatedPlay
    confidence_score: float
    applied_modules: List[str] = field(default_factory=list)
    safety_flags: List[str] = field(default_factory=list)
    explanations: List[str] = field(default_factory=list)
