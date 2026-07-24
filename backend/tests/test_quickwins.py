"""
Unit tests for the "safe code quick-wins" slice:

  D5 — team-name matching unified on rapidfuzz (fbref_base._match_team)
  O2 — plain-language rationale (engine.rationale.humanize / confidence_band)
  O3 — versioned, additive Prediction schema (engine.types.Prediction)
  D7 — weather fields present on the Prediction contract

These exercise only pure logic — no live database or network. A dummy
DATABASE_URL is set before importing fbref_base because app.database.db raises
at import time when it is unset (it does not connect at import, so a fake URL is
fine for importing the module).
"""
import os

os.environ.setdefault("DATABASE_URL", "postgresql://u:p@localhost:5432/test")

from app.engine.types import Prediction, Corridor, TranslatedPlay
from app.engine.rationale import humanize, confidence_band
from app.services.data_providers.fbref_base import _match_team


# ── D5: rapidfuzz team matching ───────────────────────────────────────────────

def _candidates():
    return ["Arsenal", "Aston Villa", "Manchester United", "Atlético Madrid",
            "Nottingham Forest", "Wolverhampton Wanderers"]


def test_match_team_exact():
    assert _match_team("Arsenal", _candidates()) == "Arsenal"


def test_match_team_accent_insensitive():
    # "Atletico Madrid" (no accent) should resolve to "Atlético Madrid".
    assert _match_team("Atletico Madrid", _candidates()) == "Atlético Madrid"


def test_match_team_fuzzy_typo_and_suffix():
    # Minor typo / suffix noise should still resolve via WRatio.
    assert _match_team("Nottingham Forest FC", _candidates()) == "Nottingham Forest"
    assert _match_team("Man United", _candidates()) == "Manchester United"


def test_match_team_no_false_match():
    # A clearly unrelated name must not match anything.
    assert _match_team("Real Madrid", _candidates()) is None


def test_match_team_empty_candidates():
    assert _match_team("Arsenal", []) is None


# ── O2: confidence bands ──────────────────────────────────────────────────────

def test_confidence_band_thresholds():
    assert confidence_band(0.95) == "high"
    assert confidence_band(0.78) == "high"
    assert confidence_band(0.70) == "medium"
    assert confidence_band(0.65) == "medium"
    assert confidence_band(0.60) == "low"
    assert confidence_band(None) == "low"


# ── O2/O3: rationale + schema on a constructed Prediction ─────────────────────

def _make_prediction(lean, market, score, modules, weather_tag=None):
    return Prediction(
        league_code="ENG-PL",
        fixture="Arsenal vs Chelsea",
        corridor=Corridor(low=1.5, high=4.5, lean=lean),
        translated_play=TranslatedPlay(market=market, confidence="MEDIUM"),
        confidence_score=score,
        applied_modules=modules,
        safety_flags=["SinglesOnly"],
        explanations=["raw dev note"],
        weather_tag=weather_tag,
    )


def test_schema_defaults_are_additive_and_versioned():
    pred = _make_prediction("over", "O1.75", 0.7, [])
    assert pred.schema_version == "2.3"
    assert pred.rationale == []          # default until populated
    assert pred.weather_tag is None
    assert pred.weather_impact is None


def test_humanize_lead_sentence_reflects_lean_and_market():
    pred = _make_prediction("under", "U3.75", 0.8, ["InlineVeto"])
    lines = humanize(pred)
    assert lines, "rationale should never be empty"
    assert "toward Under" in lines[0]
    assert "U3.75" in lines[0]
    assert "high confidence" in lines[0]


def test_humanize_maps_modules_to_plain_sentences():
    pred = _make_prediction(
        "over", "O2.25", 0.82,
        ["BurstSentinel_FORCED_OVER", "DET_Detonation", "MFR_TO_LIFT"],
    )
    lines = humanize(pred)
    joined = " ".join(lines)
    assert "high-tempo and chaotic" in joined          # BurstSentinel
    assert "volatile and high-scoring" in joined       # DET
    assert "higher Over line" in joined                # MFR_TO_LIFT


def test_humanize_handles_version_suffixed_module_names():
    # ULR is appended as "ULR_v1.3.1_LT" — startswith matching must catch it.
    pred = _make_prediction("under", "U4.25", 0.66, ["ULR_v1.3.1_LT"])
    joined = " ".join(humanize(pred))
    assert "Low match tempo" in joined


def test_humanize_includes_weather_line_when_present():
    pred = _make_prediction("over", "O1.75", 0.7, ["MFR_Soft"], weather_tag="Heavy Rain")
    joined = " ".join(humanize(pred))
    assert "Heavy Rain" in joined


def test_humanize_omits_weather_line_when_clear():
    pred = _make_prediction("over", "O1.75", 0.7, ["MFR_Soft"], weather_tag="Clear")
    joined = " ".join(humanize(pred))
    assert "Clear" not in joined


# ── Engine end-to-end: rationale + schema flow through evaluate_athena ─────────

def test_evaluate_athena_populates_rationale_and_schema():
    from app.engine.pipeline import evaluate_athena
    from app.engine.types import MatchRequest
    from datetime import date

    req = MatchRequest(
        league_code="ENG-PL",
        home_team="Arsenal",
        away_team="Chelsea",
        match_date=date(2026, 3, 5),
        tempo_index=0.6,
        p_two_plus=0.72,
        support_idx_over_delta=0.05,
    )
    pred = evaluate_athena(req, 0.5, 0.5, 0.5)
    assert pred.schema_version == "2.3"
    assert isinstance(pred.rationale, list) and len(pred.rationale) >= 1
    # Weather is not applied at the engine level — defaults intact.
    assert pred.weather_tag is None
    assert pred.weather_impact is None
