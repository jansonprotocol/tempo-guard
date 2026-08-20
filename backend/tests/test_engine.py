"""
Engine tests — pure logic, no data files or network required.

Covers the prediction pipeline, the plain-language rationale layer and the
Prediction schema contract.
"""
from datetime import date

import pytest

from app.engine.pipeline import evaluate_athena
from app.engine.rationale import confidence_band, humanize
from app.engine.types import Corridor, MatchRequest, Prediction, TranslatedPlay


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


# ── Confidence bands ──────────────────────────────────────────────────────────

@pytest.mark.parametrize("score,band", [
    (0.95, "high"), (0.78, "high"),
    (0.70, "medium"), (0.65, "medium"),
    (0.60, "low"), (None, "low"),
])
def test_confidence_band(score, band):
    assert confidence_band(score) == band


# ── Rationale ─────────────────────────────────────────────────────────────────

def test_rationale_lead_sentence():
    lines = humanize(_make_prediction("under", "U3.75", 0.8, ["InlineVeto"]))
    assert lines
    assert "toward Under" in lines[0]
    assert "U3.75" in lines[0]
    assert "high confidence" in lines[0]


def test_rationale_maps_modules_to_sentences():
    joined = " ".join(humanize(_make_prediction(
        "over", "O2.25", 0.82,
        ["BurstSentinel_FORCED_OVER", "DET_Detonation", "MFR_TO_LIFT"],
    )))
    assert "high-tempo and chaotic" in joined
    assert "volatile and high-scoring" in joined
    assert "higher Over line" in joined


def test_rationale_handles_version_suffixed_modules():
    joined = " ".join(humanize(_make_prediction("under", "U4.25", 0.66, ["ULR_v1.3.1_LT"])))
    assert "Low match tempo" in joined


def test_rationale_weather_line_only_when_notable():
    assert "Heavy Rain" in " ".join(
        humanize(_make_prediction("over", "O1.75", 0.7, [], weather_tag="Heavy Rain"))
    )
    assert "Clear" not in " ".join(
        humanize(_make_prediction("over", "O1.75", 0.7, [], weather_tag="Clear"))
    )


# ── Schema ────────────────────────────────────────────────────────────────────

def test_schema_defaults_are_additive():
    pred = _make_prediction("over", "O1.75", 0.7, [])
    assert pred.schema_version == "2.3"
    assert pred.rationale == []
    assert pred.weather_tag is None
    assert pred.weather_impact is None


# ── Engine end-to-end ─────────────────────────────────────────────────────────

def _req(**kw):
    base = dict(
        league_code="ENG-PL", home_team="Arsenal", away_team="Chelsea",
        match_date=date(2026, 3, 5), tempo_index=0.6, p_two_plus=0.72,
        support_idx_over_delta=0.05,
    )
    base.update(kw)
    return MatchRequest(**base)


def test_engine_populates_rationale_and_schema():
    pred = evaluate_athena(_req(), 0.5, 0.5, 0.5)
    assert pred.schema_version == "2.3"
    assert isinstance(pred.rationale, list) and pred.rationale
    assert pred.weather_tag is None


def test_engine_is_deterministic():
    a = evaluate_athena(_req(), 0.5, 0.5, 0.5)
    b = evaluate_athena(_req(), 0.5, 0.5, 0.5)
    assert a.translated_play.market == b.translated_play.market
    assert a.confidence_score == b.confidence_score


def test_low_goal_expectation_leans_under():
    """A low p_two_plus must trigger the under guard regardless of tempo."""
    pred = evaluate_athena(_req(p_two_plus=0.55, tempo_index=0.7), 0.5, 0.5, 0.5)
    assert pred.corridor.lean == "under"
    assert pred.translated_play.market.startswith("U")


def test_over_bias_shifts_lean_toward_over():
    """League bias must actually move the prediction — calibration depends on it."""
    neutral = evaluate_athena(_req(), 0.5, 0.5, 0.5)
    over_biased = evaluate_athena(_req(), 1.0, 0.0, 0.5)
    assert over_biased.confidence_score != neutral.confidence_score or \
           over_biased.translated_play.market != neutral.translated_play.market
