# backend/app/engine/rationale.py
"""
Plain-language rationale layer for ATHENA predictions.

The engine's ``explanations`` list is written for developers — it contains raw
signal values and module names (e.g. "BurstSentinel: support_delta=0.12 p2p=0.8
→ chaos profile → Over floor unlocked."). That is invaluable for debugging but
opaque to an end user.

``humanize()`` turns a finished Prediction into a short list of readable
sentences: a lead sentence describing the lean + recommended market + confidence
band, followed by one plain-English sentence per meaningful module that fired.

This module is intentionally dependency-free (it only reads attributes off the
Prediction) so it can be unit-tested without a database or the rest of the app.
"""
from __future__ import annotations

from typing import Any, List

# ── Confidence bands ──────────────────────────────────────────────────────────
# Mirrors the thresholds used in translate_play() for line selection so the
# user-facing band is consistent with how the engine actually reasons.
_HIGH_CONF = 0.78
_MED_CONF  = 0.65


def confidence_band(score: float | None) -> str:
    """Map a raw confidence_score (0–1) to a user-facing band label."""
    s = float(score or 0.0)
    if s >= _HIGH_CONF:
        return "high"
    if s >= _MED_CONF:
        return "medium"
    return "low"


# ── Module → sentence mapping ─────────────────────────────────────────────────
# Ordered by priority so the most decisive factors are listed first. Each key is
# matched against the fired module names with startswith(), which tolerates the
# version suffixes the engine appends (e.g. "ULR_v1.3.1_LT", "UnderGuard_HARD").
_MODULE_PHRASES: List[tuple[str, str]] = [
    ("InlineVeto",
     "Input data was incomplete, so the engine defaulted to the safer Under side."),
    ("BurstSentinel",
     "Both teams profile as high-tempo and chaotic, which forces the pick toward Over."),
    ("GateB",
     "Very low tempo and weak attacking signals blocked any Over exposure."),
    ("UnderGuard_HARD",
     "Goal expectation is low despite the tempo — a strong Under signal."),
    ("UnderGuard_SOFT",
     "Goal expectation is a little soft, leaning gently toward Under."),
    ("ULR",
     "Low match tempo favours fewer goals, supporting the Under ceiling."),
    ("DEG_Degradation",
     "Both teams' recent defensive form is slipping, nudging the total down."),
    ("DET_Detonation",
     "Recent matches have been volatile and high-scoring, widening the Over outlook."),
    ("BILATERAL_CHAOS_ESCALATOR",
     "Both sides are individually volatile, so the goal range was widened."),
    ("MFR_TO_LIFT",
     "Strong attacking momentum opens up a higher Over line."),
    ("MFR_Soft",
     "Mild attacking momentum supports the Over floor."),
    ("EPS_PhaseStability",
     "Goal totals have been inconsistent lately, so the ceiling was trimmed."),
    ("S-LOCK",
     "A borderline lean flip was held steady to avoid noise."),
    ("CeilingCushion",
     "A safety cushion was added, preferring a slightly higher Under line."),
]

_LEAN_TEXT = {
    "over":     "toward Over",
    "under":    "toward Under",
    "balanced": "with no strong lean either way",
}


def _lead_sentence(lean: str, market: str, band: str) -> str:
    lean_txt = _LEAN_TEXT.get(lean, lean)
    return (
        f"ATHENA leans {lean_txt} and recommends the {market} market "
        f"at {band} confidence."
    )


def _pick_sentences(market: str, win_prob: float, edge: float) -> List[str]:
    """
    State why this market was chosen, in the terms it was actually chosen on.

    The market is picked by comparing the modelled chance of the line landing
    against the chance in an ordinary fixture of the same league, so those are
    the two numbers worth showing. Saying "medium confidence" instead would
    describe a different quantity than the one that made the decision.
    """
    out = [
        f"Modelled chance of {market} landing here: {win_prob:.0%}."
    ]
    if edge >= 0.03:
        out.append(
            f"That is {edge:+.0%} better than a typical fixture in this league — "
            f"the clearest gap on the ladder, which is why this line was taken."
        )
    elif edge > 0.005:
        out.append(
            f"Only {edge:+.0%} better than a typical fixture in this league, so "
            f"the line is safe rather than sharp."
        )
    else:
        out.append(
            "No line stood out against a typical fixture in this league, so the "
            "safest one was taken. This is a thin call."
        )
    return out


def humanize(prediction: Any) -> List[str]:
    """
    Build a plain-language rationale for a Prediction.

    Accepts any object exposing ``corridor.lean``, ``translated_play.market``,
    ``confidence_score`` and ``applied_modules`` (duck-typed on purpose to keep
    this module import-light and testable in isolation).
    """
    modules = list(getattr(prediction, "applied_modules", None) or [])
    lean = getattr(prediction.corridor, "lean", "balanced")
    market = getattr(prediction.translated_play, "market", "?")
    band = confidence_band(getattr(prediction, "confidence_score", 0.0))

    # The corridor lean is the flowchart's verdict, and the market may no
    # longer come from the flowchart. When probability selection overrides it,
    # quoting the old lean produces a flat contradiction — "leans toward Under
    # and recommends the O1.0 market" — which is what a Serie B fixture printed
    # before this. The market itself is the truth about which side is being
    # played; the lean is only worth mentioning when it agrees.
    side = "over" if market.upper().startswith("O") else "under"
    if lean != side:
        lean = side
        overridden = True
    else:
        overridden = False

    sentences: List[str] = [_lead_sentence(lean, market, band)]

    if overridden:
        sentences.append(
            "The signal read the other way on raw tempo, but this fixture sits "
            "above its league's scoring average, so the value is on this side."
        )

    win_prob = getattr(prediction, "pick_win_prob", None)
    edge = getattr(prediction, "pick_edge", None)
    if win_prob is not None and edge is not None:
        sentences.extend(_pick_sentences(market, win_prob, edge))

    weather_tag = getattr(prediction, "weather_tag", None)
    if weather_tag and weather_tag != "Clear":
        sentences.append(
            f"Forecast conditions ({weather_tag}) were factored into the goal outlook."
        )

    # Several module phrases argue for a side. When the published market is on
    # the other one, they are describing a decision that was replaced, so they
    # are dropped rather than printed alongside a contradicting tip.
    _UNDER_PHRASES = ("GateB", "UnderGuard_HARD", "UnderGuard_SOFT", "ULR",
                      "DEG_Degradation", "EPS_PhaseStability", "CeilingCushion",
                      "InlineVeto")
    _OVER_PHRASES = ("BurstSentinel", "DET_Detonation", "MFR_TO_LIFT",
                     "MFR_Soft", "BILATERAL_CHAOS_ESCALATOR")

    for key, phrase in _MODULE_PHRASES:
        if not any(m == key or m.startswith(key) for m in modules):
            continue
        if side == "over" and key in _UNDER_PHRASES:
            continue
        if side == "under" and key in _OVER_PHRASES:
            continue
        sentences.append(phrase)

    return sentences
