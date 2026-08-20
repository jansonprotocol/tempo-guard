"""
Module flag tests — guard the measured prune decisions.

The defaults in ModuleFlags encode an ablation over nine leagues and ~2,900
matches. These tests make that deliberate rather than incidental: if someone
flips a default back on, they should have to change a test that says why it was
turned off.
"""
from datetime import date

import pytest

from app.engine.pipeline import evaluate_athena
from app.engine.types import MatchRequest, ModuleFlags


def _req(**kw):
    base = dict(
        league_code="ENG-PL", home_team="Arsenal", away_team="Chelsea",
        match_date=date(2026, 3, 5), tempo_index=0.6, p_two_plus=0.72,
        support_idx_over_delta=0.05, deg_pressure=0.2, det_boost=0.5,
        home_det=0.5, away_det=0.5, eps_stability=0.5,
    )
    base.update(kw)
    return MatchRequest(**base)


# ── Measured defaults ─────────────────────────────────────────────────────────

@pytest.mark.parametrize("module", ["burst_sentinel", "det"])
def test_harmful_modules_are_off_by_default(module):
    """
    Ablation: burst_sentinel cost 1.91% across all nine leagues tested and det
    cost 0.45%. Both are off unless a league explicitly re-enables them.
    """
    assert getattr(ModuleFlags(), module) is False


@pytest.mark.parametrize("module", ["gate_b", "eps", "bilateral"])
def test_inert_modules_are_off_by_default(module):
    """These changed zero predictions across the ablation set."""
    assert getattr(ModuleFlags(), module) is False


@pytest.mark.parametrize("module", ["ulr", "under_guard", "deg", "mfr"])
def test_contributing_modules_stay_on(module):
    assert getattr(ModuleFlags(), module) is True


def test_all_on_restores_every_module():
    flags = ModuleFlags.all_on()
    assert flags.disabled() == []


def test_disabled_lists_the_pruned_set():
    assert set(ModuleFlags().disabled()) == {
        "burst_sentinel", "gate_b", "det", "eps", "bilateral",
    }


# ── Flags actually take effect ────────────────────────────────────────────────

def test_burst_sentinel_can_be_re_enabled():
    """
    A fixture that satisfies every BurstSentinel condition must force Over when
    the module is on, and must not mention it when off. This proves the toggle
    is wired, not decorative.
    """
    req = _req(support_idx_over_delta=0.15, p_two_plus=0.85, tempo_index=0.75)

    on = evaluate_athena(req, 0.5, 0.5, 0.5, module_flags=ModuleFlags.all_on())
    assert any(m.startswith("BurstSentinel") for m in on.applied_modules)

    off = evaluate_athena(req, 0.5, 0.5, 0.5)   # default: pruned
    assert not any(m.startswith("BurstSentinel") for m in off.applied_modules)


def test_default_flags_match_explicit_defaults():
    """Passing no flags must equal passing a default ModuleFlags()."""
    req = _req()
    a = evaluate_athena(req, 0.5, 0.5, 0.5)
    b = evaluate_athena(req, 0.5, 0.5, 0.5, module_flags=ModuleFlags())
    assert a.translated_play.market == b.translated_play.market
    assert a.applied_modules == b.applied_modules


def test_under_guard_toggle_changes_the_market():
    """under_guard is the engine's only route to an Under market."""
    req = _req(p_two_plus=0.55)      # low goal expectation -> hard under guard
    on = evaluate_athena(req, 0.5, 0.5, 0.5)
    assert on.translated_play.market.startswith("U")

    off = evaluate_athena(req, 0.5, 0.5, 0.5,
                          module_flags=ModuleFlags(under_guard=False))
    assert not any(m.startswith("UnderGuard") for m in off.applied_modules)


# ── Dead code stays dead ──────────────────────────────────────────────────────

def test_removed_dead_modules_are_gone():
    """
    inline_veto and s_lock were unreachable: quality_ok was hardcoded True, and
    s_lock compared the lean against itself. Both were removed; neither should
    reappear without a real trigger behind it.
    """
    import app.engine.pipeline as pipeline
    assert not hasattr(pipeline, "inline_veto")
    assert not hasattr(pipeline, "s_lock")


# ── Tempo signal ──────────────────────────────────────────────────────────────

def test_tempo_index_is_not_saturated():
    """
    Regression guard. tempo_index previously clipped at 0.9 and was then scaled
    past its ceiling, pinning ~63% of matches to the maximum and starving every
    low-tempo module. Typical match totals must map to the middle of the range.
    """
    from app.data.features import TEMPO_BASE, TEMPO_SPAN

    def tempo(mu):
        return min(max((mu - TEMPO_BASE) / TEMPO_SPAN, 0.05), 0.95)

    typical = [tempo(mu) for mu in (2.4, 2.75, 3.2)]
    assert all(0.2 < t < 0.8 for t in typical), typical
    # and the range must still separate low from high scoring fixtures
    assert tempo(4.5) - tempo(1.8) > 0.5
