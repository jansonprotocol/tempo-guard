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
    """All five were pruned by ablation and stay off."""
    assert set(ModuleFlags().disabled()) == {
        "burst_sentinel", "gate_b", "det", "eps", "bilateral",
    }


# ── Probability-based market selection ────────────────────────────────────────

def test_prob_select_is_on_with_the_measured_floor():
    """
    Enabled on measurement, not preference.

    The floor is load-bearing: edge is widest for lines in the middle of the
    goal distribution, so without one the selector chases the most volatile
    line available — at 0.55 it won only 60% of the time. Anyone lowering it
    should expect the strike rate to follow.

    IT IS ALSO COUPLED TO `features.MU_SHRINK`, and that coupling is why this
    test exists rather than a bare constant check. The floor is ABSOLUTE, so
    its behaviour depends entirely on how spread out mu is. 0.79 was correct
    against an unshrunk mu. Once mu was pulled toward the league mean, the same
    0.79 stopped being a floor and became a funnel: `U4.25` took 88-95% of tips
    in five leagues, breaking the floor's own stated criterion of keeping the
    top line under half of calls.

    0.75 restores the mix (top line 54% -> 34%) and improves realised edge
    while holding strike above 80%. Once the floor was fixed, the shrink could
    be tightened too — 0.79 had been masking how much shrinkage was warranted —
    and MU_SHRINK moved 0.60 -> 0.35, taking the weighted calibration gap to
    -0.6 with realised edge at +2.23.

    If either constant moves, re-run scripts/floor_after_shrink.py — changing
    one without the other silently breaks the market mix.
    """
    from app.data import features
    from app.engine import market_select

    assert ModuleFlags().prob_select is True
    assert market_select.MIN_WIN_PROB == 0.75
    assert features.MU_SHRINK == 0.35


def test_prob_select_never_offers_a_market_below_the_floor():
    """
    The floor is what separates this from the 60%-strike version. A pick that
    does not clear it should be impossible for any realistic fixture.
    """
    from app.engine import market_select

    for mu in (1.6, 2.0, 2.4, 2.7, 3.0, 3.4, 4.0):
        for lmu in (2.3, 2.7, 3.1):
            market, _edge, p = market_select.choose(mu, lmu)
            assert p >= market_select.MIN_WIN_PROB - 1e-9, (mu, lmu, market, p)


def test_prob_select_reads_the_goal_estimate():
    """
    The whole point: the market must respond to the goal estimate. The
    flowchart it replaces never read mu at all, which is why four separate
    improvements to mu produced no change in output.
    """
    from app.engine import market_select

    quiet = market_select.choose(2.0, 2.7)[0]
    lively = market_select.choose(3.6, 2.7)[0]
    assert quiet.startswith("U")
    assert lively.startswith("O")


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


# ── Two-lane output: safe and sharp ───────────────────────────────────────────

def _lane_req(tempo):
    from app.engine.types import MatchRequest
    return MatchRequest(
        league_code="ENG-PL", home_team="A", away_team="B",
        match_date=date(2026, 3, 5), tempo_index=tempo,
        p_two_plus=0.80, support_idx_over_delta=0.06,
    )


def test_sharp_lane_is_silent_on_ordinary_fixtures():
    """
    A fixture sitting at its league's scoring norm gets no sharper play. Most
    matches are ordinary and should stay on the safe lane.
    """
    from app.engine.pipeline import evaluate_athena

    # tempo 0.40 -> mu 2.70, exactly the norm passed below
    pred = evaluate_athena(_lane_req(0.40), 0.5, 0.5, 0.5,
                           norm_mean=2.70, norm_std=0.50)
    assert pred.lanes is not None
    assert pred.lanes.safe.market == pred.translated_play.market
    assert pred.lanes.sharp is None


def test_sharp_lane_offers_over_on_unusually_high_expectation():
    from app.engine.pipeline import evaluate_athena

    # tempo 0.80 -> mu 3.90, well above a 2.70 norm at 0.50 sigma. Also clears
    # the confidence veto: P(3+ goals | mu=3.90) is 0.75. At the old fixture of
    # mu=3.60 it was 0.697, a whisker under the bar.
    pred = evaluate_athena(_lane_req(0.80), 0.5, 0.5, 0.5,
                           norm_mean=2.70, norm_std=0.50)
    assert pred.lanes.sharp is not None
    assert pred.lanes.sharp.market.startswith("O")
    assert pred.lanes.sharp_tier == "3+ goals"
    assert pred.lanes.league_z >= 0.70


def test_sharp_lane_is_league_relative():
    """
    The same fixture must read differently in a high-scoring league and a
    low-scoring one — 3.0 expected goals is unremarkable in the Bundesliga and
    notable in Serie A. A global threshold would just describe the league.
    """
    from app.engine.pipeline import evaluate_athena

    # mu = 3.90. The old fixture used mu = 3.00, which the confidence veto now
    # rejects outright and rightly so: it asked for 3+ goals on a 58% chance.
    req = _lane_req(0.80)
    in_low = evaluate_athena(req, 0.5, 0.5, 0.5, norm_mean=2.45, norm_std=0.50)
    in_high = evaluate_athena(req, 0.5, 0.5, 0.5, norm_mean=3.70, norm_std=0.50)

    assert in_low.lanes.league_z > in_high.lanes.league_z
    assert in_low.lanes.sharp is not None       # unusual for a low-scoring league
    assert in_high.lanes.sharp is None          # ordinary for a high-scoring one


def test_sharp_lane_uses_expectation_spread_not_result_spread():
    """
    Regression guard. The trigger once standardised against the spread of
    actual match totals (~1.65) rather than of goal expectations (~0.45-0.75).
    Expectations are rolling averages and vary far less, so every z-score was
    divided by roughly 2.5 too much and no fixture in Serie A or Argentina ever
    reached the threshold — the sharp Under lane could not fire at all.
    """
    from app.engine.pipeline import evaluate_athena

    req = _lane_req(0.75)   # mu = 3.75, i.e. +1.05 goals above a 2.70 norm

    correct = evaluate_athena(req, 0.5, 0.5, 0.5, norm_mean=2.70, norm_std=0.50)
    wrong = evaluate_athena(req, 0.5, 0.5, 0.5, norm_mean=2.70, norm_std=1.65)

    assert correct.lanes.league_z > wrong.lanes.league_z
    assert correct.lanes.sharp is not None
    assert wrong.lanes.sharp is None


def test_sharp_lane_fires_both_directions():
    """
    The sharp lane must be able to go Under as well as Over. It once could not:
    the trigger used the wrong standard deviation, so no Serie A or Argentine
    fixture ever reached the Under threshold and the lane was Over-only.
    """
    from app.engine.pipeline import evaluate_athena
    from app.engine.types import MatchRequest

    def req(tempo, p2p):
        return MatchRequest(
            league_code="X", home_team="A", away_team="B",
            match_date=date(2026, 3, 5), tempo_index=tempo, p_two_plus=p2p,
            support_idx_over_delta=0.0,
        )

    # Serie A-like norm: 2.38 expected goals, spread 0.53
    high = evaluate_athena(req(0.80, 0.85), 0.5, 0.5, 0.5,
                           norm_mean=2.38, norm_std=0.53)
    low = evaluate_athena(req(0.10, 0.55), 0.5, 0.5, 0.5,
                          norm_mean=2.38, norm_std=0.53)

    assert high.lanes.sharp is not None and high.lanes.sharp.market.startswith("O")
    assert low.lanes.sharp is not None and low.lanes.sharp.market.startswith("U")


# ── Sharp lane confidence veto ────────────────────────────────────────────────

def test_sharp_lane_vetoes_coin_flips():
    """
    The z-gate asks whether a fixture is unusual. It never asked whether the
    rung it then reaches for is achievable, so it published plays like O2.5 —
    needing 3+ goals — on fixtures where the model put that at 58%.

    Measured over 10,159 fixtures, dropping those lifted the lane from 60.2%
    strike / +4.86% edge to 65.6% / +6.79%. Both rose, because the plays removed
    were bad on both counts rather than merely risky.
    """
    from app.engine.pipeline import SHARP_MIN_WIN, evaluate_athena
    from app.engine import market_select

    # mu = 3.00: unusual for a 2.45-goal league, but only a 58% shot at 3+.
    marginal = evaluate_athena(_lane_req(0.50), 0.5, 0.5, 0.5,
                               norm_mean=2.45, norm_std=0.50)
    assert market_select.p_win("O2.5", 3.00) < SHARP_MIN_WIN
    assert marginal.lanes.league_z >= 0.70, "the gate itself should still fire"
    assert marginal.lanes.sharp is None, "but the veto should suppress the play"


def test_sharp_veto_never_publishes_below_the_floor():
    """Whatever the gate offers must clear the floor, in either direction."""
    from app.engine.pipeline import SHARP_MIN_WIN, evaluate_athena
    from app.engine import market_select

    for tempo in (0.05, 0.20, 0.40, 0.60, 0.80, 0.95):
        for norm in (2.30, 2.70, 3.10):
            pred = evaluate_athena(_lane_req(tempo), 0.5, 0.5, 0.5,
                                   norm_mean=norm, norm_std=0.50)
            if pred.lanes.sharp is None:
                continue
            mu = 1.5 + tempo * 3.0
            p = market_select.p_win(pred.lanes.sharp.market, mu)
            assert p >= SHARP_MIN_WIN - 1e-9, (tempo, norm, pred.lanes.sharp.market, p)


def test_sharp_veto_only_vetoes_never_chooses():
    """
    The model gets a veto, not a vote. Letting it pick the sharp market by
    predicted edge was measured and lost to the z-gate by nearly two points of
    realised edge at a matched fire rate — it takes the bets it is most
    confident about, and those are disproportionately the ones it gets wrong.

    So the published market must always be one the gate chose.
    """
    from app.engine.pipeline import (
        MILD_UNDER, SHARP_OVER, SHARP_UNDER, evaluate_athena,
    )

    allowed = {SHARP_OVER[0], SHARP_UNDER[0], MILD_UNDER[0]}
    for tempo in (0.05, 0.25, 0.50, 0.75, 0.95):
        pred = evaluate_athena(_lane_req(tempo), 0.5, 0.5, 0.5,
                               norm_mean=2.70, norm_std=0.50)
        if pred.lanes.sharp is not None:
            assert pred.lanes.sharp.market in allowed
