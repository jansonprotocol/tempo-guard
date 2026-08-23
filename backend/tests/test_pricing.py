"""
Break-even pricing.

The point of these is that `1 / p_win` is NOT the answer for most rungs, so
each test pins a property that would have to break before the old shortcut
became right again.
"""
from __future__ import annotations

import math

import pytest

from app.engine import market_select, pricing


def test_settlement_matches_the_asian_convention():
    # U3.25 = half U3.0 + half U3.5, so 3 goals is half won, not a full win.
    assert pricing.settle_fraction("U3.25", 2) == 1.0
    assert pricing.settle_fraction("U3.25", 3) == 0.5
    assert pricing.settle_fraction("U3.25", 4) == -1.0
    # U3.75 = half U3.5 + half U4.0, so 4 goals is half lost, not a full loss.
    assert pricing.settle_fraction("U3.75", 3) == 1.0
    assert pricing.settle_fraction("U3.75", 4) == -0.5
    # A whole line pushes on its number.
    assert pricing.settle_fraction("U3.0", 3) == 0.0
    assert pricing.settle_fraction("O2.0", 2) == 0.0
    # A .5 line never pushes.
    for t in range(8):
        assert abs(pricing.settle_fraction("O1.5", t)) == 1.0


def test_half_lines_agree_with_one_over_p():
    """
    A .5 line cannot push, so the shortcut is exactly right there.

    Not bit-identical: `market_select` truncates the Poisson tail at 12 goals
    and this module at 16, so an Over line picks up a little more tail here.
    At the top of the range that gap is real — P(13..16 goals | mu=3.4) is
    5.7e-5, about 7e-5 of the price. `pricing` is the more accurate side, and
    `market_select`'s cut is left alone because moving it would shift live tip
    output. 2e-4 still catches any settlement bug, which would show up in the
    percents, not the fifth decimal.
    """
    for mu in (1.9, 2.6, 3.4):
        for m in ("O1.5", "O2.5", "U3.5"):
            assert pricing.break_even(m, mu) == pytest.approx(
                1 / market_select.p_win(m, mu), rel=2e-4
            )


def test_pushing_lines_are_dearer_than_one_over_p():
    """
    The engine counts a push as a win, so `1 / p` under-prices any rung that
    can push or half-win on the boundary. This is the error the module exists
    to fix; if it ever inverts, the settlement table is wrong.
    """
    mu = 2.60
    for m in ("U3.0", "U3.25", "U4.25", "O1.75", "O2.75"):
        assert pricing.break_even(m, mu) > 1 / market_select.p_win(m, mu)


def test_half_losing_lines_are_cheaper_than_one_over_p():
    """The mirror case: a rung that only half-loses is worth MORE than `1/p`."""
    mu = 2.60
    for m in ("U3.75", "O2.25"):
        assert pricing.break_even(m, mu) < 1 / market_select.p_win(m, mu)


def test_break_even_is_the_zero_ev_price():
    for mu in (2.1, 2.6, 3.3):
        for m in ("U3.0", "U3.25", "U4.25", "O1.5", "O2.25", "O2.75"):
            be = pricing.break_even(m, mu)
            assert pricing.expected_value(m, mu, be) == pytest.approx(0.0, abs=1e-9)


def test_buy_from_carries_the_margin():
    be = pricing.break_even("U3.0", 2.6)
    assert pricing.buy_from("U3.0", 2.6) == pytest.approx(be * 1.05)
    assert pricing.expected_value("U3.0", 2.6, pricing.buy_from("U3.0", 2.6)) > 0


def test_shorter_lines_are_never_dearer():
    """Within a tier, a softer settlement must not cost more to break even."""
    mu = 2.6
    assert pricing.break_even("U3.75", mu) < pricing.break_even("U3.5", mu)
    assert pricing.break_even("U3.5", mu) < pricing.break_even("U3.25", mu)
    assert pricing.break_even("U3.25", mu) < pricing.break_even("U3.0", mu)
    assert pricing.break_even("O2.25", mu) < pricing.break_even("O2.5", mu)
    assert pricing.break_even("O2.5", mu) < pricing.break_even("O2.75", mu)


def test_rejects_non_total_markets():
    with pytest.raises(ValueError):
        pricing.settle_fraction("BTTS", 2)


def test_readme_fixture_tables_are_in_kickoff_order():
    """The log is read top-down to decide what to look at next, so both fixture
    tables and the bet table must run earliest kickoff first. Rows get appended
    in the order fixtures are PRICED, which is not that order, so this pins it —
    `python scripts/sort_tables.py` fixes any drift."""
    from scripts import sort_tables

    text = sort_tables.README.read_text()
    assert sort_tables.sort_tables(text) == text, (
        "README fixture tables are out of kickoff order; "
        "run python scripts/sort_tables.py")
