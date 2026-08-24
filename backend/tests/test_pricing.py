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


def test_playable_parses_a_full_slate():
    """The live tables are usually short or empty, so the parser is pinned
    against the archived 23 Aug log instead: 70 graded fixtures carrying every
    cell format the block has to read — team lanes with a club prefix, bold
    markup on either half, floor annotations, `— none`, `— no tip, X has 3
    rows`, and both result conventions (Tip 1 graded by the status cell, Tip 2
    carrying its own tick).

    Those counts are frozen history and cannot move, so a change in them is a
    parser change, not a data change.
    """
    from scripts import playable

    log = (playable.ROOT / "archive" / "2026-08-23-first-calibrated-slate"
           / "log.md").read_text()

    # An empty pending table must not run the scan on into the completed one.
    assert playable.rows_of(log, playable.PENDING) == []
    assert len(playable.rows_of(log, playable.COMPLETED)) == 70

    lanes = playable.collect(log)
    assert len(lanes) == 86
    # 86 lanes over 51 fixtures: most rows carry both, some only one.
    assert len(playable.fixtures(log)) == 51
    assert sum(1 for r in lanes if r[9] == "✅") == 69
    assert all(r[7] > playable.MIN_EDGE for r in lanes)
    # Sorted by kickoff, like every other table in the log.
    assert [r[0] for r in lanes] == sorted(r[0] for r in lanes)

    # And the filter is doing real work rather than just dropping empty cells.
    # 140 cells: 25 held no tip at all, 29 were published under the threshold —
    # mostly U4.25 and U3.0 rungs around 88%, which win constantly and cannot
    # be bought at a price that pays for them. That is the whole point of the
    # block: they flatter the engine's hit rate and are unbuyable, so they
    # belong in the log above and not in this count.
    dropped = [cell for c in playable.rows_of(log, playable.COMPLETED)
               for w, cell in ((1, c[4]), (2, c[5]))
               if playable.lane(cell, c[1], w) is None]
    assert len(dropped) == 54
    assert sum(1 for cell in dropped if playable.LANE.match(cell)) == 29


def test_playable_block_does_not_feed_on_itself():
    """The block renders the completed table's own header and sits ABOVE it.

    Read without scoping, a search for that header finds the block's copy first
    and the block is derived from its own previous output. That failure passes
    `--check` — a block built from itself is trivially up to date — so nothing
    but the counts would ever show it. Spliced in above the tables, the derived
    lanes must be identical to the ones derived without it.
    """
    from scripts import playable

    log = (playable.ROOT / "archive" / "2026-08-23-first-calibrated-slate"
           / "log.md").read_text()
    assert playable.NEXT in log

    at = log.index(playable.NEXT)
    spliced = log[:at] + playable.render(log) + "\n" + log[at:]
    assert playable.collect(spliced) == playable.collect(log)
    assert playable.fixtures(spliced) == playable.fixtures(log)
    # And the header counts read the table below, not the block above it.
    assert playable.rows_of(spliced, playable.COMPLETED) == \
        playable.rows_of(log, playable.COMPLETED)


def test_curse_haircut_only_touches_the_top_edge_band():
    """Ranking by an estimate selects the fixtures whose estimate came in high,
    so the top band is overconfident by construction. Measured at -2.5 on
    7,576 tips and -2.9 on a second population, against ~0 in every band below
    it, so the correction is a step at CURSE_EDGE and not a curve."""
    mu = 2.60
    plain = pricing.buy_from("O1.5", mu)

    # No edge given: the caller never claimed one, so nothing is applied.
    assert pricing.buy_from("O1.5", mu, stated_edge=None) == plain
    # Below the threshold, unchanged — those bands measured within noise of 0.
    assert pricing.buy_from("O1.5", mu, stated_edge=0.02) == plain
    assert pricing.buy_from("O1.5", mu, stated_edge=pricing.CURSE_EDGE) == plain
    # Above it, dearer by exactly the measured haircut.
    dear = pricing.buy_from("O1.5", mu, stated_edge=0.05)
    assert dear == pytest.approx(plain * (1 + pricing.CURSE_HAIRCUT))
    # It only ever raises the bar. A haircut that could lower a price would be
    # manufacturing value out of a known overconfidence.
    assert dear > plain


def test_curse_haircut_reprices_toward_the_measured_hit_rate():
    """The size is not a guess: the top band said 81.6% and returned 79.1%, so
    the honest probability is 0.969 of stated and the price it needs is 1/0.969
    of the quoted one. The constant must stay within a rounding of that."""
    assert pricing.CURSE_HAIRCUT == pytest.approx(1 / (79.1 / 81.6) - 1, abs=0.004)


def test_board_matches_fixtures_tsv():
    """Every block on the page renders from config/fixtures.tsv — header
    counts, playable cards, pending and completed cards, league badges. One
    pin replaces the five that guarded the old pipe tables: if any rendered
    number drifts from the data, this fails and `python scripts/board.py`
    fixes it."""
    from scripts import board

    text = board.README.read_text()
    assert board.rewrite(text) == text, (
        "README board is stale; run python scripts/board.py")


def test_fixtures_tsv_is_well_formed():
    """The typed source: seven tab-separated columns per row, kickoff parseable,
    status either empty, LIVE, or a graded mark. A malformed row here is the
    new version of a broken pipe table, so it fails loudly."""
    from scripts import board

    for f in board.load():
        assert len(f.kickoff) == 16 and f.kickoff[4] == "-", f.kickoff
        assert " v " in f.teams, f.teams
        assert (f.status == "" or f.status.startswith(("✅", "❌", "LIVE"))
                or f.status.startswith("🔴")), f.status
