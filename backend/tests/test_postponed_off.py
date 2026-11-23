"""A postponed card leaves the board, and the fill brings it back at its
new date (the bettor, 13 Nov: "remove them. Automatic card filler will
bring them back when a new date has been given")."""
import datetime as dt
from types import SimpleNamespace as NS

from scripts import board, feedslate

CLOSED = board.close_off("Postponed")


def row(ko, teams, status, code="ENG-L1"):
    return NS(kickoff=ko, teams=teams, status=status, code=code)


def test_a_closed_row_is_settled_and_off_every_pending_tab():
    f = board.Fixture("2026-11-14 16:00", "ENG-L1", "League One",
                      "Stevenage v Mansfield Town", "U4.25 85%", "", CLOSED)
    assert board.off_closed(CLOSED) and f.settled
    assert not board.is_off(CLOSED)


def test_the_fill_re_adds_it_at_a_new_date():
    fx = [row("2026-11-14 16:00", "Stevenage v Mansfield Town", CLOSED)]
    assert not feedslate._on_board("ENG-L1", "Stevenage", "Mansfield Town",
                                   dt.date(2026, 11, 25), fx)


def test_but_not_at_its_old_date_where_the_feed_still_lists_it():
    fx = [row("2026-11-14 16:00", "Stevenage v Mansfield Town", CLOSED)]
    assert feedslate._on_board("ENG-L1", "Stevenage", "Mansfield Town",
                               dt.date(2026, 11, 14), fx)


def test_an_ordinary_row_still_blocks_a_duplicate_a_day_either_side():
    fx = [row("2026-11-14 16:00", "Stevenage v Mansfield Town", "")]
    assert feedslate._on_board("ENG-L1", "Stevenage", "Mansfield Town",
                               dt.date(2026, 11, 15), fx)
