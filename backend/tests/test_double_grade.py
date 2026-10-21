"""
One match grades one row.

Found 21 Oct while reading why STRONG's hitrate fell over the weekend:
Millonarios v Jaguares de Córdoba was played once — 17 Oct, 4-1 — and
the record counted it twice. The sweep graded it onto the 18 Oct row;
scripts/stuck.py then re-asked WIDE for the 20 Oct row, matched the same
match by club name, and graded it there too. The 20 Oct row still carries
the drift note stuck.py writes for exactly this, "[played 2026-10-17]".
A second one was in the record the same way (Qingdao Hainiu v Beijing,
0-3 on both 9 and 10 Oct), so a MISS was doubled in ★ STRONG and a HIT
in LIVE INPLAY GOOD.

The board grows a second row for one pair of clubs whenever a match is
postponed: the OFF mark holds the old row at its own kickoff for ever,
because the first-sight stamp is keyed on it, while the fill adds the
replay at the new date. With two rows there is nothing in a club name to
say which of them a match answers — the day it was played says it, and
the nearest row owns the match.
"""
import pytest

from scripts import board, stuck


def _fx(kickoff: str, teams: str = "Millonarios v Jaguares de Córdoba",
        code: str = "COL-PA", status: str = ""):
    return board.Fixture(kickoff, code, "Primera A", teams,
                         "U4.25 85.9% +2.6% · buy≥1.17", "", status)


def _ev(day: str):
    """An ESPN event, only the fields the guard reads."""
    return {"date": f"{day}T01:05Z"}


A = _fx("2026-10-18 01:05")      # took the match from the sweep
B = _fx("2026-10-20 01:30")      # re-asked wide, found the same match


def test_the_nearest_row_owns_the_match():
    played = _ev("2026-10-17")           # one day off A, three off B
    assert stuck._nearer_row(played, A, [B]) is None, \
        "A is nearest, so A keeps it"
    assert stuck._nearer_row(played, B, [A]) is A, \
        "B must not grade a match that belongs to A"


def test_a_row_with_no_sibling_is_never_refused():
    assert stuck._nearer_row(_ev("2026-10-17"), A, []) is None


def test_a_tie_is_not_taken_away():
    """Equally near is not NEARER: the first row to reach the match keeps
    it and the other stays stuck, which is what gets it marked moved or
    parked rather than silently graded."""
    played = _ev("2026-10-19")           # one day either side
    assert stuck._nearer_row(played, A, [B]) is None
    assert stuck._nearer_row(played, B, [A]) is None


def test_an_undated_event_is_left_alone():
    """No date, no way to say whose match it is — the guard abstains
    rather than refusing a grade it cannot reason about."""
    assert stuck._nearer_row({}, B, [A]) is None
    assert stuck._nearer_row({"date": "not a date"}, B, [A]) is None


def test_siblings_are_the_same_clubs_in_the_same_competition():
    other = _fx("2026-10-20 01:30", "Millonarios v Once Caldas")
    cup = _fx("2026-10-20 01:30", code="COL-CUP")
    fixtures = [A, B, other, cup]
    sibs = stuck._siblings(A, fixtures)
    assert B in sibs
    assert other not in sibs, "different opponent"
    assert cup not in sibs, "a different competition is a different match"
    assert A not in sibs, "a row is not its own sibling"


def test_a_row_at_the_same_kickoff_is_not_a_sibling():
    """Two rows on one kickoff are a duplicate row, not a replay, and
    nothing here should start preferring one of them."""
    twin = _fx("2026-10-18 01:05")
    assert stuck._siblings(A, [A, twin]) == []


def test_the_nickname_matcher_pairs_the_clubs():
    """Siblings are matched with the sweep's own club matcher, so a
    spelling that differs between the two rows still pairs."""
    spelled = _fx("2026-10-20 01:30", "Millonarios FC v Jaguares Córdoba")
    assert stuck._siblings(A, [A, spelled]) == [spelled]


def test_event_falls_back_to_the_old_behaviour_without_the_board():
    """_event(f, cache) with no fixtures cannot run the guard, and must
    not pretend to: the signature stays usable."""
    import inspect
    sig = inspect.signature(stuck._event)
    assert sig.parameters["fixtures"].default is None
