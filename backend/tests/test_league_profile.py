"""
League after profile — and the three ways it can lie to you.

"Is there some merit in have a look at league level performance inside
those profiles ... Those numbers might be thin, but perhaps useful."

There is, but only behind three filters, and the first run showed why
each is needed:

  STRATIFY. Leagues sit unevenly across profiles, so a raw league rate
  mostly reports which profiles that league lands in.
  REPLICATE. SUI-SL had the largest z of any league, +2.63, from
  thirteen cards in ONE profile. It clears a Wilson interval too. It is
  nothing.
  HOLD OVER TIME. ENG-L2 ran the same direction in all three profiles
  it appears in — and +5.0 in the first half against -14.4 in the
  second. One bad stretch wearing the shape of a trend.

Only MEX-LMX and DEN-SL survived all three on the first run.
"""
import pytest

from scripts import league_profile as lp, webapp as w


def _c(code, profile, hit, date="2026-10-01"):
    return dict(code=code, profile=profile, hit=hit, date=date,
                pl=0.2 if hit else -1.0, best=1.20)


# ── stratification ─────────────────────────────────────────────────────────

def test_expected_uses_the_profile_a_card_is_actually_in():
    """A league sitting only in a strong profile must be measured
    against that profile, not against the board."""
    rows = ([_c("AAA", "hi", True) for _ in range(10)]
            + [_c("BBB", "hi", True) for _ in range(18)]
            + [_c("BBB", "hi", False) for _ in range(2)]
            + [_c("CCC", "lo", False) for _ in range(10)]
            + [_c("BBB", "lo", True) for _ in range(5)]
            + [_c("BBB", "lo", False) for _ in range(5)])
    by = {"hi": [r for r in rows if r["profile"] == "hi"],
          "lo": [r for r in rows if r["profile"] == "lo"]}
    E, V, n = lp.expected(rows, "BBB", by)
    assert n == 30
    # 20 cards judged against the hi profile (AAA, all hits -> 100%)
    # and 10 against the lo profile (CCC, all misses -> 0%)
    assert E == pytest.approx(20 * 1.0 + 10 * 0.0)
    assert V >= 0


def test_a_league_does_not_help_set_the_rate_it_is_judged_by():
    """Leave-one-out. Without it a league that IS most of a profile
    would be compared against itself and could never look unusual."""
    rows = ([_c("BIG", "p", False) for _ in range(18)]
            + [_c("BIG", "p", True) for _ in range(2)]
            + [_c("OTH", "p", True) for _ in range(10)])
    by = {"p": rows}
    E, _V, n = lp.expected(rows, "BIG", by)
    assert n == 20
    assert E == pytest.approx(20 * 1.0), \
        "BIG must be judged against OTH alone, not against the pooled rate"


def test_a_profile_with_too_few_other_cards_is_skipped():
    """Nothing to compare against is not a comparison."""
    rows = [_c("AAA", "p", True) for _ in range(9)] + [_c("BBB", "p", False)]
    E, V, n = lp.expected(rows, "AAA", {"p": rows})
    assert (E, V, n) == (0.0, 0.0, 0)


# ── the two survival tests ────────────────────────────────────────────────

def _survive(rows):
    import collections
    by = collections.defaultdict(list)
    for r in rows:
        by[r["profile"]].append(r)
    return {d["code"]: d for d in lp.survivors(rows, by)}


def _filler(profile, n, hits, code="FILL", date="2026-10-01"):
    return ([_c(code, profile, True, date) for _ in range(hits)]
            + [_c(code, profile, False, date) for _ in range(n - hits)])


def test_one_profile_only_does_not_survive():
    """The SUI-SL case: a big z from a single stratum."""
    rows = (_filler("p1", 40, 34) + _filler("p2", 40, 34, date="2026-10-20")
            + [_c("ONE", "p1", True, "2026-10-01") for _ in range(15)])
    got = _survive(rows)
    assert "ONE" in got
    assert got["ONE"]["profiles"] <= 1
    assert not got["ONE"]["survives"], \
        "a league in one profile has not replicated anything"


def test_a_direction_that_flips_between_halves_does_not_survive():
    """The ENG-L2 case: consistent across profiles, one bad stretch."""
    rows = _filler("p1", 60, 51) + _filler("p2", 60, 51, date="2026-10-20")
    rows += [_c("FLIP", "p1", True, "2026-10-01") for _ in range(10)]
    rows += [_c("FLIP", "p2", False, "2026-10-20") for _ in range(10)]
    got = _survive(rows)
    assert not got["FLIP"]["survives"]


def test_a_league_under_in_every_profile_and_both_halves_survives():
    rows = (_filler("p1", 60, 51) + _filler("p2", 60, 51, date="2026-10-20"))
    for d in ("2026-10-01", "2026-10-20"):
        p = "p1" if d == "2026-10-01" else "p2"
        rows += [_c("BAD", p, True, d) for _ in range(5)]
        rows += [_c("BAD", p, False, d) for _ in range(5)]
    got = _survive(rows)
    assert got["BAD"]["across"] and got["BAD"]["over_time"]
    assert got["BAD"]["survives"]
    assert got["BAD"]["hit"] < got["BAD"]["exp"]


def test_a_thin_league_is_not_tested_at_all():
    rows = _filler("p1", 60, 51) + [_c("TINY", "p1", False) for _ in range(3)]
    assert "TINY" not in _survive(rows)


# ── the honesty rails ──────────────────────────────────────────────────────

def test_feed_roi_is_only_offered_where_the_price_was_payable():
    """Everywhere else the pill is bought live at a price the feed never
    printed, so its feed ROI says nothing (CLAUDE.md, 14 Oct)."""
    assert lp.play_profiles() == {w.PROFILES[0], w.PROFILES[1]}
    import pathlib
    src = pathlib.Path(lp.__file__).read_text(encoding="utf-8")
    assert "prof in PLAY" in src, "the ROI column must be gated on that set"


def test_the_report_says_how_many_tests_it_ran():
    """Thirty-five leagues means about two look significant for free;
    a report that hides the count invites reading one of them."""
    import pathlib
    src = pathlib.Path(lp.__file__).read_text(encoding="utf-8")
    assert "will look real for nothing" in src


def test_it_writes_nothing():
    """He asked for this without touching what is there."""
    import pathlib
    src = pathlib.Path(lp.__file__).read_text(encoding="utf-8")
    for danger in ("write_text(", "open(", ".to_csv", "w.BAND_MOVES["):
        assert danger not in src, danger
