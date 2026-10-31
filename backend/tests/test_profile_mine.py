"""
Mining a profile bucket without finding things that are not there.

The bettor, 31 Oct, on the unlabelled plain-watch row — 161 cards at
80.7%: "Can we filter it down more and perhaps find some profiles
unknown yet. Hopefully to strengthen bands that are still thin?"

A bucket that size will hand you a 85% slice for nothing if you cut it
twenty ways, and the eye goes straight to it. So the tool's job is as
much to refuse a finding as to make one: fixed dimensions chosen before
the answer is seen, a Wilson interval on every candidate, a
first-half/second-half split, and the overlap between candidates,
because four findings that share their cards are one finding.
"""
import math

import pytest

from scripts import profile_mine as pm


# ── the interval, which is what stops a 30-card cell reading as fact ───────

def test_wilson_brackets_the_rate():
    lo, hi = pm.wilson(26, 30)          # 86.7%
    assert lo < 86.7 < hi
    assert 68 < lo < 73 and 94 < hi < 96


def test_wilson_is_narrower_with_more_cards():
    """The whole point: the same rate, more cards, a tighter claim."""
    w30 = pm.wilson(26, 30)
    w300 = pm.wilson(260, 300)
    assert (w300[1] - w300[0]) < (w30[1] - w30[0]) / 2


def test_wilson_does_not_run_off_the_scale():
    """A normal approximation at 97% on 30 cards puts the upper bound
    above 100, which would read as certainty."""
    lo, hi = pm.wilson(29, 30)
    assert 0 <= lo <= 100 and 0 <= hi <= 100


def test_wilson_handles_an_empty_cell():
    assert pm.wilson(0, 0) == (0.0, 0.0)


def test_a_30_card_cell_cannot_separate_85_from_80():
    """The number the bettor is actually waiting on. If this ever
    passes, the arithmetic has changed and the advice should too."""
    lo, _hi = pm.wilson(round(0.85 * 30), 30)
    assert lo < 80, "85% on 30 cards must still overlap an 80% baseline"
    lo300, _ = pm.wilson(round(0.85 * 300), 300)
    assert lo300 > 80, "...and ~300 is where it stops overlapping"


def test_the_band_mover_acts_inside_the_noise():
    """Not a complaint about the mover, a fact worth keeping visible:
    MOVE_MIN_N is far below where a 5-point difference is readable."""
    from scripts import tier_optimize
    lo, _ = pm.wilson(round(0.85 * tier_optimize.MOVE_MIN_N),
                      tier_optimize.MOVE_MIN_N)
    assert lo < 80


# ── the slicing, fixed in advance ──────────────────────────────────────────

def _card(**kw):
    base = dict(date="2026-10-01", code="ENG-PL", fixture="A v B",
                lane="U4.5", claim=84.0, label="orange", best=1.20,
                gap=-1.0, band="0–3% under", seven=False, side="U",
                hit=True, pl=0.20, profile="x")
    base.update(kw)
    return base


def test_the_dimensions_are_fixed_not_discovered():
    """Fishing one more dimension out after seeing the answer is how a
    bucket this size produces a profile that is not there."""
    got = pm.slices([_card(), _card(side="O", lane="O1.5")])
    kinds = {k.split(" = ")[0].split()[0] for k in got}
    assert {"band", "side", "lane", "label", "code"} <= kinds
    assert any(k.startswith("claim ") for k in got)
    assert any(k.startswith("price ") for k in got)
    assert "all seven rates" in got and "fewer than seven" in got


def test_every_card_lands_in_the_seven_split_exactly_once():
    rs = [_card(seven=True), _card(seven=False), _card(seven=False)]
    got = pm.slices(rs)
    assert len(got["all seven rates"]) + len(got["fewer than seven"]) == len(rs)


def test_the_price_buckets_do_not_overlap_or_leak():
    rs = [_card(best=b) for b in (1.01, 1.14, 1.15, 1.24, 1.25, 3.0)]
    got = pm.slices(rs)
    buckets = [v for k, v in got.items() if k.startswith("price ")]
    assert sum(len(v) for v in buckets) == len(rs)


def test_agg_reports_hits_rate_and_breakeven():
    rs = [_card(hit=True, pl=0.25), _card(hit=True, pl=0.25),
          _card(hit=False, pl=-1.0), _card(hit=True, pl=0.25)]
    n, h, hr, roi, be, avg = pm.agg(rs)
    assert (n, h) == (4, 3)
    assert hr == 75.0
    assert be == pytest.approx(100 / 75)
    assert roi == pytest.approx(100 * (0.25 * 3 - 1.0) / 4)


def test_agg_on_nothing_is_none():
    assert pm.agg([]) is None


# ── it must not invent its own population ──────────────────────────────────

def test_it_counts_the_same_cards_as_the_profiles_page():
    """profile_mine and profile_days must read ONE record. If they ever
    drift the mining is describing a board nobody is looking at."""
    import collections
    from scripts import profile_days
    a = collections.Counter(r["profile"] for r in profile_days.record())
    b = collections.Counter(r["profile"] for r in pm.cards())
    if not a:
        pytest.skip("no settled counted cards in the record yet")
    assert a == b


def test_a_profile_name_that_prefixes_another_is_not_folded_in():
    """'★ STRONG' is the start of '★ STRONG FROM', and a prefix match
    silently reported the two together — 48 extra cards and a different
    answer."""
    import pathlib
    src = pathlib.Path(pm.__file__).read_text(encoding="utf-8")
    body = src[src.index("def main"):]
    assert 'r["profile"] == want' in body
    exact = body.index('r["profile"] == want')
    prefix = body.find("startswith(want")
    assert prefix == -1 or exact < prefix, \
        "the exact match must be tried before any prefix match"
