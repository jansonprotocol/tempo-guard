"""
The two things that stopped new cards reaching the board, 15-21 Oct.

board.verify used to identify a pending card by its FIXTURE NAME, and
asked whether that name turned up in more than one tab pane. Two pending
rows can honestly carry the same two clubs — scripts/stuck.py marks a
postponed card "PP Moved to <date>" and never rewrites its kickoff, so
the board keeps the old row while the fill adds the replay at its new
date — and the check called that the board contradicting itself. It
raised, the raise killed the board fill before `git add`, and 412 priced
cards were discarded every morning for six days.

The identity is now the ROW: (fixture, kickoff), read off the card's own
data-fx and data-k attributes. These tests pin the pattern against both
the legitimate case it used to reject and the real duplications it still
has to catch.

The second test covers odds_api.cached, which is what lets the two-hourly
refresh fill from listings it has already paid for.
"""
import html
import json
import re
import time

import pytest

from scripts import odds_api as oa


# ── the identity pattern, as board.verify applies it ──────────────────────────

def _count(panes: list[str], name: str, kickoff: str) -> int:
    """Cards in `panes` that are this exact (fixture, kickoff) row."""
    pat = re.compile(f'data-fx="{re.escape(html.escape(name))}"'
                     f'[^>]*?data-k="{re.escape(html.escape(kickoff))}"')
    return sum(len(pat.findall(p)) for p in panes)


def _card(name: str, kickoff: str) -> str:
    """A card element shaped as webapp._card writes it."""
    return (f'<details class="card play" data-fx="{html.escape(name)}" '
            f'data-t="haystack" data-lg="league" data-p="0.9" data-e="1.0" '
            f'data-ou="u" data-hr="80" data-k="{html.escape(kickoff)}">'
            f'<summary>x</summary>y</details>')


FX = "Independiente Santa Fe v Jaguares de Córdoba"
KO_OFF = "2026-10-15 01:30"      # postponed, kickoff never rewritten
KO_NEW = "2026-10-22 01:30"      # the replay the feed re-listed


def test_two_meetings_of_the_same_clubs_are_two_rows():
    """The case that broke the fill: one name, two rows, two tabs."""
    declined = _card(FX, KO_OFF)
    lanes = _card(FX, KO_NEW)
    assert _count([declined, lanes], FX, KO_OFF) == 1
    assert _count([declined, lanes], FX, KO_NEW) == 1


def test_one_row_rendered_in_two_tabs_still_fails():
    """What the check exists for — a card the board put in two panes."""
    playable = _card(FX, KO_OFF)
    watch = _card(FX, KO_OFF)
    assert _count([playable, watch], FX, KO_OFF) == 2


def test_one_row_rendered_twice_in_one_tab_still_fails():
    pane = _card(FX, KO_OFF) + _card(FX, KO_OFF)
    assert _count([pane], FX, KO_OFF) == 2


def test_a_missing_row_is_still_missing():
    """The name is on the page, but not this row — nothing stands in."""
    assert _count([_card(FX, KO_NEW)], FX, KO_OFF) == 0


def test_the_pattern_stays_inside_one_tag():
    """data-fx from one element must not pair with data-k from the next."""
    pane = (f'<details data-fx="{html.escape(FX)}"></details>'
            f'<details data-fx="Other v Club" data-k="{KO_OFF}"></details>')
    assert _count([pane], FX, KO_OFF) == 0


def test_accents_and_quotes_survive_the_round_trip():
    """A club with an apostrophe is escaped on the page; the pattern has
    to look for it in the form the page actually wrote (Inter Club
    d'Escaldes is a real refusal in config/unresolved.tsv)."""
    fx = "Mjällby AIF v Inter Club d'Escaldes"
    assert _count([_card(fx, KO_NEW)], fx, KO_NEW) == 1


# ── the free fill: is this league's listing already paid for? ────────────────

@pytest.fixture
def cache(tmp_path, monkeypatch):
    """Point odds_api at a throwaway cache file."""
    path = tmp_path / "odds_cache.json"
    monkeypatch.setattr(oa, "CACHE", path)
    return path


def _sport(code: str) -> str:
    return oa.SPORT[code]


def test_cached_is_false_with_no_cache(cache):
    assert oa.cached("ENG-PL") is False


def test_cached_is_true_for_a_fresh_listing(cache):
    cache.write_text(json.dumps({_sport("ENG-PL"): {"at": time.time(),
                                                    "events": []}}))
    assert oa.cached("ENG-PL") is True


def test_cached_is_false_once_the_listing_goes_stale(cache):
    old = time.time() - (oa.FRESH_MIN * 60 + 1)
    cache.write_text(json.dumps({_sport("ENG-PL"): {"at": old, "events": []}}))
    assert oa.cached("ENG-PL") is False, \
        "a stale listing costs two credits to re-ask, so it is not free"


def test_cached_is_false_for_another_league(cache):
    """The free fill must not read one league's listing as another's."""
    cache.write_text(json.dumps({_sport("ENG-PL"): {"at": time.time(),
                                                    "events": []}}))
    assert oa.cached("ESP-LL") is False


def test_cached_is_false_for_a_competition_with_no_feed_key(cache):
    cache.write_text(json.dumps({}))
    assert oa.cached("COL-PA") is False
