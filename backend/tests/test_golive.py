"""
A card reaches Running without waiting for a deploy.

The bettor, 21 Oct: "The running board is slow. Some cards only come
live after the match has been 15-30 minutes live. There for users cant
react on the scorelines."

Which tab a card sat in came only from the deploy, and the poller said
so out loud — no live span, "needs the deploy", skip. So the chain
between the whistle and a visible scoreline was: ESPN notices, a sweep
pass renders a 15 MB page, a deploy publishes it, the host builds, a CDN
serves it. The score itself was already arriving within one poll in
web/live.json; it just had nowhere on the card to land.

Now it does. The page creates the live clock, swaps the pre-match bar
for the live one — priced from the record embedded in the page, so the
browser invents nothing — moves the card to Running, and corrects the
tab counts. The two things that could go wrong are both pinned here:
the page must not form a second opinion about which cards Running is
for, and it must not keep a pre-kickoff price on a running match.
"""
import re

import pytest

from scripts import board, webapp as w


@pytest.fixture(scope="module")
def page() -> str:
    path = w.ROOT / "web" / "index.html"
    if not path.exists():
        pytest.skip("no rendered page — build it first: "
                    "cd backend && python scripts/board.py --allow-stale")
    return path.read_text()


@pytest.fixture(scope="module")
def fixtures():
    return board.load()


def test_runs_when_live_is_the_verify_partition_without_the_clock(fixtures):
    """ONE DEFINITION OF RUNNING. board.verify partitions the pending
    tabs with running_call plus two clauses — a live-unsafe card is
    declined, and a STRONG WATCH card reaches Running only where
    live_good backs a live buy. runs_when_live is that same test with
    odds_api.started() taken out, so it can be answered before kickoff.
    On every card that HAS started the two must agree exactly; if they
    ever stop, the page and the verify are calling different cards
    Running, which is the drift verify exists to catch."""
    checked = 0
    for f in fixtures:
        if f.settled or not w.odds_api.started(f.kickoff):
            continue
        verify_says = bool(
            w.running_call(f) and not w.live_unsafe(f)
            and (w.tier_of(f) not in ("strong watch", "strong watch from")
                 or w.live_good(f)))
        assert w.runs_when_live(f) == verify_says, f.teams
        checked += 1
    if not checked:
        pytest.skip("no started, unsettled card on the board to compare on")


def test_a_postponed_or_settled_card_never_runs(fixtures):
    for f in fixtures:
        if f.settled or (f.status or "").startswith("PP"):
            assert not w.runs_when_live(f), f.teams


def test_a_declined_card_never_runs(fixtures):
    """"A declined card belongs in Athena lanes whether or not it has
    started" — and it gets no live bar either, so the page must have no
    way to move it onto Running."""
    for f in fixtures:
        if f.settled or not w.live_unsafe(f):
            continue
        assert not w.runs_when_live(f), f.teams


def test_the_page_stamps_data_run_on_exactly_those_cards(page, fixtures):
    """The page's own answer, read back off it."""
    for f in fixtures:
        if f.settled:
            continue
        tag = re.search(
            f'<details class="card [a-z ]*?"( data-run="1")? '
            f'data-fx="{re.escape(__import__("html").escape(f.teams))}"',
            page)
        if not tag:
            continue                      # not on a pending tab
        assert bool(tag.group(1)) == w.runs_when_live(f), f.teams


def test_every_card_the_render_put_on_running_carries_the_stamp(page):
    """Otherwise a card the deploy placed on Running would be one the
    page would never have moved there — the same disagreement, caught
    from the other side."""
    i = page.index('id="t-running"')
    ends = [x for x in (page.find('class="tabpane"', i + 1),
                        page.find("</section>", i + 1)) if x > 0]
    pane = page[i:min(ends)] if ends else page[i:]
    cards = re.findall(r'<details class="card[^>]*?>', pane)
    for c in cards:
        assert 'data-run="1"' in c, c[:120]


def test_the_pre_match_bar_carries_what_the_swap_needs(page):
    """data-lb is the lane the live bar prices and data-pk the dim
    pre-kickoff line the old number moves down to. Without them the
    browser would have to invent a price, and a bar with an invented
    price is worse than no bar."""
    bars = re.findall(r'<div class="buybar"[^>]*>', page)
    assert bars, "no pre-match bar on the board — nothing to check"
    withlb = [b for b in bars if "data-lb=" in b]
    assert withlb, ("no pre-match bar carries data-lb; a card rendered "
                    "before kickoff could not go live in the browser")
    for b in withlb:
        # _prekick_inner reads the same market_bar the bar itself printed,
        # so a bar that has a lane always has a pre-kickoff line too.
        assert "data-pk=" in b, b[:140]


def test_the_poller_no_longer_waits_for_a_deploy(page):
    """The line this whole change is about."""
    assert "function goLive(" in page
    assert "function runningGrid(" in page
    assert "function tabCounts(" in page
    # the old early return is gone: a card with no live span is now a
    # card to take live, not a card to skip
    assert "// not rendered as running: needs the deploy" not in page
    assert "goLive(card, c.min !== undefined)" in page, \
        "the swap has to know whether there is a minute to price at"


def test_the_live_swap_drops_the_pre_match_price(page):
    """"A price measured on cards bought BEFORE kickoff is not the price
    to pay at 60' with a goal in." So the bar is reclassified and its
    text replaced in the same breath, and a settled card keeps neither
    the bar nor the pre-kickoff line."""
    js = page[page.index("function goLive("):page.index("function runningGrid(")]
    assert 'bar.classList.add("live")' in js
    assert "bar.innerHTML" in js, "the pre-match price must not survive"
    assert 'bar.removeAttribute("title")' in js
    assert ".best-tag" in js, "the star points at what to buy; nothing is left"
    settled = page[page.index("if (c.settled) {"):]
    settled = settled[:settled.index("continue;")]
    assert ".buybar" in settled and ".prekick" in settled, \
        "a settled card shows no price at all"


def test_the_stamp_is_an_element_the_poller_can_find(page):
    """It was bare text in the meta row, and bare text cannot be
    swapped for a clock."""
    assert 'class="stamp">' in page
    assert '.meta .stamp' in page


def test_the_tab_counts_can_be_corrected(page):
    """A card that moves must not leave the numbers behind it."""
    for t in ("playable", "watch", "running", "lanes"):
        m = re.search(f'data-t="{t}"[^>]*>.*?<span class="dim">'
                      r'(<span class="n">\d+</span>)', page, re.S)
        assert m, t


# ── one bad card may not take the board down ────────────────────────────────

def test_a_zero_claim_prices_nothing_instead_of_raising():
    """Four COL-PA cards print "O1.5 0.0%", and 1/(0/100) raised
    ZeroDivisionError out of the middle of _card -> _lanebar -> _needs.
    webapp.main() died with it, no index.html was written, and the page
    the site serves sat unchanged for thirty hours while the board
    underneath it went on being graded — three odds refreshes in a row.

    A rate of zero cannot imply a price at any margin, so it is read the
    way a missing claim is read: no bar on that lane, every other card
    untouched."""
    dead = "O1.5 0.0% **−97.0%** · buy≥0.04"
    assert w._needs(dead, None) == (None, None, False)
    assert w._needs(dead, "orange") == (None, None, False)
    # Zero is the only degenerate value that can reach here: _claim's
    # regex captures digits and never a sign, so "-2.0%" reads as 2.0
    # and prices like any other thin claim. The guard is `<= 0` anyway,
    # because a bar that cannot be computed should not be computed.
    assert w._claim("O1.5 -2.0% · buy≥1.10") == 2.0


def test_a_real_claim_still_prices():
    price, hit, solid = w._needs("U4.5 85.0% +2.0% · buy≥1.20", None)
    assert hit == 0.85 and price > 1 and solid is False


def test_every_pending_card_on_the_board_renders():
    """The guard that would have caught it: no card may raise while the
    page is being built."""
    from scripts import board
    reads = {}
    for f in board.load():
        if f.settled:
            continue
        try:
            w._card(f, "play", reads)
        except Exception as exc:                      # noqa: BLE001
            raise AssertionError(f"{f.teams} ({f.kickoff}) raised "
                                 f"{type(exc).__name__}: {exc}") from exc
