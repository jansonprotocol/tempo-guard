"""
The league cross-check: does this competition back this profile?

"Look at Mexico for example at sw from. 79%. Strong watch has a higher
expectancy. A big veto > decline there would help then ... put a little
confirmation or veto under the give final athena lane ... Also put a
little current nummer it reflects on next it ... Let athena also keep
track of those numbers so they stay up to date. Numbers are somewhere
to tin to immediately start declining. But good data to have to start."

Two things have to hold or the line is worse than nothing: it must
compare a league with its OWN profile (leagues sit unevenly across
profiles, so a raw league rate mostly reports which profiles it lands
in), and it must move nothing.
"""
import pytest

from scripts import webapp as w


@pytest.fixture(autouse=True)
def cell(monkeypatch):
    """A hand-built LEAGUE_CELL, so the words are pinned to numbers
    rather than to whatever the board happens to hold today."""
    P = w.PROFILES[5]                       # STRONG WATCH FROM
    monkeypatch.setitem(w.LEAGUE_CELL, (P, ""), (374, 322))      # 86.1%
    monkeypatch.setitem(w.LEAGUE_CELL, (P, "MEX-LMX"), (34, 27))  # 79.4%
    monkeypatch.setitem(w.LEAGUE_CELL, (P, "TUR-SL"), (17, 15))   # 88.2%
    monkeypatch.setitem(w.LEAGUE_CELL, (P, "ENG-CH"), (23, 19))   # 82.6%
    monkeypatch.setitem(w.LEAGUE_CELL, (P, "TINY"), (4, 4))       # 100% on 4
    return P


def call(prof, code):
    rd = w.league_read(prof, code)
    if not rd:
        return None
    if rd["n"] < w.LEAGUE_MIN_N:
        return "thin"
    if rd["gap"] <= -w.LEAGUE_VETO:
        return "veto"
    if rd["gap"] >= -w.LEAGUE_OK:
        return "ok"
    return "close"


# ── the words ──────────────────────────────────────────────────────────────

def test_his_own_example_vetoes(cell):
    """MEX-LMX in STRONG WATCH FROM: 79% on 34 against a profile of 86%."""
    rd = w.league_read(cell, "MEX-LMX")
    assert rd["n"] == 34
    assert rd["rate"] == pytest.approx(79.4, abs=0.1)
    assert rd["gap"] < -w.LEAGUE_VETO
    assert call(cell, "MEX-LMX") == "veto"


def test_a_league_above_its_profile_confirms(cell):
    assert call(cell, "TUR-SL") == "ok"


def test_a_league_a_little_under_is_close_not_a_veto(cell):
    """ENG-CH reads 83% against an 86% profile. It looks strong beside
    Mexico and is still slightly under its own profile — which is the
    distinction the line exists to make."""
    assert call(cell, "ENG-CH") == "close"


def test_a_thin_cell_says_thin_and_nothing_else(cell):
    """100% on four cards is a coin, not a confirmation."""
    rd = w.league_read(cell, "TINY")
    assert rd["rate"] == 100.0
    assert call(cell, "TINY") == "thin"


def test_a_league_with_no_record_in_this_profile_says_nothing(cell):
    assert w.league_read(cell, "NOWHERE") is None


# ── the arithmetic that makes it honest ────────────────────────────────────

def test_the_baseline_leaves_the_league_out(cell):
    """A league must never help set the bar it is judged against —
    otherwise one that IS most of a profile can never look unusual."""
    rd = w.league_read(cell, "MEX-LMX")
    assert rd["base_n"] == 374 - 34
    assert rd["base"] == pytest.approx(100 * (322 - 27) / (374 - 34), abs=0.1)
    assert rd["base"] > rd["rate"]


def test_a_league_that_is_almost_the_whole_profile_is_not_compared(monkeypatch):
    """Nothing left to compare against is not a comparison."""
    P = "p"
    monkeypatch.setitem(w.LEAGUE_CELL, (P, ""), (30, 24))
    monkeypatch.setitem(w.LEAGUE_CELL, (P, "BIG"), (25, 20))
    assert w.league_read(P, "BIG") is None     # only 5 others


def test_the_gap_is_against_the_profile_not_the_board(cell, monkeypatch):
    """The same league rate must read differently in a strong profile
    than in a weak one. That is the whole point of stratifying."""
    weak = "weak-profile"
    monkeypatch.setitem(w.LEAGUE_CELL, (weak, ""), (100, 70))     # 70%
    monkeypatch.setitem(w.LEAGUE_CELL, (weak, "MEX-LMX"), (34, 27))
    assert call(cell, "MEX-LMX") == "veto"     # 79% under an 86% profile
    assert call(weak, "MEX-LMX") == "ok"       # the same 79% over a 70% one


# ── it is a flag, and only a flag ──────────────────────────────────────────

def test_the_thresholds_are_ordered():
    assert 0 < w.LEAGUE_OK < w.LEAGUE_VETO
    assert w.LEAGUE_MIN_N >= 10, \
        "under ten cards nothing is resolvable; it may only say thin"


def test_the_cross_check_moves_nothing():
    """It must not reach the tier, the decline, the tabs or the record.
    'Numbers are somewhere to tin to immediately start declining.'"""
    import inspect
    for fn in (w.league_read, w.league_call, w.build_league,
               w._crosscheck_html):
        src = inspect.getsource(fn)
        for danger in ("is_declined =", "BAND_MOVES[", "tier_of(f) =",
                       "DECLINE", "play_lock"):
            assert danger not in src, f"{fn.__name__} touches {danger}"
    # and nothing else consults it
    import pathlib
    whole = pathlib.Path(w.__file__).read_text(encoding="utf-8")
    callers = [ln for ln in whole.splitlines()
               if "league_call(" in ln and "def " not in ln]
    for ln in callers:
        assert ("_crosscheck_html" in ln or "rd = league_call" in ln
                or "league_cell_log" in ln or "rd = " in ln), ln


def test_every_profile_has_a_short_name_for_the_card():
    """The unlabelled names carry their whole definition and do not fit
    on a card line."""
    for p in w.PROFILES:
        assert p in w.SHORT_LEAGUE_PROFILE, p
        assert len(w.SHORT_LEAGUE_PROFILE[p]) <= 22, p


def test_the_log_header_names_what_it_is():
    assert "TRACKED ONLY" in w._LCL_HEAD
    for col in ("code", "profile", "base", "gap", "call"):
        assert col in w._LCL_HEAD
