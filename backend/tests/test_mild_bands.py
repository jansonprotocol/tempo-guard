"""
The two bands that changed sides on 31 Oct.

The bettor had deliberately stopped betting the tips and watched the
profile table instead, because the bands were too thin to trust blind.
What he saw: "Strong 3-6% is become MILD and Mild 0-3% under is Strong.
You may swap those for me."

The record agrees, and the gap widened rather than closed as volume
came in:

  ★ STRONG · 3–6% over   20 cards  80.0%  +0.9%  b/e 1.25
      cumulative 86% (7) -> 93% (14) -> 83% (18) -> 80% (20)
  MILD · 0–3% under      36 cards  88.9%  +7.9%  b/e 1.12
      cumulative 92% (13) -> 86% (22) -> 91% (34) -> 89% (36)

So STRONG's 3–6% over is worse than MILD overall (81.9%) and MILD's
0–3% under is better than STRONG overall (84.0%). For these two cells
the band overrides the claim; everywhere else the claim still decides.
"""
import pytest

from scripts import webapp as w


MID = (w.MILD_CLAIM_LO + w.MILD_CLAIM_HI) / 2      # inside the soft middle
LOW = w.MILD_CLAIM_LO - 5                          # below it: plain STRONG
HIGH = w.MILD_CLAIM_HI + 5                         # above it: plain STRONG


# ── the rule itself ─────────────────────────────────────────────────────────

def test_the_claim_middle_is_still_mild_in_an_untouched_band():
    assert w.mild_cell(MID, "0–3% over") is True
    assert w.mild_cell(LOW, "0–3% over") is False
    assert w.mild_cell(HIGH, "0–3% over") is False


def test_the_3_to_6_over_band_is_mild_whatever_the_claim():
    """STRONG gave it up: 80.0% and +0.9% at a feed price, which is a
    loss at a book he can reach."""
    for claim in (LOW, MID, HIGH, 95.0):
        assert w.mild_cell(claim, "3–6% over") is True, claim


def test_the_0_to_3_under_band_is_strong_whatever_the_claim():
    """The cheapest cell on the page — 88.9%, break-even 1.12 — and it
    was being filed under the softer tier."""
    for claim in (LOW, MID, HIGH, 95.0):
        assert w.mild_cell(claim, "0–3% under") is False, claim


def test_a_missing_claim_is_not_mild_on_its_own():
    assert w.mild_cell(None, "0–3% over") is False
    # ...but a swapped band still decides without one
    assert w.mild_cell(None, "3–6% over") is True
    assert w.mild_cell(None, "0–3% under") is False


def test_the_two_bands_do_not_overlap():
    """One cell cannot be told to go both ways."""
    assert not (set(w.MILD_BANDS) & set(w.STRONG_BANDS))


def test_the_swapped_bands_are_real_band_names():
    """A typo here would silently never match and the swap would be a
    no-op that still reads as shipped."""
    names = {n for _lo, _hi, n in w.GAP_BANDS}
    for band in w.MILD_BANDS + w.STRONG_BANDS:
        assert band in names, f"{band!r} is not a gap band"


def test_a_band_strong_gave_up_entirely_is_not_reachable_here():
    """STRONG_WEAK_BANDS are WEAK — no play at all — and are answered
    before the STRONG/MILD split. Moving one of them between the two
    play tiers would quietly resurrect it."""
    assert not (set(w.STRONG_WEAK_BANDS) & set(w.MILD_BANDS + w.STRONG_BANDS))


# ── the record reads it the same way ────────────────────────────────────────

def _prof(claim, gap):
    """profile_name for an all-seven card at a STRONG price."""
    return w.profile_name("U4.5", "green", claim, gap,
                          w.TIER_MIN["strong"] + 0.20, True)


def test_profile_name_files_the_swapped_cells_in_the_new_tier():
    assert _prof(HIGH, 4.5) == w.PROFILES[1], "3–6% over -> MILD"
    assert _prof(MID, -1.5) == w.PROFILES[0], "0–3% under -> ★ STRONG"


def test_profile_name_leaves_every_other_cell_where_it_was():
    assert _prof(MID, 1.5) == w.PROFILES[1]       # claim middle, 0–3% over
    assert _prof(HIGH, 1.5) == w.PROFILES[0]      # high claim, 0–3% over
    assert _prof(LOW, 7.0) == w.PROFILES[0]       # 6–10% over, untouched
    assert _prof(MID, 7.0) == w.PROFILES[1]


def test_the_pill_and_the_record_share_one_definition():
    """mild() must not carry a second copy of the rule: a board that
    says STRONG while the record files MILD is exactly the drift
    board.verify exists to catch."""
    import inspect
    src = inspect.getsource(w.mild)
    assert "mild_cell" in src or "card_profile" in src
    assert "MILD_CLAIM_LO <= " not in src.split("mild_cell")[0], \
        "mild() is re-deriving the claim test instead of asking for it"


# ── the live board, so the numbers behind the rule stay honest ──────────────

@pytest.fixture(scope="module")
def cells():
    from scripts import profile_days as pd
    rows = pd.record()
    for r in rows:
        r["band"] = w.gap_band(r["gap"])
    return rows


def _rate(rows, prof, band=None):
    rs = [r for r in rows if r["profile"] == prof
          and (band is None or r["band"] == band)]
    return (len(rs), 100 * sum(r["hit"] for r in rs) / len(rs)) if rs else (0, 0)


def test_the_swap_separates_the_two_tiers_on_the_live_record(cells):
    """If this ever inverts, the swap has stopped being right and the
    bettor should be told rather than left on a stale rule."""
    sn, sr = _rate(cells, w.PROFILES[0])
    mn, mr = _rate(cells, w.PROFILES[1])
    if sn < 30 or mn < 30:
        pytest.skip("not enough settled cards in both tiers to compare")
    assert sr > mr, (f"★ STRONG {sr:.1f}% on {sn} is no longer above "
                     f"MILD {mr:.1f}% on {mn} — the swap needs revisiting")


def test_the_moved_cells_are_counted_and_thin(cells):
    """Both are small and both have swung; the rule is a judgement on a
    direction, not a settled measurement. This records the size so a
    later reader knows what it rested on."""
    n_mild, _ = _rate(cells, w.PROFILES[1], w.MILD_BANDS[0])
    n_strong, _ = _rate(cells, w.PROFILES[0], w.STRONG_BANDS[0])
    if not (n_mild and n_strong):
        pytest.skip("the moved cells have no settled cards yet")
    assert n_mild >= 8 and n_strong >= 8
