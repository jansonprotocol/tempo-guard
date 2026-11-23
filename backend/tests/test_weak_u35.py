"""WEAK's U3.5 lane at 10%+ over is its own cell and promotes to STRONG on
its own bar (the bettor, 23 Nov: "once it hits 25 cards and still is above
80% hitrate, it can promote to strong")."""
from scripts import webapp as w


def prof(lane, seven=True, gap=12.0, best=1.45, claim=84.0):
    return w.profile_name(lane, "orange", claim, gap, best, seven)


def test_u35_at_10_over_is_its_own_cell_on_both_routes_into_weak():
    assert prof("U3.5", seven=True) == w.WEAK_U35
    assert prof("U3.5", seven=False) == w.WEAK_U35


def test_every_other_weak_card_stays_weak():
    assert prof("U4.5") == "WEAK"
    assert prof("O1.5") == "WEAK"
    # U3.5 in a band other than 10%+ over is not this cell
    assert prof("U3.5", seven=False, gap=4.0) == "WEAK"


def test_its_bar_is_25_cards_at_80_and_it_is_a_no_play_cell_until_then():
    assert w.WEAK_U35_UP_N == 25 and w.WEAK_U35_UP_HIT == 80.0
    assert w.profile_kind(w.WEAK_U35) == "no play"
    assert w.WEAK_U35 in w.PROFILES


def test_the_fast_cells_are_the_four_he_named_and_share_one_bar():
    plain = w.U_ + "plain play (fewer than 7, under the floor)"
    assert w.STRONG_FAST_CELLS == {
        (w.WEAK_U35, "10%+ over"), ("WEAK", "3–6% over"),
        (plain, "3–6% over"), (plain, "0–3% over")}
    assert w.STRONG_FAST_N == 25 and w.STRONG_FAST_HIT == 80.0
    assert all(p in w.PROFILES for p, _b in w.STRONG_FAST_CELLS)
    assert all(b in {x[2] for x in w.GAP_BANDS} for _p, b in w.STRONG_FAST_CELLS)
