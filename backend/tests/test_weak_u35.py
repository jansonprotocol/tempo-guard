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
