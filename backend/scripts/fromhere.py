"""The "from here" line: what a running card's lane is worth NOW.

A totals card carries a claim, P(total <= K) or P(total >= K) over the
ninety. That claim pins the card's expected goals (mu) through the
Poisson it was priced from, and once the ball is rolling the goals still
to come are Poisson(mu x the share of the match left). So at a known
score and minute, "does this lane still land from here" and its fair
price are one line of arithmetic on a number the card already prints.
Nothing here touches the engine: the mu is read back out of the claim,
never re-derived.

MEASURED, 7 Sep 2026, at half time — the one in-play moment the store
records — on 16,458 bank totals cards joined to their half-time score:

  * the UNDER read is calibrated in every band. U4.5 at 0/1/2/3/4 goals
    says 97.9/92.8/79.5/54.0/16.1 and lands 98.9/94.3/80.7/54.8/15.0;
    U3.5 at 0/1/2/3 says 95.9/86.3/63.6/28.0 and lands 95.3/86.5/61.9/26.8.
  * the OVER read is exact once a goal is in and conservative before:
    O1.5 with one goal at half time says 82.7 and lands 82.6; at 0-0 it
    says 52.0 and lands 59.9. (The first cut read overs ten points
    hopeful — that was the back-solve one goal too strict, fixed 8 Sep,
    not the football.)
  * the second half carries SHARE of a match's goals.

THE LADDER, 19 Sep. The read was only ever pointed at the card's own
lane, and a running match walks away from that lane — it goes safe and
pays nothing, or it dies, or it lands and there is nothing left to hold
(the bettor: "it now only tracks the tip 1 lane ... up to the point it's
either insane or not worth the trouble"). `ladder()` prices the whole
totals ladder off the SAME mu at the same minute and keeps the rungs
whose fair price is worth a bet. Measured the same day on the 21,623
bank cards that carry a half-time score: with the card's own rung
excluded, every rung in the window reads within about a point —
O1.5 +1.4, O2.5 +1.0, O3.5 +0.1, O4.5 −0.0, U2.5 −0.2, U3.5 +0.5,
U4.5 +0.0, U5.5 +1.1 — so a mu back-solved from one rung prices its
neighbours honestly. Two rungs are treated apart: O0.5 reads +10.3
CONSERVATIVE (the 0-0 case the module already knew about, and the safe
side for a bettor), and U1.5 reads 3.2 points HOPEFUL on 6,143 cards,
which is the wrong side, so U1.5 is not offered at all.

THE SECOND HALF IS 52 MINUTES LONG, not 47 (19 Sep). Against the board's
own live log — 9,990 second-half observations on 380 settled cards —
goals still to come ran to 1.10x the read at 66-75', 1.29x at 76-85' and
2.16x past 85': the clock spent the half's goals over 45 minutes and
added two at the whistle, and a second half is longer than that. Fitting
the length that flattens the curve gives seven minutes, which is a
football number, not a curve: 0.98-1.02x in every bucket from the
restart to the last kick. Half time is untouched by it — the half still
carries SHARE and still has all of it left at 45' — so the 7 Sep
calibration above stands unchanged. By side, past the 76th minute, the
read went from +7.7 on overs and −5.1 on unders to +0.7 and +0.5.

The line is a READ for the bettor holding an in-play price against it,
the same way buy>= is a read for a pre-match price. It is not a verdict
and it moves no rule. See config/hypotheses.tsv, 7 and 19 Sep.
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import liveline

SHARE = 0.558          # second-half share of goals, measured 7 Sep
# No haircut on overs. The first cut carried ten points because the
# over read looked hopeful at half time — an artefact of back-solving
# the over's mu one goal too high (see mu_from_claim). Re-measured 8 Sep
# with the arithmetic right: O1.5 with one goal at half time says 82.7
# and lands 82.6; at 0-0 it says 52.0 and lands 59.9, eight points
# CONSERVATIVE, which is the safe side for a bettor holding a price.
OVER_HAIRCUT = 0.0
# How much longer than 45 minutes a second half runs. Fitted 19 Sep on
# the board's live log (see the header): the clock used to hold two
# minutes and spend them only past the 90th, which left the last quarter
# of an hour reading a third to a half of the goals it should. Seven
# minutes, spread across the whole half, flattens it. The half's SHARE
# of the goals does not change — it is spread over 52 minutes instead of
# 45, so half time reads exactly as it did.
STOPPAGE = 7
# The rungs the advisor may offer, and the prices worth offering them
# at. Whole and quarter lines are left out: a book prices a running
# match on the halves, and these are the rungs a bettor can actually
# take. U1.5 is out because it is the one rung the read prices HOPEFUL
# (−3.2 on 6,143 cards) — every other rung here lands within a point of
# what it says, or on the conservative side.
LADDER = ("O0.5", "O1.5", "O2.5", "O3.5", "O4.5",
          "U2.5", "U3.5", "U4.5", "U5.5")
SHORT = 1.25           # shorter than this and there is nothing to win
LONG = 3.00            # longer than this it is a different bet, not a rung
# Two rungs, longest price first (the bettor, 19 Sep: "keep it at max
# 1-2 with best value at the time"). Three filled the box and the two
# that survive are the ones worth looking at: every rung here is priced
# off the same mu, so none of them is better VALUE than another in the
# sense of beating its own fair price — what differs is the price on
# offer, and he said which end of the window he wants ("around 1.50 and
# everything above 1.75"). Longest first is that, and with two slots the
# short safe rescue still shows whenever it is one of only two.
ROWS = 2

_MIN = re.compile(r"LIVE\s+(?:(?P<ht>HT)|(?P<m>\d+)'(?:\+\d+')?)")
_CLAIM = re.compile(r"(\d+(?:\.\d+)?)%")


def pois_cdf(k: int, lam: float) -> float:
    if k < 0:
        return 0.0
    return sum(math.exp(-lam) * lam ** i / math.factorial(i) for i in range(k + 1))


def turns_on(lane: str) -> int:
    """The goal count a totals lane turns on.

    A quarter line is read at its nearest half: U4.25 lands at four (a
    half-win, which the board counts as a hit), O2.25 needs three. A
    WHOLE over line pushes on the line and the board counts a push as a
    hit, so O1.0 "lands" at one goal (the claim it prints is P(at least
    one) — 91.2 measured against 88.4 said on 433 Série B cards); a half
    or quarter over line needs the next goal up. Unders are the mirror:
    U3.0 and U4.25 both land at three and four."""
    line = float(lane[1:])
    if lane[0] == "U" or line == math.floor(line):
        return int(math.floor(line))
    return int(math.floor(line)) + 1


def mu_from_claim(lane: str, p: float) -> tuple[float, int, bool]:
    """(mu, K, under): the expected goals that make P(lane) == p on the
    ninety, the goal count the lane turns on, and its side."""
    under = lane[0] == "U"
    k = turns_on(lane)
    lo, hi = 0.05, 8.0
    for _ in range(60):
        mid = (lo + hi) / 2
        # under lands at <= k goals; over lands at >= k goals, which is
        # 1 - P(<= k-1). (The first cut had 1 - P(<= k) here, one goal
        # too strict, which back-solved every over's mu a goal too high
        # and made the over read look hopeful — found 8 Sep.)
        pu = pois_cdf(k, mid) if under else 1 - pois_cdf(k - 1, mid)
        if (pu > p) == under:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2, k, under


def minute_of(status: str) -> Optional[tuple[int, bool]]:
    """(minute, in second half) from a live status, or None."""
    m = _MIN.search(status or "")
    if not m:
        return None
    if m.group("ht"):
        return 45, True
    minute = int(m.group("m"))
    return minute, minute >= 45


def remaining_share(minute: int, second: bool) -> float:
    """The share of the ninety's goals still to come.

    The second half carries SHARE of them and runs 45 + STOPPAGE
    minutes, so at the restart all of SHARE is still to come and it
    drains to nothing at the last kick rather than at the 90th."""
    if not second:
        return (45 - min(minute, 45)) / 45 * (1 - SHARE) + SHARE
    half = 45 + STOPPAGE
    return SHARE * min(max(90 + STOPPAGE - minute, 0), half) / half


def _state(cell: str, teams: str, status: str) -> Optional[dict]:
    """Everything the arithmetic needs, read off a running card once:
    the lane's market, the expected goals its claim implies, the goals
    already scored towards it, and the minute. None when the card is not
    running, the lane is not a total, or the claim is missing."""
    sc = liveline.score_of(status or "")
    when = minute_of(status or "")
    if sc is None or when is None:
        return None
    got = liveline._goals_for(cell, teams, *sc)
    if got is None:
        return None
    market, goals = got
    cm = _CLAIM.search(cell)
    if not cm:
        return None
    claim = float(cm.group(1)) / 100
    if not 0.01 < claim < 0.995:
        return None
    mu, k, under = mu_from_claim(market, claim)
    mk = liveline._MARKET.search(cell)
    return dict(market=market, mu=mu, k=k, under=under, goals=goals,
                minute=when[0], second=when[1],
                team=bool(mk and mk.group("team")))


def read(cell: str, teams: str, status: str) -> Optional[dict]:
    """P(this lane lands from here) and its fair price, or None when the
    card is not running, the lane is not a total, or the claim is missing."""
    st = _state(cell, teams, status)
    if st is None:
        return None
    mu, k, under, goals = st["mu"], st["k"], st["under"], st["goals"]
    when = (st["minute"], st["second"])
    rem = mu * remaining_share(*when)
    if under:
        need = k - goals
        p = pois_cdf(need, rem) if need >= 0 else 0.0
    else:
        need = k - goals
        p = 1.0 if need <= 0 else max(0.0, 1 - pois_cdf(need - 1, rem) - OVER_HAIRCUT)
    return dict(p=p, fair=(1 / p if p > 0 else None), mu=mu,
                minute=when[0], needs=max(need, 0), under=under)


def line(cell: str, teams: str, status: str) -> str:
    """The short form for a card: "from here 53% · fair 1.88"."""
    r = read(cell, teams, status)
    if r is None:
        return ""
    if r["p"] <= 0.0:
        return ""                          # liveline already says "gone"
    if not r["under"] and r["needs"] == 0:
        return ""                          # liveline already says "landed"
    if r["p"] >= 0.995:
        return "from here: as good as landed"
    return f"from here {r['p'] * 100:.0f}% · fair {r['fair']:.2f}"


def rung_p(rung: str, mu: float, goals: int, minute: int, second: bool) -> float:
    """P(this rung lands from here) on a given expected-goals figure.

    Every rung on the LADDER is a half line, so there is no push to
    carry: an under wants at most k more goals, an over wants k+1."""
    k = turns_on(rung)
    need = k - goals
    lam = mu * remaining_share(minute, second)
    if rung[0] == "U":
        return pois_cdf(need, lam) if need >= 0 else 0.0
    return 1.0 if need <= 0 else max(0.0, 1 - pois_cdf(need - 1, lam))


def ladder(cell: str, teams: str, status: str) -> list[dict]:
    """The rungs that are still worth a price on this running match.

    THE CARD'S LANE IS ONE RUNG OF A LADDER and a match walks away from
    it. Thirty minutes of nothing and the under is 1.04 — right, and
    worth nothing to buy. A third goal and it is gone. An O1.5 lands and
    the card has nothing left to say. In every one of those the bettor
    is still watching a match he has a read on, and the read still
    prices the rest of the ladder: same mu, same minute, same score.

    THE PRICE WINDOW IS THE WHOLE SELECTION RULE. A rung is kept when
    its fair price falls between SHORT and LONG — nothing shorter,
    because there is nothing to win, and nothing longer, because that is
    a different bet rather than a change of rung. That one filter turns
    out to produce every move the bettor asked for by hand, without a
    rule per case: a stalled 0-0 late on offers O0.5 and nothing else, a
    1-1 in the second half offers the tighter under one side and the
    over the other, three goals in offers the under with room, and a
    card whose over has already landed offers the unders. The window
    finds them because they are the same thing — the rungs a running
    match has made interesting.

    The card's OWN rung never appears: the from-here line already prints
    it, and a card does not need one number twice. Own means SETTLES THE
    SAME, not spelled the same — U4.25 and U4.5 both turn on the fourth
    goal, O1.75 and O1.5 both on the second, and offering the bettor his
    own bet back under a rounder name is the one thing this must not do.

    A TEAM TOTAL GETS NOTHING. Its mu is one team's goals, and a match
    ladder priced off it would be arithmetic about the wrong football.

    Returns at most ROWS rungs, LONGEST PRICE FIRST — the window has
    already thrown out everything not worth a bet, so what is left is
    ordered by what it pays. This PRICES NOTHING: no bar, no colour, no
    verdict moves, and none of these rungs is a play. It is what a price
    would have to beat.
    """
    st = _state(cell, teams, status)
    if st is None or st["team"]:
        return []
    side, own = st["market"][0], st["k"]
    mu, goals = st["mu"], st["goals"]
    minute, second = st["minute"], st["second"]
    out = []
    for rung in LADDER:
        k = turns_on(rung)
        if rung[0] == side and k == own:
            continue
        p = rung_p(rung, mu, goals, minute, second)
        if p <= 0 or p >= 1:
            continue
        fair = 1 / p
        if not SHORT <= fair <= LONG:
            continue
        if rung[0] != side:
            how = "flip"
        elif (rung[0] == "U") == (k > own):
            how = "more room"
        else:
            how = "tighter"
        out.append(dict(rung=rung, p=p, fair=fair, how=how))
    out.sort(key=lambda r: -r["fair"])
    return out[:ROWS]


def holding(cell: str, teams: str, status: str) -> Optional[dict]:
    """The card's OWN rung inside the ladder box: what it is, where the
    score has left it, and what it is worth from here.

    WHY IT MOVED IN HERE (the bettor, 19 Sep, of the two gold lines the
    card used to carry under its lane: "this text in yellow can go, the
    new live viewer is the main live advisor"). The state and the
    from-here read were two loose lines saying something about a lane
    the box deliberately never quotes — the ladder skips the card's own
    rung so it does not print one number twice. Dropping them outright
    would have left the lane he may actually be holding as the only rung
    on the card with no live number at all, so they are one line inside
    the box instead of two outside it.

    `prog` is liveline's word for the score — "room for 2", "needs 1
    more", "✓ landed" — and it only changes when a sweep brings a goal;
    `p` and `fair` move with the clock like everything else here.
    """
    r = read(cell, teams, status)
    if r is None:
        return None
    st = _state(cell, teams, status)
    return dict(rung=st["market"], p=r["p"],
                fair=r["fair"] if r["p"] > 0 else None,
                prog=liveline.progress(cell, teams, status))


def ladder_cell(cells, teams: str, status: str) -> Optional[str]:
    """Which of a card's lanes carries the ladder — the first that can.

    The ladder is a statement about the MATCH, not about the lane it
    hangs under: a card whose tip 1 and tip 2 are both match totals
    priced the same match twice, and printed the same three rungs twice
    under each. One card, one ladder.

    A match total qualifies even when the price window comes back empty
    — the box still has the holding and the rung record to carry, and a
    running lane with nothing under it at all is what this replaced.
    """
    for c in cells:
        st = _state(c, teams, status)
        if st and not st["team"]:
            return c
    return None


def also(cell: str, teams: str, status: str) -> str:
    """The ladder in one line: "also live: U4.5 more room 67% · fair 1.49"."""
    rows = ladder(cell, teams, status)
    if not rows:
        return ""
    return "also live: " + " · ".join(
        f"{r['rung']} {r['how']} {r['p'] * 100:.0f}% fair {r['fair']:.2f}"
        for r in rows)
