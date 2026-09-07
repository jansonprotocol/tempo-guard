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
  * the OVER read is about ten points too hopeful while the lane still
    needs goals: O1.5 at 0-0 says 70.6 and lands 59.9, at one goal says
    91.6 and lands 82.6. A goalless half is evidence the card's mu was
    too high, and the arithmetic cannot know that, so an over that still
    needs goals carries OVER_HAIRCUT.
  * the second half carries SHARE of a match's goals.

The line is a READ for the bettor holding an in-play price against it,
the same way buy>= is a read for a pre-match price. It is not a verdict
and it moves no rule. See config/hypotheses.tsv, 7 Sep.
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
OVER_HAIRCUT = 0.10    # measured at HT: over reads land ~10 points under
STOPPAGE = 2           # minutes of goals still to come at 90'+

_MIN = re.compile(r"LIVE\s+(?:(?P<ht>HT)|(?P<m>\d+)'(?:\+\d+')?)")
_CLAIM = re.compile(r"(\d+(?:\.\d+)?)%")


def pois_cdf(k: int, lam: float) -> float:
    if k < 0:
        return 0.0
    return sum(math.exp(-lam) * lam ** i / math.factorial(i) for i in range(k + 1))


def mu_from_claim(lane: str, p: float) -> tuple[float, int, bool]:
    """(mu, K, under): the expected goals that make P(lane) == p on the
    ninety, the goal count the lane turns on, and its side. A quarter
    line is read at its nearest half: U4.25 lands at four (a half-win,
    which the board counts as a hit), O2.25 needs three."""
    line = float(lane[1:])
    under = lane[0] == "U"
    k = int(math.floor(line)) if under else int(math.floor(line)) + 1
    lo, hi = 0.05, 8.0
    for _ in range(60):
        mid = (lo + hi) / 2
        pu = pois_cdf(k, mid) if under else 1 - pois_cdf(k, mid)
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
    """The share of the ninety's goals still to come."""
    if not second:
        return (45 - min(minute, 45)) / 45 * (1 - SHARE) + SHARE
    left = max(90 + STOPPAGE - minute, 0) if minute >= 90 else 90 - minute
    return SHARE * min(left, 45) / 45


def read(cell: str, teams: str, status: str) -> Optional[dict]:
    """P(this lane lands from here) and its fair price, or None when the
    card is not running, the lane is not a total, or the claim is missing."""
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
