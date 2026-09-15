"""What this league's market has actually landed, by claim band.

    from scripts import cellrates
    cellrates.line("NED-ED", "U4.25 82.4% +2.4% · buy≥1.27")
    -> "80–85 band here · 80.9% on 236"

WHY THIS EXISTS (the bettor, 15 Sep). He lost two Dutch unders and asked
whether the board could have told him: do unders in the Dutch and German
leagues land as well as they price, and does a HIGHER claim inside one
market land better than a middling one?

The measurement said three things, and only one of them survived.

  1. The claim is already the landing estimate. Across the bank, kept
     cards, unders land 79.1 / 84.5 / 87.9 / 92.3 up the claim bands and
     overs 82.6 / 89.6 — monotone, and every band at or above its claim.
     There is nothing broken for a correction to fix.

  2. A market's GAP DOES NOT PERSIST. Split the bank in half by date and
     correlate each league x side cell's gap in one half against the
     other: r = +0.20, on a spread of about 3 points. Norwegian overs ran
     -8.0 then +0.8; Swiss unders -1.6 then -10.4. A rule that declined
     on the gap would decline noise, and out of sample the cards such a
     rule flagged as weak landed 85.6% against a 85.1% average — the
     warning points the WRONG WAY. An empirical landing table scored
     +0.00003 Brier against the raw claim, 95% CI [-0.00033, +0.00040].

  3. But the SPREAD inside a cell is real where it is steep. Dutch
     Eredivisie unders land 80.9% in the 80-85 band and 92.6% in 85-90 —
     a +11.7 point slope against the bank's +3.4 — and it holds in both
     halves of the record (82.1 -> 90.7, then 80.3 -> 100.0).

So this module PRICES NOTHING. It changes no bar, no colour, no verdict
and no hit rate; it is a sentence on the card saying what this league's
side has done in this claim band before, so the bettor buying an 82%
Dutch under can see that the band lands 81% and the one above it lands
93%. Point 2 is the reason it is not allowed to be more than a sentence.

Reads the bank through bankrates, so declined cards are already out and
the number on the card is the same record the tiles count.
"""
from __future__ import annotations

import re
from functools import lru_cache

from scripts import bankrates as br

# The claim bands. These REFINE webapp.claim_band's cuts rather than
# repeating them: every band here falls entirely inside one of the price
# bar's three, so a card can never sit in one band for its bar and a
# contradicting one for its history — but 85+ is split at 90 and 75–80
# at 75, because that is where the reading is. The bank's unders land
# 87.9% claiming 85–90 and 92.3% claiming 90 and up; rolling those into
# one number would hide the very spread the line exists to show.
BANDS = ((0.0, 75.0, "under 75"), (75.0, 80.0, "75–80"), (80.0, 85.0, "80–85"),
         (85.0, 90.0, "85–90"), (90.0, 101.0, "90+"))

MIN_CELL = 40      # cards before a league x side x band can speak
MIN_SIDE = 60      # cards before the league x side fallback can speak

SIDE = re.compile(r"(?:^|[^A-Za-z])([OU])\d")


def side_of(cell: str | None) -> str:
    """"O" or "U" for a totals lane, "" for a result lane (1X, DNB1...)."""
    m = SIDE.search((cell or "").replace("*", ""))
    return m.group(1) if m else ""


def band_of(claim: float | None) -> str | None:
    if claim is None:
        return None
    for lo, hi, name in BANDS:
        if lo <= claim < hi:
            return name
    return None


@lru_cache(maxsize=1)
def table() -> dict:
    """(code, side, band) and (code, side) -> dict(n, hit, says).

    Both keys live in one dict: the three-part key is the answer when the
    cell is thick enough, and the two-part key is the fallback.
    """
    acc: dict = {}
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            starred = m.get("pk") == 3
            cell = m.get("t3") if starred else m.get("tip")
            got = br._hit(m.get("m3") if starred else m.get("mark"))
            claim = br._claim(cell)
            s = side_of(cell)
            if got is None or claim is None or not s:
                continue
            if not br.counts(m, code):      # declined cards are out here too
                continue
            b = band_of(claim)
            for key in ((code, s, b), (code, s)):
                a = acc.setdefault(key, [0, 0, 0.0])
                a[0] += 1
                a[1] += bool(got)
                a[2] += claim
    return {k: dict(n=v[0], hit=v[1] / v[0] * 100, says=v[2] / v[0])
            for k, v in acc.items() if v[0]}


def cell(code: str, lane: str | None) -> dict | None:
    """What this league has done with this side at this claim band.

    Returns n / hit / says / gap / band / side / source, where source is
    "band" for the league x side x band cell and "side" for the whole
    league x side when the band is too thin to speak. None when neither
    clears its floor — a young league says nothing rather than guessing.
    """
    s = side_of(lane)
    claim = br._claim(lane)
    if not s or claim is None:
        return None
    b = band_of(claim)
    for key, floor, src in (((code, s, b), MIN_CELL, "band"),
                            ((code, s), MIN_SIDE, "side")):
        row = table().get(key)
        if row and row["n"] >= floor:
            return dict(row, band=b, side=s, source=src,
                        gap=row["hit"] - row["says"])
    return None


def line(code: str, lane: str | None) -> str | None:
    """The one short sentence the card prints, or None."""
    c = cell(code, lane)
    if not c:
        return None
    word = "over" if c["side"] == "O" else "under"
    where = f"{c['band']} band here" if c["source"] == "band" else f"{word}s here"
    return f"{where} · {c['hit']:.1f}% on {c['n']:,}"


def tip(code: str, lane: str | None, league: str = "") -> str:
    """The hover behind that sentence — what it is and what it is not."""
    c = cell(code, lane)
    if not c:
        return ""
    word = "overs" if c["side"] == "O" else "unders"
    what = (f"{league or code} {word} claiming {c['band']}%"
            if c["source"] == "band" else f"{league or code} {word}, every claim band")
    return (f"{what} have landed {c['hit']:.1f}% on {c['n']:,} bank cards, declined "
            f"cards out, against an average claim of {c['says']:.1f}% "
            f"({c['gap']:+.1f}). "
            + ("Too few cards in this claim band to read on its own, so this is "
               "the whole market. " if c["source"] == "side" else "")
            + "READ ONLY — it moves no bar, no colour and no hit rate. A market's "
              "gap against its claim does NOT carry from one period to the next "
              "(r = +0.20 between the two halves of the bank), so this is history "
              "to weigh, never a forecast: what it is good for is the SPREAD "
              "between bands, which is real where it is steep — Dutch unders land "
              "80.9% claiming 80–85 and 92.6% claiming 85–90.")
