"""What cards like THIS one have landed — the card's own profile, in its
league and everywhere.

    from scripts import cardgrid
    cardgrid.rows("NED-ED", "orange", 1, 82.3)
    -> [{"lab": "orange · 1 strike · tip 1 <83",
         "here": (89, 116), "all": (4021, 5012), "fp": (89, 116)},
        {"lab": "orange · 1 strike", ...}]

WHY THIS EXISTS (the bettor, 16 Sep). He had been typing the same search
into Ask Athena over and over — pick a league, then "orange, strike 1,
tip 1 <83" — and then doing it again one filter looser to see whether
the claim band was carrying the number or the profile was. That is a
question about the card in front of him, so the card answers it.

TWO ROWS, because the pair is the reading. The tight row is the card's
whole profile — colour, strike count and claim band. The wide row drops
the claim band and keeps the rest, which is the control: if the tight
row is five points under the wide one, the band is doing the work; if
they agree, the band is noise and the profile is. One number alone
cannot tell those apart, which is exactly why he was running the search
twice.

TWO COLUMNS: this league, then the same profile across every league.
The first shipping went out with the final pick in the second column,
which is what the Ask Athena tiles show — and inside a COLOUR-FILTERED
slice that is the same lane as tip 1 unless a gated DNB took the star,
which is 437 of 28,643 bank cards. The two columns printed a different
percentage on 140 of 853 rows and a card does not need one number twice.
The bank-wide column earns the space instead: it says whether a weak
line is this LEAGUE or this PROFILE, which is the question a league
number on its own can never answer. Tip 1 in both columns, so they are
the same measurement on two populations. The final pick is still carried
on every row for the hover — nothing was lost, it just left the face.

THE CLAIM BAND IS A CEILING, NOT A BUCKET — "tip 1 <83" for a card
claiming 82.3, exactly as he types it. A bucket (82–83) would be a
different question and a much thinner sample.

DECLINED CARDS ARE OUT, the same rule the Ask Athena tiles use by
default, so the number on the card is the number he would get by typing
the search himself. That is the whole point of precomputing it, and it
means a declined card has no grid: its own colour is out of the record.

This module PRICES NOTHING. No bar, no colour, no verdict, no hit rate
moves because of it — it reports what the record holds for a profile.
"""
from __future__ import annotations

import math
from functools import lru_cache

from scripts import bankrates as br

MIN_N = 20      # cards before a cell may quote a percentage

ALL = "*"       # the index key for "every league", beside the real codes


def band_ceiling(claim: float) -> int:
    """The "<83" a card claiming 82.3 sits under. Whole percents, the way
    the search box is typed: floor and step one."""
    return int(math.floor(claim)) + 1


@lru_cache(maxsize=1)
def _index() -> dict:
    """(code, label, strikes) -> [(tip 1 claim, tip 1 hit, final pick hit)],
    with ALL in place of the code for the bank-wide population.

    A list rather than a table of counts, because the claim ceiling is a
    per-card number: there is no fixed set of bands to pre-total. The
    per-league lists are short and the ALL lists run to a few thousand,
    which is still nothing against the 684 cards that ask.

    The strike count is taken with each card's OWN code, so the score
    strike stays silent outside Europe exactly as it does on the board —
    the ALL bucket is a union of correctly-counted cards, never a
    recount under one league's rules.

    A row with no tip 1 claim is left out entirely rather than counted in
    the wide row only: the two rows have to be the same population minus
    one filter, or the comparison the grid exists for is not a
    comparison.
    """
    out: dict = {}
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            lab = m.get("g") or ""
            if not lab or not br.counts(m, code):
                continue
            claim = br._claim(m.get("tip"))
            if claim is None:
                continue
            t1 = br._hit(m.get("mark"))
            star = br._hit(m.get("m3") if m.get("pk") == 3 else m.get("mark"))
            if t1 is None and star is None:
                continue
            row = (claim, t1, star)
            nst = len(br.strikes(m, code))
            out.setdefault((code, lab, nst), []).append(row)
            out.setdefault((ALL, lab, nst), []).append(row)
    return out


def _tally(rows, ceiling: float | None) -> dict:
    hh = hn = fh = fn = 0
    for claim, t1, star in rows:
        if ceiling is not None and claim >= ceiling:
            continue
        if t1 is not None:
            hn += 1
            hh += t1
        if star is not None:
            fn += 1
            fh += star
    return {"t1": (hh, hn), "fp": (fh, fn)}


def rows(code: str, label: str | None, nstrikes: int,
         claim: float | None) -> list[dict]:
    """The two filtered lines for one card, tight first. [] when the
    card's profile holds nothing the record counts."""
    if not label or claim is None:
        return []
    here = _index().get((code, label, nstrikes))
    everywhere = _index().get((ALL, label, nstrikes))
    if not here and not everywhere:
        return []
    cap = band_ceiling(claim)
    st = f"{nstrikes} strike{'s' if nstrikes != 1 else ''}" if nstrikes \
        else "no strikes"
    out = []
    for lab, ceiling in ((f"{label} · {st} · tip 1 <{cap}", float(cap)),
                         (f"{label} · {st}", None)):
        h = _tally(here or [], ceiling)
        a = _tally(everywhere or [], ceiling)
        if not h["t1"][1] and not a["t1"][1]:
            continue
        out.append(dict(lab=lab, here=h["t1"], all=a["t1"], fp=h["fp"]))
    return out
