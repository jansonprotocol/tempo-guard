"""What cards like THIS one have landed — the card's own profile, in the
bank, in its own league.

    from scripts import cardgrid
    cardgrid.rows("NED-ED", "orange", 1, 82.3)
    -> [{"lab": "orange · 1 strike · tip 1 <83", "t1": (74, 94), "fp": (73, 93)},
        {"lab": "orange · 1 strike",             "t1": (...),    "fp": (...)}]

WHY THIS EXISTS (the bettor, 16 Sep). He had been typing the same search
into Ask Athena over and over — pick a league, then "orange, strike 1,
tip 1 <83" — reading the TIP 1 tile and the FINAL PICK · <colour> tile,
and then doing it again one filter looser to see whether the claim band
was carrying the number or the profile was. That is a question about the
card in front of him, so the card answers it: the same two searches, the
same two tiles, precomputed per card.

Two rows, because the pair is the reading. The tight row is the card's
whole profile — colour, strike count and claim band. The wide row drops
the claim band and keeps the rest, which is the control: if the tight
row is five points over the wide one, the band is doing the work; if
they agree, the band is noise and the profile is the signal. One number
alone cannot tell those apart, which is exactly why he was running the
search twice.

Two columns, because the tiles are the two lanes worth comparing: tip 1
is what the engine said first, the final pick is what it would actually
have staked. Where they part, the star is earning or losing its keep.

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


def band_ceiling(claim: float) -> int:
    """The "<83" a card claiming 82.3 sits under. Whole percents, the way
    the search box is typed: floor and step one."""
    return int(math.floor(claim)) + 1


@lru_cache(maxsize=1)
def _index() -> dict:
    """(code, label, strikes) -> [(tip 1 claim, tip 1 hit, final pick hit)].

    A list rather than a table of counts, because the claim ceiling is a
    per-card number: there is no fixed set of bands to pre-total. The
    lists are short — a league's biggest colour runs to a few hundred —
    so the scan per card costs nothing.

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
            key = (code, lab, len(br.strikes(m, code)))
            out.setdefault(key, []).append((claim, t1, star))
    return out


def _tally(rows, ceiling: float | None) -> dict:
    t1h = t1n = fph = fpn = 0
    for claim, t1, star in rows:
        if ceiling is not None and claim >= ceiling:
            continue
        if t1 is not None:
            t1n += 1
            t1h += t1
        if star is not None:
            fpn += 1
            fph += star
    return {"t1": (t1h, t1n), "fp": (fph, fpn)}


def rows(code: str, label: str | None, nstrikes: int,
         claim: float | None) -> list[dict]:
    """The two filtered lines for one card, tight first. [] when the
    card's profile holds nothing the record counts."""
    if not label or claim is None:
        return []
    pool = _index().get((code, label, nstrikes))
    if not pool:
        return []
    cap = band_ceiling(claim)
    st = f"{nstrikes} strike{'s' if nstrikes != 1 else ''}" if nstrikes \
        else "no strikes"
    out = []
    for lab, ceiling in ((f"{label} · {st} · tip 1 <{cap}", float(cap)),
                         (f"{label} · {st}", None)):
        r = _tally(pool, ceiling)
        if not r["t1"][1] and not r["fp"][1]:
            continue
        out.append(dict(lab=lab, **r))
    return out
