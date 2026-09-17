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

DECLINED CARDS ARE OUT of the counted pool, the same rule the Ask Athena
tiles use by default, so the number on a playable card is the number he
would get by typing the search himself.

A DECLINED CARD READS ITS OWN POOL. The first shipping gave it nothing —
a red card's colour is out of the record, so every cell came back empty
and a hundred and three cards on the board carried no grid at all (the
bettor: "some show nothing"). But "what have cards like this one landed"
has an answer for a red card too; it is simply an answer drawn from the
declined rows, which is the mode both search bars already have. So a
declined card is measured against declined cards, marked ⛔ on the face
so it can never be read as the record. Like against like, always: a
counted card never draws on a declined one and a declined card never
borrows the record's number.

THE LABEL IS NORMALISED ONTO THE CLAIM LADDER. The forward log froze
"super green" on thirteen cards stamped before 12 Sep, when the ladder
(guard_slices.LADDER) replaced the tier vocabulary — and the bank, which
is relabelled on every read, has no such colour. Those cards matched
nothing and showed nothing. Any label the bank does not carry is now
re-read off the card's own claim, exactly as bankrates.relabel does for
a bank row, so the two vocabularies can never silently miss each other
again.

This module PRICES NOTHING. No bar, no colour, no verdict, no hit rate
moves because of it — it reports what the record holds for a profile.
"""
from __future__ import annotations

import math
from functools import lru_cache

from scripts import bankrates as br
from scripts import cellrates

MIN_N = 20      # cards before a cell may quote a percentage

ALL = "*"       # the index key for "every league", beside the real codes

# The colours the bank stores. Anything else is an older vocabulary and
# gets re-read off the claim ladder.
KNOWN = ("green+", "green", "orange", "pink", "red", "super red", "released")


def band_ceiling(claim: float) -> int:
    """The "<83" a card claiming 82.3 sits under. Whole percents, the way
    the search box is typed: floor and step one."""
    return int(math.floor(claim)) + 1


def ladder_label(label: str | None, claim: float | None) -> str | None:
    """The card's colour in the vocabulary the BANK uses.

    A red, super red or released card keeps its label — that is the tier
    and the score, not a rung. Everything else the bank does not know is
    re-read off the claim ladder, which is where "super green" (the
    pre-12-Sep name for the top tier) comes back as the rung its claim
    actually sits on.
    """
    if not label or label in KNOWN:
        return label
    if label.endswith("red") or claim is None:
        return label
    from scripts.guard_slices import LADDER
    for name, floor in LADDER:
        if claim >= floor:
            return name
    return "pink"


@lru_cache(maxsize=1)
def _index() -> dict:
    """(pool, code, label, strikes, side) -> [(tip 1 claim, tip 1 hit,
    final pick hit)], with ALL in place of the code for the bank-wide
    population and `pool` "in" for the counted rows, "out" for the
    declined ones.

    A list rather than a table of counts, because the claim ceiling is a
    per-card number: there is no fixed set of bands to pre-total. The
    per-league lists are short and the ALL lists run to a few thousand,
    which is still nothing against the 684 cards that ask.

    The strike count is taken with each card's OWN code, so the score
    strike stays silent outside Europe exactly as it does on the board —
    the ALL bucket is a union of correctly-counted cards, never a
    recount under one league's rules.

    A row with no tip 1 claim is left out entirely rather than counted in
    the wide row only: the rungs have to be the same population minus one
    filter each, or the comparison the grid exists for is not a
    comparison.
    """
    out: dict = {}
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            lab = m.get("g") or ""
            if not lab:
                continue
            claim = br._claim(m.get("tip"))
            if claim is None:
                continue
            t1 = br._hit(m.get("mark"))
            star = br._hit(m.get("m3") if m.get("pk") == 3 else m.get("mark"))
            if t1 is None and star is None:
                continue
            row = (claim, t1, star)
            pool = "in" if br.counts(m, code) else "out"
            nst = len(br.strikes(m, code))
            side = cellrates.side_of(m.get("tip"))
            lab = ladder_label(lab, claim)
            # One key per population the ladder can ask for. Six shapes,
            # written once here rather than unioned at query time: the
            # card's own profile, then each way of loosening it.
            #   (colour, strikes, side)  the profile
            #   (colour, strikes, -)     either side
            #   (colour, -, -)           the colour alone
            #   (-, strikes, -)          this many strikes, any colour
            #   (-, -, side)             this side, any colour
            #   (-, -, -)                everything
            for cd in (code, ALL):
                for k in ((lab, nst, side), (lab, nst, None), (lab, None, None),
                          (None, nst, None), (None, None, side),
                          (None, None, None)):
                    out.setdefault((pool, cd) + k, []).append(row)
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


def rows(code: str, label: str | None, nstrikes: int, claim: float | None,
         side: str = "", declined: bool = False) -> list[dict]:
    """The card's profile, one rung per line, tightest first.

    THE LADDER DROPS ONE FILTER PER STEP, which is the whole design: a
    line is only worth reading against the line under it, and that is
    only a reading if exactly one thing changed.

        orange · 1 strike · under · tip 1 <82     the card, entire
        orange · 1 strike · under                 without the claim band
        orange · 1 strike                         without the side either

    The band goes first because it is the thinnest filter and the one he
    was researching; the side goes second because it is worth less —
    measured 16 Sep, dropping it moves the bank-wide number by a point or
    more on 204 of 657 board cards and by three or more on 60, against
    the band's 309 of 712 cells at three or more.

    A tip 1 that is not a totals lane has no side, and the bank holds no
    such row at all, so those cards get the two-rung ladder.

    THE LADDER KEEPS GOING WHEN THE LEAGUE CANNOT ANSWER INSIDE THE
    PROFILE. On 218 of 684 board cards no rung reached MIN_N in the
    league column — HamKam v Molde is orange with one strike on an over,
    and the Eliteserien has never had one — so the card printed two
    dashes and a count and the whole left-hand column said nothing (the
    bettor: "when a card doesn't have anything replace it with an
    alternate search that can say something more useful but still within
    its profile"). Two more rungs are appended, one at a time, only
    while nothing has reached the floor: the colour on its own, and then
    the whole record. They are the SAME LADDER continued — each still
    drops exactly one filter, so the nesting and the reading hold — and
    they rescue 121 and 66 of those cards. The last 31 are leagues the
    bank barely holds at all, and there the card says so.

    ADJACENT RUNGS THAT TALLY THE SAME ARE COLLAPSED. If the claim band
    cuts nothing — a card claiming 84.9 whose colour already ends at 85 —
    the two lines are one population printed twice, which is the fault
    the second column had before it became the bank-wide one. The looser
    label survives, because the looser label is the one that describes
    the population honestly.

    `declined` picks the pool: a declined card is measured against
    declined cards and a counted one against the record, never across.
    """
    label = ladder_label(label, claim)
    if not label or claim is None:
        return []
    pool = "out" if declined else "in"
    idx = _index()
    def pop(colour, nst, sd):
        return idx.get((pool, code, colour, nst, sd), []), \
               idx.get((pool, ALL, colour, nst, sd), [])

    cap = band_ceiling(claim)
    st = f"{nstrikes} strike{'s' if nstrikes != 1 else ''}" if nstrikes \
        else "no strikes"
    word = {"O": "over", "U": "under"}.get(side, "")
    base = f"{label} · {st}"
    # (label, colour, strikes, side, claim ceiling), tightest first. The
    # first four are the CHAIN — each drops exactly one filter from the
    # one above, so a line reads against its neighbour. The last three
    # are SLICES, not chain steps: they cross the profile rather than
    # loosen it, and they exist because a chain from four filters can
    # only be five rungs long, which is not enough to fill three lines
    # in a league the bank holds thinly.
    rungs = ([(f"{base} · {word} · tip 1 <{cap}", label, nstrikes, side, float(cap)),
              (f"{base} · {word}", label, nstrikes, side, None)] if word else
             [(f"{base} · tip 1 <{cap}", label, nstrikes, None, float(cap))])
    rungs += [(base, label, nstrikes, None, None),
              (f"{label} · any strikes", label, None, None, None)]
    CHAIN = len(rungs)
    if word:
        rungs.append((f"{word} · any colour", None, None, side, None))
    rungs.append((f"{st} · any colour", None, nstrikes, None, None))
    rungs.append(("every graded card", None, None, None, None))

    out: list[dict] = []
    for i, (lab, colour, nst, sd, ceiling) in enumerate(rungs):
        # Keep going until THREE rungs have a league cell thick enough to
        # quote (the bettor, 17 Sep: "let the search roll over till 3 are
        # filled"). Rung 4 and beyond are only reached because the ones
        # above it came back empty, so nothing is being skipped over.
        if i >= CHAIN and sum(1 for r in out if r["here"][1] >= MIN_N) >= 3:
            break
        if i >= CHAIN and not any(not r["wide"] for r in out):
            break
        here, everywhere = pop(colour, nst, sd)
        h, a = _tally(here, ceiling), _tally(everywhere, ceiling)
        if not h["t1"][1] and not a["t1"][1]:
            continue
        # ↓ marks a SLICE, not a chain step: "orange · any strikes" is
        # the chain dropping one more filter and reads against the line
        # above it, while "under · any colour" crosses the profile and
        # does not.
        row = dict(lab=lab, here=h["t1"], all=a["t1"], fp=h["fp"],
                   wide=i >= CHAIN)
        # Same population as the rung above it: that filter cut nothing,
        # so the tighter line goes and the looser label stays.
        if out and (out[-1]["here"], out[-1]["all"]) == (row["here"], row["all"]):
            out.pop()
        out.append(row)
    return out
