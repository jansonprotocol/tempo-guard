"""Reads config/ladder_rates.tsv — a rung's record, for the page.

    from scripts import ladderrates
    ladderrates.cell("NED-ED", "U4.5")
    -> {"base_n": 647, "base": 80.1, "given_n": 293, "given": 82.9,
        "tip": 2.9, "here": True}

The writer is scripts/ladder_rates.py and its header carries what the
numbers mean. This side only reads, falls back and formats.

THE FALLBACK IS THE WHOLE OF THE LOGIC HERE. A league the store barely
holds cannot answer for a rung — the Algerian Ligue 1 has 165 matches in
three seasons and a 50% rung there is worth plus or minus eight points —
so a cell under MIN_N is answered by the every-league row instead, and
the page is told which one it got (`here`). The alternative was printing
a number with no sample behind it, which is the one thing a rate on a
card must never do.

NOTHING HERE PRICES ANYTHING. These are frequencies, published for
reading; no bar, no claim and no hit rate moves because of them.
"""
from __future__ import annotations

from functools import lru_cache
from pathlib import Path

from scripts.ladder_rates import ALL, MIN_N, OUT, RUNGS, SEASONS  # noqa: F401


@lru_cache(maxsize=1)
def table() -> dict:
    """(code, rung) -> the row, or {} when the file has not been written."""
    if not OUT.exists():
        return {}
    out: dict = {}
    for ln in OUT.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 6:
            continue
        code, rung = p[0], p[1]
        out[(code, rung)] = dict(
            base_n=int(p[2]), base=float(p[3]),
            given_n=int(p[4]),
            given=float(p[5]) if p[5] else None,
            tip=float(p[6]) if len(p) > 6 and p[6] else None)
    return out


def cell(code: str, rung: str) -> dict | None:
    """This league's row for the rung, or the every-league row when the
    league is too thin to answer. `here` says which one came back."""
    t = table()
    row = t.get((code, rung))
    if row and row["base_n"] >= MIN_N:
        return dict(row, here=True)
    row = t.get((ALL, rung))
    return dict(row, here=False) if row else None


def line(code: str, rung: str) -> str:
    """"U4.5 lands 80% here" — the short form."""
    c = cell(code, rung)
    if not c:
        return ""
    where = "here" if c["here"] else "everywhere"
    return f"{rung} lands {c['base']:.0f}% {where}"


def given_rows() -> list[dict]:
    """Every rung Athena actually gives, most-given first: what it
    landed, what the rung lands anyway, and the difference.

    This is the split the bettor asked for (19 Sep): "instead of how
    often the tip 1 under/over hits, show how often the actual given
    ladder rung hits". One pooled number cannot say that the rung the
    board gives most often is the one its selection adds least to.
    """
    t = table()
    out = []
    for rung in RUNGS:
        row = t.get((ALL, rung))
        if not row or row["given_n"] < MIN_N or row["tip"] is None:
            continue
        # The stored `base` on the every-league row is a raw pooled
        # frequency across every league's matches; the cards are not
        # spread that way, so it is NOT the control for `given` and
        # printing the two side by side would read as a contradiction
        # (86.7 against 86.4, tip +1.2). The control is the matched base
        # the writer scored each card against, which is given − tip.
        out.append(dict(row, rung=rung, matched=row["given"] - row["tip"]))
    out.sort(key=lambda r: -r["given_n"])
    return out


def rates_for(code: str) -> tuple[dict, bool, int]:
    """(rung -> the rate it lands at, is it this league's own, n).

    One flag and one count for the whole card, because the fallback is a
    property of the LEAGUE's sample, not of a rung: if the store holds
    too little of a league to answer for one rung it holds too little to
    answer for any of them."""
    out, here, n = {}, True, 0
    for rung in RUNGS:
        c = cell(code, rung)
        if not c:
            continue
        out[rung] = round(c["base"], 1)
        here, n = c["here"], c["base_n"]
    return out, here, n
