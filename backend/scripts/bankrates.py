"""Hit rates for DISPLAY, derived from the bank with the declined cards out.

The bettor's rule, 7 Sep: a red or super-red card is never allowed to be
played, so it is never allowed into a hit rate either. It stays in the
bank for analysis and is still graded — it just does not count. Since
12 Sep the same goes for every card the board DECLINES by its live tag:
an unstaked card — an Athena lane, or a watch card a few percent short
of its price bar — whose lane and printed-edge band the board's last
three weeks have measured as live unsafe (scripts/livebands.py). The
bettor: "everything that now is a declined card must be removed from
what now actual hitrates are. Remove them out of bank hitrates and
session hitrates."

Everything here is read from config/matchbank_retro.json, which already
carries the guard's label on every card (matchbank.guard writes it) and,
where a closing price exists, the verdict at that price — so a bank card
has a lane the way a board card does, and the same tag.

TWO FILES ARE DELIBERATELY NOT TOUCHED. config/league_hitrates.tsv is an
ENGINE input — market_select reads its hit and play_hit columns for the
REL debit and the buy-from blend — and config/guard_slices.tsv is the
confluence score's table. Recomputing either without the declined cards
would change what the engine says, which is a rules change, and PRE-ALFA
2 is a no-touch run. So the display reads THIS module and the engine
goes on reading its own files; the two are allowed to differ, and the
difference is exactly the declined cards.

Every reader falls back to the typed file when the bank is missing, so a
fresh checkout with no bank still renders.
"""
from __future__ import annotations

import json
import re
from functools import lru_cache
from pathlib import Path

from scripts import livebands

ROOT = Path(__file__).resolve().parents[2]
BANK = ROOT / "config" / "matchbank_retro.json"

CLAIM = re.compile(r"(\d+(?:\.\d+)?)%")
EDGE = re.compile(r"%\s*\**([+\-−]\d+(?:\.\d+)?)%")
GRADED = ("✅", "❌", "◦")
PLAYABLE_EDGE = 1.0     # the board's playable bar: printed edge >= +1%

_BANK: dict | None = None


def bank() -> dict:
    global _BANK
    if _BANK is None:
        _BANK = json.loads(BANK.read_text()) if BANK.exists() else {}
    return _BANK


def not_red(m: dict) -> bool:
    """The 7 Sep rule on its own: a labelled red card does not count."""
    g = m.get("g") or ""
    return not g.endswith("red")


def lane(m: dict) -> str | None:
    """The bank card's lane for the live tag, read the way the board reads
    a live card: 'priced' where the closing price cleared the bar,
    'watch' where it fell inside the watch band of it, 'athena' for the
    rest — a card the price never cleared, or one no closing price
    reached, which is where an unquoted board card files too. None for
    a red card and for an unlabelled one."""
    g = m.get("g") or ""
    if not g or g.endswith("red"):
        return None
    v = m.get("v")
    if v in ("normal", "strong"):
        return "priced"
    if v == "no play" and m.get("bp") and m.get("need"):
        from scripts.webapp import WATCH_BAND
        if m["bp"] >= m["need"] * (1 - WATCH_BAND):
            return "watch"
    return "athena"


def tag(m: dict) -> str | None:
    """The bank card's live-safety word, by its lane and the printed edge
    of its starred lane (tip 3 where the DNB gate took it, else tip 1)."""
    ln = lane(m)
    if not ln:
        return None
    cell = m.get("t3") if m.get("pk") == 3 else m.get("tip")
    return livebands.tag(ln, _edge(cell))


def flip(m: dict) -> str | None:
    """"tip 3" / "tip 2" when this bank card is an unsafe priced play whose
    pill would read flipped (livebands.flip), else None."""
    return livebands.flip(lane(m), tag(m), bool(m.get("t2")), bool(m.get("t3")))


def counts(m: dict) -> bool:
    """The one predicate, the bank's copy of webapp.counts: a labelled
    red card does not count, and neither does an unstaked card — Athena
    lane or watch — tagged live unsafe. A priced play counts whatever
    its tag says, and an unlabelled card always did."""
    if not not_red(m):
        return False
    return not (lane(m) in ("athena", "watch") and tag(m) == "unsafe")


def _claim(cell: str | None) -> float | None:
    mm = CLAIM.search(cell or "")
    return float(mm.group(1)) if mm else None


def _edge(cell: str | None) -> float | None:
    """The printed edge after the claim: "U4.25 84.0% **−0.5%** …" -> -0.5."""
    mm = EDGE.search(cell or "")
    return float(mm.group(1).replace("−", "-")) if mm else None


def _hit(mark: str | None) -> bool | None:
    """True/False for a graded lane, None for an ungraded one. A push is a
    hit — the board's convention everywhere."""
    if mark not in GRADED:
        return None
    return mark != "❌"


@lru_cache(maxsize=1)
def display_rates() -> dict[str, dict]:
    """Per league: n, hit, says, gap, play_hit, play_n — declined cards out.

    Mirrors the columns of league_hitrates.tsv so the badge and the
    Retrosim row can be built the same way from either source.

    CACHED, and it matters: this walks the whole bank (32,000 cards), and
    the league badge on every card asked for it afresh — 1,813 times per
    render on 7 Sep, 214 of the render's 222 profiled seconds, which is
    what made the live sweep's passes take two minutes each. The bank
    does not change during a render, so the answer is computed once.
    """
    out = {}
    for code, comp in bank().items():
        n = h = 0
        says = 0.0
        pn = ph = 0
        for m in comp.get("matches", []):
            if not counts(m):
                continue
            got = _hit(m.get("mark"))
            if got is None:
                continue
            c = _claim(m.get("tip"))
            n += 1
            h += got
            says += (c or 0) / 100
            # PLAYABLE means what league_hitrates.tsv always meant: tip 1
            # above the +1% edge bar — hundreds of cards per league. Not
            # the priced plays (v normal/strong): those are the fifty-odd
            # cards per league that cleared the PRICE bar at closing, the
            # market-paid-long corner the gap finding describes, and on
            # 7-8 Sep the badge was reading them — Turkey (64.7 −20.1) on
            # 51 cards while its cards land 84.8 across 619.
            e = _edge(m.get("tip"))
            if e is not None and e >= PLAYABLE_EDGE:
                pn += 1
                ph += got
        if n:
            out[code] = dict(n=n, hit=h / n, says=says / n,
                             gap=(h / n) - (says / n),
                             play_hit=(ph / pn) if pn else None, play_n=pn)
    return out


@lru_cache(maxsize=4)
def baselines(n: int = 300, min_n: int = 30) -> dict[str, dict] | None:
    """Per league, over its most recent `n` bank cards, declined cards out:
    hits/count/summed-claim for fp, t1, t2, t3 — the shape baselines.tsv
    carries, so the hero bar and the tier table read either source alike.
    """
    if not bank():
        return None
    out = {}
    for code, comp in bank().items():
        recent = sorted(comp.get("matches", []), key=lambda x: x.get("d", ""))[-n:]
        acc = {k: [0, 0, 0.0] for k in ("fp", "t1", "t2", "t3")}
        for m in recent:
            if not counts(m):
                continue
            for key, mark, cell in (("t1", m.get("mark"), m.get("tip")),
                                    ("t2", m.get("m2"), m.get("t2")),
                                    ("t3", m.get("m3"), m.get("t3"))):
                got = _hit(mark)
                if got is None:
                    continue
                acc[key][1] += 1
                acc[key][0] += got
                acc[key][2] += (_claim(cell) or 0)
            star_mark = m.get("m3") if m.get("pk") == 3 else m.get("mark")
            star_cell = m.get("t3") if m.get("pk") == 3 else m.get("tip")
            got = _hit(star_mark)
            if got is not None:
                acc["fp"][1] += 1
                acc["fp"][0] += got
                acc["fp"][2] += (_claim(star_cell) or 0)
        if acc["t1"][1] >= min_n:
            out[code] = acc
    return out or None
