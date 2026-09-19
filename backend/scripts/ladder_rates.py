"""How often each rung of the totals ladder actually lands, per league.

    python scripts/ladder_rates.py            print the table
    python scripts/ladder_rates.py --write    rewrite config/ladder_rates.tsv
    python scripts/ladder_rates.py --league NED-ED

WHY THIS EXISTS (the bettor, 19 Sep). The advisor started quoting rungs
the card does not carry — "O0.5 flip 67% fair 1.49" — and the obvious
next question is the one he would have typed into Ask Athena: how often
does O0.5 land in THIS league. There was nowhere to look it up. Nothing
on this board knew a rung's record; every hit rate it holds is a rate
for a CARD, and a card is one rung out of thirteen.

TWO NUMBERS PER CELL, because they answer two different questions and
the pair is the reading.

  BASE  — of every match this league played, how often the rung landed.
          Counted straight off the stored results, no cards involved.
          This is what the rung is worth before anyone picks anything,
          and it is the only number available for a rung Athena never
          gives (it gives four lines and no others).

  GIVEN — of the matches where Athena's tip 1 was a lane on this rung,
          how often the rung landed. The card's own record.

GIVEN MINUS BASE IS THE TIP, which is the whole point of writing them
side by side. If Athena's U4.5 calls land 88% in a league whose matches
land U4.5 86% of the time, the selection is worth two points; if they
match, the claim was the league talking. Matched league by league (see
`given`), that is what the bank says today:

    U4.5   15,624 cards   given 86.7   base 85.6   tip +1.2
    O1.5    7,114 cards   given 82.3   base 78.5   tip +3.7
    U3.5    4,803 cards   given 81.3   base 77.6   tip +3.6
    O0.5    1,072 cards   given 90.1   base 89.2   tip +0.9

CROSS-CHECKED ON THE ERA. The cards span 2022-2026 and the base window
is three seasons, so the control could in principle be measuring a
different football from the one the cards were priced in. Scoring every
card against its league's rate in its OWN season instead gives +1.1 /
+4.2 / +2.8 / +1.8 against the +1.2 / +3.7 / +3.6 / +0.9 above — the
same ordering and the same conclusion, within a point. The windows
barely differ because 96% of the bank is 2024 or later; the three-season
cells are the ones with a sample behind them, so they are what is kept.

THE SECOND REASON HE ASKED (his words: "more honest numbers to display
for the tip givers"). The board's headline says tip 1 lands about 83%,
pooled. Split by the rung actually given it runs 90.1 / 86.7 / 82.3 /
81.3 — an eight-point spread inside one number, and the ordering is not
the ordering of the tip: the rung that lands most often is the one the
selection adds least to. Athena gives U4.5 more than all the other
rungs together and beats the blind rung by a point on it. One number
cannot say any of that.

RE-SETTLED FROM THE FINAL SCORE, never from the card's mark. Athena
prices quarter and whole lines — U4.25, U3.0, O1.0 — and their marks do
not mean what a half line means: U3.0 PUSHES at exactly three goals,
where a true U3.5 wins outright. Reading marks would quietly book those
as no-result and cost U3.5 its best cases. So every cell asks the same
question of both populations — did the RUNG land — and the answer comes
off the stored total. The rate says whether it landed, not what it paid.

THREE SEASONS, measured rather than chosen. Football has got more
goal-heavy and a league's rung rate drifts with it: predicting a held-out
season's rate from the seasons before it, over 111 held-out
league-seasons and seven rungs each, the mean absolute error runs 3.76
points from one season, 3.21 from two, 3.11 from three, 3.32 from five
and 3.53 from the whole history. Three is the floor of that curve. Note
what the curve also says: even at its best this is a THREE-POINT number,
so it is context and never a price.

DECLINED CARDS ARE OUT of the given column, the same rule every other
displayed rate on this board uses.

This module PRICES NOTHING. It is written to a config file of its own
that the engine never reads — config/league_hitrates.tsv and
config/guard_slices.tsv are engine inputs and are not touched here.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import bankrates as br
from scripts import fromhere, liveline

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "ladder_rates.tsv"

# The rungs a book prices a running match on, as the bettor listed them.
RUNGS = [f"O{i}.5" for i in range(6)] + [f"U{i}.5" for i in range(7)]
SEASONS = 3     # the window, fitted 19 Sep — see the header
ALL = "*"       # the every-league row, beside the real codes
MIN_N = 60      # matches before a cell is worth quoting


def lands(rung: str, total: int) -> bool:
    line = float(rung[1:])
    return total < line if rung[0] == "U" else total > line


def base() -> dict:
    """(code, rung) -> (hits, n) over the league's last SEASONS seasons."""
    from app.data import store
    out: dict = {}
    for code in store.available_leagues():
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        d = df.dropna(subset=["hg", "ag"])
        if "season" in d:
            keep = sorted(s for s in d["season"].dropna().unique())[-SEASONS:]
            if keep:
                d = d[d["season"].isin(keep)]
        totals = [int(h) + int(a) for h, a in zip(d["hg"], d["ag"])]
        for rung in RUNGS:
            hit = sum(1 for t in totals if lands(rung, t))
            for cd in (code, ALL):
                h, n = out.get((cd, rung), (0, 0))
                out[(cd, rung)] = (h + hit, n + len(totals))
    return out


def given(b: dict) -> dict:
    """(code, rung) -> (hits, n, matched base): the cards Athena actually
    gave on that rung, re-settled from the final score, carrying the
    base rate of each card's OWN league.

    THE MATCHED BASE IS WHY THIS CARRIES A THIRD FIGURE. Pooled straight,
    the every-league row said Athena's O1.5 beat the rung by +8.3 and its
    O0.5 lost to it by −2.3, and both were league mix rather than
    football: the cards are not spread across leagues the way the matches
    are, so a pooled base is a different population, not a control.
    Scored against each card's own league the same numbers read +3.7 and
    +0.9. A per-league row is matched by construction; the every-league
    row has to be told.
    """
    out: dict = {}
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            tip = m.get("tip") or ""
            mk = liveline._MARKET.search(tip)
            if not mk or mk.group("team"):
                continue              # a team total is not a match rung
            if not br.counts(m, code):
                continue              # declined cards are out of the record
            sc = liveline.score_of(m.get("score") or "")
            if sc is None:
                continue
            market = mk.group("mk") or mk.group("mk2")
            k = fromhere.turns_on(market)
            rung = f"U{k}.5" if market[0] == "U" else f"O{k - 1}.5"
            if rung not in RUNGS:
                continue
            hit = lands(rung, sc[0] + sc[1])
            bh, bn = b.get((code, rung), (0, 0))
            for cd in (code, ALL):
                h, n, s = out.get((cd, rung), (0, 0, 0.0))
                out[(cd, rung)] = (h + hit, n + 1,
                                   s + (bh / bn if bn else 0.0))
    return out


def rows() -> list[tuple]:
    """(league, rung, base_n, base, given_n, given, tip), tip being the
    league-matched worth of the selection or None where it cannot be
    said."""
    b = base()
    g = given(b)
    out = []
    for (code, rung), (bh, bn) in sorted(b.items()):
        if not bn:
            continue
        gh, gn, gs = g.get((code, rung), (0, 0, 0.0))
        tip = None
        if gn and gs:
            tip = (gh / gn - gs / gn) * 100
        out.append((code, rung, bn, bh / bn * 100,
                    gn, gh / gn * 100 if gn else None, tip))
    return out


HEADER = """\
# How often each rung of the totals ladder lands, per league. Written by
# scripts/ladder_rates.py, refreshed by scripts/board.py when it is a day
# old; read by scripts/ladderrates.py for the app and the README.
#
# DISPLAY ONLY. The engine never reads this file. config/league_hitrates.tsv
# and config/guard_slices.tsv are engine inputs and nothing here touches them.
#
#   base_n/base    every match the league played in its last {seasons}
#                  seasons, and how often the rung landed in them. No
#                  cards: this is the rung before anyone picks anything.
#   given_n/given  the matches where Athena's tip 1 was a lane on this
#                  rung, and how often the rung landed. Declined cards
#                  are out, as in every other rate on this board.
#   tip            what the selection was worth: given against the base
#                  of each card's OWN league. On a league row that is
#                  just given minus base; on the "{all}" row it is scored
#                  card by card, because the cards are not spread across
#                  leagues the way the matches are and a pooled base is
#                  a different population rather than a control. Pooled
#                  straight, O1.5 read +8.3 and O0.5 −2.3; matched, they
#                  read +3.7 and +0.9.
#
# given MINUS base is the selection's worth. Both are re-settled from the
# final score rather than read off the card's mark, because a quarter or
# whole line does not settle the way a half line does (U3.0 PUSHES at
# three goals where U3.5 wins) and the two columns have to be the same
# question asked of two populations.
#
# The window is three seasons, fitted: predicting a held-out season from
# the seasons before it, the mean absolute error runs 3.76 points from
# one season, 3.21 from two, 3.11 from three, 3.32 from five and 3.53
# from the whole history. Even at its best this is a three-point number.
# It is context, never a price.
#
# "{all}" is every league pooled. A missing given column means Athena has
# never given that rung there.
# league\trung\tbase_n\tbase\tgiven_n\tgiven\ttip
"""


def write() -> int:
    body = "".join(
        f"{code}\t{rung}\t{bn}\t{bp:.1f}\t{gn}\t"
        f"{'' if gp is None else f'{gp:.1f}'}\t"
        f"{'' if tp is None else f'{tp:+.1f}'}\n"
        for code, rung, bn, bp, gn, gp, tp in rows())
    OUT.write_text(HEADER.format(seasons=SEASONS, all=ALL) + body)
    return body.count("\n")


def main() -> None:
    if "--write" in sys.argv:
        print(f"ladder rates written: {write()} cells -> {OUT}")
        return
    want = (sys.argv[sys.argv.index("--league") + 1]
            if "--league" in sys.argv else ALL)
    print(f"{'rung':6} {'base_n':>7} {'base':>7} {'given_n':>8} {'given':>7} "
          f"{'tip':>6}")
    for code, rung, bn, bp, gn, gp, tp in rows():
        if code != want:
            continue
        tip = f"{tp:+.1f}" if tp is not None and gn >= MIN_N else ""
        print(f"{rung:6} {bn:7d} {bp:7.1f} {gn:8d} "
              f"{'' if gp is None else f'{gp:7.1f}'} {tip:>6}")


if __name__ == "__main__":
    main()
