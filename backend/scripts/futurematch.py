"""
Put a slate on the board — the second of the three operating commands.

Until today this command did not exist as a script: every slate was priced
by throwaway code, and the card-cell format ("U4.25 89.5% +5.3% ·
buy≥1.18", the bolding of sub-bar edges, the (team)/(floor −x)
annotations) lived only in scratchpad blocks that died with their session.
A format that four surfaces depend on was retyped from memory each time.
Now it lives here, once.

Two modes, both ending in a full render + verify so nothing can be added
half-way:

    python scripts/futurematch.py slate.tsv
        Price every fixture in the file and APPEND them to the board.
        Input rows:  kickoff<TAB>CODE<TAB>League name<TAB>Home v Away
        A fixture the engine abstains on is still added, with the reason
        printed in its Tip 1 cell — an abstention is an answer.

    python scripts/futurematch.py --reprice [--revive]
        Re-run every pending, not-yet-live row through the CURRENT engine
        and rewrite its tip cells. Run this after any engine change
        (constants, floors, debits): board rows are typed at slate time
        and do not move by themselves. Used three times on 27-28 Aug by
        hand before it was a script.

        An abstained row is skipped by default — an abstention is an
        answer, and re-asking it every run would let a card flicker into
        existence on noise. --revive re-asks the pending abstentions
        only, for the case the abstention was caused by a DATA defect
        since fixed: a name merge or a new alias makes history the engine
        never had available, so the old answer was to a different
        question. Settled and live rows are untouched either way.

What this command writes, precisely:
    config/fixtures.tsv        the new or re-priced rows
    README.md                  via board.main(): header, playable block,
                               pending and completed cards
    web/index.html             every tab, tiles, hero — same render
followed by board.verify(), which refuses a board where any surface
disagrees. Bets, ROI and hitrates are DERIVED at render time and need no
writing here.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts.board import FIXTURES, load
from scripts.two_tips import buy_parts, tips


def _fmt_edge1(e: float) -> str:
    s = f"{e*100:+.1f}%".replace("-", "−")
    return s if e >= 0.01 else f"**{s}**"


def _buy_str(m: str, mu: float, p: float, e: float, lg: str) -> str:
    """buy≥ on the blended probability, and in brackets the margin the
    printed price still holds over the tip's own break-even — negative
    when the blend reaches down to make a lane buyable."""
    b, mg = buy_parts(m, mu, p, e, lg)
    return f"buy≥{b:.2f} ({mg*100:+.1f}% margin)".replace("(-", "(−")


def cell1(r: dict, lg: str) -> str:
    m, p, e = r["t1"]
    return (f"{m} {p*100:.1f}% {_fmt_edge1(e)} · "
            f"{_buy_str(m, r['mu'], p, e, lg)}")


def cell2(r: dict, lg: str, home: str, away: str) -> str:
    if not r["t2"]:
        return "— none"
    m, p, e, why = r["t2"]
    label, note = m, why
    if why == "team total":
        side = home if m.startswith("TA") else away
        label = f"**{side} {m.split()[-1]}**"
        note = "team"
    else:
        note = why.replace("-", "−")
    return (f"{label} {p*100:.1f}% {e*100:+.1f}% ({note}) · "
            f"{_buy_str(m, r['mu'], p, e, lg)}").replace("+−", "−")


def cell3(r: dict) -> str:
    """The result lane's cell. Priced at plain break-even plus margin —
    the buy-blend stays out of it: the league's playable record describes
    totals lanes and says nothing about a double chance. Probation is
    printed on the cell, the same way the cup lane wore it."""
    if not r.get("t3"):
        return ""
    lane, p, e = r["t3"]
    from app.engine import pricing
    be = (1 / p) * (1 + pricing.DEFAULT_MARGIN)
    return (f"{lane} {p*100:.1f}% {e*100:+.1f}% · buy≥{be:.2f} · "
            f"probation")


def price(code: str, teams: str, day: str):
    """(tip1, tip2, tip3 cells) for one fixture, or an abstention row."""
    h, a = (x.strip() for x in teams.split(" v ", 1))
    try:
        r = tips(code, h, a, date.fromisoformat(day))
    except Exception as exc:
        return f"— no tip: {exc}", "—", ""
    if r is None:
        return ("— no tip: engine abstained (thin history or an "
                "unresolved name)"), "—", ""
    return cell1(r, code), cell2(r, code, h, a), cell3(r)


def add_slate(path: Path) -> None:
    rows, playable = [], 0
    for ln in path.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        ko, code, league, teams = ln.split("\t")[:4]
        t1, t2, t3 = price(code, teams, ko.split(" ")[0])
        if not t1.startswith("—"):
            edge = float(t1.split("%")[1].split()[-1].replace("−", "-")
                         .replace("**", "").lstrip("+"))
            playable += edge > 1.0
        rows.append(f"{ko}\t{code}\t{league}\t{teams}\t{t1}\t{t2}\t\t{t3}")
        print(f"  {teams}: {t1}" + (f"   ·   T3 {t3}" if t3 else ""))
    FIXTURES.write_text(FIXTURES.read_text().rstrip("\n") + "\n"
                        + "\n".join(rows) + "\n")
    print(f"{len(rows)} fixtures added · ~{playable} playable at tip 1")


def reprice(revive: bool = False) -> None:
    lines = FIXTURES.read_text().split("\n")
    changed = 0
    for f in load():
        if f.settled or f.status or " v " not in f.teams:
            continue                      # live and graded rows never move
        if f.tip1.startswith("—") and not revive:
            continue                      # an abstention is an answer
        n1, n2, n3 = price(f.code, f.teams, f.kickoff.split(" ")[0])
        if n1.startswith("—") or (n1 == f.tip1 and n2 == f.tip2
                                  and n3 == f.tip3):
            continue
        for i, ln in enumerate(lines):
            p = ln.split("\t")
            if len(p) in (7, 8) and p[0] == f.kickoff and p[3] == f.teams:
                p[4], p[5] = n1, n2
                # a 7-column row grows its result-lane column here
                p = p[:7] + [n3]
                lines[i] = "\t".join(p)
                changed += 1
                if f.tip1.split()[0] != n1.split()[0]:
                    print(f"  MARKET CHANGED  {f.teams}: "
                          f"{f.tip1.split()[0]} -> {n1.split()[0]}")
                break
    FIXTURES.write_text("\n".join(lines))
    print(f"{changed} pending rows re-priced on the current engine")


def main() -> None:
    args = [a for a in sys.argv[1:]]
    if "--reprice" in args:
        reprice(revive="--revive" in args)
    elif args:
        add_slate(Path(args[0]))
    else:
        print(__doc__)
        return
    from scripts import board
    board.main()


if __name__ == "__main__":
    main()
