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

    python scripts/futurematch.py --reprice
        Re-run every pending, not-yet-live row through the CURRENT engine
        and rewrite its tip cells. Run this after any engine change
        (constants, floors, debits): board rows are typed at slate time
        and do not move by themselves. Used three times on 27-28 Aug by
        hand before it was a script.

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
from scripts.two_tips import buy_value, tips


def _fmt_edge1(e: float) -> str:
    s = f"{e*100:+.1f}%".replace("-", "−")
    return s if e >= 0.01 else f"**{s}**"


def cell1(r: dict, lg: str) -> str:
    m, p, e = r["t1"]
    b = buy_value(m, r["mu"], p, e, lg)
    return f"{m} {p*100:.1f}% {_fmt_edge1(e)} · buy≥{b:.2f}"


def cell2(r: dict, lg: str, home: str, away: str) -> str:
    if not r["t2"]:
        return "— none"
    m, p, e, why = r["t2"]
    b = buy_value(m, r["mu"], p, e, lg)
    label, note = m, why
    if why == "team total":
        side = home if m.startswith("TA") else away
        label = f"**{side} {m.split()[-1]}**"
        note = "team"
    else:
        note = why.replace("-", "−")
    return (f"{label} {p*100:.1f}% {e*100:+.1f}% ({note}) · "
            f"buy≥{b:.2f}").replace("+−", "−")


def price(code: str, teams: str, day: str):
    """(tip1 cell, tip2 cell) for one fixture, or an abstention row."""
    h, a = (x.strip() for x in teams.split(" v ", 1))
    try:
        r = tips(code, h, a, date.fromisoformat(day))
    except Exception as exc:
        return f"— no tip: {exc}", "—"
    if r is None:
        return ("— no tip: engine abstained (thin history or an "
                "unresolved name)"), "—"
    return cell1(r, code), cell2(r, code, h, a)


def add_slate(path: Path) -> None:
    rows, playable = [], 0
    for ln in path.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        ko, code, league, teams = ln.split("\t")[:4]
        t1, t2 = price(code, teams, ko.split(" ")[0])
        if not t1.startswith("—"):
            edge = float(t1.split("%")[1].split()[-1].replace("−", "-")
                         .replace("**", "").lstrip("+"))
            playable += edge > 1.0
        rows.append(f"{ko}\t{code}\t{league}\t{teams}\t{t1}\t{t2}\t")
        print(f"  {teams}: {t1}")
    FIXTURES.write_text(FIXTURES.read_text().rstrip("\n") + "\n"
                        + "\n".join(rows) + "\n")
    print(f"{len(rows)} fixtures added · ~{playable} playable at tip 1")


def reprice() -> None:
    lines = FIXTURES.read_text().split("\n")
    changed = 0
    for f in load():
        if f.settled or f.status or " v " not in f.teams \
                or f.tip1.startswith("—"):
            continue                      # live and graded rows never move
        n1, n2 = price(f.code, f.teams, f.kickoff.split(" ")[0])
        if n1.startswith("—") or (n1 == f.tip1 and n2 == f.tip2):
            continue
        for i, ln in enumerate(lines):
            p = ln.split("\t")
            if len(p) == 7 and p[0] == f.kickoff and p[3] == f.teams:
                p[4], p[5] = n1, n2
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
        reprice()
    elif args:
        add_slate(Path(args[0]))
    else:
        print(__doc__)
        return
    from scripts import board
    board.main()


if __name__ == "__main__":
    main()
