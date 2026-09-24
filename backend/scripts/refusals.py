"""
The refusal ledger: what Athena's NO on a price has been worth.

THE BETTOR, 24 Sep, after Portugal v Wales was dropped on price (1.19
against a bar of 1.22) and finished 1-0: "Nevertheless, rejecting the
bet on value is also athena work."

He is right and the board had no way to show it. Every rate this
project publishes measures cards it PRICED — hit rate, ROI, the
playable badge. The cards it declined to buy leave no trace except an
absence, so the one output nobody could see was the refusal. This
script measures it, and the measurement says the refusal is a
different kind of judgement from a tip:

    A REFUSED CARD WINS MORE OFTEN AND PAYS LESS.

Measured on every settled card carrying a first-sight price stamp in
config/forward_log.tsv — the lane, the bar and the best quote as they
stood when the board first saw the card, so nothing here is re-priced
after the fact:

                    n     hit     ROI at the quote offered
    bought         230   75.4%          +1.6%
    refused        629   83.0%          -2.4%

Both halves agree (bought +0.4 / +2.6, refused -3.0 / -1.8). So the
refusal is NOT a forecast that the card will lose: refused cards land
eight points MORE often than the ones bought. It is a statement that
the price does not pay for the risk, and buying them anyway turns a
+1.6% book into a -2.4% one — a four-point swing on 859 cards.

TWO THINGS THAT MAKE IT STRONGER THAN IT LOOKS. The ROI is computed on
`best`, the best quote across the EU books, which is usually Pinnacle
and is not a price this bettor can reach ("I can go to pinnacle, but
I'll get NL/EU given prices"). A reachable price is shorter, so the
real return on a refused card is worse than -2.4%. And the window is
short — the forward log starts 1 Sep — so this is three weeks, not a
season.

ONE OPEN QUESTION, NAMED AND NOT ACTED ON. Split the refusals by how
far under the bar the quote sat, and the band nearest the bar is the
one where refusing has cost money:

    0-3% under   119   85.6%   +7.7%    (halves +16.0 / +2.4)
    3-5% under    91   74.7%   -8.4%    (halves -13.0 / -5.0)
    5%+ under    419   84.1%   -4.0%    (halves  -5.3 / -2.5)

That is the WATCH band (webapp.WATCH_BAND, 5%) cut in two, and the two
halves disagree about its size — +16.0 collapsing to +2.4 on 46 and 73
cards is what noise looks like, and the 3-5% band next door is
negative in both halves. No rule moves on this. It is written here so
the next run can see whether it survives.

Usage:  python scripts/refusals.py            print the ledger
        python scripts/refusals.py --bands    add the distance bands
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
FORWARD = ROOT / "config" / "forward_log.tsv"
MARKS = ("✅", "❌", "◦")


def grades() -> dict[tuple[str, str, str], str]:
    """(match date, fixture, which) -> the board's mark.

    Tip 1's mark lives in the STATUS column and tips 2 and 3 carry
    theirs on the cell — the one asymmetry that makes this join look
    empty if you read all three the same way.
    """
    from scripts.board import load
    out = {}
    for f in load():
        if not f.settled:
            continue
        day = f.kickoff.split(" ")[0]
        if f.status[:1] in MARKS and f.tip1 and not f.tip1.startswith("—"):
            out[(day, f.teams, "1")] = f.status[:1]
        for which, cell in ((2, f.tip2), (3, f.tip3)):
            c = (cell or "").strip()
            if c[:1] in MARKS:
                out[(day, f.teams, str(which))] = c[:1]
    return out


def rows() -> list[dict]:
    """One row per settled card that carried a first-sight price stamp."""
    g = grades()
    out = []
    for ln in FORWARD.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 13:
            continue
        mark = g.get((p[1], p[3], p[4]))
        if mark is None:
            continue
        try:
            bar, best = float(p[9]), float(p[11])
        except ValueError:
            continue
        if best <= 1.0:            # an unquoted lane is not a refusal
            continue
        out.append(dict(date=p[1], code=p[2], teams=p[3], which=p[4],
                        rung=p[5], label=p[7], bar=bar, best=best, mark=mark,
                        bought=best >= bar, under=(1 - best / bar) * 100))
    out.sort(key=lambda r: r["date"])
    return out


def score(rs: list[dict]) -> tuple[float | None, int, float | None, int]:
    """(hit%, graded n, ROI% at the quote offered, staked n). A push
    returns the stake, which is the board's own convention."""
    if not rs:
        return None, 0, None, 0
    live = [r for r in rs if r["mark"] in ("✅", "❌")]
    hit = (sum(1 for r in live if r["mark"] == "✅") / len(live) * 100
           if live else None)
    ret = sum(r["best"] if r["mark"] == "✅" else 1.0 if r["mark"] == "◦"
              else 0.0 for r in rs)
    return hit, len(live), (ret - len(rs)) / len(rs) * 100, len(rs)


def _line(name: str, rs: list[dict]) -> str:
    hit, hn, roi, n = score(rs)
    if not n:
        return f"  {name:22} —"
    return (f"  {name:22} n={n:4}  hit {hit:5.1f}% on {hn:4}  "
            f"ROI {roi:+6.1f}%")


def main() -> None:
    rs = rows()
    if not rs:
        print("no settled card carries a first-sight price stamp yet")
        return
    mid = rs[len(rs) // 2]["date"]
    bought = [r for r in rs if r["bought"]]
    refused = [r for r in rs if not r["bought"]]
    print(f"the refusal ledger — {len(rs)} settled cards stamped at first "
          f"sight, {rs[0]['date']} to {rs[-1]['date']}")
    print("ROI is what buying at the quote offered would have returned; a "
          "refused card that wins is a winner Athena turned down.")
    for name, pop in (("BOUGHT", bought), ("REFUSED", refused)):
        print(_line(name, pop))
        print(_line(f"  first half (<{mid})", [r for r in pop if r["date"] < mid]))
        print(_line("  second half", [r for r in pop if r["date"] >= mid]))
    if "--bands" in sys.argv:
        print("refusals by how far the quote sat under the bar:")
        for lab, lo, hi in (("0-3% under", 0, 3), ("3-5% under", 3, 5),
                            ("5%+ under", 5, 10 ** 6)):
            pop = [r for r in refused if lo <= r["under"] < hi]
            print(_line(lab, pop))
            print(_line("  first half", [r for r in pop if r["date"] < mid]))
            print(_line("  second half", [r for r in pop if r["date"] >= mid]))


if __name__ == "__main__":
    main()
