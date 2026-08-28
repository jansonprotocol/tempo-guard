"""
Derive the README's headline numbers from the log instead of typing them.

The header carried `1 / 1 settled · 100%` while the completed table below it
held eleven graded fixtures. That is the same failure as the fixture tables
drifting out of kickoff order: a number maintained by hand next to the data it
describes, updated on some passes and not others, and wrong in the one place a
reader looks first.

So it is computed. Tip 1 and Tip 2 come from the completed table's own grading
marks — the tick that settles a fixture is the tick that moves the counter. The
bet line comes from `scripts/ledger.py`, which already settles the book through
real settlement fractions.

    python scripts/headline.py            rewrite the header from the tables
    python scripts/headline.py --check    exit 1 if it is stale, change nothing

The check runs in the test suite, so the header cannot silently go stale again.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import ledger, playable

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"

COMPLETED = "| Result | League | Teams | Tip 1 | Tip 2 | Kickoff |"


def tally() -> tuple[int, int, int, int]:
    """(tip1 hits, tip1 settled, tip2 hits, tip2 settled) from the log."""
    # Scoped below the playable block, which renders the same header — see
    # playable.source. Unscoped, this counts the block instead of the table.
    text = playable.source(README.read_text())
    body = text.split(COMPLETED, 1)[1] if COMPLETED in text else ""
    h1 = n1 = h2 = n2 = 0
    for ln in body.splitlines():
        if not ln.startswith("|") or ln.count("|") != 7 or "---" in ln:
            continue
        c = [x.strip() for x in ln.split("|")]
        status, tip2 = c[1], c[5]
        # A fixture the engine withheld is not a miss — it never took a view.
        if status.startswith("✅"):
            h1 += 1
            n1 += 1
        elif status.startswith("❌"):
            n1 += 1
        if tip2.startswith("✅"):
            h2 += 1
            n2 += 1
        elif tip2.startswith("❌"):
            n2 += 1
    return h1, n1, h2, n2


def bets() -> tuple[int, int, float]:
    """(hits, settled, ROI %) straight from the ledger's own settlement."""
    fixtures = ledger.read_fixtures()
    staked = returned = 0.0
    n = hits = 0
    for ln in ledger.BETS.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        name, rung, odds, side = parts[0], parts[1], float(parts[2]), parts[3]
        # A position cashed out at stake (cashout flag, column 5) is realised
        # money: it settles the moment it is flagged, at exactly 1.00x, no
        # matter what the fixture later does — the fixture's result belongs
        # to whatever bet replaced it, never to this one twice.
        if len(parts) > 4 and parts[4] == "1":
            staked += 1
            returned += 1.0
            n += 1
            hits += 1
            continue
        fx = fixtures.get(name)
        # ledger.bet_state is the one settlement gate: DNB and double
        # chance on the result, totals through settle_fraction, and a
        # clinched over settles from the live score without waiting.
        s = ledger.bet_state(rung, side, fx) if fx else None
        if s is None:
            continue
        returned += max(s, 0.0) * odds + (1 - abs(s))
        staked += 1
        n += 1
        hits += s >= 0
    roi = (returned / staked - 1) * 100 if staked else 0.0
    return hits, n, roi


COUNTER = "**Tip 1 — "


def counter_line() -> str:
    """The tally printed above the completed table.

    Same defect as the header had, one section lower: a count maintained by
    hand next to the rows it counts. It read 2/2 while eleven fixtures sat
    graded beneath it.
    """
    h1, n1, h2, n2 = tally()
    return f"**Tip 1 — {h1} / {n1}**   ·   **Tip 2 — {h2} / {n2}**"


def played() -> tuple[int, int, int, int]:
    """The same count over the lanes that could actually be bought.

    Deliberately a second number rather than a replacement for the first. The
    engine is judged on every fixture it priced, which is the only honest test
    of the model; the bankroll is judged on the subset carrying enough edge to
    be worth a stake. They answer different questions and they will not agree.
    """
    lanes = playable.collect(README.read_text())

    def side(which: int) -> tuple[int, int]:
        b = [r for r in lanes if r[3] == which]
        return (sum(1 for r in b if r[9] == "✅"),
                sum(1 for r in b if r[9] in ("✅", "❌")))

    return side(1) + side(2)


def _cell(h: int, n: int) -> str:
    return f"{h:3} / {n:<3}" + (f"{h / n * 100:6.1f}%" if n else "       ")


def render() -> str:
    h1, n1, h2, n2 = tally()
    p1, q1, p2, q2 = played()
    bh, bn, roi = bets()
    pct = h1 / n1 * 100 if n1 else 0.0
    return "\n".join([ln.rstrip() for ln in [
        f"## CURRENT CONFIRMED HITRATE: {pct:.1f}%",
        "",
        f"    lane                        Tip 1              Tip 2",
        f"    all matches            {_cell(h1, n1)}    {_cell(h2, n2)}",
        f"    played lanes  >+1%     {_cell(p1, q1)}    {_cell(p2, q2)}",
        f"    placed bets            {_cell(bh, bn)}    ROI {roi:+.1f}%",
        "",
        "**All matches** is the engine: every fixture priced, bet or not. "
        "**Played lanes** is the same count over the lanes with real edge — "
        "what was buyable, tracked in its own block below. **Placed bets** is "
        "the book. Derived by `python scripts/headline.py`, never typed · "
        "over/under markets only · live tips, not backtests",
    ]])


def main() -> None:
    """Retired from README duty — the board renders the header now.

    The spans this script owned still exist, but the completed table it
    tallied from does not, so running the old rewrite would zero the header.
    Deferring keeps old habits and old hooks harmless.
    """
    from scripts import board

    board.main()


if __name__ == "__main__":
    main()
