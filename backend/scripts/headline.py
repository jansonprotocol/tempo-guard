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

from scripts import ledger

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"

COMPLETED = "| Result | League | Teams | Tip 1 | Tip 2 | Kickoff |"


def tally() -> tuple[int, int, int, int]:
    """(tip1 hits, tip1 settled, tip2 hits, tip2 settled) from the log."""
    text = README.read_text()
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
        fx = fixtures.get(name)
        if fx is None or fx["hg"] is None:
            continue
        goals = (fx["hg"] + fx["ag"]) if side == "-" else (
            fx["hg"] if side == "H" else fx["ag"])
        s = ledger.pricing.settle_fraction(rung, goals)
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


def render() -> str:
    h1, n1, h2, n2 = tally()
    bh, bn, roi = bets()
    pct = h1 / n1 * 100 if n1 else 0.0
    head = [f"## CURRENT CONFIRMED HITRATE: {pct:.1f}%", ""]
    line = f"**Tip 1 {h1} / {n1} settled**"
    if n2:
        line += f" · **Tip 2 {h2} / {n2}**"
    if bn:
        line += f" · **bets {bh} / {bn}, ROI {roi:+.1f}%**"
    head.append(line + " · over/under markets only · live tips, not backtests")
    return "\n".join(head)


def main() -> None:
    text = README.read_text()
    start = text.index("## CURRENT CONFIRMED HITRATE")
    end = text.index("live tips, not backtests") + len("live tips, not backtests")
    new = text[:start] + render() + text[end:]

    # And the tally above the completed table, from the same count.
    cs = new.index(COUNTER)
    ce = new.index("\n", cs)
    new = new[:cs] + counter_line() + new[ce:]
    if "--check" in sys.argv:
        if new != text:
            print("README headline is STALE. Run: python scripts/headline.py")
            print("  want:", render().replace("\n", " | "))
            sys.exit(1)
        print("headline matches the log")
        return
    if new == text:
        print("headline already current")
        return
    README.write_text(new)
    print(render())


if __name__ == "__main__":
    main()
