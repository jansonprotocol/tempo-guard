"""
The lanes that were actually buyable, filtered out of the two fixture tables.

The log measures the ENGINE: every fixture Athena priced, whether or not there
was a bet in it. That number answers "is the model right" and it should keep
being tracked as it is. It does not answer the question the bankroll cares
about, because roughly a third of published tips carry an edge near zero — they
are the base rate wearing a probability, and they are correctly skipped.

So this derives a third view over the SAME rows: every lane, Tip 1 and Tip 2
alike, carrying a positive edge. One fixture can contribute two lanes, one, or
none. Nothing is re-predicted and nothing is typed — the block is generated from
the tables, so it cannot drift out of step with them the way a hand-maintained
count does.

    python scripts/playable.py            rewrite the block from the tables
    python scripts/playable.py --check    exit 1 if stale, change nothing

MIN_EDGE is the filter, and it is deliberately a single constant rather than a
judgement made row by row. It sits at **+1.0%**, not at zero, and the reason is
measured: over 7,576 tips on 23 Aug the band under +1% stated edge delivered
**+0.3 points** of real edge over base rate, against +1.7, +2.7 and +4.3 for the
bands above it. A lane at +0.4% is arithmetically positive and worth nothing —
it is the base rate wearing a probability, and counting it would put lanes in
this block that no one would sensibly buy.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"

MIN_EDGE = 1.0

PENDING = "| Live | League | Teams | Tip 1 | Tip 2 | Kickoff |"
COMPLETED = "| Result | League | Teams | Tip 1 | Tip 2 | Kickoff |"
# ANCHOR, not HEADING, is what finds the existing block. Raising MIN_EDGE
# changes the heading text, and matching on the full heading meant the rewrite
# could not see the block it had written under the old threshold: it left the
# stale one in place and appended a second. A prefix cannot go stale that way.
ANCHOR = "## Playable lanes"
HEADING = f"{ANCHOR} — edge above +{MIN_EDGE:.0f}%"
# The block sits at the top, above the two tables it is derived from. It reads
# backwards on the page — the summary before its source — and that is the point:
# it is the number the bankroll acts on, so it is the one that should be read
# first.
NEXT = "## Pending FUTURE match bettips"

# "U4.25 80.6% +1.4% · buy≥1.33"  ·  "**Burnley O0.5** 80.5% +11.4% (team)"
# The market may carry a team prefix and either half may be bold, so the label
# is taken as everything before the probability rather than matched positively.
LANE = re.compile(
    r"^(?:✅|❌)?\s*\**\s*(.*?)\s*\**\s*(\d+(?:\.\d+)?)%\s*\**\s*"
    r"([+\-−]\d+(?:\.\d+)?)%")
BUY = re.compile(r"buy≥([\d.]+)")


def source(text: str) -> str:
    """The region the two fixture tables live in: everything below the block.

    The block renders the SAME six columns as the completed table, on purpose,
    and it now sits above it. So an unscoped search for that header finds the
    block's own header first and the block starts feeding on its own output —
    self-reference that survives `--check`, because a block derived from itself
    is trivially up to date. Everything derived here reads strictly below the
    heading the block ends at.
    """
    return text[text.index(NEXT):] if NEXT in text else text


def rows_of(text: str, header: str) -> list[list[str]]:
    """The rows of one table, stopping at the blank line that ends it.

    The stop is unconditional and that matters: an earlier version only broke
    out once it had collected a row, so an EMPTY pending table did not end the
    scan — it ran on into the completed table below and counted every fixture
    there a second time. Both tables are empty on a fresh slate, which is
    exactly when the block is first built.
    """
    text = source(text)
    if header not in text:
        return []
    out = []
    # [0] is the tail of the header line itself, always empty; the table starts
    # on the line after it.
    for ln in text.split(header, 1)[1].splitlines()[1:]:
        if not ln.startswith("|"):
            break
        if ln.count("|") == 7 and "---" not in ln:
            out.append([x.strip() for x in ln.split("|")])
    return out


def lane(cell: str, status: str, which: int) -> tuple | None:
    """(label, probability, edge, buy-from, result) or None if not playable."""
    m = LANE.match(cell)
    if not m:
        return None
    label = m.group(1).strip(" *·")
    if not label or label.startswith("—"):
        return None
    edge = float(m.group(3).replace("−", "-"))
    if edge <= MIN_EDGE:
        return None
    bf = BUY.search(cell)
    # Tip 1's result is the status cell; Tip 2 carries its own tick.
    # A push grades as a win: the standing offset plays U3.0 as U3.5, so
    # the actual bet wins where the printed rung pushes.
    if which == 1:
        res = ("✅" if status.startswith(("✅", "◦")) else
               "❌" if status.startswith("❌") else None)
    else:
        res = ("✅" if cell.startswith(("✅", "◦")) else
               "❌" if cell.startswith("❌") else None)
    return (label, m.group(2), edge, bf.group(1) if bf else None, res)


def collect(text: str) -> list[tuple]:
    out = []
    for header in (PENDING, COMPLETED):
        for c in rows_of(text, header):
            status, league, fixture, kickoff = c[1], c[2], c[3], c[6]
            for which, cell in ((1, c[4]), (2, c[5])):
                got = lane(cell, status, which)
                if got:
                    out.append((kickoff, league, fixture, which, status) + got)
    return sorted(out, key=lambda r: r[0])


def fixtures(text: str) -> list[tuple]:
    """One row per fixture, Tip 1 and Tip 2 side by side as the tables have it.

    A fixture is listed when EITHER lane clears the threshold, and the other
    cell then says why it is not in rather than being blanked. "Tip 2 only" is
    a real fact about a fixture; an empty cell reads like missing data.
    """
    out = []
    for header in (PENDING, COMPLETED):
        for c in rows_of(text, header):
            l1 = lane(c[4], c[1], 1)
            l2 = lane(c[5], c[1], 2)
            if l1 or l2:
                out.append((c[6], c[2], c[3], c[1], c[4], c[5], l1, l2))
    return sorted(out, key=lambda r: r[0])


# "✅ HIT — 1-1 (decided, 20')" -> "1-1 (decided, 20')". Used when Tip 1 is
# below the threshold: the fixture still belongs here on Tip 2, but Tip 1's
# grading mark must not appear, or the row reads as a hit this block counted.
GRADE = re.compile(r"^(?:✅|❌)\s*(?:HIT|MISS)?\s*—?\s*")


def _cell(raw: str, keep: bool) -> str:
    if keep:
        return raw
    # A cell that never held a tip already explains itself — "— none", "— no
    # tip, Amedspor has 1 row". Only a real tip needs the threshold named.
    return raw if raw.startswith("—") else f"— under +{MIN_EDGE:.0f}%"


def counter_line(lanes: list[tuple]) -> str:
    def split(which: int | None) -> str:
        b = [r for r in lanes if which is None or r[3] == which]
        h = sum(1 for r in b if r[9] == "✅")
        n = sum(1 for r in b if r[9] in ("✅", "❌"))
        return f"{h} / {n}" + (f"   ·   {h / n * 100:.1f}%" if n else "")

    return (f"**Playable — {split(None)}**"
            f"   ·   **Tip 1 — {split(1)}**   ·   **Tip 2 — {split(2)}**")


def render(text: str) -> str:
    lanes = collect(text)
    head = [
        HEADING, "",
        "The two fixture tables below measure the ENGINE: every fixture Athena "
        "priced, bet or no bet. That number answers *is the model right*, and "
        "it stays as it is. This block answers the different question the "
        "bankroll asks — **of the lanes that were actually buyable, how many "
        "landed?** A tip at zero edge is the base rate wearing a probability; "
        "it is correctly skipped, and it does not belong in a hit rate that "
        "claims to describe what can be played.", "",
        f"So: every lane from both tables carrying an edge above "
        f"**+{MIN_EDGE:.0f}%**, Tip 1 and Tip 2 alike, laid out exactly as the "
        f"tables below are. A fixture is listed when either lane clears the "
        f"threshold and the other cell says why it did not, so both lanes, one "
        f"lane or neither can be in play on any given row — the counter above "
        f"the table counts lanes, not rows. The threshold is not zero on "
        f"purpose — measured over 7,576 tips, lanes under +1% stated edge "
        f"returned **+0.3 points** of real edge over the base rate, against "
        f"+1.7 to +4.3 for everything above. Arithmetically positive, worth "
        f"nothing. Derived from those tables by `python scripts/playable.py` "
        f"and pinned by a test — nothing here is typed, so it cannot drift out "
        f"of step with the rows it counts.", "",
        counter_line(lanes), "",
        "| Result | League | Teams | Tip 1 | Tip 2 | Kickoff |",
        "|---|---|---|---|---|---|",
    ]
    for k, league, teams, status, c1, c2, l1, l2 in fixtures(text):
        head.append(f"| {status if l1 else GRADE.sub('', status)} | {league} | "
                    f"{teams} | {_cell(c1, bool(l1))} | {_cell(c2, bool(l2))} "
                    f"| {k} |")
    return "\n".join(head) + "\n"


def rewrite(text: str) -> str:
    block = render(text)
    if ANCHOR in text:
        start = text.index(ANCHOR)
        end = text.index(NEXT, start)
        return text[:start] + block + "\n" + text[end:]
    # First run: the block goes between the completed table and the bet table.
    at = text.index(NEXT)
    return text[:at] + block + "\n" + text[at:]


def main() -> None:
    text = README.read_text()
    new = rewrite(text)
    if "--check" in sys.argv:
        if new != text:
            print("Playable-lanes block is STALE. "
                  "Run: python scripts/playable.py")
            sys.exit(1)
        print("playable lanes match the tables")
        return
    if new == text:
        print("playable lanes already current")
        return
    README.write_text(new)
    n = len(collect(text))
    print(f"playable lanes rebuilt: {n} lanes with edge above {MIN_EDGE:+.1f}%")


if __name__ == "__main__":
    main()
