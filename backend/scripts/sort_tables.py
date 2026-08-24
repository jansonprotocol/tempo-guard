"""
Keep the README fixture tables in kickoff order, earliest first.

Rows get appended to the tables as screenshots arrive, which is the order they
were PRICED in, not the order they kick off. That made the pending table read
17:15, then 13:35, then 14:00 — useless for deciding what to look at next.

This sorts the rows of every fixture table in place by the timestamp in the
last column. It is a text-level operation: the header, the separator, the
prose around the table and the exact contents of every cell are untouched, so
running it twice changes nothing and running it after an edit is always safe.

Ties keep their existing order (Python's sort is stable), so two 14:00 kickoffs
stay in whatever order they were added.

Usage:  python scripts/sort_tables.py [--check]

        --check  exit 1 if anything is out of order, and change nothing.
                 For the test suite, which pins this so the tables cannot
                 silently drift back out of order.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"

# The tables to sort, by the header row that opens each one. Every one of them
# ends in a kickoff/date column; the "Actual placed bets" table carries only a
# time, which is fine because it never spans days.
HEADERS = (
    "| Live | League | Teams | Tip 1 | Tip 2 | Kickoff |",
    "| Result | League | Teams | Tip 1 | Tip 2 | Kickoff |",
    "| Kickoff | Fixture | Athena Tip 1 | Lane taken | Odds | Buy from | EV |",
)

# 2026-08-23 14:00, 2026-08-23, or a bare 14:00.
STAMP = re.compile(r"(\d{4}-\d{2}-\d{2})?\s*(\d{2}:\d{2})?\s*$")


def key(row: str, col: int) -> str:
    """Sort key for one table row: the timestamp, zero-padded for text order."""
    cells = [c.strip() for c in row.split("|")]
    m = STAMP.search(cells[col])
    date, time = (m.group(1), m.group(2)) if m else (None, None)
    # A row with no time sorts to the start of its day, and a row with no date
    # at all sorts by time alone — the placed-bets table has no date column.
    return f"{date or ''} {time or '00:00'}"


def sort_tables(text: str) -> str:
    lines = text.splitlines()
    out: list[str] = []
    i = 0
    while i < len(lines):
        line = lines[i]
        out.append(line)
        if line.strip() not in HEADERS:
            i += 1
            continue
        # The fixture tables carry the kickoff in the LAST cell; the placed-bets
        # table leads with it. Splitting on "|" gives an empty cell at each end,
        # so the last real cell is -2 and the first is 1.
        col = 1 if line.strip().startswith("| Kickoff |") else -2
        i += 1
        out.append(lines[i])                      # the |---|---| separator
        i += 1
        # Collect the body, stepping OVER stray blank lines that sit between
        # rows. Appending a batch of fixtures just before the next heading tends
        # to leave one behind, and a blank line ends a markdown table: the page
        # then renders two tables, and this sorter used to sort each half
        # separately and report the result as correctly ordered. Absorbing the
        # blank here means the fault repairs itself and `--check` catches it.
        block: list[str] = []
        while i < len(lines):
            if lines[i].startswith("|"):
                block.append(lines[i])
                i += 1
                continue
            j = i
            while j < len(lines) and not lines[j].strip():
                j += 1
            if j < len(lines) and lines[j].startswith("|"):
                i = j                              # blank line inside the table
                continue
            break
        out.extend(sorted(block, key=lambda r: key(r, col)))
        # And exactly one blank line after the table, so the next heading is not
        # glued to the final row.
        if i < len(lines) and lines[i].strip():
            out.append("")
    return "\n".join(out) + "\n"


def main() -> None:
    text = README.read_text()
    sorted_text = sort_tables(text)
    if "--check" in sys.argv:
        if sorted_text != text:
            print("README fixture tables are NOT in kickoff order.")
            print("Run: python scripts/sort_tables.py")
            sys.exit(1)
        print("fixture tables are in kickoff order")
        return
    if sorted_text == text:
        print("already sorted, nothing written")
        return
    README.write_text(sorted_text)
    print("README fixture tables sorted by kickoff, earliest first")


if __name__ == "__main__":
    main()
