"""Measure the live-safety bands on the board's own record and label them.

    python scripts/livebands.py            measure, write config/live_bands.tsv
    python scripts/livebands.py --dry      measure, print, write nothing
    python scripts/livebands.py --days 21  the window (default 21)

THE RULE (the bettor, 12 Sep). A card the board did not stake — an Athena
lane or a watch card — carries a live-safety tag by its lane and its
printed-edge band. The bands were first set by hand off the session's
tally; from here they are MEASURED every two days on the board's settled
cards, red cards out, and labelled by hit rate:

    at or above 79%   safe
    77% to 79%        cautious
    under 77%         unsafe

A band with fewer than MIN_N graded cards in the window keeps the SEED
label (webapp.LIVE_TAG, the bettor's hand table) — a label read off a
dozen cards would flip every fortnight on noise, which is the opposite of
a signal. The window is rolling so the labels follow the regime the
bettor is actually playing in, not the bank: the bank says the deep-
negative Athena band is its best slice (+4.6 on 3,539 cards) and the
first week of September ran it at 68.8%, and it is the second number
the tag exists for.

Display only. Nothing here touches the engine, the guard's slices, the
labels or the record; it decides which unstaked cards are OFFERED for a
live buy and where they file (Declined takes the unsafe ones).

Writes config/live_bands.tsv, read by webapp.live_tag — a typed table
like the others, so the page and the README render from the same rule.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "live_bands.tsv"

SAFE_AT = 79.0          # hit rate at or above which a band is "safe"
CAUTIOUS_AT = 77.0      # below this it is "unsafe"; between, "cautious" (the bettor, 12 Sep: 77)
MIN_N = 15              # fewer graded cards than this: keep the seed label (the bettor, 12 Sep: 15)
DAYS = 21               # rolling window, in days, of settled kickoffs
BANDS = ("+1 up", "−1..+1", "−4..−1", "−4 down")
LANES = ("athena", "watch")


def label(hit_pct: float | None, n: int, seed: str) -> tuple[str, str]:
    """(label, source): measured when n is enough, else the seed."""
    if hit_pct is None or n < MIN_N:
        return seed, "seed"
    if hit_pct >= SAFE_AT:
        return "safe", "measured"
    if hit_pct >= CAUTIOUS_AT:
        return "cautious", "measured"
    return "unsafe", "measured"


def measure(days: int = DAYS, today: dt.date | None = None) -> list[dict]:
    """One row per lane and band: n, hit, said, label, source."""
    from scripts import board, webapp
    today = today or dt.date.today()
    since = (today - dt.timedelta(days=days)).isoformat()
    tally: dict[tuple[str, str], list] = {(l, b): [0, 0, 0.0] for l in LANES for b in BANDS}
    for f in board.load():
        if not f.settled or f.kickoff[:10] < since or not f.tip1 or f.tip1.startswith("—"):
            continue
        if not webapp.counts(f):                       # red cards are not in the record
            continue
        call = webapp.was_called(f)
        mark = call["mark"] if call else "no row"
        lane = ("watch" if mark == "watch" else
                "athena" if mark in ("no play", "no row") else None)
        if lane is None:                               # a priced play: no tag
            continue
        cell = f.tip3 if webapp._star_any(f) == 3 else f.tip1
        e, c = webapp._edge(cell), webapp._claim(cell)
        if e is None or c is None:
            continue
        t = tally[(lane, webapp.edge_band(e))]
        t[0] += 1
        t[1] += f.status.startswith("✅")
        t[2] += c
    rows = []
    for lane in LANES:
        for band in BANDS:
            n, h, c = tally[(lane, band)]
            hit = (h / n * 100) if n else None
            lab, src = label(hit, n, webapp.LIVE_TAG[lane][band])
            rows.append(dict(lane=lane, band=band, n=n, hit=hit,
                             said=(c / n) if n else None, label=lab, source=src))
    return rows


def write(rows: list[dict], days: int, today: dt.date) -> None:
    lines = [
        "# The live-safety bands, MEASURED on the board's settled cards, red",
        "# cards out, over a rolling window; written by scripts/livebands.py",
        "# every two days (the bank-refresh workflow) and read by",
        "# scripts/webapp.py for the live tag on every unstaked card. A band",
        f"# under {MIN_N} cards keeps the seed label (the bettor's hand table,",
        f"# 12 Sep). Thresholds: safe >= {SAFE_AT:.0f}, cautious >= {CAUTIOUS_AT:.0f}, else unsafe.",
        f"# window\t{days} days to {today.isoformat()}",
        "# lane\tband\tn\thit\tsaid\tlabel\tsource",
    ]
    for r in rows:
        lines.append("\t".join([
            r["lane"], r["band"], str(r["n"]),
            "" if r["hit"] is None else f"{r['hit']:.1f}",
            "" if r["said"] is None else f"{r['said']:.1f}",
            r["label"], r["source"]]))
    OUT.write_text("\n".join(lines) + "\n")


def read() -> dict[tuple[str, str], dict] | None:
    """The written table, or None when it does not exist yet."""
    if not OUT.exists():
        return None
    out = {}
    for ln in OUT.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 7:
            continue
        out[(p[0], p[1])] = dict(n=int(p[2]), hit=(float(p[3]) if p[3] else None),
                                 said=(float(p[4]) if p[4] else None),
                                 label=p[5], source=p[6])
    return out


def main() -> None:
    dry = "--dry" in sys.argv
    days = DAYS
    if "--days" in sys.argv:
        days = int(sys.argv[sys.argv.index("--days") + 1])
    today = dt.date.today()
    rows = measure(days, today)
    print(f"live bands, last {days} days to {today}, red cards out:")
    for r in rows:
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        print(f"  {r['lane']:6} {r['band']:8} n={r['n']:3}  hit {hit}  -> {r['label']:8} ({r['source']})")
    if not dry:
        write(rows, days, today)
        print(f"written: {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
