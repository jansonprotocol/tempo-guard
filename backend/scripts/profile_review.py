"""Review the board's price profiles and move them between Declined and the board.

    python scripts/profile_review.py          measure, write config/profile_review.tsv
    python scripts/profile_review.py --dry    measure, print, write nothing

THE ASK (the bettor, 28 Sep): "an option that checks on these declined
profiles, that whenever there is enough evidence a profile can get
released back into the playable/watch boards. Also vice versa, now
playable profiles into declined."

A PROFILE is one cell of a small grid fixed in advance, so the review
cannot go looking for the one slice that happens to win:

    group   counted | rule (declined by a board rule: strike combo, live
            unsafe, the O1.5 band) | red | super red
    side    O | U           (result lanes are left out)
    band    the price against the play bar, in the seven bands the card's
            VS BAR line and the Found bets panel print

2 x 7 x 4 = 56 cells, measured on the board's settled cards at the price
the card was first seen at (the forward log), graded off the final score.
The group is the card's BASE status — what the tier and the older rules
say before this review — so a released or declined cell is still measured
on the cards it was defined by and cannot eat itself (the trap
livebands.release_study documents).

THE BAR, and it is deliberately high because 56 cells are looked at every
two days and some will look good by chance:

    MIN_N      30 settled cards in the cell
    MIN_HALF   10 in each half, split at the cell's own median date
    MIN_ROI    5% flat-stake return, in the direction of the move
    BOTH       both halves on the same side of zero
    T_MIN      2.33 — the return's t-statistic, about 1 in 100 by chance

A counted cell that clears it downward is DECLINED; a declined cell (rule,
red or super red) that clears it upward is RELEASED. Everything else is
HOLD, and a cell that stops clearing drops back to HOLD at the next run,
so every move revokes itself. The return is at the feed's best EU price,
which is optimistic for the bettor's own books, so a release is the more
generous call of the two; the bar is the same both ways all the same.

Read by webapp.is_declined and webapp.verdict. Runs in the bank-refresh
workflow every two days, after livebands.
"""
from __future__ import annotations

import math
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "profile_review.tsv"

MIN_N = 30
MIN_HALF = 10
MIN_ROI = 0.05
T_MIN = 2.33
GROUPS = ("counted", "rule", "red", "super red")


def cards() -> list[dict]:
    """Every settled board card with a first-sight price on an O/U lane."""
    from scripts import forward_settle as fs
    from scripts import webapp as w
    from scripts.board import load
    out = []
    for f in load():
        if not f.settled:
            continue
        key = w.profile_of(f)
        if not key:
            continue
        c = w.was_called(f)
        r = c["row"]
        m = re.search(r"(\d+)-(\d+)\s*$", f.status)
        if not m:
            continue
        got = fs._settle(r["lane"], int(m[1]), int(m[2]))
        if got is None:
            continue
        s, hit = got
        best = float(r["best"])
        out.append(dict(key=key, d=f.kickoff[:10], hit=hit,
                        pl=s * (best - 1) if s > 0 else s))
    return out


def judge(group: str, x: list[dict]) -> dict:
    n = len(x)
    row = dict(n=n, hit=None, roi=None, a=None, b=None, t=None, label="hold")
    if not n:
        return row
    x = sorted(x, key=lambda c: c["d"])
    a, b = x[: n // 2], x[n // 2:]
    m = sum(c["pl"] for c in x) / n
    row.update(hit=sum(c["hit"] for c in x) / n, roi=m,
               a=sum(c["pl"] for c in a) / len(a) if a else None,
               b=sum(c["pl"] for c in b) / len(b) if b else None)
    if n < 2:
        return row
    sd = math.sqrt(sum((c["pl"] - m) ** 2 for c in x) / (n - 1))
    row["t"] = m / (sd / math.sqrt(n)) if sd else None
    if n < MIN_N or len(a) < MIN_HALF or len(b) < MIN_HALF or row["t"] is None:
        return row
    if (group == "counted" and m <= -MIN_ROI and row["a"] < 0 and row["b"] < 0
            and row["t"] <= -T_MIN):
        row["label"] = "decline"
    elif (group != "counted" and m >= MIN_ROI and row["a"] > 0 and row["b"] > 0
            and row["t"] >= T_MIN):
        row["label"] = "release"
    return row


def review() -> list[tuple]:
    from scripts import webapp as w
    cs = cards()
    rows = []
    for g in GROUPS:
        for side in ("O", "U"):
            for _lo, _hi, band in w.GAP_BANDS:
                x = [c for c in cs if c["key"] == (g, side, band)]
                rows.append((g, side, band, judge(g, x)))
    return rows


def _fmt(v, pct=True):
    if v is None:
        return ""
    return f"{v * 100:+.1f}" if pct else f"{v:+.2f}"


def main() -> None:
    rows = review()
    lines = [
        "# The two-day profile review (scripts/profile_review.py): each cell",
        "# of group x side x price band on the board's settled cards, at the",
        "# first-sight best price. 'decline' takes a counted cell off the",
        "# board; 'release' puts a declined one back; 'hold' does nothing.",
        f"# Bar: n>={MIN_N}, >={MIN_HALF} a half, |ROI|>={MIN_ROI*100:.0f}%, "
        f"both halves one side of zero, |t|>={T_MIN}.",
        "# group\tside\tband\tn\thit\troi\thalf_a\thalf_b\tt\tlabel"]
    for g, side, band, r in rows:
        lines.append("\t".join([
            g, side, band, str(r["n"]),
            f"{r['hit'] * 100:.1f}" if r["hit"] is not None else "",
            _fmt(r["roi"]), _fmt(r["a"]), _fmt(r["b"]),
            _fmt(r["t"], pct=False), r["label"]]))
    moved = [(g, s, b, r) for g, s, b, r in rows if r["label"] != "hold"]
    for g, s, b, r in rows:
        if r["n"] >= 12 or r["label"] != "hold":
            print(f"{g:9} {s} {b:11} {r['n']:4} "
                  f"{(r['roi'] or 0) * 100:+6.1f}%  t={_fmt(r['t'], False):>6}  "
                  f"{r['label']}")
    print(f"{len(moved)} cell(s) moved: "
          + (", ".join(f"{g} {s} {b} -> {r['label']}" for g, s, b, r in moved)
             or "none"))
    if "--dry" not in sys.argv:
        tmp = OUT.with_suffix(".tsv.tmp")
        tmp.write_text("\n".join(lines) + "\n")
        tmp.replace(OUT)
        print(f"wrote {OUT}")


if __name__ == "__main__":
    main()
