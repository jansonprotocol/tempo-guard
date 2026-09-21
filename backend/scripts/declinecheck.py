"""Every two days: how is each rule that files a card under Declined doing?

    python scripts/declinecheck.py          print, and write config/declinecheck.tsv
    python scripts/declinecheck.py --dry    print only

THE ASK (the bettor, 21 Sep): "every 2 days run and check on the rules for
cards that pass or decline — which board a card goes to — depending on how
the sample batch is now running. Mostly weighed by real completed
futurematch cards and maybe supported by bank data."

WHAT ADJUSTS BY ITSELF, AND WHAT THIS ADDS. The rules are already
re-measured on the two-day run: scripts/livebands.py rewrites the live
tag's bands (which decide "unsafe" on every lane, the priced lane since
21 Sep included), the strike combos and the release profile, so a band
that recovers to 77 stops declining and one that falls under it starts,
with nobody's permission. What nothing printed was a VERDICT per rule —
did the cards each rule declined actually land worse than the cards it
kept? That is what this reports, and it reports it twice:

  BOARD   the board's own settled cards — real futurematch cards, priced
          at slate time, graded by the sweep. This is the column the
          bettor asked to weigh most. Two windows: the last DAYS days, and
          every settled card on the board.
  BANK    the bank, both halves by date, as the profile study measures.

WHY THE BANDS STAY BANK-FIRST even though the board column comes first
here: measured 13 Sep, bands fitted on the board's own three weeks pointed
the WRONG WAY out of sample — the session's "unsafe" cards landed 88.0%
against 84.5% for its "safe" ones — while the bank's per-league bands
separated them 85.5 to 75.0. The board feeds the bank every two days
through scripts/ingest_board.py, so its cards ARE in the measurement;
they are not allowed to be the whole of it. This file is where the two
are read side by side, so the day they disagree it is visible.

One predicate, not a copy. The reason a card is declined is read from the
same functions the board and the bank file it by — webapp.is_declined and
bankrates.counts — so this cannot say a rule is doing well while the
board applies a different one.

Report only. Nothing here writes a band, a label or a hit rate.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "declinecheck.tsv"
DAYS = 21

REASONS = ("tier red", "live unsafe · athena", "live unsafe · watch",
           "live unsafe · priced", "strike combo")


def _board_reason(f) -> str | None:
    from scripts import webapp
    if not webapp.is_declined(f):
        return None
    lab = webapp.label_any(f)
    if lab and lab.endswith("red"):
        return "tier red"
    lane = webapp.record_lane(f)
    if lane in ("athena", "watch", "priced") and webapp.record_tag(f) == "unsafe":
        return f"live unsafe · {lane}"
    return "strike combo"


def _bank_reason(m: dict, code: str) -> str | None:
    from scripts import bankrates as br
    if br.counts(m, code):
        return None
    if not br.not_red(m):
        return "tier red"
    lane = br.lane(m)
    if lane in ("athena", "watch", "priced") and br.tag(m, code) == "unsafe":
        return f"live unsafe · {lane}"
    return "strike combo"


def _tally() -> dict[tuple, list]:
    """(source, window, reason-or-kept) -> [n, hits]."""
    t: dict[tuple, list] = {}

    def add(key, hit):
        t.setdefault(key, [0, 0])
        t[key][0] += 1
        t[key][1] += hit

    from scripts import board
    since = (dt.date.today() - dt.timedelta(days=DAYS)).isoformat()
    for f in board.load():
        if not f.settled or f.status[:1] not in ("✅", "❌"):
            continue
        if not f.tip1 or f.tip1.startswith("—"):
            continue
        hit = f.status[:1] == "✅"
        why = _board_reason(f) or "kept"
        for window in (("board", "all"),) + ((("board", f"{DAYS}d"),) if f.kickoff[:10] >= since else ()):
            add(window + (why,), hit)

    from scripts import bankrates as br
    cards = []
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            got = br._hit(m.get("mark"))
            if got is None:
                continue
            cards.append((m["d"], bool(got), _bank_reason(m, code) or "kept"))
    cards.sort()
    mid = cards[len(cards) // 2][0] if cards else ""
    for d, hit, why in cards:
        add(("bank", "A" if d < mid else "B", why), hit)
        add(("bank", "all", why), hit)
    return t


def rows() -> list[dict]:
    t = _tally()
    out = []
    for source, window in (("board", f"{DAYS}d"), ("board", "all"),
                           ("bank", "A"), ("bank", "B"), ("bank", "all")):
        kn, kh = t.get((source, window, "kept"), (0, 0))
        kept = (kh / kn * 100) if kn else None
        for why in REASONS:
            n, h = t.get((source, window, why), (0, 0))
            hit = (h / n * 100) if n else None
            # the verdict: a rule earns its keep when what it declined
            # landed under what it kept; "thin" under 15 cards, the live
            # tag's own floor, so a rule is never judged on a handful
            verdict = ("thin" if n < 15 or kept is None else
                       "separates" if hit < kept - 2 else
                       "no separation" if hit <= kept + 2 else "BACKWARDS")
            out.append(dict(source=source, window=window, rule=why, n=n, hit=hit,
                            kept_n=kn, kept=kept, verdict=verdict))
    return out


def write(rs: list[dict]) -> None:
    lines = [
        "# How each rule that files a card under Declined is doing: the cards it",
        "# declined against the cards it kept. Written by scripts/declinecheck.py",
        "# on the two-day bank refresh; report only, nothing reads it back.",
        f"# BOARD rows are the board's own settled cards (last {DAYS} days, and all);",
        "# BANK rows are the bank in two halves by date and pooled.",
        "# verdict: separates = declined landed 2+ under kept; BACKWARDS = 2+ over;",
        "# thin = under 15 declined cards, not judged.",
        f"# written\t{dt.date.today().isoformat()}",
        "# source\twindow\trule\tn\thit\tkept_n\tkept\tverdict",
    ]
    for r in rs:
        lines.append("\t".join([
            r["source"], r["window"], r["rule"], str(r["n"]),
            "" if r["hit"] is None else f"{r['hit']:.1f}",
            str(r["kept_n"]), "" if r["kept"] is None else f"{r['kept']:.1f}",
            r["verdict"]]))
    OUT.write_text("\n".join(lines) + "\n")


def main() -> None:
    rs = rows()
    last = None
    for r in rs:
        head = (r["source"], r["window"])
        if head != last:
            kept = "  —  " if r["kept"] is None else f"{r['kept']:5.1f}%"
            print(f"{r['source'].upper()} {r['window']:4}  kept {kept} on {r['kept_n']}")
            last = head
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        print(f"   {r['rule']:22} n={r['n']:5}  declined land {hit}  -> {r['verdict']}")
    if "--dry" not in sys.argv:
        write(rs)
        print(f"written: {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
