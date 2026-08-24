"""
The fixture board: every block in README.md rendered from one typed file.

`config/fixtures.tsv` is the source of truth — one row per fixture, graded by
filling its `status` column. This renders all of it: the headline counts, the
playable block, the pending and completed cards. The pipe tables the page used
to carry were both the display AND the data, which meant five scripts parsed
the README and every layout change broke all of them; now layout is this
file's private business and the data never moves.

Each fixture is a CARD, two rows — the example the layout follows:

    | 🔵 25-08 21:00 — Valencia v Real Betis | Tip 1        | Tip 2        |
    | LaLiga (80.5 −0.1)                     | U4.25 88.0%… | U3.75 74.0%… |

GitHub strips CSS from READMEs, so there is no real color control and no
forced dark mode — the page renders in the viewer's own theme. Block identity
comes from the glyphs instead: 🟢 playable, 🔵 pending, ✅/❌ completed, and a
colored GitHub callout opening each section.

    python scripts/board.py            render README from fixtures.tsv
    python scripts/board.py --check    exit 1 if stale, change nothing

Badges per league come from config/league_hitrates.tsv, the playable filter
and lane parsing from scripts/playable.py (which remains the reader for the
ARCHIVED pipe-table logs), and the bet line from the ledger.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import playable
from scripts.league_badges import rates

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
FIXTURES = ROOT / "config" / "fixtures.tsv"

MIN_EDGE = playable.MIN_EDGE

# The rendered region: from the playable heading to the placed-bets heading.
# The placed-bets table keeps its own shape (different data), and the header
# span at the top of the page is rendered separately by render_header. Two
# START spellings, because the first render REPLACES the heading it anchored
# on — the same trap the playable block hit when its threshold renamed it.
STARTS = ("## 🟢 Playable lanes", "## Playable lanes")
END = "### Actual placed bets"
HEAD_START = "## CURRENT CONFIRMED HITRATE"
HEAD_END = "live tips, not backtests"


class Fixture:
    __slots__ = ("kickoff", "code", "league", "teams", "tip1", "tip2", "status")

    def __init__(self, kickoff, code, league, teams, tip1, tip2, status):
        self.kickoff, self.code, self.league = kickoff, code, league
        self.teams, self.tip1, self.tip2 = teams, tip1, tip2
        self.status = status

    @property
    def settled(self) -> bool:
        return self.status[:1] in ("✅", "❌")

    def lane(self, which: int):
        cell = self.tip1 if which == 1 else self.tip2
        return playable.lane(cell, self.status or "—", which)


def load() -> list[Fixture]:
    out = []
    for ln in FIXTURES.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        parts = ln.split("\t")
        if len(parts) != 7:
            raise ValueError(f"fixtures.tsv row needs 7 columns: {ln!r}")
        out.append(Fixture(*parts))
    return sorted(out, key=lambda f: f.kickoff)


def _badge(f: Fixture) -> str:
    b = rates().get(f.code)
    return f"{f.league} {b}" if b else f.league


def _stamp(f: Fixture) -> str:
    """`25-08 21:00` — compact, the year belongs to the file not the card."""
    d, t = f.kickoff.split(" ")
    _y, m, day = d.split("-")
    return f"{day}-{m} {t}"


def _mark(f: Fixture) -> str:
    if f.settled:
        return f.status.split("—")[-1].strip() if "—" in f.status else f.status
    return f.status or ""


def _card(f: Fixture, glyph: str, tip1: str, tip2: str) -> str:
    head = f"{glyph} {_stamp(f)} — **{f.teams}**"
    if f.settled:
        head = f"{f.status[:1]} {_mark(f)} — {_stamp(f)} — **{f.teams}**"
    elif f.status:                      # LIVE
        head = f"🔴 {f.status} — **{f.teams}**"
    return "\n".join([
        f"| {head} | Tip 1 | Tip 2 |",
        "|:--|:--|:--|",
        f"| {_badge(f)} | {tip1} | {tip2} |",
    ])


def _cell(raw: str) -> str:
    """One tip cell, buy-from dropped to its own line like the example."""
    if raw.startswith("—") or not raw:
        return raw or "—"
    return raw.replace(" · buy≥", "<br>buy≥")


def _tallies(fixtures: list[Fixture]):
    t = {1: [0, 0], 2: [0, 0]}          # published lanes: hits, settled
    p = {1: [0, 0], 2: [0, 0]}          # playable lanes
    for f in fixtures:
        if not f.settled:
            continue
        for which in (1, 2):
            cell = f.tip1 if which == 1 else f.tip2
            m = playable.LANE.match(cell)
            if m and not m.group(1).strip(" *·").startswith("—"):
                mark = f.status[:1] if which == 1 else cell[:1]
                if mark in ("✅", "❌"):
                    t[which][1] += 1
                    t[which][0] += mark == "✅"
            got = f.lane(which)
            if got and got[4] in ("✅", "❌"):
                p[which][1] += 1
                p[which][0] += got[4] == "✅"
    return t, p


def render_header(fixtures: list[Fixture]) -> str:
    from scripts.headline import bets

    t, p = _tallies(fixtures)
    (h1, n1), (h2, n2) = t[1], t[2]
    (p1, q1), (p2, q2) = p[1], p[2]
    bh, bn, roi = bets()
    pct = h1 / n1 * 100 if n1 else 0.0

    def cell(h, n):
        return f"{h:3} / {n:<3}" + (f"{h / n * 100:6.1f}%" if n else "       ")

    return "\n".join(ln.rstrip() for ln in [
        f"## CURRENT CONFIRMED HITRATE: {pct:.1f}%",
        "",
        f"    lane                        Tip 1              Tip 2",
        f"    all matches            {cell(h1, n1)}    {cell(h2, n2)}",
        f"    played lanes  >+1%     {cell(p1, q1)}    {cell(p2, q2)}",
        f"    placed bets            {cell(bh, bn)}    ROI {roi:+.1f}%",
        "",
        "**All matches** is the engine: every fixture priced, bet or not. "
        "**Played lanes** is the same count over the lanes with real edge — "
        "what was buyable, tracked in its own block below. **Placed bets** is "
        "the book. Rendered by `python scripts/board.py` from "
        "`config/fixtures.tsv`, never typed · over/under markets only · "
        "live tips, not backtests",
    ])


def render_board(fixtures: list[Fixture]) -> str:
    _t, p = _tallies(fixtures)
    (p1, q1), (p2, q2) = p[1], p[2]
    ph, pn = p1 + p2, q1 + q2

    def counter(label, h, n):
        return f"**{label} — {h} / {n}" + (f"   ·   {h/n*100:.1f}%**" if n else "**")

    out = [
        "## 🟢 Playable lanes — edge above +1%", "",
        "> [!TIP]",
        "> The block the bankroll follows: every lane carrying an edge above "
        "**+1%**, Tip 1 and Tip 2 alike. A tip at zero edge is the base rate "
        "wearing a probability — measured over 7,576 tips, lanes under +1% "
        "stated edge returned +0.3 points of real edge against +1.7 to +4.3 "
        "for everything above. A cell below the threshold says so instead of "
        "hiding; the counter counts lanes, not cards.", "",
        counter("Playable", ph, pn) + "   ·   " + counter("Tip 1", p1, q1)
        + "   ·   " + counter("Tip 2", p2, q2), "",
    ]
    for f in fixtures:
        l1, l2 = f.lane(1), f.lane(2)
        if not l1 and not l2:
            continue
        c1 = _cell(f.tip1) if l1 else (f.tip1 if f.tip1.startswith("—")
                                       else f"— under +{MIN_EDGE:.0f}%")
        c2 = _cell(f.tip2) if l2 else (f.tip2 if f.tip2.startswith("—")
                                       else f"— under +{MIN_EDGE:.0f}%")
        out += [_card(f, "🟢", c1, c2), ""]

    pending = [f for f in fixtures if not f.settled]
    out += [
        "## 🔵 Pending FUTURE match bettips", "",
        "> [!NOTE]",
        "> Every fixture Athena has priced that has not finished, playable or "
        "not — this and the completed block are the ENGINE's record. The "
        "typed source is `config/fixtures.tsv`; grade a fixture there and "
        "re-render with `python scripts/board.py`. The numbers after each "
        "league are its **(hit gap)** over its last 200 replayed matches — "
        "read the gap before trusting a row.", "",
    ]
    for f in pending:
        out += [_card(f, "🔵", _cell(f.tip1), _cell(f.tip2)), ""]
    if not pending:
        out += ["*(no open fixtures)*", ""]

    done = [f for f in fixtures if f.settled]
    t, _p = _tallies(fixtures)
    out += [
        "## ⚪ Completed FUTURE match bettips", "",
        counter("Tip 1", *t[1]) + "   ·   " + counter("Tip 2", *t[2]), "",
    ]
    for f in done:
        out += [_card(f, "⚪", _cell(f.tip1), _cell(f.tip2)), ""]
    if not done:
        out += ["*(nothing settled yet on this slate)*", ""]

    return "\n".join(out)


def rewrite(text: str) -> str:
    hs = text.index(HEAD_START)
    he = text.index(HEAD_END, hs) + len(HEAD_END)
    fixtures = load()
    text = text[:hs] + render_header(fixtures) + text[he:]

    s = min((text.index(m) for m in STARTS if m in text), default=None)
    if s is None:
        raise ValueError("board region not found in README")
    e = text.index(END, s)
    return text[:s] + render_board(fixtures) + "\n" + text[e:]


def main() -> None:
    text = README.read_text()
    new = rewrite(text)
    if "--check" in sys.argv:
        if new != text:
            print("Board is STALE. Run: python scripts/board.py")
            sys.exit(1)
        print("board matches fixtures.tsv")
        return
    if new == text:
        print("board already current")
        return
    README.write_text(new)
    f = load()
    print(f"board rendered: {len(f)} fixtures, "
          f"{sum(1 for x in f if not x.settled)} pending")


if __name__ == "__main__":
    main()
