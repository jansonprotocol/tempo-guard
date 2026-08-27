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
ENDS = ("### 🟡 Actual placed bets", "### Actual placed bets")
# The board region ends where the hypothesis ledger begins. That anchor is
# a comment rather than a heading on purpose: a heading can be renamed by
# the very render that keys off it, which is the trap STARTS carries two
# spellings to survive.
TAIL = "<!-- HYPOTHESES:START -->"
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
        # "FT ..." is finished with nothing to grade — an abstained fixture
        # that played out. It moves to the completed block but feeds no tally.
        return self.status[:1] in ("✅", "❌") or self.status.startswith("FT")

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


def _head(f: Fixture, glyph: str) -> str:
    if f.status.startswith("FT"):
        return f"⚪ {f.status[3:] or f.status} · {_stamp(f)} **{f.teams}**"
    if f.settled:
        return f"{f.status[:1]} {_mark(f)} · {_stamp(f)} **{f.teams}**"
    if f.status:                        # LIVE
        return f"🔴 {f.status} **{f.teams}**"
    return f"{glyph} {_stamp(f)} **{f.teams}**"


def _cards(entries: list[tuple[Fixture, str, str, str]]) -> list[str]:
    """Every match its own card, floated so they flow two abreast.

    Markdown tables are block elements — two of them can only stack — so each
    card is a small HTML table carrying `align="left"`, the one layout
    attribute GitHub's sanitizer allows through. Floated cards sit side by
    side where the viewport is wide and wrap underneath each other where it
    is narrow, which is exactly the mobile behaviour asked for, with no CSS
    anywhere. `<br clear="all">` ends the float so the next section's text
    cannot ride up alongside the last card.
    """
    from scripts import liveline
    out = []
    for f, glyph, t1, t2 in entries:
        tie = liveline.tie_note(f.teams, f.status)
        # The tie is context, never an input: Athena prices one match's
        # goals and has no concept of an aggregate.
        row = (f'<tr><td colspan="3"><sub>🏆 {_html(tie)}</sub></td></tr>'
               if tie else "")
        out.append(
            '<table align="left">'
            f'<tr><th align="left">{_html(_head(f, glyph))}</th>'
            '<th align="left">Tip 1</th><th align="left">Tip 2</th></tr>'
            f'<tr><td>{_html(_badge(f))}</td>'
            f'<td>{_html(t1)}</td><td>{_html(t2)}</td></tr>'
            f'{row}</table>')
    if out:
        out += ['', '<br clear="all">', '']
    return out


def _html(s: str) -> str:
    """Markdown bold does not render inside an HTML table, so ** becomes <b>."""
    parts = s.split("**")
    for i in range(1, len(parts), 2):
        parts[i] = f"<b>{parts[i]}</b>"
    return "".join(parts)


def _live(f, cell: str) -> str:
    """The lane's state at the current score — empty unless in play."""
    from scripts import liveline
    if f.settled or not f.status:
        return ""
    s = liveline.progress(cell, f.teams, f.status)
    return f" · <i>{s}</i>" if s else ""


def _cell(raw: str) -> str:
    """One tip cell: probability line on top, buy-from below, annotation last.

    `(team)`, `(floor −9.1)`, `(lower edge)` widen the top line unevenly, so
    they ride the second line instead — the top line stays `RUNG P% +E%`
    across every card, which is what keeps a six-column row readable.
    """
    if raw.startswith("—") or not raw:
        return raw or "—"
    import re
    m = re.match(r"^(.*?)\s*(\([^)]*\))?\s*· (buy≥\S+)\s*$", raw)
    if not m:
        return raw.replace(" · buy≥", "<br>buy≥")
    top, note, buy = m.groups()
    return f"{top}<br>{buy}" + (f" · {note[1:-1]}" if note else "")


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
    entries = []
    for f in fixtures:
        l1, l2 = f.lane(1), f.lane(2)
        if not l1 and not l2:
            continue
        c1 = _cell(f.tip1) + _live(f, f.tip1) if l1 else (
            f.tip1 if f.tip1.startswith("—")
            else f"— under +{MIN_EDGE:.0f}%")
        c2 = _cell(f.tip2) + _live(f, f.tip2) if l2 else (
            f.tip2 if f.tip2.startswith("—")
                                       else f"— under +{MIN_EDGE:.0f}%")
        entries.append((f, "🟢", c1, c2))
    out += _cards(entries)

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
    out += _cards([(f, "🔵", _cell(f.tip1) + _live(f, f.tip1),
                _cell(f.tip2) + _live(f, f.tip2)) for f in pending])
    if not pending:
        out += ["*(no open fixtures)*", ""]

    done = [f for f in fixtures if f.settled]
    t, _p = _tallies(fixtures)
    out += [
        "## ⚪ Completed FUTURE match bettips", "",
        counter("Tip 1", *t[1]) + "   ·   " + counter("Tip 2", *t[2]), "",
    ]
    out += _cards([(f, "⚪", _cell(f.tip1), _cell(f.tip2)) for f in done])
    if not done:
        out += ["*(nothing settled yet on this slate)*", ""]

    return "\n".join(out)


HYPOTHESES = ROOT / "config" / "hypotheses.tsv"
HYP_START = "<!-- HYPOTHESES:START -->"
HYP_END = "<!-- HYPOTHESES:END -->"
DOT = {"green": "🟢", "orange": "🟠", "red": "🔴"}


def load_hypotheses() -> list[tuple[str, str, str, str, str]]:
    """The ledger of everything tried, newest first within each verdict."""
    out = []
    for ln in HYPOTHESES.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        if len(parts) == 5:
            out.append(tuple(parts))
    return out


def render_hypotheses() -> str:
    """Every idea this project has tested, and what killed or kept it.

    The red rows are the point of this table. A rejected idea that is not
    written down gets re-proposed every fortnight, and each time it costs
    a day to re-measure — so the number that killed it is recorded beside
    it, and the entry is never deleted. Green is what survived two
    windows; orange is honest about what is still unfinished, including
    the cup lane that is live but on probation.
    """
    rows = load_hypotheses()
    by = {k: [r for r in rows if r[0] == k]
          for k in ("green", "orange", "red")}
    out = [HYP_START, "",
           "## The ledger of everything tried", "",
           f"Every feature suggestion and hypothesis put through the bar — "
           f"{len(by['green'])} verified, {len(by['orange'])} unfinished, "
           f"{len(by['red'])} declined. Typed in "
           f"`config/hypotheses.tsv`; this table and the app's Patches page "
           f"both render from it, so they cannot disagree.", ""]
    heads = (
        ("green", "🟢 Verified and helping",
         "Cleared two separate time windows and is live in the engine "
         "today."),
        ("orange", "🟠 Unfinished",
         "Measured but not concluded, or shipped on **probation** and "
         "still waiting on live results."),
        ("red", "🔴 Declined",
         "Tested and rejected, with the number that killed it. Kept "
         "deliberately — a dead idea that stays written down does not get "
         "re-proposed."),
    )
    for key, title, blurb in heads:
        out += [f"### {title} — {len(by[key])}", "", blurb, "",
                "| | Date | Area | Hypothesis | Verdict |",
                "|---|---|---|---|---|"]
        for status, date, area, name, verdict in by[key]:
            # A pipe is a column separator here, and "|elo gap|" is a real
            # phrase in this ledger — escape or the row collapses.
            def cell(s):
                return _html(s).replace("|", "\\|")
            out.append(f"| {DOT[status]} | {date[5:]} | {area} | "
                       f"**{cell(name)}** | {cell(verdict)} |")
        out.append("")
    out.append(HYP_END)
    return "\n".join(out)


def render_bets() -> list[str]:
    """The placed-bets block, settled by the ledger's own rules.

    This was the last hand-typed block on the page and it went stale the
    same way every hand-typed number here has: the counter read 0 / 0 while
    eleven bets sat settled. Now bets.tsv carries the bet and its note, the
    fixture result comes from fixtures.tsv, and the settlement marks are
    computed — full/half win, push, half/full loss, DNB included.
    """
    from scripts import headline, ledger

    fixtures = ledger.read_fixtures()
    bh, bn, roi = headline.bets()
    out = [
        "### 🟡 Actual placed bets", "",
        f"**Settled: {bh} / {bn}  ·  ROI {roi:+.1f}%  ·  flat stakes** — "
        "settled through real settlement fractions by the ledger; a push or "
        "half-win counts as a hit, a half-loss does not. Notes travel with "
        "the bet in `config/bets.tsv`.", "",
        "| Result | Fixture | Lane | Odds | Return | Note |",
        "|---|---|---|---|---|---|",
    ]
    MARK = {1.0: ("✅", "won"), 0.5: ("✅½", "half won"), 0.0: ("◦", "push"),
            -0.5: ("❌½", "half lost"), -1.0: ("❌", "lost")}
    for ln in ledger.BETS.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        name, rung, odds, side = parts[0], parts[1], float(parts[2]), parts[3]
        note = parts[6] if len(parts) > 6 else ""
        lane = rung if side == "-" else f"{rung} ({'home' if side == 'H' else 'away'})"
        # Cashed out at stake: realised at 1.00x now, whatever the fixture
        # does later — same convention as headline.bets().
        if len(parts) > 4 and parts[4] == "1":
            out.append(f"| ◦ | {name} | {lane} | {odds:.2f} | 1.00x | {note} |")
            continue
        fx = fixtures.get(name)
        if fx is None or fx["hg"] is None:
            out.append(f"| — open | {name} | {lane} | {odds:.2f} | — | {note} |")
            continue
        if rung == "DNB":
            gf, ga = ((fx["hg"], fx["ag"]) if side == "H"
                      else (fx["ag"], fx["hg"]))
            s = 1.0 if gf > ga else 0.0 if gf == ga else -1.0
        elif rung in ("1X", "X2"):
            # Double chance: the named side or the draw. No push exists —
            # the bet wins unless the other side wins outright.
            s = -1.0 if ((fx["hg"] > fx["ag"]) if rung == "X2"
                         else (fx["ag"] > fx["hg"])) else 1.0
        else:
            goals = (fx["hg"] + fx["ag"]) if side == "-" else (
                fx["hg"] if side == "H" else fx["ag"])
            s = ledger.pricing.settle_fraction(rung, goals)
        ret = max(s, 0.0) * odds + (1 - abs(s))
        mark, _w = MARK[s]
        out.append(f"| {mark} | {name} | {lane} | {odds:.2f} "
                   f"| {ret:.2f}x | {note} |")
    out.append("")
    return out


def rewrite(text: str) -> str:
    hs = text.index(HEAD_START)
    he = text.index(HEAD_END, hs) + len(HEAD_END)
    fixtures = load()
    text = text[:hs] + render_header(fixtures) + text[he:]

    s = min((text.index(m) for m in STARTS if m in text), default=None)
    if s is None:
        raise ValueError("board region not found in README")
    e = min(text.index(m, s) for m in ENDS if m in text)
    e2 = text.index(TAIL, e)
    text = (text[:s] + render_board(fixtures) + "\n"
            + "\n".join(render_bets()) + "\n" + text[e2:])

    hs = text.index(HYP_START)
    he = text.index(HYP_END, hs) + len(HYP_END)
    return text[:hs] + render_hypotheses() + text[he:]


def liveline_score(status: str):
    from scripts import liveline
    return liveline.score_of(status)


def verify(quiet: bool = False) -> None:
    """Every fixture, on every surface, every time.

    The board has gone wrong twice in ways a human eye missed: a derived
    block silently kept a stale copy of the data, and a generated script
    failed to parse so the app rendered blank while the file still looked
    fine. Both would have been caught by counting. So the renderer counts:
    each fixture must appear in the README and in the app tab its state
    puts it in, the tallies must match the fixtures they claim to
    summarise, the ledger must carry every bet, and the app's script must
    parse. A mismatch raises — a wrong board is worse than no board.
    """
    import json
    import shutil
    import subprocess
    import tempfile

    fixtures = load()
    readme = README.read_text()
    app_path = ROOT / "web" / "index.html"
    app = app_path.read_text() if app_path.exists() else ""
    bad: list[str] = []

    # Each surface escapes its own way — the README keeps apostrophes,
    # the app writes them as &#x27; — so a name is looked for in the form
    # that surface would have written it, not in one canonical spelling.
    import html as _h

    def in_readme(name: str) -> bool:
        return _html(name) in readme

    def in_app(name: str, where: str) -> bool:
        return _h.escape(name) in where or _html(name) in where

    playable = [f for f in fixtures if not f.settled
                and (f.lane(1) or f.lane(2))]
    pending = [f for f in fixtures if not f.settled]
    done = [f for f in fixtures if f.settled]

    # 1. The README carries every fixture, whatever its state.
    for f in fixtures:
        if not in_readme(f.teams):
            bad.append(f"README is missing {f.teams!r}")

    # 2. The header tallies describe the fixtures they sit above.
    t_lane, p_lane = _tallies(fixtures)
    for which, (hits, n) in t_lane.items():
        if n and f"{hits:3} / {n:<3}" not in readme:
            bad.append(f"README header lost the Tip {which} tally "
                       f"({hits}/{n})")

    # 3. Every bet in the ledger reaches the placed-bets block.
    from scripts import ledger
    bets = [ln.split("\t")[0] for ln in ledger.BETS.read_text().splitlines()
            if ln.strip() and not ln.startswith("#")]
    for name in set(bets):
        if not in_readme(name):
            bad.append(f"placed-bets block is missing {name!r}")

    if app:
        # 4. The app's five pages and four tabs still exist.
        for pid in ("p-home", "p-sessions", "p-retrosim", "p-patches",
                    "p-about"):
            if f'id="{pid}"' not in app:
                bad.append(f"app page {pid} vanished")
        panes = {}
        for pid in ("t-playable", "t-bets", "t-lanes", "t-done"):
            if f'id="{pid}"' not in app:
                bad.append(f"app tab {pid} vanished")
                continue
            i = app.index(f'id="{pid}"')
            ends = [x for x in (app.find('class="tabpane"', i + 1),
                                app.find('<div id="learn"', i + 1),
                                app.find("</section>", i + 1)) if x > 0]
            panes[pid] = app[i:min(ends)] if ends else app[i:]

        # 5. Each fixture appears in the tab its state puts it in — and a
        #    playable one appears in BOTH, since Playable filters the
        #    Athena lanes rather than removing from them.
        for pid, want in (("t-playable", playable), ("t-lanes", pending),
                          ("t-done", done)):
            body = panes.get(pid, "")
            for f in want:
                if not in_app(f.teams, body):
                    bad.append(f"app tab {pid} is missing {f.teams!r}")
            got = body.count('<details class="card')
            if got != len(want):
                bad.append(f"app tab {pid} shows {got} cards, "
                           f"expected {len(want)}")

        # 6. The ledger reaches the app too.
        for name in set(bets):
            if not in_app(name, panes.get("t-bets", "")):
                bad.append(f"app Found bets is missing {name!r}")

        # 7. A live fixture must say what the score did to its lanes.
        from scripts import liveline
        for f in fixtures:
            if f.settled or not f.status or not liveline.score_of(f.status):
                continue
            for which in (1, 2):
                cell = f.tip1 if which == 1 else f.tip2
                if cell.strip() in ("", "—", "— none"):
                    continue
                state = liveline.progress(cell, f.teams, f.status)
                if state and state not in app:
                    bad.append(f"app lost the live state {state!r} for "
                               f"{f.teams!r}")

        # 8. The generated script parses. A syntax error hides every page.
        node = shutil.which("node")
        if node and "<script>" in app:
            js = app[app.index("<script>") + 8:app.rindex("</script>")]
            with tempfile.NamedTemporaryFile("w", suffix=".js",
                                             delete=False) as fh:
                fh.write(js)
                path = fh.name
            r = subprocess.run([node, "--check", path],
                               capture_output=True, text=True)
            if r.returncode != 0:
                bad.append(f"app script does not parse: "
                           f"{r.stderr.strip().splitlines()[-1]}")

        # 9. The lookup bank is intact and shaped as the page expects.
        bank_path = ROOT / "web" / "matchbank.json"
        if bank_path.exists():
            try:
                bank = json.loads(bank_path.read_text())
                for key in ("comps", "alias", "names"):
                    if key not in bank:
                        bad.append(f"matchbank.json lost {key!r}")
            except Exception as exc:
                bad.append(f"matchbank.json is unreadable: {exc}")

    # 10. The Engine state block describes the engine that is actually
    #     running. It is prose, so nothing forced it to keep up: the block
    #     sat dated 24 Aug while DEFENSE_BLEND, the whole Club Elo cup lane
    #     and the 0.82 cup floor shipped underneath it. Now every constant
    #     it names is read back out of the live code and compared.
    import re
    from app.data import club_elo, features
    from app.engine import market_select
    live = {
        "MU_SHRINK": features.MU_SHRINK,
        "TEAM_SHRINK": features.TEAM_SHRINK,
        "BIG_MATCH_DEBIT": features.BIG_MATCH_DEBIT,
        "TEAM_RATE_FLOOR": features.TEAM_RATE_FLOOR,
        "DEFENSE_BLEND": features.DEFENSE_BLEND,
        "VENUE_BLEND": features.VENUE_BLEND,
        "MIN_WIN_PROB": market_select.MIN_WIN_PROB,
        "HIGH_SAYS_DEBIT": market_select.HIGH_SAYS_DEBIT,
        "HIGH_SAYS_FROM": market_select.HIGH_SAYS_FROM,
        "B1": club_elo.B1, "B2": club_elo.B2, "B3": club_elo.B3,
        "B0_FALLBACK": club_elo.B0_FALLBACK,
        "OVER_SAYS_DEBIT": club_elo.OVER_SAYS_DEBIT,
        "MAX_STALE_DAYS": club_elo.MAX_STALE_DAYS,
    }
    block = readme[readme.index("## Engine state"):
                   readme.index("### Recalibration")]
    for name, value in live.items():
        m = re.search(rf"^    {re.escape(name)}\s+(−?-?[\d.]+)",
                      block, re.M)
        if not m:
            bad.append(f"Engine state block never names {name}")
            continue
        shown = float(m.group(1).replace("−", "-"))
        if abs(shown - float(value)) > 1e-9:
            bad.append(f"Engine state says {name} {shown}, "
                       f"the code runs {value}")

    # 11. No fixture quietly rots. A match that kicked off hours ago and
    #     still shows nothing means the last update touched one row and
    #     left its neighbours behind — the exact failure sweep.py exists
    #     to prevent, so the board refuses to render until it is swept.
    from datetime import datetime, timedelta
    stale_cutoff = datetime.now() - timedelta(hours=4)
    rotting = []
    for f in fixtures:
        if f.settled:
            continue
        try:
            ko = datetime.strptime(f.kickoff, "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        if ko < stale_cutoff and not liveline_score(f.status):
            rotting.append(f"{f.teams} (kicked off {f.kickoff})")
    if rotting:
        bad.append("these kicked off over four hours ago and carry no "
                   "result — run scripts/sweep.py:\n    "
                   + "\n    ".join(rotting))

    if bad:
        raise SystemExit("BOARD VERIFY FAILED\n  " + "\n  ".join(bad))
    if not quiet:
        print(f"verified: {len(fixtures)} fixtures across README and "
              f"{'app' if app else 'README only'} "
              f"({len(playable)} playable, {len(pending)} pending, "
              f"{len(done)} completed, {len(set(bets))} bets)")


def main() -> None:
    text = README.read_text()
    new = rewrite(text)
    if "--check" in sys.argv:
        if new != text:
            print("Board is STALE. Run: python scripts/board.py")
            sys.exit(1)
        print("board matches fixtures.tsv")
        verify()
        return
    if new == text:
        # The README can be current while the app is not — they are
        # written by different code from the same sources, and only one
        # of them is compared above. So the app is always re-rendered.
        print("board already current")
        from scripts import webapp
        webapp.main()
        verify()
        return
    README.write_text(new)
    f = load()
    print(f"board rendered: {len(f)} fixtures, "
          f"{sum(1 for x in f if not x.settled)} pending")
    # The web app derives from the same sources; rendering them together is
    # what keeps the page and the README incapable of disagreeing.
    from scripts import webapp
    webapp.main()
    verify()


if __name__ == "__main__":
    main()
