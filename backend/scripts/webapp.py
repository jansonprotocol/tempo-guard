"""
The board as a web app: one self-contained page, derived like everything.

Same doctrine as the README — fixtures.tsv, bets.tsv, league_hitrates.tsv
and patchlog.tsv are the typed sources, and this renders them; nothing on
the page is hand-written twice. `board.py` calls this at the end of every
render, so the app and the README can never disagree. The output is a
single static file with inline CSS/JS (hash-routed pages, no framework,
no build step) — Vercel serves `web/` as-is.

Pages:  Home (tabs: Playable · Found bets · Athena lanes · Completed)
        Past sessions · Retrosim · Patches · About

Usage:  python scripts/webapp.py          (also runs inside board.py main)
"""
from __future__ import annotations

import html
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, headline, ledger
from scripts.league_badges import rates

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "web" / "index.html"
TITLE = "ATHENA — TEMPO GUARD"
STAGE = "PRE-ALFA 1"          # bumped at each stage transition, deliberately
SESSION_NO = 4                # bumped when a run closes and a new one opens
SESSION_START = "28 Aug"      # the reset date of the current run

# The archived eras: frozen history, recorded once (the numbers live in
# archive/*/log.md and the README's archive section; they never change).
SESSIONS = [
    dict(name="The cup run", dates="24–27 Aug 2026",
         nums=[("Tip 1", "59/72 · 81.9%"), ("Tip 2", "41/58 · 70.7%"),
               ("Playable", "43/51 · 84.3%"), ("Bets", "25/35 · ROI −7.5%")],
         patches=["Cups reopened on the probationary Club Elo lane and "
                  "graded 12/16 on their first playoff night",
                  "Rules 5 and 6 became numbers; the board became this app",
                  "The calibration day: the retrosim page forced every "
                  "number to be defended — HIGH_SAYS_DEBIT, per-league "
                  "floors under an ROI constraint, board-wide 82.4 → 83.7",
                  "Closed at 81.9% tips / −7.5% ROI: the gap between "
                  "hitrate and price is the lesson Session #4 inherits"]),
    dict(name="First calibrated slate", dates="23–24 Aug 2026",
         nums=[("Tip 1", "56/65 · 86.2%"), ("Tip 2", "37/50 · 74.0%"),
               ("Bets", "22/27 · ROI +6.1%")],
         patches=["Five engine defects found and fixed while it ran",
                  "Rules 1–4 measured here: buy≥ discipline, flat 4% stakes, "
                  "the winner's-curse haircut, in-play rung pricing",
                  "Sixty-five settled tips became the measuring stick every "
                  "later change validates against"]),
    dict(name="Pre-calibration", dates="20–23 Aug 2026",
         nums=[("Tip 1", "84.2%"), ("Bets", "ROI −10.1%")],
         patches=["The first live boards, priced ~10.8 points optimistic",
                  "The era that taught the founding lesson: a strike rate "
                  "bought at prices that cannot pay for it is a loss",
                  "Archived untouched as the honesty baseline"]),
]


def _fmt(cell: str) -> str:
    s = html.escape(cell)
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    return s.replace(" · ", "<br>")


def _alignment(name: str, rung: str, side: str, tipmap: dict) -> str:
    """How the placed bet relates to what Athena published for that match.

    The ledger's own history is that most positions are NOT the printed
    rung — thirty of thirty-five one evening were Rule-6 ladder plays on a
    neighbouring line — so a bare fixture name says nothing about whether
    a bet followed the engine or overrode it. This names the relation:
    the tip itself, a Rule-6 neighbour of it, the Rule-5 DNB lane (which
    never has a printed rung), or off the board entirely.
    """
    got = tipmap.get(name)
    if got is None:
        return "—"
    t1, t2, t2side = got
    if rung == "DNB":
        return "Rule 5 · DNB"
    if rung in ("1X", "X2", "12"):
        return "own read · DC"
    if side in ("H", "A"):
        # a team-total position aligns only with a team-lane Tip 2
        if t2 and t2side and side == t2side and rung == t2:
            return "= Tip 2 (team)"
        return "team lane, own read"
    for label, tip in (("Tip 1", t1), ("Tip 2", t2)):
        if not tip or t2side and label == "Tip 2":
            continue
        if rung == tip:
            return f"= {label}"
        if rung[0] == tip[0] and abs(float(rung[1:]) - float(tip[1:])) <= 0.5:
            softer = (float(rung[1:]) > float(tip[1:])) == (rung[0] == "U")
            return f"{label} · R6 {'softer' if softer else 'harder'}"
    return "off board"


def _tipmap() -> dict:
    """fixture name -> (tip1 market, tip2 market, tip2 team side or None)."""
    out = {}
    for f in board.load():
        def market(cell, teams):
            c = re.sub(r"[✅❌◦]½?\s*", "", cell).replace("**", "").strip()
            if c.startswith("—"):
                return None, None
            toks = c.split()
            for i, tk in enumerate(toks[:4]):
                if re.fullmatch(r"[OU]\d+(?:\.\d+)?", tk):
                    if i == 0:
                        return tk, None
                    # a team lane leads with the club: "MC Alger O0.5"
                    club = " ".join(toks[:i])
                    h, a = (x.strip() for x in teams.split(" v ", 1)) \
                        if " v " in teams else (teams, "")
                    return tk, ("H" if club == h else "A" if club == a
                                else None)
            return None, None
        t1, _s1 = market(f.tip1, f.teams)
        t2, s2 = market(f.tip2, f.teams)
        out[f.teams] = (t1, t2, s2)
    return out


def _align_cls(a: str) -> str:
    if a.startswith("="):
        return "hit"
    if "R6" in a or a.startswith("Rule 5"):
        return "rule"
    if a == "off board":
        return "off"
    return ""


def _baselines() -> dict[str, float] | None:
    """Average per-league baseline hitrate for each tip, from the derived
    file scripts/baselines.py writes (each league's most recent 300
    fixtures replayed as-of, push counted as a hit, leagues weighted
    equally). None — and no bar — when the file is missing: a missing
    baseline is better than a stale-looking typed one."""
    path = ROOT / "config" / "baselines.tsv"
    if not path.exists():
        return None
    sums: dict[str, list[float]] = {"fp": [], "t1": [], "t2": [], "t3": []}
    # The claim behind those same lanes, where the replay recorded it — a
    # hitrate is only half a verdict without what was promised beside it.
    says: dict[str, list[float]] = {"fp": [], "t1": [], "t2": [], "t3": []}
    for ln in path.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        p = ln.split("\t")
        for key, hi, ni, si in (("t1", 1, 2, 7), ("t2", 3, 4, 8),
                                ("t3", 5, 6, 9)):
            if int(p[ni]) >= 30:
                sums[key].append(int(p[hi]) / int(p[ni]))
                if len(p) > si:
                    says[key].append(float(p[si]) / int(p[ni]))
    # The final pick — the card's starred lane — replayed over the same
    # window by scripts/final_pick.py --write. It leads the bar because
    # it is the one number describing what a reader following the star
    # would have scored.
    fp = ROOT / "config" / "final_pick.tsv"
    if fp.exists():
        for ln in fp.read_text().splitlines():
            if not ln.strip() or ln.startswith("#"):
                continue
            p = ln.split("\t")
            if int(p[2]) >= 30:
                sums["fp"].append(int(p[1]) / int(p[2]))
                if len(p) > 3:
                    says["fp"].append(float(p[3]) / int(p[2]))
    # (hitrate, claim) per lane — the claim stays None until the replay
    # that records it has been re-run, and the bar then omits it.
    out = {k: (sum(v) / len(v) * 100,
               sum(says[k]) / len(says[k]) * 100 if says[k] else None)
           for k, v in sums.items() if v}
    return out or None


def _read_tiers() -> str:
    """The About page's proof that tip 3 ignores league tier — derived
    from baselines.tsv at render so it moves when the replay is re-run."""
    path = ROOT / "config" / "baselines.tsv"
    if not path.exists():
        return ""
    tiers = {"under 80%": [], "80–85%": [], "85%+": []}
    for ln in path.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if int(p[2]) < 30:
            continue
        t1 = int(p[1]) / int(p[2])
        t3 = int(p[5]) / int(p[6]) if int(p[6]) >= 30 else None
        key = "under 80%" if t1 < .80 else "80–85%" if t1 < .85 else "85%+"
        tiers[key].append((t1, t3))
    rows = ""
    for name, g in tiers.items():
        if not g:
            continue
        t3s = [t3 for _t1, t3 in g if t3 is not None]
        rows += (f"<tr><td>tip 1 {name}</td><td>{len(g)}</td>"
                 f"<td>{sum(t1 for t1, _ in g)/len(g)*100:.1f}%</td>"
                 f"<td>{sum(t3s)/len(t3s)*100:.1f}%</td></tr>") if t3s else ""
    return (f'<div class="wrap"><table class="tiertable"><tr><th>league tier'
            f"</th><th>leagues</th><th>avg tip 1</th><th>avg tip 3</th></tr>"
            f"{rows}</table></div>")


def _bets_rows() -> list[dict]:
    fixtures = ledger.read_fixtures()
    tipmap = _tipmap()
    MARK = {1.0: "✅", 0.5: "✅½", 0.0: "◦", -0.5: "❌½", -1.0: "❌"}
    out = []
    for ln in ledger.BETS.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        name, rung, odds, side = parts[0], parts[1], float(parts[2]), parts[3]
        note = parts[6] if len(parts) > 6 else ""
        lane = rung if side == "-" else (
            f"{rung} ({'home' if side == 'H' else 'away'})")
        align = _alignment(name, rung, side, tipmap)
        fx = fixtures.get(name)
        prob = ledger.bet_prob(rung, side, fx) if fx else None
        # Column 5 is the cash-out's return multiple — "1" for the full
        # stake, a fraction for a partial (see headline.bets).
        if len(parts) > 4 and parts[4] not in ("", "0"):
            got = float(parts[4])
            out.append(dict(mark="◦" if got >= 1 else "❌",
                            name=name, lane=lane, odds=odds,
                            ret=f"{got:.2f}x", note=note, align=align,
                            prob=prob))
            continue
        # ledger.bet_state, same gate as the README block: FT for anything
        # that can still move, immediate for a clinched over.
        s = ledger.bet_state(rung, side, fx) if fx else None
        if s is None:
            out.append(dict(mark="open", name=name, lane=lane, odds=odds,
                            ret="—", note=note, align=align, prob=prob))
            continue
        ret = max(s, 0.0) * odds + (1 - abs(s))
        out.append(dict(mark=MARK[s], name=name, lane=lane, odds=odds,
                        ret=f"{ret:.2f}x", note=note, align=align,
                        prob=prob))
    return out


# The store carries era-split club names — "Real Madrid", "Real Madrid CF"
# and "Real Madrid C.F." are three rows of the same club, because different
# source files spell it differently. The board never cared (each fixture
# resolves its own frame), but a lookup form must, or El Clásico hides under
# a spelling the visitor did not type. So the bank is keyed on the engine's
# own canonical form, with every spelling aliased to it.
_JS_STOP = {"fc", "fk", "cf", "sc", "ac", "afc", "bk", "if", "sk",
            "club", "cp"}


def _jsnorm(s: str) -> str:
    """Mirror of the page's norm() — the alias map is keyed by it."""
    import unicodedata
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    s = re.sub(r"[.\-'()/]", " ", s)
    return " ".join(w for w in s.split() if w not in _JS_STOP)


def _key(name: str) -> str:
    """One club, one key. The engine's canonical form, minus the stray
    single letters that punctuation leaves behind ("real madrid c f")."""
    from app.data.features import _canonical
    k = _canonical(name)
    trimmed = " ".join(t for t in k.split()
                       if not (len(t) == 1 and t.isalpha()))
    return trimmed or k


_READS_CACHE = ROOT / "config" / "reads_cache.json"


def _reads(fixtures) -> dict:
    """(code|teams|date) -> [keywords, sentence]. Computed once per fixture
    — the read is as-of the match date, so it never changes — and cached so
    board renders stay fast."""
    import json as _json

    from scripts.reads import fixture_read
    cache = (_json.loads(_READS_CACHE.read_text())
             if _READS_CACHE.exists() else {})
    dirty = False
    for f in fixtures:
        key = f"{f.code}|{f.teams}|{f.kickoff.split(' ')[0]}"
        if key not in cache:
            cache[key] = fixture_read(f.code, f.teams, f.kickoff)
            dirty = True
    if dirty:
        _READS_CACHE.write_text(
            _json.dumps(cache, ensure_ascii=False, indent=0))
    return cache


def _sortkeys(f) -> str:
    """The numbers a card can be reordered by, stamped onto the element.

    Sorting happens in the browser, so each card has to carry its own keys
    rather than the page re-deriving them from rendered text — a string
    scrape would break the first time a cell's wording changed. The card's
    LEADING lane supplies probability, edge and side: Tip 1 by
    construction is the higher-probability rung, so it leads whenever it
    carries numbers, and Tip 2 stands in when Tip 1 abstained.

    A card with no numbers at all (an abstention, a stale-Elo row) gets
    −1, which parks it at the bottom of every numeric sort instead of
    scattering it through the middle.
    """
    from scripts.playable import LANE
    p = e = -1.0
    side = "z"                    # z sorts last: neither over nor under
    for cell in (f.tip1, f.tip2):
        m = LANE.match(cell)
        if not m:
            continue
        label = m.group(1).strip(" *·")
        if not label or label.startswith("—"):
            continue
        p = float(m.group(2))
        e = float(m.group(3).replace("−", "-"))
        rung = re.search(r"\b([OU])\d", label)
        side = ("o" if rung.group(1) == "O" else "u") if rung else "z"
        break
    badge = rates().get(f.code) or ""
    hr = re.search(r"([\d.]+)", badge)
    return (f'data-p="{p}" data-e="{e}" data-ou="{side}" '
            f'data-hr="{hr.group(1) if hr else -1}" '
            f'data-k="{html.escape(f.kickoff)}"')


def _gradekeys(f) -> str:
    """Each settled card carries its own four grades, so the filter bar
    can recount hitrates for whatever subset is on screen — a league, a
    team, a rung — without the page re-deriving anything from text.

    The four match the tiles exactly: tip 1 and tip 2 count only at the
    playable standard, tip 3 counts whole (it only ever prints above its
    own bar), and the final pick is the ★ lane's own grade.
    """
    if not f.settled:
        return ""
    def mark(which):
        src = f.status if which == 1 else (f.tip2 if which == 2 else f.tip3)
        m = src.lstrip()[:1]
        return m if m in ("✅", "❌", "◦") else None

    out = []
    for which, key in ((1, "g1"), (2, "g2"), (3, "g3")):
        if which < 3 and not f.lane(which):
            continue
        if which == 3 and not f.tip3.strip():
            continue
        m = mark(which)
        if m:
            out.append(f'{key}="{0 if m == "❌" else 1}"')
    pick = 1 if f.lane(1) else (3 if f.tip3.strip() else 1)
    m = mark(pick)
    if m:
        out.append(f'gf="{0 if m == "❌" else 1}"')
    return (" data-" + " data-".join(out)) if out else ""


_T1RATES: dict | None = None


def _t1_rates() -> dict:
    """Per-league tip 1 baseline from the 300-window replay — the tier
    every protocol-driven accent derives from, same file as the About
    page's proof table."""
    global _T1RATES
    if _T1RATES is None:
        _T1RATES = {}
        path = ROOT / "config" / "baselines.tsv"
        if path.exists():
            for ln in path.read_text().splitlines():
                if ln.startswith("#") or not ln.strip():
                    continue
                p = ln.split("\t")
                if int(p[2]) >= 30:
                    _T1RATES[p[0]] = int(p[1]) / int(p[2])
    return _T1RATES


def _weak_leagues() -> set:
    """The protocol's 'read tip 3 first' tier: tip 1 baseline under 80%."""
    return {c for c, r in _t1_rates().items() if r < 0.80}


def _card(f, kind: str, reads: dict) -> str:
    badge = rates().get(f.code)
    league = html.escape(f.league) + (
        f' <span class="badge">{html.escape(badge)}</span>' if badge else "")
    if f.status.startswith("FT"):
        head = f"⚪ {html.escape(f.status[3:] or f.status)}"
    elif f.settled:
        head = f"{f.status[:1]} {html.escape(board._mark(f))}"
    elif f.status:
        head = f'<span class="live">🔴 {html.escape(f.status)}</span>'
    else:
        head = f"🕑 {board._stamp(f)}"

    # The protocol's accent: the lane to read first on this card.
    # Reading guidance only, so settled cards drop it.
    best = 0
    if not f.settled:
        # Athena marks exactly ONE preferred lane per card (the bettor's
        # rule, 30 Aug), and the order is what the measurement supports.
        # final_pick.py replayed the chooser over 16,554 fixtures: tip 2
        # picks graded 12.7 points BELOW what tip 1 would have done on
        # those same fixtures, tip 3 picks 5.9 below, and a sub-bar tip 1
        # still landed 84.5% — a thin edge means the league baseline is
        # already high, not that the tip is weak. So the star can only
        # ever be tip 1 or tip 3 (the bettor's rule after reading that
        # table): a playable tip 1 first, then a printed result lane,
        # then tip 1 as the engine's own pick. Tip 2 is never starred,
        # whatever its badge. The star means "read this first", never
        # "this is the better bet" — the buy≥ bracket decides that.
        if f.lane(1):
            best = 1
        elif f.tip3.strip():
            best = 3
        elif f.tip1.strip() and not f.tip1.startswith("—"):
            best = 1
    star = ('<span class="best-tag" title="The lane to read first on this '
            'card — not a claim that it is the better bet; the buy≥ '
            'bracket decides that">★ read first</span>')

    def lane(which, cell):
        if cell.strip() in ("", "—", "— none"):
            return ""
        pl = " pl" if (not f.settled and f.lane(which)) else ""
        if which == best:
            pl += " best"
        # While the match runs, say what the score has done to this lane.
        live = ""
        if not f.settled and f.status:
            from scripts import liveline
            s = liveline.progress(cell, f.teams, f.status)
            if s:
                cls = ("gone" if s.startswith("✗") or "gone" in s
                       else "won" if s.startswith("✓") else "")
                live = (f'<div class="prog {cls}">{html.escape(s)}</div>')
        return (f'<div class="lane{pl}"><span class="which">Tip {which}'
                f"</span> {_fmt(cell)}"
                f"{star if which == best else ''}{live}</div>")

    read = reads.get(f"{f.code}|{f.teams}|{f.kickoff.split(' ')[0]}")
    kw = (f'<div class="kw">🧠 {html.escape(read[0])}</div>' if read else "")
    from scripts import liveline
    tie = liveline.tie_note(f.teams, f.status)
    tie_html = (f'<div class="tie">🏆 {html.escape(tie)} '
                f'<span class="dim">Context only — Athena prices the '
                f'match total and does not see the tie.</span></div>'
                if tie else "")
    # On the PLAYABLE tab a card leads with the lane that put it there.
    # A fixture whose Tip 1 sits under the bar can still be playable
    # through Tip 2 — leading with the sub-bar Tip 1 put a −1.5% headline
    # on the playable tab, which read as a mistake and effectively was
    # one. Everywhere else Tip 1 leads as before; the expanded body
    # always carries the other lane.
    lead, rest = (1, f.tip1), (2, f.tip2)
    if kind == "play" and not f.settled and not f.lane(1) and f.lane(2):
        lead, rest = (2, f.tip2), (1, f.tip1)
    # Tip 3 rides the card FACE, not the fold — a lane nobody sees is a
    # lane that can never earn its way off probation.
    t3 = (f'<div class="lane{" best" if best == 3 else ""}">'
          f'<span class="which">Tip 3</span> '
          f'{_fmt(f.tip3)} <span class="dim">· result lane</span>'
          f'{star if best == 3 else ""}</div>'
          if f.tip3.strip() else "")
    # Protocol step 2, applied to the layout itself: in a league whose
    # tip 1 baseline runs under 80% — or a consensus-capped one — a card
    # whose tip 1 did not clear the playable bar leads with tip 3
    # instead. Same principle as the playable tab's lead swap: a card
    # leads with the lane worth reading first. Settled cards keep the
    # tip 1 order so grading reads consistently.
    face = f"{lane(*lead)}{t3}"
    if (t3 and not f.settled and not f.lane(1)
            and (f.code in _weak_leagues()
                 or "capped" in (rates().get(f.code) or ""))):
        face = f"{t3}{lane(*lead)}"
    top = (f'<div class="teams">{html.escape(f.teams)}'
           f'<span class="more">more ▾</span></div>'
           f'<div class="meta">{head} · {league}</div>{kw}'
           f"{face}")
    body = lane(*rest) + tie_html
    if read:
        body += f'<div class="read">{read[1]}</div>'
    if not body:
        body = '<div class="read dim">nothing more on this one</div>'
    return (f'<details class="card {kind}" '
            f'data-t="{html.escape(f.teams.lower())} '
            f'{html.escape(f.league.lower())} {f.code.lower()}" '
            f"{_sortkeys(f)}{_gradekeys(f)}>"
            f"<summary>{top}</summary>{body}</details>")


SORTS = (("k", "kickoff"), ("p", "probability"), ("e", "edge"),
         ("hr", "league hitrate"), ("o", "overs first"),
         ("u", "unders first"))


def _grid(cards, kind, reads):
    """One tab's cards, with the sort bar that reorders them in place."""
    if not cards:
        return '<p class="dim">nothing here right now</p>'
    bar = "".join(
        f'<button class="sortb{" on" if key == "k" else ""}" '
        f'data-s="{key}">{label}</button>' for key, label in SORTS)
    return (f'<div class="sortbar"><span class="dim">sort</span>{bar}</div>'
            '<div class="grid">'
            + "".join(_card(f, kind, reads) for f in cards)
            + "</div>")


def _hitrates_rows() -> str:
    """One row per league. The gap is colored only when it clears two
    standard errors of its OWN row — a −2.1 on 200 fixtures is one SE from
    honest, and painting it red taught exactly the wrong lesson: sixteen
    'broken' leagues that re-measured into one small shared bias.

    The buy-from column is the average buy≥ a card would have printed for
    that league's tips — the ROI half of the story. A league can hit 90%
    and still be unbuyable if its rungs price at 1.10."""
    import math
    rows = []
    for ln in (ROOT / "config" / "league_hitrates.tsv").read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        lg, n, hit, gap = parts[:4]
        buy = parts[4] if len(parts) > 4 and parts[4] else "—"
        p_hit = parts[5] if len(parts) > 5 and parts[5] else ""
        p_n = parts[6] if len(parts) > 6 and parts[6] else ""
        from app.engine.market_select import CONSENSUS_CAP_LEAGUES
        play = (f'{p_hit}% <span class="dim">({p_n})</span>'
                if p_hit else
                '<span class="dim" title="published probability capped at '
                'the league consensus — no lane here can claim edge">'
                'capped</span>'
                if lg in CONSENSUS_CAP_LEAGUES else "—")
        g = float(gap.replace("−", "-"))
        p = float(hit) / 100
        se = math.sqrt(max(p * (1 - p), 1e-9) / int(n)) * 100
        cls = "dim" if abs(g) < 2 * se else "pos" if g > 0 else "neg"
        rows.append(f"<tr><td>{html.escape(lg)}</td><td>{hit}%</td>"
                    f"<td>{play}</td>"
                    f'<td class="{cls}">{gap}</td><td>{buy}</td>'
                    f'<td class="dim">{n}</td></tr>')
    return "".join(rows)


def _patch_rows() -> str:
    out, last_date = [], None
    for ln in (ROOT / "config" / "patchlog.tsv").read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        date, area, note = ln.split("\t")
        d = date if date != last_date else ""
        last_date = date
        out.append(f'<tr><td class="dim">{d}</td>'
                   f'<td><span class="area">{html.escape(area)}</span></td>'
                   f"<td>{html.escape(note)}</td></tr>")
    return "".join(out)


def _hypothesis_html() -> str:
    """The ledger of everything tried, grouped by verdict.

    Same file the README renders, so the two cannot drift. The declined
    group is the largest and is shown in full rather than folded away —
    what did NOT work is the more expensive half of this project's
    knowledge, and hiding it is how an idea gets proposed twice.
    """
    from scripts.board import load_hypotheses
    rows = load_hypotheses()
    heads = (
        ("green", "Verified and helping",
         "Cleared two separate time windows and is live in the engine "
         "today."),
        ("orange", "Unfinished",
         "Measured but not concluded, or shipped on probation and still "
         "waiting on live results to confirm it."),
        ("red", "Declined",
         "Tested and rejected, with the number that killed it. Kept on "
         "purpose — a dead idea that stays written down does not get "
         "re-proposed every fortnight."),
    )
    out = []
    for key, title, blurb in heads:
        got = [r for r in rows if r[0] == key]
        body = "".join(
            f'<tr><td class="dim">{html.escape(d[5:])}</td>'
            f'<td><span class="area">{html.escape(a)}</span></td>'
            f"<td><b>{html.escape(n)}</b><br>"
            f'<span class="dim">{html.escape(v)}</span></td></tr>'
            for _s, d, a, n, v in got)
        out.append(
            f'<h3 class="hyp {key}"><span class="dot"></span>{title}'
            f'<span class="n">{len(got)}</span></h3>'
            f'<p class="dim">{blurb}</p>'
            f'<div class="wrap"><table class="hyptable">{body}</table></div>')
    return "".join(out)


def _learn() -> str:
    """The teaching block: one example card, each part explained in a line."""
    card = """<details class="card play" open>
<summary><div class="teams">Real Madrid v Real Sociedad<span class="more">more \u25be</span></div>
<div class="meta">\U0001f551 26-08 21:00 \u00b7 LaLiga <span class="badge">(81.7 +3.4)</span></div>
<div class="kw">\U0001f9e0 elite attack vs leaky defence</div>
<div class="lane pl"><span class="which">Tip 1</span> O1.5 81.2% +6.8%<br>buy\u22651.32 (+8.0% margin)</div></summary>
<div class="lane pl"><span class="which">Tip 2</span> <b>Real Madrid O1.5</b> 67.0% +24.7% (team)<br>buy\u22651.43 (\u22124.2% margin)</div>
<div class="read"><b>Real Madrid</b>: elite attack, elite defence, in form \u2014 against <b>Real Sociedad</b>: leaky defence, struggling. That pairing \u2014 firepower against a defence that leaks \u2014 is where the goals in this tip come from.</div>
</details>"""
    rows = [
        ("The matchup", "home team first, away team second \u2014 venue "
         "matters and is already in the numbers."),
        ("\U0001f551 / \U0001f534 / \u2705\u274c", "the clock before "
         "kickoff, red while live, then the verdict with the final score "
         "once graded."),
        ("The league line", "the competition. On cup cards, \u00b7 "
         "probationary marks a lane still earning trust with live "
         "results."),
        ("(81.7 +3.4)", "the league's proven PLAYABLE record: how its "
         "above-bar lanes \u2014 the ones worth betting \u2014 actually land, and "
         "how far that sits from the league's all-tips baseline. A "
         "consensus-cap league shows (82.0 capped): no lane there may "
         "claim edge, so it has no playable record to show."),
        ("\U0001f9e0 the read", "what Athena measured in this matchup, in "
         "keywords \u2014 tap the card for the full story in sentences."),
        ("Tip 1", "the engine's best market. O1.5 = over 1.5 goals in the "
         "match; 81.2% = claimed probability; +6.8% = edge over a typical "
         "match."),
        ("buy\u22651.32 (+8.0% margin)", "the minimum odds that make this tip "
         "worth money. The price listens to the tip AND the league's "
         "playable record \u2014 a tip below that record leans on it (0.4/0.6), "
         "one above it mostly trusts itself (0.8/0.2) \u2014 so lower-"
         "probability lanes become reachable at real-world odds. The "
         "bracket is what margin the printed price still holds over the "
         "tip's own break-even: negative means the blend is reaching down "
         "to make the lane buyable, eyes open."),
        ("Green border / lane", "a playable lane: edge above +1%. These "
         "are the tips with real value \u2014 the rest are shown for "
         "honesty, not for money."),
        ("Tip 2 \u00b7 more \u25be", "the second lane. (team) means a "
         "TEAM total \u2014 here Real Madrid alone to score 2+ \u2014 a "
         "different market from the match total, offered when its edge "
         "beats the ladder's runner-up. On other cards (floor \u2212x) "
         "says how far Tip 2 sits below the confidence bar."),
        ("\U0001f9e0 the story", "tap any card open: the read in full "
         "sentences \u2014 every phrase maps to something measured (attack "
         "and defence bands, form, table stakes, cup Elo), never "
         "invented."),
    ]
    items = "".join(f'<tr><td class="mk"><b>{k}</b></td><td>{v}</td></tr>'
                    for k, v in rows)
    return (f'<div id="learn"><h2>\U0001f393 Learn Athena \u2014 how to '
            f'read a block</h2><div class="grid" style="max-width:420px">'
            f"{card}</div><div class=\"wrap\" style=\"margin-top:10px\">"
            f"<table>{items}</table></div>"
            # window. is not optional here: an inline handler runs with
            # the element in its scope chain, and Element.prototype has a
            # scrollTo of its own — so a bare scrollTo scrolled the
            # button's own (unscrollable) content and silently did nothing.
            f'<button class="btn" onclick="window.scrollTo({{top:0,'
            f'behavior:\'smooth\'}})">\u2191 Back to top</button></div>')


def _check_js(page: str) -> None:
    """A syntax error in the generated script blanks the whole app — the
    router never runs, so every page stays hidden. This page is written by
    Python f-strings, where one collapsed backslash does exactly that, so
    the JS is parsed before it can ship."""
    import shutil
    import subprocess
    import tempfile
    node = shutil.which("node")
    if not node:
        return
    js = page[page.index("<script>") + 8:page.rindex("</script>")]
    with tempfile.NamedTemporaryFile("w", suffix=".js", delete=False) as fh:
        fh.write(js)
        path = fh.name
    r = subprocess.run([node, "--check", path], capture_output=True,
                       text=True)
    if r.returncode != 0:
        raise SystemExit(f"generated JS is broken:\n{r.stderr}")


def main() -> None:
    fixtures = board.load()
    t, p = board._tallies(fixtures)
    (h1, n1), (h2, n2) = t[1], t[2]
    (p1, q1), _ = p[1], p[2]
    bh, bn, roi = headline.bets()
    # Tip 3 grades off its own column marks — on probation it feeds no
    # other tally, but its record is public from the first settled lane.
    # A ◦ (DNB draw) counts as a hit, same convention as everywhere.
    # Hindsight rows — session fixtures that settled before the lane
    # existed, graded retroactively at the bettor's request — are counted
    # but named in the tile, so the live record can never hide behind them.
    h3 = n3 = hs3 = 0
    for f in fixtures:
        mark = f.tip3.lstrip()[:1] if f.tip3.strip() else ""
        if mark in ("✅", "❌", "◦"):
            n3 += 1
            h3 += mark != "❌"
            hs3 += "hindsight" in f.tip3

    reads = _reads(fixtures)
    pending = [f for f in fixtures if not f.settled]
    playable = [f for f in pending if f.lane(1) or f.lane(2)]
    # "Athena lanes" is everything the engine published for this run, the
    # playable ones included — the playable tab is a filter on top of it,
    # not a slice taken out of it.
    waiting = pending
    done = [f for f in fixtures if f.settled][::-1]

    def tile(label, value, sub):
        return (f'<div class="tile"><div class="v">{value}</div>'
                f'<div class="l">{label}</div><div class="s">{sub}</div></div>')

    # The six-tile row the bettor specified: every lane family's record,
    # then the money — PLAYABLE lanes only (edge above the bar), because
    # those are the lanes anyone actually acts on; the sub-bar band is
    # published for honesty, not for the scoreboard. Tip 3 qualifies
    # whole: it only ever prints above its own floor and edge bar.
    from scripts.playable import LANE
    (pb1, pq1), (pb2, pq2) = p[1], p[2]
    # The FINAL PICK tile: the card's starred lane, graded on every
    # completed fixture of this session — the same chooser the board
    # renders (playable tip 1, else a printed result lane, else tip 1),
    # so the tile answers "what would following the ★ have scored".
    fh = fn = fp3 = 0
    fsays: list[float] = []
    for f in fixtures:
        if not f.settled:
            continue
        pick = 1 if f.lane(1) else (3 if f.tip3.strip() else 1)
        src = f.tip1 if pick == 1 else f.tip3
        mark = f.status[:1] if pick == 1 else f.tip3.lstrip()[:1]
        if mark not in ("✅", "❌", "◦"):
            continue
        fn += 1
        fh += mark != "❌"
        fp3 += pick == 3
        sm = LANE.match(src.lstrip("✅❌◦ "))
        if sm:
            fsays.append(float(sm.group(2)))
    # The claim each family carried into those graded lanes, so a low
    # tile reads as WHAT IT PROMISED, not as failure: measured this week,
    # every says band delivers its claim in both half-windows — the only
    # honest "filter" is the expectation printed beside the outcome.
    says = {1: [], 2: [], 3: []}
    for f in fixtures:
        for which, cell in ((1, f.tip1), (2, f.tip2), (3, f.tip3)):
            if not f.lane(which) if which < 3 else False:
                continue
            src = cell if which < 3 else f.tip3
            mark = (f.status[:1] if which == 1 else src.lstrip()[:1])
            if mark not in ("✅", "❌", "◦"):
                continue
            m = LANE.match(src.lstrip("✅❌◦ "))
            if m:
                says[which].append(float(m.group(2)))
    def claims(w):
        return (f" · claims {sum(says[w])/len(says[w]):.1f}"
                if says[w] else "")
    tiles = "".join([
        tile("final pick", f"{fh / fn * 100:.1f}%" if fn else "—",
             f"the ★ lane · {fh}/{fn}"
             + (f" · claims {sum(fsays)/len(fsays):.1f}" if fsays else "")),
        tile("tip 1", f"{pb1 / pq1 * 100:.1f}%" if pq1 else "—",
             f"playable · {pb1}/{pq1}{claims(1)}"),
        tile("tip 2", f"{pb2 / pq2 * 100:.1f}%" if pq2 else "—",
             f"playable · {pb2}/{pq2}{claims(2)}"),
        tile("tip 3", f"{h3 / n3 * 100:.1f}%" if n3 else "—",
             (f"{h3}/{n3} · probation{claims(3)}" +
              (f" · {hs3} hindsight" if hs3 else "")) if n3
             else "probation · first grades tonight"),
        tile("taken bets", f"{bh / bn * 100:.1f}%" if bn else "—",
             f"your lanes · {bh}/{bn} hits"),
        tile("roi", f"{roi:+.1f}%", f"flat stakes · {bn} settled"),
    ])

    bet_rows = _bets_rows()
    # Any lane priced through the cache this render is written back, so
    # the next one costs nothing.
    from scripts import lane_price
    lane_price.flush()

    # Longest run of consecutive hits over the settled book, in the order
    # the bets were logged — pushes count as hits, the board's convention.
    streak = best_streak = 0
    for b in bet_rows:
        if b["mark"] == "open":
            continue
        streak = 0 if b["mark"].startswith("❌") else streak + 1
        best_streak = max(best_streak, streak)

    read_tiers = _read_tiers()
    base = _baselines()
    basebar = ""
    if base:
        def cell(name, key):
            hit, said = base[key]
            claim = (f'<span class="dim"> vs {said:.1f} said</span>'
                     if said else "")
            return f"{name} <b>{hit:.1f}%</b>{claim}"
        cells = " · ".join(
            cell(n, k) for n, k in (("final pick", "fp"), ("tip 1", "t1"),
                                    ("tip 2", "t2"), ("tip 3", "t3"))
            if k in base)
        basebar = (f'<div class="basebar">Baselines — hit vs said: {cells} '
                   f'<span class="dim">— every tip replayed over each '
                   f'league’s last 300 matches, averaged</span></div>')

    def _bet_tr(b):
        # Every field is searchable through the same bar the cards use —
        # the row carries its own lowercase haystack in data-t.
        prob = f'{b["prob"]*100:.1f}%' if b["prob"] else "—"
        hay = " ".join([b["name"], b["lane"], b["align"], b["note"],
                        b["mark"], f'{b["odds"]:.2f}', prob]).lower()
        return (f'<tr data-t="{html.escape(hay)}">'
                f'<td class="mk">{b["mark"]}</td>'
                f'<td>{html.escape(b["name"])}</td>'
                f'<td>{html.escape(b["lane"])}</td>'
                f'<td class="dim">{prob}</td>'
                f'<td>{b["odds"]:.2f}</td><td>{b["ret"]}</td>'
                f'<td><span class="align {_align_cls(b["align"])}">'
                f'{html.escape(b["align"])}</span></td>'
                f'<td class="note">{html.escape(b["note"])}</td></tr>')

    bets_html = "".join(_bet_tr(b) for b in bet_rows)
    # The book's price profile in one line: what the average ticket pays,
    # over everything logged and over what has already settled — the
    # number to hold the ROI against (an 84% hit rate only pays at these
    # odds if they average above ~1.19).
    all_odds = [b["odds"] for b in bet_rows]
    set_odds = [b["odds"] for b in bet_rows if b["mark"] != "open"]
    probs = [b["prob"] for b in bet_rows if b["prob"]]
    bets_meta = (
        f'<p class="dim">average odds <b>{sum(all_odds)/len(all_odds):.2f}'
        f'</b> across {len(all_odds)} positions'
        + (f' · <b>{sum(set_odds)/len(set_odds):.2f}</b> on the '
           f'{len(set_odds)} settled' if set_odds else "")
        + (f' · average probability <b>'
           f'{100*sum(probs)/len(probs):.1f}%</b> on the {len(probs)} '
           f'the engine priced' if probs else "") + "</p>"
        if all_odds else "")

    sessions_html = ""
    for s in SESSIONS:
        nums = "".join(f'<div class="tile"><div class="v">{v}</div>'
                       f'<div class="l">{k}</div></div>'
                       for k, v in s["nums"])
        patches = "".join(f"<li>{html.escape(x)}</li>" for x in s["patches"])
        sessions_html += (
            f'<div class="session"><h3>{html.escape(s["name"])} '
            f'<span class="dim">{s["dates"]}</span></h3>'
            f'<div class="tiles">{nums}</div>'
            f"<p><b>Patches &amp; calibrations of this era</b></p>"
            f"<ul>{patches}</ul></div>")

    # Session banner dates: start is the run's reset; end follows the
    # latest fixture on the board, so it never goes stale by hand.
    MONTHS = ("Jan Feb Mar Apr May Jun Jul Aug Sep Oct Nov Dec").split()
    last = max(f.kickoff for f in fixtures).split(" ")[0]
    _y, _m, _d = last.split("-")
    session_end = f"{int(_d)} {MONTHS[int(_m) - 1]} {_y}"

    # The match bank the visitor form answers from: the retrosim bank
    # (matchbank.py) plus everything on the current board.
    import json as _json
    retro_path = ROOT / "config" / "matchbank_retro.json"
    bank = _json.loads(retro_path.read_text()) if retro_path.exists() else {}
    for f in fixtures:
        comp = bank.setdefault(f.code, dict(name=f.league, teams=[],
                                            matches=[]))
        if " v " not in f.teams:
            continue
        hh, aa = (x.strip() for x in f.teams.split(" v ", 1))
        for nm in (hh, aa):
            if nm not in comp["teams"]:
                comp["teams"].append(nm)
        entry = dict(d=f.kickoff.split(" ")[0], h=hh, a=aa,
                     tip=re.sub(r"\*\*(.+?)\*\*", r"\1", f.tip1),
                     score="", mark="", src="board")
        if f.settled and "—" in f.status:
            entry["score"] = f.status.split("—")[-1].strip()
            entry["mark"] = f.status[:1]
        if f.tip2.strip() not in ("", "—", "— none"):
            entry["t2"] = re.sub(r"\*\*(.+?)\*\*", r"\1", f.tip2)
        rd = reads.get(f"{f.code}|{f.teams}|{f.kickoff.split(' ')[0]}")
        if rd:
            entry["kw"] = rd[0]
        comp["matches"].append(entry)
    # The hero banner's number: Tip 1 over the most recent 300 graded
    # lanes AT THE PLAYABLE STANDARD (edge above +1%) — the lanes the
    # site actually offers, not every tip the engine ran. A recency
    # window rather than the all-time table average, by choice: the
    # headline should describe the engine AS IT PRICES TODAY, and 300 is
    # wide enough that one cold weekend cannot swing it while recent
    # calibration work still shows up in it. The full instruments keep
    # replaying everything (retrosim at n=800 per league feeds the bank
    # and the table); only the headline derives from the freshest 300.
    # Sub-bar tips are excluded (measured 27 Aug as the band that grades
    # below its own claim) and the consensus-cap leagues contribute
    # nothing because no lane there can badge playable. Derived from the
    # same bank the Ask Athena form answers from, so it refreshes with
    # the bank and can never be a typed number that quietly rots.
    graded = []
    for comp in bank.values():
        for m in comp["matches"]:
            if m.get("mark") not in ("✅", "✅½", "◦", "❌"):
                continue
            e = re.search(r"([+\-−]\d+(?:\.\d+)?)%\s*(?:\(|·|$)",
                          m.get("tip", ""))
            if not e:
                continue
            if float(e.group(1).replace("−", "-")) > 1.0:
                graded.append((m["d"], m["mark"]))
    graded.sort(reverse=True)
    window = graded[:300]
    # A push counts as a hit, same as everywhere on the board: the
    # standing offset plays the rung a notch softer, which wins there.
    hero_rate = (sum(1 for _d, mk in window
                     if mk.startswith("✅") or mk == "◦")
                 / len(window) * 100) if len(window) >= 100 else None
    hero_sub = f" — {hero_rate:.1f}% hitrate" if hero_rate else ""
    hero_fine = "Tip 1 · the 300 most recent graded playable lanes"
    live_line = (f"Live so far: Tip 1 <b>{h1 / n1 * 100:.1f}%</b> on "
                 f"{h1}/{n1} settled, found bets <b>{roi:+.1f}%</b> ROI "
                 f"on {bh}/{bn}." if n1 else "First results land tonight.")

    # Visitors think of a UCL qualifier as a UCL game, so the form does
    # too: the -Q competitions fold into their parents for lookup. The
    # board itself keeps the distinction (different baselines, different
    # badges) — this merge is presentation only.
    for q, parent in (("UCL-Q", "UCL"), ("UEL-Q", "UEL"),
                      ("UECL-Q", "UECL")):
        if q in bank:
            names = {"UCL": "UEFA Champions League",
                     "UEL": "UEFA Europa League",
                     "UECL": "UEFA Conference League"}
            dst = bank.setdefault(parent, dict(
                name=names[parent], teams=[], matches=[]))
            dst["teams"] += bank[q]["teams"]
            dst["matches"] += bank[q]["matches"]
            del bank[q]
    # Canonical keys: one club, one identity, every spelling aliased to
    # it. Three signals decide which spellings are the same club: the
    # engine's canonical form, the Club Elo identity (one external name
    # per club), and config/club_nicknames.tsv for what people actually
    # type. The display name is the shortest spelling in the group —
    # "Lyon" over "Olympique Lyonnais" — unless the nickname file names
    # one explicitly.
    from app.data import club_elo as _ce
    nick, prefer = {}, {}
    nick_path = ROOT / "config" / "club_nicknames.tsv"
    if nick_path.exists():
        for ln in nick_path.read_text().splitlines():
            if ln.strip() and not ln.startswith("#") and "\t" in ln:
                a, shown = (x.strip() for x in ln.split("\t", 1))
                nick[_jsnorm(a)] = shown

    def base_key(name):
        # One club, one key: the Club Elo identity when it knows the club,
        # the engine's canonical form otherwise.
        ext = (_ce._names().get(name)
               or _ce._norm_index().get(_ce._norm(name)))
        return _key(ext) if ext else _key(name)

    def group(name):
        # A nickname resolves to its display name, and that display name
        # goes through the SAME base_key — otherwise "PSG" keys as
        # "paris saint germain" while the fixtures key as "paris sg".
        shown = nick.get(_jsnorm(name))
        return base_key(shown if shown else name)

    alias, spellings = {}, {}
    for comp in bank.values():
        for m in comp["matches"]:
            for fld, side in (("h", "kh"), ("a", "ka")):
                g = group(m[fld])
                m[side] = g
                alias[_jsnorm(m[fld])] = g
                spellings.setdefault(g, set()).add(m[fld])
    for shown in set(nick.values()):
        prefer[base_key(shown)] = shown
    for a, shown in nick.items():
        alias[a] = base_key(shown)
    for g, names in spellings.items():
        alias.setdefault(_jsnorm(g), g)
        if g not in prefer:
            prefer[g] = min(names, key=lambda n: (len(n), n))
    for comp in bank.values():
        shown = {group(nm) for nm in comp["teams"]}
        shown |= {m["kh"] for m in comp["matches"]}
        shown |= {m["ka"] for m in comp["matches"]}
        comp["teams"] = sorted({prefer.get(g, g) for g in shown})
        comp["matches"].sort(key=lambda m: m["d"])
    live = {g for comp in bank.values() for m in comp["matches"]
            for g in (m["kh"], m["ka"])}
    names = {g: prefer.get(g, g) for g in live}
    (OUT.parent / "matchbank.json").write_text(
        _json.dumps(dict(comps=bank, alias=alias, names=names),
                    ensure_ascii=False))

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{TITLE}</title>
<link rel="icon" href="athena-logo.png">
<style>
:root {{ color-scheme:dark; --bg:#0e1116; --card:#161b24; --edge:#232a36; --tx:#dbe2ee;
  --dim:#8b95a7; --green:#37c26b; --blue:#4f8ef7; --gold:#e8b93c; }}
* {{ box-sizing:border-box; margin:0; }}
body {{ background:var(--bg); color:var(--tx); font:15px/1.45 system-ui,
  -apple-system,"Segoe UI",Roboto,sans-serif; max-width:1200px;
  margin:0 auto; padding:0 16px 16px; }}
nav {{ display:flex; align-items:center; gap:4px; flex-wrap:wrap;
  padding:12px 0; border-bottom:1px solid var(--edge); margin-bottom:14px; }}
nav img {{ width:38px; height:38px; margin-right:8px; }}
nav .brand {{ margin-right:auto; }}
nav .brand b {{ letter-spacing:.05em; }}
nav .brand .stage {{ display:block; color:var(--gold); font-size:10px;
  letter-spacing:.22em; }}
nav a {{ color:var(--dim); text-decoration:none; padding:7px 10px;
  border-radius:8px; font-size:13px; }}
nav a.on {{ color:var(--tx); background:var(--card); }}
.tabs {{ display:flex; gap:6px; flex-wrap:wrap; margin:12px 0; }}
.tabs a {{ color:var(--dim); text-decoration:none; padding:7px 12px;
  border:1px solid var(--edge); border-radius:20px; font-size:13px; }}
.tabs a.on {{ color:var(--bg); background:var(--green);
  border-color:var(--green); font-weight:600; }}
.tabs a.on.gold {{ background:var(--gold); }}
.tabs a.on.blue {{ background:var(--blue); }}
.tabs a.on.grey {{ background:var(--dim); }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
  gap:10px; margin:10px 0; }}
.tile {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:12px 14px; }}
.tile .v {{ font-size:22px; font-weight:700; }}
.tile .l {{ color:var(--dim); font-size:11px; text-transform:uppercase;
  letter-spacing:.08em; margin-top:2px; }}
.tile .s {{ color:var(--dim); font-size:12px; }}
.session {{ color:var(--gold); font-size:12px; letter-spacing:.18em;
  text-transform:uppercase; margin:2px 0 8px; }}
.fcounts {{ display:grid; grid-template-columns:repeat(4,1fr); gap:6px;
  margin:0 0 12px; }}
.fc {{ background:var(--card); border:1px solid var(--edge);
  border-radius:8px; padding:7px 9px; text-align:center; }}
.fc .v {{ font-size:17px; font-weight:700; }}
.fc .l {{ color:var(--dim); font-size:10px; text-transform:uppercase;
  letter-spacing:.1em; margin-top:1px; }}
.fc .s {{ color:var(--dim); font-size:11px; }}
.basebar {{ background:var(--card); border:1px solid var(--edge);
  border-radius:8px; padding:7px 12px; margin:8px 0; font-size:12px; }}
.basebar b {{ color:var(--gold); }}
.basebar .dim {{ font-size:11px; }}
.ask {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:12px 14px; margin:0 0 12px; font-size:13px; }}
.askrow {{ display:grid; gap:8px; margin-top:10px;
  grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); }}
.askrow input, .askrow select {{ background:#111622; border:1px solid
  var(--edge); border-radius:8px; color:var(--tx); padding:8px 10px;
  font:inherit; font-size:13px; }}
.askbtn {{ margin:0; }}
.combo {{ position:relative; }}
.combo input {{ width:100%; }}
.sug {{ display:none; position:absolute; z-index:20; left:0; right:0;
  top:calc(100% + 3px); background:#111622; border:1px solid var(--edge);
  border-radius:8px; max-height:230px; overflow-y:auto; }}
.sug.on {{ display:block; }}
.sug div {{ padding:9px 11px; cursor:pointer; font-size:13px;
  border-bottom:1px solid var(--edge); }}
.sug div:last-child {{ border-bottom:0; }}
.sug div:hover, .sug div.pick {{ background:var(--card); }}
.sug .why {{ color:var(--dim); font-size:11px; }}
#ask-out {{ margin-top:10px; }}
#ask-out .grid {{ max-width:460px; }}
.askerr {{ color:#e07a6a; font-size:13px; padding:8px 2px; }}
.btn {{ display:block; width:100%; background:var(--card);
  border:1px solid var(--edge); border-radius:8px; color:var(--gold);
  padding:9px 12px; margin:2px 0 8px; font:inherit; font-size:13px;
  cursor:pointer; text-align:center; }}
#learn {{ margin-top:26px; border-top:1px solid var(--edge);
  padding-top:6px; }}
input#q {{ width:100%; background:var(--card); border:1px solid var(--edge);
  border-radius:8px; color:var(--tx); padding:9px 12px; margin:2px 0 10px; }}
h2 {{ font-size:16px; margin:16px 0 10px; }}
h3 {{ font-size:15px; margin:14px 0 8px; }}
.grid {{ display:grid; grid-template-columns:repeat(auto-fill,
  minmax(330px,1fr)); gap:10px; }}
.card {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:12px 14px; border-left:3px solid var(--edge); }}
.card.play {{ border-left-color:var(--green); }}
.card.pend {{ border-left-color:var(--blue); }}
.card.done {{ opacity:.85; }}
.teams {{ font-weight:700; }}
.meta {{ color:var(--dim); font-size:12px; margin:3px 0 8px; }}
.badge {{ color:var(--gold); }}
.live {{ color:#ff5d5d; font-weight:600; }}
.lane {{ background:#111622; border-radius:7px; padding:7px 9px;
  font-size:13px; margin-top:6px; }}
.lane.pl {{ outline:1px solid #234d33; }}
.lane.best {{ outline:1px solid rgba(230,195,92,.55); background:#171c2a;
  box-shadow:0 0 10px rgba(230,195,92,.10) inset; }}
.best-tag {{ color:var(--gold); font-size:10px; text-transform:uppercase;
  letter-spacing:.12em; margin-left:8px; white-space:nowrap; }}
.lane .which {{ color:var(--dim); font-size:10px; text-transform:uppercase;
  letter-spacing:.1em; margin-right:6px; }}
.prog {{ margin-top:5px; font-size:11px; letter-spacing:.04em;
  color:var(--gold); }}
.prog.won {{ color:var(--green); }}
.prog.gone {{ color:#e07a6a; }}
.tie {{ margin-top:8px; font-size:12px; color:var(--tx);
  background:#111622; border-radius:7px; padding:8px 10px; }}
summary {{ cursor:pointer; list-style:none; }}
summary::-webkit-details-marker {{ display:none; }}
.more {{ float:right; color:var(--dim); font-size:11px; font-weight:400; }}
details[open] .more {{ visibility:hidden; }}
.kw {{ color:var(--gold); font-size:12px; margin:2px 0 6px; }}
.read {{ color:var(--dim); font-size:13px; margin-top:8px; line-height:1.5;
  border-top:1px solid var(--edge); padding-top:8px; }}
.read b {{ color:var(--tx); }}
table {{ width:100%; border-collapse:collapse; font-size:13px;
  background:var(--card); border-radius:10px; overflow:hidden; }}
td, th {{ padding:7px 9px; border-top:1px solid var(--edge);
  vertical-align:top; text-align:left; }}
td.mk {{ white-space:nowrap; }}
td.note, .dim {{ color:var(--dim); }}
td.pos {{ color:var(--green); }} td.neg {{ color:#e07a6a; }}
.area {{ background:#111622; border-radius:5px; padding:2px 7px;
  font-size:11px; color:var(--gold); }}
.align {{ background:#111622; border-radius:5px; padding:2px 7px;
  font-size:11px; white-space:nowrap; color:var(--dim); }}
.align.hit {{ color:var(--green); }}
.align.rule {{ color:var(--gold); }}
.align.off {{ color:#e07a6a; }}
.hero {{ position:relative; border-radius:12px; overflow:hidden;
  background-size:cover; background-position:right center;
  min-height:230px; margin-bottom:14px; display:flex; align-items:center;
  border:1px solid var(--edge); }}
.hero-text {{ padding:26px 30px; max-width:52%; }}
.hero-text h1 {{ margin:0 0 8px; font-size:clamp(19px,3.2vw,30px);
  letter-spacing:.4px; color:#f4f0e4;
  text-shadow:0 1px 8px rgba(0,0,0,.85); }}
.hero-text .tag {{ margin:0; font-size:clamp(13px,1.9vw,17px);
  color:var(--gold); font-weight:650;
  text-shadow:0 1px 6px rgba(0,0,0,.9); }}
.hero-text .fine {{ margin:7px 0 0; font-size:11px; color:#b9b39f;
  text-shadow:0 1px 4px rgba(0,0,0,.9); }}
@media (max-width:640px) {{
  .hero {{ min-height:150px; }}
  .hero-text {{ max-width:78%; padding:16px 18px; }}
}}
.pagebanner {{ width:100%; max-height:260px; object-fit:cover;
  object-position:center 30%; border-radius:12px;
  border:1px solid var(--edge); margin-bottom:16px; display:block; }}
.session {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:14px 16px; margin-bottom:12px; }}
.session ul {{ margin:4px 0 2px 18px; color:var(--dim); }}
.sortbar {{ display:flex; flex-wrap:wrap; align-items:center; gap:6px;
  margin:0 0 12px; font-size:12px; }}
.sortbar .dim {{ margin-right:2px; }}
button.sortb {{ background:var(--card); border:1px solid var(--edge);
  color:var(--dim); border-radius:20px; padding:4px 11px; font:inherit;
  font-size:12px; cursor:pointer; }}
button.sortb:hover {{ color:var(--fg); border-color:var(--gold); }}
button.sortb.on {{ background:var(--gold); border-color:var(--gold);
  color:#12161f; font-weight:600; }}
th.sortcol {{ cursor:pointer; user-select:none; white-space:nowrap; }}
th.sortcol:hover {{ color:var(--gold); }}
th.sortcol::after {{ content:"\\2195"; opacity:.3; margin-left:5px; }}
th.sortcol.desc::after {{ content:"\\2193"; opacity:1;
  color:var(--gold); }}
th.sortcol.asc::after {{ content:"\\2191"; opacity:1; color:var(--gold); }}
h3.hyp {{ display:flex; align-items:center; gap:9px; margin:26px 0 4px;
  font-size:15px; }}
h3.hyp .dot {{ width:11px; height:11px; border-radius:50%; flex:none; }}
h3.hyp.green .dot {{ background:var(--green); }}
h3.hyp.orange .dot {{ background:#e0a23c; }}
h3.hyp.red .dot {{ background:#e07a6a; }}
h3.hyp .n {{ background:var(--card); border:1px solid var(--edge);
  border-radius:20px; padding:1px 9px; font-size:11px; color:var(--dim); }}
.hyptable td:first-child {{ white-space:nowrap; width:1%; }}
.hyptable td:nth-child(2) {{ width:1%; }}
.hyptable td b {{ font-weight:650; }}
.about p {{ margin:10px 0; max-width:74ch; }}
.about h3 {{ margin:26px 0 6px; font-size:15px; color:var(--gold); }}
.runs {{ display:grid; gap:10px; margin:12px 0 4px; }}
.run {{ background:var(--card); border:1px solid var(--edge);
  border-left:3px solid var(--gold); border-radius:8px; padding:11px 14px;
  max-width:74ch; }}
.run b {{ display:block; margin-bottom:3px; }}
.run .when {{ color:var(--dim); font-size:12px; }}
.feats {{ display:grid; gap:8px; margin:12px 0 4px;
  grid-template-columns:repeat(auto-fit,minmax(240px,1fr)); max-width:74ch; }}
.feat {{ background:var(--card); border:1px solid var(--edge);
  border-radius:8px; padding:9px 12px; display:flex; gap:10px;
  align-items:flex-start; }}
.feat .ico {{ font-size:20px; line-height:1.3; }}
.feat b {{ display:block; font-size:13px; }}
.feat span {{ color:var(--dim); font-size:12px; }}
.about .mission {{ font-size:18px; font-weight:700; color:var(--gold);
  margin:14px 0; }}
.tiertable {{ margin:6px 0 10px; }}
.tiertable td, .tiertable th {{ padding:4px 12px 4px 0; }}
.page {{ display:none; }} .page.on {{ display:block; }}
.tabpane {{ display:none; }} .tabpane.on {{ display:block; }}
.wrap {{ overflow-x:auto; }}
footer {{ color:var(--dim); font-size:12px; margin:26px 0 8px; }}
</style></head><body>
<nav>
 <img src="athena-logo.png" alt="ATHENA">
 <span class="brand"><b>{TITLE}</b><span class="stage">{STAGE}</span></span>
 <a href="#home" data-p="home">Home</a>
 <a href="#sessions" data-p="sessions">Past sessions</a>
 <a href="#retrosim" data-p="retrosim">Retrosim</a>
 <a href="#patches" data-p="patches">Patches</a>
 <a href="#about" data-p="about">About</a>
</nav>

<section class="page" id="p-home">
 <div class="hero" style="background-image:url('banner-home.jpg')">
  <div class="hero-text">
   <h1>{TITLE}</h1>
   <p class="tag">The most accurate football predictor{hero_sub}</p>
   <p class="fine">{hero_fine}</p>
  </div>
 </div>
 {basebar}
 <div class="session">SESSION #{SESSION_NO} · {SESSION_START} – {session_end}{
    f" · longest hit streak <b>{best_streak}</b>" if best_streak else ""}</div>
 <div class="tiles">{tiles}</div>
 <div class="ask">
  <b>🔎 Ask Athena</b> <span class="dim">— look up any matchup it has run;
  past matches are a retrosim (each competition's last ~200 games).</span>
  <div class="askrow">
   <input type="date" id="ask-d">
   <select id="ask-lg"><option value="">League…</option></select>
   <div class="combo"><input id="ask-a" placeholder="Team A"
    autocomplete="off" onfocus="ensureBank()"><div class="sug"
    id="sug-a"></div></div>
   <div class="combo"><input id="ask-b" placeholder="Team B"
    autocomplete="off" onfocus="ensureBank()"><div class="sug"
    id="sug-b"></div></div>
   <button class="btn askbtn" onclick="askAthena()">Enter</button>
  </div>
  <div id="ask-out"></div>
 </div>
 <div class="tabs">
  <a href="#home/playable" data-t="playable">🟢 Playable lanes
   <span class="dim">{len(playable)}</span></a>
  <a href="#home/bets" data-t="bets" class="gold">🟡 Found bets
   <span class="dim">{bh}/{bn}</span></a>
  <a href="#home/lanes" data-t="lanes" class="blue">🔵 Athena lanes
   <span class="dim">{len(waiting)}</span></a>
  <a href="#home/done" data-t="done" class="grey">⚪ Completed
   <span class="dim">{len(done)}</span></a>
 </div>
 <button class="btn" onclick="document.getElementById('learn')
  .scrollIntoView({{behavior:'smooth'}})">🎓 Learn Athena — how to read
  these blocks</button>
 <input id="q" placeholder="filter — team, league, code…"
  oninput="for(const c of document.querySelectorAll('.card,#t-bets tr[data-t]'))
  c.style.display=(c.dataset.t||'').includes(this.value.toLowerCase())?'':'none';
  recount()">
 <div class="fcounts" id="fcounts"></div>
 <div class="tabpane" id="t-playable">{_grid(playable, "play", reads)}</div>
 <div class="tabpane" id="t-bets">{bets_meta}<div class="wrap"><table>
  <tr><th>·</th><th>Fixture</th><th>Lane</th><th>Prob</th><th>Odds</th>
  <th>Return</th><th>Athena says</th><th>Note</th></tr>
  {bets_html}</table></div></div>
 <div class="tabpane" id="t-lanes">{_grid(waiting, "pend", reads)}</div>
 <div class="tabpane" id="t-done">{_grid(done, "done", reads)}</div>
 {_learn()}
</section>

<section class="page" id="p-sessions">
 <img class="pagebanner" src="banner-sessions.jpg" alt="">
 <h2>Past sessions</h2>
 <p class="dim">Every era is archived whole and never edited — the numbers
 below are how each run actually ended.</p>
 {sessions_html}
</section>

<section class="page" id="p-retrosim">
 <img class="pagebanner" src="banner-retrosim.jpg" alt="">
 <h2>Retrosim confirmed hitrates</h2>
  <p class="dim"><b>Hit</b> grades every Tip 1 the league produced,
 filter or no filter; <b>Playable hit</b> is the same replay narrowed to
 the lanes the board actually offers (edge above +1%) — the number the
 Playable tab lives on, with that subset's tip count in brackets. This
 very column exposed seven leagues whose above-bar lanes ran a flat 6–7
 points hot in both half-windows while their consensus lanes underclaimed:
 there the published probability is now <b>capped</b> at the league
 consensus, so no lane can claim edge and none can badge playable.</p>
<p class="dim">Every league's Tip 1, replayed as-of on the current build
 over its most recent fixtures — up to 800, capped at two seasons.
 <b>hit</b> is what landed; <b>gap</b> is hit minus what the engine
 claimed — near zero means the engine tells the truth about itself, and
 at this sample size a row inside ±3 is within noise of honest. All
 published debits included. <b>Click a column to sort</b> — again to
 reverse.</p>
 <p class="dim"><b>What this table is and is not:</b> a backtest on the
 real as-of code path — nothing dated after a match is read when pricing
 it — but the engine's constants were tuned on windows that overlap these
 fixtures, so read it as the engine describing itself, not as
 out-of-sample proof. The number that carries no such caveat is the
 <a href="#home">confirmed hitrate</a> on the Home page, measured only on
 tips published before kickoff. A push or half-win counts as a hit, a
 half-loss as a miss — the same rule the stated probability is computed
 with, so the gap compares like with like.</p>
 <div class="wrap"><table id="retro" class="sortable">
 <tr><th data-sort="t">League</th><th data-sort="n">Hit</th>
 <th data-sort="n">Playable hit</th>
 <th data-sort="n">Gap</th><th data-sort="n">Buy from</th>
 <th data-sort="n">n</th></tr>
 {_hitrates_rows()}</table></div>
</section>

<section class="page" id="p-patches">
 <h2>Patchlist &amp; notes</h2>
 <p class="dim">What changed and why, one line each. The full evidence
 behind every line lives in the repository's README and scripts.</p>
 <div class="wrap"><table><tr><th>Date</th><th>Area</th><th>Change</th>
 </tr>{_patch_rows()}</table></div>

 <h2 style="margin-top:34px">The ledger of everything tried</h2>
 <p class="dim">Every feature suggestion and hypothesis put through the
 bar. Green cleared it, orange has not finished, red was rejected — and
 the number that rejected it is kept beside it.</p>
 {_hypothesis_html()}
</section>

<section class="page about" id="p-about">
 <img class="pagebanner" src="banner-about.jpg" alt="">
 <h2>About</h2>
 <p class="mission">Mission: give the best possible accuracy on
 value bets.</p>
 <p><b>ATHENA</b> is the engine — a prediction machine that reads nothing
 but results: goals scored, goals conceded, when, where, against whom. It
 never sees a bookmaker's odds when it predicts; prices only enter
 afterwards, to decide whether a prediction is worth money.</p>
 <p><b>TEMPO GUARD</b> is Athena translated to football: the over/under
 goals market. It estimates how many goals a match should produce, offers
 the two best rungs on the totals ladder as Tip 1 and Tip 2, and states —
 honestly — how confident it is and what price makes each tip worth
 buying (the <b>buy≥</b> number). Athena itself is sport-agnostic; later
 builds could point the same engine at other sports.</p>
 <p><b>How to read the board:</b> a <b>playable lane</b> is a tip whose
 stated probability beats a typical match by more than 1% — the ones with
 real edge. <b>Found bets</b> are positions actually taken, settled at
 real prices. Everything on every page is computed from three small data
 files; no number is ever typed by hand, so nothing can quietly go
 stale.</p>
 <h3>What Athena can read</h3>
 <div class="feats">
  <div class="feat"><div class="ico">⚽</div><div><b>Team scoring &amp;
   conceding</b><span>every goal for and against, split by home and
   away, blended with the opponent's defence</span></div></div>
  <div class="feat"><div class="ico">📈</div><div><b>Team form</b>
   <span>recent matches weigh more than old ones, and a scoring streak
   that breaks from a team's own record is treated with
   suspicion</span></div></div>
  <div class="feat"><div class="ico">🏟️</div><div><b>League
   cultures</b><span>each league has its own goal rhythm — a 2.1-goal
   league and a 3.2-goal league are priced as different worlds</span>
   </div></div>
  <div class="feat"><div class="ico">🏆</div><div><b>International cup
   culture</b><span>continental ties run on a Club Elo strength lane
   with their own goal climate — still probationary</span></div></div>
  <div class="feat"><div class="ico">↕️</div><div><b>Over &amp; under
   predictions</b><span>the whole totals ladder, match and team lanes —
   Tips 1 and 2 are the two best rungs it finds</span></div></div>
  <div class="feat"><div class="ico">🥇</div><div><b>Possible
   winners</b><span>the result lane — double chance and draw no bet as
   Tip 3, on probation until live results confirm it</span></div></div>
  <div class="feat"><div class="ico">🎯</div><div><b>Team totals</b>
   <span>one side's goals alone — sometimes the strongest signal is
   that a single team scores, whatever the other does</span></div></div>
  <div class="feat"><div class="ico">🧮</div><div><b>Honest
   probabilities</b><span>debits, caps and floors measured on two
   separate time windows before any of them is allowed to
   ship</span></div></div>
  <div class="feat"><div class="ico">💰</div><div><b>Prices as a
   decision layer only</b><span>odds never touch a prediction — they
   only decide, afterwards, whether a tip is worth buying</span></div>
   </div>
 </div>

 <h3>How to read a card — the bettor's protocol</h3>
 <p>Written down 30 Aug after the first hundred positions, so the next
 run inherits it instead of rediscovering it. Four steps, in order:</p>
 <p><b>1. Start at the league badge.</b> It shows the playable record and
 its distance from baseline — <i>(83.7 +1.7)</i>. The delta matters as
 much as the level: a plus sign means the league is running above its own
 nature. At 84%+ with a plus, tip 1 is the engine's home turf — read it
 first and buy it when the bookmaker clears the buy≥.</p>
 <p><b>2. In a weak or capped league with no playable tip 1, read tip
 3.</b> Tip 3 does not inherit a league's tip 1 weakness — it reads who
 is stronger, not how many goals, a signal that survives totals-chaos:</p>
 {read_tiers}
 <p class="dim">Corrected the same day it was written. Replaying the
 star's chooser over 16,554 fixtures showed every deviation from tip 1
 grading WORSE on hitrate, and — the real surprise — a tip 1 that misses
 the playable bar still lands <b>84.3%</b>: a thin edge means the
 league's baseline is already high, not that the tip is weak. Dropping
 tip 2 from the chooser recovered most of the loss (79.5% → 82.1%
 against always-tip-1's 83.5%), so the star is now only ever tip 1 or
 tip 3. The residue is honest and stays: where the star leaves a
 sub-bar tip 1 for a result lane, that lane lands 77.9% against tip 1's
 84.3% on the same fixtures — a swap that pays only if the result lane
 is priced at least 8% above the total. Which is exactly what the buy≥
 bracket is for, and why the star says read first, not bet this.</p>
 <p><b>3. Between two result-lane prints at the same probability, prefer
 DNB or 1X over 12.</b> The 15,048-fixture dive found DNB underclaims —
 the higher it says, the more it is right (+4.2 overall, +8.7 in its top
 band) — while 12 is the family's one overclaimer (−2.0 on 7,884
 prints).</p>
 <p><b>4. The price gate never sleeps.</b> Whatever the card says, the
 buy≥ and its margin bracket are the final word — no ticket below a
 negative-bracket lane's asking price, no ticket on a lane the engine
 never priced, DNB never under ~1.35 (Rule 7). A great prediction at the
 wrong price is still a losing bet; that is the founding lesson of this
 project.</p>

 <p><b>What it does not see:</b> Athena prices one match's goal total.
 It has no concept of a two-legged tie, an aggregate score, or what a
 side needs on the night — and that matters, because a team chasing a
 deficit plays differently from one protecting a lead. Rather than
 pretend otherwise, cup cards print the aggregate picture beside the tip
 and label it context. It also cannot see team news, red cards before
 they happen, or the weather.</p>
 <p><b>How it stays honest:</b> every constant in the engine must prove
 itself on two separate time windows before it ships; every era is
 archived untouched, including the ones that lost money; new lanes (like
 the international cups) run <b>probationary</b> until live results
 confirm the backtests. When the engine doesn't know, it says nothing —
 an abstained match is an answer, not a failure.</p>

 <h3>Run sessions — how this project is measured</h3>
 <p>Athena is not developed against a fixed test set. It is developed
 against <b>runs</b>: a run opens, the engine publishes tips on real
 fixtures for as long as the run lasts, and the run closes when the slate
 does. Whatever it scored is then frozen and archived, untouched, and the
 next run starts from the engine the last one ended with. A run is
 therefore both the product and the experiment — the only honest test of
 a change is the next run's number, because that is the only sample the
 engine has never seen.</p>
 <p>Three runs so far, and the arc between them is the whole story:</p>
 <div class="runs">
  <div class="run"><b>Pre-calibration <span class="when">· 20–23 Aug
   2026</span></b>Tip 1 landed 84.2% and the run still LOST money —
   ROI −10.1%. The engine was pricing about 10.8 points optimistic, so
   every tip was bought at a price that could not pay for its real
   strike rate. This is the founding lesson of the project and the reason
   no number here is ever quoted without the price beside it: a strike
   rate you overpaid for is a loss. <b>Proof in hindsight (27 Aug):</b>
   this same weekend, replayed through the finished engine, grades 84.2%
   taking every tip — the losing record was undisciplined volume on an
   uncalibrated engine, not the slate itself.</div>
  <div class="run"><b>First calibrated slate <span class="when">· 23–24
   Aug 2026</span></b>Tip 1 56/65 (86.2%), Tip 2 37/50 (74.0%), and the
   bets turned positive at ROI +6.1% on 22/27. Five engine defects were
   found and fixed while it ran, and Rules 1–4 were measured here — buy≥
   discipline, flat 4% stakes, the winner's-curse haircut, in-play rung
   pricing. Those 65 settled tips became the measuring stick every later
   change is validated against.</div>
  <div class="run"><b>Session #3 <span class="when">· 24–27 Aug
   2026 · closed</span></b>The cup run. Cups had been taken off the
   board entirely at −11.4, and this run reopened them on a Club Elo
   strength lane — still <b>probationary</b>. Rules 5 and 6 became
   numbers, the board became this app, and four separate ideas were
   measured and declined for the same reason: real signal, no edge.
   Building the Retrosim page then forced a calibration day: measuring
   every league at real sample size exposed the old table as mostly
   noise, found one true engine-wide bias (fixed with a debit), lifted
   the fourteen weakest leagues with per-league floors — and rolled two
   of those floors back when the new buy-from column showed their
   hitrate was bought with unbuyable odds. A page built to display
   numbers ended up correcting the engine that produces them. Closed at
   Tip 1 <b>81.9%</b> on 72 settled, found bets <b>−7.5%</b> ROI — the
   gap between hitrate and price is the lesson the next run inherits.</div>
  <div class="run"><b>Session #{SESSION_NO} <span class="when">· {SESSION_START}
   2026 – running</span></b>The current run: the first full session on the
   calibrated floors, the high-says debit and the buy-from discipline —
   everything Session #3 measured, now facing fixtures it has never
   seen. {live_line}</div>
 </div>
 <p class="dim">The per-run numbers, frozen at close, are on the
 <a href="#sessions">Past sessions</a> page. What each run changed, and
 everything it tried and rejected, is on
 <a href="#patches">Patches &amp; notes</a>.</p>
</section>

<footer>Derived from config/fixtures.tsv · bets.tsv · league_hitrates.tsv ·
patchlog.tsv — nothing on this page is typed by hand. Cup lanes are
probationary. Not betting advice; bet responsibly.</footer>

<script>
function route() {{
  const h = (location.hash || "#home").slice(1).split("/");
  const page = ["home","sessions","retrosim","patches","about"]
    .includes(h[0]) ? h[0] : "home";
  const tab = ["playable","bets","lanes","done"].includes(h[1])
    ? h[1] : "playable";
  for (const s of document.querySelectorAll(".page"))
    s.classList.toggle("on", s.id === "p-" + page);
  for (const a of document.querySelectorAll("nav a"))
    a.classList.toggle("on", a.dataset.p === page);
  for (const s of document.querySelectorAll(".tabpane"))
    s.classList.toggle("on", s.id === "t-" + tab);
  for (const a of document.querySelectorAll(".tabs a"))
    a.classList.toggle("on", a.dataset.t === tab);
}}
// The filter bar's own scoreboard: every settled card carries its four
// grades, so whatever the filter leaves on screen — one league, one
// team, one rung — gets counted live. No filter means the whole run.
function recount() {{
  const box = document.getElementById("fcounts");
  if (!box) return;
  const t = {{gf: [0, 0], g1: [0, 0], g2: [0, 0], g3: [0, 0]}};
  for (const c of document.querySelectorAll(".card.done")) {{
    if (c.style.display === "none") continue;
    for (const k of ["gf", "g1", "g2", "g3"]) {{
      const v = c.dataset[k];
      if (v === undefined) continue;
      t[k][1]++; t[k][0] += (v === "1") ? 1 : 0;
    }}
  }}
  const cell = (label, k) => {{
    const [h, n] = t[k];
    return '<div class="fc"><div class="v">' +
      (n ? (h / n * 100).toFixed(1) + "%" : "—") +
      '</div><div class="l">' + label + '</div><div class="s">' +
      (n ? h + "/" + n : "no graded lanes") + "</div></div>";
  }};
  box.innerHTML = cell("final pick", "gf") + cell("tip 1", "g1") +
                  cell("tip 2", "g2") + cell("tip 3", "g3");
}}

addEventListener("hashchange", route); route(); recount();
window.addEventListener("error", () => {{
  // A broken form must never hide the board: re-run the router and let
  // the failure stay local to Ask Athena.
  try {{ route(); }} catch (e) {{}}
}});

// ---- sorting -------------------------------------------------------
// Cards carry their own keys (data-p, data-e, data-hr, data-ou, data-k),
// so reordering never re-reads rendered text. Highest first on every
// numeric key, because the question a sort answers here is always "what
// is the strongest one" — and cards with no numbers hold -1, so they
// settle at the bottom instead of scattering through the middle.
function sortGrid(bar, key) {{
  const grid = bar.parentElement.querySelector(".grid");
  if (!grid) return;
  const cards = [...grid.children];
  const num = c => parseFloat(c.dataset.p) || -1;
  // 0 first, 2 last; "z" (no over/under rung at all) always ends up 2.
  const rank = (c, first) => c.dataset.ou === "z" ? 2
    : c.dataset.ou === first ? 0 : 1;
  const cmp = {{
    k: (a, b) => a.dataset.k.localeCompare(b.dataset.k),
    p: (a, b) => b.dataset.p - a.dataset.p,
    e: (a, b) => b.dataset.e - a.dataset.e,
    hr: (a, b) => b.dataset.hr - a.dataset.hr,
    // Side sorts group first and rank inside the group by probability,
    // so "overs first" is a grouping, not a different ordering. The
    // ranks are explicit rather than a reversed string compare: simply
    // flipping o-vs-u also flips the abstained cards to the TOP, and
    // they belong at the bottom of every sort.
    o: (a, b) => rank(a, "o") - rank(b, "o") || num(b) - num(a),
    u: (a, b) => rank(a, "u") - rank(b, "u") || num(b) - num(a),
  }}[key];
  if (!cmp) return;
  cards.sort(cmp).forEach(c => grid.appendChild(c));
  for (const b of bar.querySelectorAll(".sortb"))
    b.classList.toggle("on", b.dataset.s === key);
}}
for (const bar of document.querySelectorAll(".sortbar"))
  bar.addEventListener("click", e => {{
    const b = e.target.closest(".sortb");
    if (b) sortGrid(bar, b.dataset.s);
  }});

// The retrosim table: click a header to sort, click again to reverse.
// "t" columns compare as text, "n" columns as numbers — the gap column
// carries a typographic minus, which parseFloat does not read, so it is
// folded to ASCII first.
for (const table of document.querySelectorAll("table.sortable")) {{
  const head = table.rows[0];
  [...head.cells].forEach((th, i) => {{
    if (!th.dataset.sort) return;
    th.classList.add("sortcol");
    th.addEventListener("click", () => {{
      // First click follows what the column is FOR: a number opens on its
      // largest, a name opens at A. After that each click toggles.
      const desc = th.dataset.dir
        ? th.dataset.dir !== "desc" : th.dataset.sort === "n";
      for (const c of head.cells) {{
        delete c.dataset.dir; c.classList.remove("asc", "desc");
      }}
      th.dataset.dir = desc ? "desc" : "asc";
      th.classList.add(desc ? "desc" : "asc");
      const val = tr => {{
        const s = tr.cells[i].textContent.trim().replace(/[−–]/g, "-");
        return th.dataset.sort === "n"
          ? (parseFloat(s.replace("%", "")) || 0) : s.toLowerCase();
      }};
      [...table.rows].slice(1)
        .sort((a, b) => {{
          const x = val(a), y = val(b);
          const r = typeof x === "number" ? x - y : x.localeCompare(y);
          return desc ? -r : r;
        }})
        .forEach(tr => table.tBodies[0].appendChild(tr));
    }});
  }});
}}

let BANK = null, LOOKUP = null, ALIAS = null, NAMES = null, DATES = null;
const norm = s => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
  .toLowerCase().replace(/[.\-'()\/]/g, " ").split(/\s+/)
  .filter(w => w && !["fc","fk","cf","sc","ac","afc","bk","if","sk",
                      "club","cp"].includes(w)).join(" ");

function refreshLeagues() {{
  if (!BANK) return;
  const sel = document.getElementById("ask-lg");
  const keep = sel.value;
  const D = document.getElementById("ask-d").value;
  sel.innerHTML = '<option value="">League…</option>';
  for (const [code, comp] of Object.entries(BANK).sort(
      (x, y) => x[1].name.localeCompare(y[1].name))) {{
    if (D && !DATES[code].has(D)) continue;
    const o = document.createElement("option");
    o.value = code; o.textContent = comp.name; sel.appendChild(o);
  }}
  sel.value = keep;
}}
async function ensureBank() {{
  if (BANK) return;
  const raw = await (await fetch("matchbank.json")).json();
  BANK = raw.comps; ALIAS = raw.alias; NAMES = raw.names;
  LOOKUP = {{}}; DATES = {{}};
  for (const [code, comp] of Object.entries(BANK)) {{
    DATES[code] = new Set();
    for (const m of comp.matches) {{
      const k = code + "|" + m.kh + "|" + m.ka;
      (LOOKUP[k] = LOOKUP[k] || []).push(m);
      DATES[code].add(m.d);
    }}
  }}
  // Every spelling and nickname a club answers to, for the suggestions.
  SEARCH = {{}};
  for (const [txt, key] of Object.entries(ALIAS)) {{
    if (!NAMES[key]) continue;
    (SEARCH[key] = SEARCH[key] || {{name: NAMES[key], txt: []}}).txt.push(txt);
  }}
  refreshLeagues();
}}
let SEARCH = {{}};

function keysInScope() {{
  const code = document.getElementById("ask-lg").value;
  if (!code || !BANK[code]) return null;
  const s = new Set();
  for (const m of BANK[code].matches) {{ s.add(m.kh); s.add(m.ka); }}
  return s;
}}
function suggest(which) {{
  const inp = document.getElementById("ask-" + which);
  const box = document.getElementById("sug-" + which);
  const q = norm(inp.value);
  if (!BANK || !q) {{ box.classList.remove("on"); return; }}
  const scope = keysInScope();
  const hits = [];
  for (const [key, e] of Object.entries(SEARCH)) {{
    if (scope && !scope.has(key)) continue;
    const exact = e.txt.some(x => x.startsWith(q));
    const loose = exact || e.txt.some(x => x.includes(q));
    if (loose) hits.push([exact ? 0 : 1, e.name.length, e.name, key]);
  }}
  hits.sort();
  box.textContent = "";
  for (const h of hits.slice(0, 8)) {{
    const row = document.createElement("div");
    row.textContent = h[2];
    // mousedown, not click: blur would close the list first on desktop.
    row.addEventListener("mousedown", ev => {{
      ev.preventDefault(); pick(which, h[2]);
    }});
    box.appendChild(row);
  }}
  box.classList.toggle("on", hits.length > 0);
}}
function pick(which, name) {{
  const inp = document.getElementById("ask-" + which);
  inp.value = name;
  document.getElementById("sug-" + which).classList.remove("on");
  maybeAuto();
}}
let TYPING = null;
// Typed names fire only once both actually resolve to a club Athena
// knows, and only after typing stops — no lookups mid-word.
function autoIfResolved() {{
  const A = document.getElementById("ask-a").value.trim();
  const B = document.getElementById("ask-b").value.trim();
  if (A && B && ALIAS[norm(A)] && ALIAS[norm(B)]) askAthena();
}}
for (const w of ["a", "b"]) {{
  const inp = document.getElementById("ask-" + w);
  inp.addEventListener("input", () => {{
    suggest(w);
    clearTimeout(TYPING);
    TYPING = setTimeout(autoIfResolved, 450);
  }});
  inp.addEventListener("blur", () => setTimeout(
    () => document.getElementById("sug-" + w).classList.remove("on"), 150));
  inp.addEventListener("keydown", e => {{
    if (e.key === "Enter") {{
      document.getElementById("sug-" + w).classList.remove("on");
      askAthena();
    }}
  }});
}}
// Typing rarely ends with a button press on a phone, so the form answers
// as soon as it has enough: both teams, or a league and a date.
function maybeAuto() {{
  const A = document.getElementById("ask-a").value.trim();
  const B = document.getElementById("ask-b").value.trim();
  const code = document.getElementById("ask-lg").value;
  const D = document.getElementById("ask-d").value;
  if ((A && B) || (code && D)) askAthena();
}}
document.getElementById("ask-d").addEventListener("change", async () => {{
  await ensureBank(); refreshLeagues(); maybeAuto();
}});
document.getElementById("ask-lg").addEventListener("change", maybeAuto);

function askCard(m, comp, note) {{
  const mark = m.mark ? m.mark + " " + (m.score || "") :
    (m.src === "board" ? "🕑 on the board" : "");
  let body = "";
  if (m.t2) body += '<div class="lane"><span class="which">Tip 2</span> '
    + m.t2.replaceAll(" · ", "<br>") + "</div>";
  return '<div class="card play"><div class="teams">'
    + m.h + " v " + m.a + '</div><div class="meta">' + mark + " · "
    + m.d + " · " + comp.name + (note ? " · " + note : "") + "</div>"
    + (m.kw ? '<div class="kw">🧠 ' + m.kw + "</div>" : "")
    + '<div class="lane pl"><span class="which">Tip 1</span> '
    + m.tip.replaceAll(" · ", "<br>") + "</div>" + body + "</div>";
}}
async function askAthena() {{
  await ensureBank();
  const out = document.getElementById("ask-out");
  const code = document.getElementById("ask-lg").value;
  const A = document.getElementById("ask-a").value.trim();
  const B = document.getElementById("ask-b").value.trim();
  const D = document.getElementById("ask-d").value;
  const wrap = h => '<div class="grid">' + h + "</div>";

  // League + date, no teams: that day's card set for the competition.
  if (!A && !B) {{
    if (!code || !D) {{
      out.innerHTML = '<div class="askerr">Fill in both teams, or pick a '
        + "league and a date to see that day's matches.</div>";
      return;
    }}
    const day = BANK[code].matches.filter(m => m.d === D);
    out.innerHTML = day.length
      ? '<div class="dim" style="margin-bottom:6px">' + day.length
        + " match" + (day.length > 1 ? "es" : "") + " — " + BANK[code].name
        + " on " + D + ":</div>"
        + wrap(day.map(m => askCard(m, BANK[code], "")).join(""))
      : '<div class="askerr">Athena has nothing for ' + BANK[code].name
        + " on " + D + ".</div>";
    return;
  }}
  if (!A || !B) {{
    out.innerHTML = '<div class="askerr">One team to go — fill both, or '
      + "clear them and pick a league and date instead.</div>";
    return;
  }}
  const kA = ALIAS[norm(A)] || norm(A), kB = ALIAS[norm(B)] || norm(B);
  const codes = code ? [code] : Object.keys(BANK);
  const all = [];
  for (const c of codes)
    for (const k of [c + "|" + kA + "|" + kB, c + "|" + kB + "|" + kA])
      for (const m of LOOKUP[k] || []) all.push([m, BANK[c]]);
  all.sort((x, y) => x[0].d.localeCompare(y[0].d));
  if (!all.length) {{
    out.innerHTML = '<div class="askerr">Athena has not run this matchup. '
      + "The board carries what the operator feeds in, and past matches "
      + "cover roughly each competition's last 200 games.</div>";
    return;
  }}
  // A date is a filter, not a hint: it narrows strictly, and when nothing
  // lands on it the answer says so and offers the dates that exist.
  if (D) {{
    const exact = all.filter(x => x[0].d === D);
    if (exact.length) {{
      out.innerHTML = wrap(exact.map(x => askCard(x[0], x[1], "")).join(""));
    }} else {{
      out.innerHTML = '<div class="askerr">No ' + A + " v " + B + " on "
        + D + ". Athena has run it on: "
        + all.map(x => x[0].d).join(", ") + ".</div>";
    }}
    return;
  }}
  out.innerHTML = '<div class="dim" style="margin-bottom:6px">' + all.length
    + " meeting" + (all.length > 1 ? "s" : "") + " on record — newest "
    + "last:</div>"
    + wrap(all.map(x => askCard(x[0], x[1], "")).join(""));
}}
</script>
</body></html>"""
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(page)
    _check_js(page)
    print(f"web app rendered: {OUT.relative_to(OUT.parents[2])} "
          f"({len(page) // 1024}KB)")


if __name__ == "__main__":
    main()
