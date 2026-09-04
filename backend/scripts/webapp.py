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

import datetime as dt
import html
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, headline, ledger, odds_api
from scripts.league_badges import rates

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "web" / "index.html"
TITLE = "ATHENA — TEMPO GUARD"
STAGE = "PRE-ALFA 2"          # bumped at each stage transition, deliberately
SESSION_NO = 6                # bumped when a run closes and a new one opens
SESSION_START = "2 Sep"       # the reset date of the current run
SESSION_DATE = "2026-09-02"   # the same, as a date: the forward log is
                              # append-only across resets and is read from
                              # here for the session's own play record

# The archived eras: frozen history, recorded once (the numbers live in
# archive/*/log.md and the README's archive section; they never change).
SESSIONS = [
    dict(name="Sessions #4–5 · the odds layer", dates="28 Aug – 1 Sep 2026",
         nums=[("Tip 1", "216/267 · 80.9%"), ("Tip 2", "135/196 · 68.9%"),
               ("Playable", "103/132 · 78.0%"), ("Bets", "113/143 · ROI −0.3%")],
         patches=["The odds layer: live prices from 26 books, the card "
                  "showing what the market pays, the line that reaches the "
                  "slip quoted (U3.0 printed, U3.5 struck)",
                  "The guard: five labels from the card's tier and an as-of "
                  "confluence score, frozen on two windows over 62,528 "
                  "replayed picks; the decline rule on top — PLAY only when "
                  "the best quote clears the label's break-even by 6%, "
                  "never on red — the first positive return at real prices",
                  "The measurement that moved the project: the edge lives in "
                  "the panel (+1.71% best of ten books, +0.62% at one) and "
                  "in the STRONG lane (+8.87% on 1,008 bets against −0.67% "
                  "for the rest), not in volume",
                  "The market disagrees most exactly where Athena is most "
                  "wrong: every card-tracking bar tested negative, so the "
                  "bar is a category rate — a blended probability, an "
                  "inverse play and per-league bars all declined with numbers",
                  "One club, one name: 552 spellings folded store-wide, "
                  "hidden rows 22% → 0.8%; and seven seasons the store never "
                  "had, 1,921 results filled from football-data",
                  "Decide at first sight: cards that clear the bar only "
                  "because the price drifted out late lose in both seasons "
                  "at both books; cards that clear early may be bought later",
                  "Closed at 80.9% tips / −0.3% ROI on 143 positions — flat, "
                  "the best a book has done here, and the reason Session #6 "
                  "plays fewer cards, not more"]),
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


_QUOTES = None


def quotes() -> dict:
    """(fixture, lane) -> what the market is actually offering.

    Written by scripts/odds_api.py --quotes. Missing file means no quotes
    and every card falls back to the engine's own bar — silence over a
    guess, and the render never calls the API itself.
    """
    global _QUOTES
    if _QUOTES is None:
        _QUOTES = {}
        path = ROOT / "config" / "odds_quotes.tsv"
        if path.exists():
            for ln in path.read_text().splitlines():
                if ln.startswith("#") or not ln.strip():
                    continue
                f = ln.split("\t")
                if len(f) < 8:
                    continue
                _QUOTES[(f[0], f[2])] = dict(
                    consensus=f[3], best=f[4], book=f[5],
                    unibet=f[6], books=f[7])
    return _QUOTES


def _fmt(cell: str, fixture: str = "", quoted: bool = True) -> str:
    """One lane's text. `quoted` is False once the match has kicked off:
    the quote file outlives the moment it was written, so injecting a
    price there would print a market that has already closed."""
    s = html.escape(cell)
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    # The engine's buy>= is its own opinion of what a lane is worth, and
    # measured against two seasons of closing prices it is unreachable —
    # 77% of a real book was struck under it and the ladder returns -5.5%
    # at market average. Where the market has actually quoted the lane,
    # the card shows THAT instead: what is on offer, what the best book
    # pays, and where. Line shopping measured +3.58 points, which is more
    # than any model change this project has found.
    # "(team)" marks a team total, which no book in the feed quotes; the
    # match ladder must never stand in for it.
    m = (None if "(team)" in cell else
         re.search(r"(?:^|[^A-Za-z])([OU]\d+(?:\.\d+)?|1X|X2|12|DNB[12])", cell))
    q = (quotes().get((fixture, _struck(m.group(1))))
         if (m and fixture and quoted) else None)
    if q:
        best = (f' · best <b>{html.escape(q["best"])}</b> '
                f'<span class="dim">{html.escape(q["book"])}</span>'
                if q["best"] != q["consensus"] else "")
        uni = (f' <span class="dim">· Unibet {html.escape(q["unibet"])}</span>'
               if q["unibet"] else "")
        s = re.sub(r"buy≥\s*[\d.]+(\s*\([^)]*\))?",
                   f'<span class="buyat">buy at min <b>{html.escape(q["consensus"])}'
                   f'</b>{best}{uni}</span>', s)
    # LEAD WITH THE LINE THAT REACHES THE SLIP. Athena publishes Asian
    # rungs; a real bet is the safer neighbour — U3.0 is struck as U3.5.
    # Printing the rung beside a price quoted for the struck line invites
    # exactly the wrong bet: Unibet pays 1.45 on U3.0 against 1.29 on
    # U3.5, so a reader chasing the bigger number takes a lane that
    # measured +0.24% where the struck one measured +0.86%.
    if m:
        rung = m.group(1)
        st = _struck(rung)
        if st != rung:
            s = s.replace(html.escape(rung),
                          f'<b class="play">{html.escape(st)}</b>'
                          f'<span class="rung" title="Athena publishes the '
                          f'Asian rung {html.escape(rung)}; the bet that '
                          f'reaches the slip is {html.escape(st)}, and every '
                          f'price and record here is for that line.">'
                          f'rung {html.escape(rung)}</span>', 1)
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


def _fold(s: str) -> str:
    """Accent-free copy, so "serie b" finds "Série B" and "brasileirao"
    finds "Brasileirão" — nobody types accents into a search box."""
    import unicodedata
    return (unicodedata.normalize("NFD", s)
            .encode("ascii", "ignore").decode("ascii"))


_COUNTRIES: dict | None = None


def _country(code: str) -> str:
    """The country behind a league code, for the search box. Typed in
    config/countries.tsv — culture, not data; the engine never reads it."""
    global _COUNTRIES
    if _COUNTRIES is None:
        _COUNTRIES = {}
        path = ROOT / "config" / "countries.tsv"
        if path.exists():
            for ln in path.read_text().splitlines():
                if ln.startswith("#") or "\t" not in ln:
                    continue
                pre, name = ln.split("\t", 1)
                _COUNTRIES[pre.strip()] = name.strip()
    return _COUNTRIES.get(code.split("-")[0], "")


def _haystack(f) -> str:
    """Everything a card can be filtered on, lowercased.

    Beyond the teams, league and code, this carries what each lane IS —
    the printed rung, and the words for its kind: a team over, a team
    under, a double chance, a draw no bet. So "team over" finds every
    card offering one, and (with the comma AND) "real madrid, team over"
    finds the ones that are both.
    """
    bits = [f.teams, f.league, f.code, _country(f.code)]
    for which, cell in ((1, f.tip1), (2, f.tip2), (3, f.tip3)):
        c = cell.strip()
        if not c or c.startswith("—"):
            # An absent lane is a searchable fact of its own ("tip 2
            # none"), but it must NOT answer a bare "tip 2" search, and
            # the filter matches by plain substring. So the token carries
            # no "tip 2" text at all; the query side rewrites the typed
            # phrase to this same sentinel.
            bits.append(f"~none{which}")
            continue
        bits += [c.replace("*", ""), f"tip{which}", f"tip {which}"]
        # The card now LEADS with the struck line, so a search for what
        # is printed on it has to find the card. The Asian rung stays
        # searchable too — the cell text already carries it — so both
        # "u3.0" and "u3.5" reach the same fixture.
        rm = re.search(r"(?:^|[^A-Za-z])([OU]\d+(?:\.\d+)?)", c)
        if rm:
            st = _struck(rm.group(1))
            if st != rm.group(1):
                bits += [st, f"tip{which} {st}", f"tip {which} {st}"]
        if f.lane(which) if which < 3 else False:
            bits.append("playable")
        if which == 3:
            m = re.match(r"^(?:[✅❌◦]\s*)?(1X|X2|12|DNB[12])", c.lstrip())
            bits.append("result lane")
            if m:
                fam = ("draw no bet dnb" if m.group(1).startswith("DNB")
                       else "double chance")
                bits += [fam, f"tip3 {fam}", f"tip 3 {fam}",
                         f"tip3 {m.group(1).lower()}",
                         f"tip 3 {m.group(1).lower()}"]
            continue
        rung = re.search(r"\b([OU])(\d+(?:\.\d+)?)", c)
        if not rung:
            continue
        side = "over" if rung.group(1) == "O" else "under"
        kind = "team" if "(team)" in c else "match"
        # Bare words find the card; the compound ones tie the word to
        # THIS lane, so "tip1 over" cannot be satisfied by tip 2's over
        # (the bettor's question, 31 Aug).
        # Both spellings of the lane prefix — "tip1 under" and "tip 1
        # under" — because either is natural to type (the bettor typed
        # the spaced one, 31 Aug).
        rung_l = rung.group(0).lower()
        for pre in (f"tip{which}", f"tip {which}"):
            bits += [f"{pre} {side}", f"{pre} {kind} {side}",
                     f"{pre} {rung_l}"]
        bits += [side, f"{kind} {side}"]
        if kind == "match":          # "ft over" reads the same to a bettor
            bits += [f"ft {side}", f"fulltime {side}"]
    if f.settled:
        mark = f.status.lstrip()[:1]
        bits.append({"✅": "hit won", "❌": "miss lost",
                     "◦": "push"}.get(mark, "finished"))
    elif f.status:
        bits.append("live")
    if "capped" in (rates().get(f.code) or ""):
        bits.append("capped")
    # The guard's label and verdict, so the bar can be asked for "green",
    # "orange", "red", "super green", "strong", "no play" (the bettor's
    # request, 2 Sep). "guard red" and "label red" are there too, because
    # a bare "red" also finds NY Red Bulls.
    if not f.settled:
        v = verdict(f, _star(f))
        if v:
            lab = v["label"]
            bits += [lab, f"guard {lab}", f"label {lab}"]
            if lab.startswith("super "):
                base = lab[6:]
                bits += [f"guard {base}", f"label {base}"]
            # A play's mark is "normal" or "strong"; "verdict play" has
            # to find both, and "verdict no play" only the rest.
            bits.append("verdict " + v["mark"])
            if v["play"]:
                bits.append("verdict play")
            if v["strong"]:
                bits.append("strong")
    raw = " ".join(bits).lower()
    folded = _fold(raw)
    return html.escape(raw if folded == raw else raw + " " + folded)


def _probkeys(f) -> str:
    """Each lane's own probability as a number, so the filter bar can be
    asked for a threshold ("tip 2 <80") instead of only for words. The
    first percentage in a lane cell is the lane's claim — what follows is
    the edge and the margin, which are not what a threshold means."""
    out = []
    for which, cell in ((1, f.tip1), (2, f.tip2), (3, f.tip3)):
        c = (cell or "").strip()
        if not c or c.startswith("—"):
            continue
        m = re.search(r"(\d+(?:\.\d+)?)%", c)
        if m:
            out.append(f'data-p{which}="{m.group(1)}"')
    g = _goals(f)
    if g is not None:
        out.append(f'data-goals="{g}"')
    return (" " + " ".join(out) + " ") if out else " "


def _gradekeys(f) -> str:
    """Each settled card carries its own four grades, so the filter bar
    can recount hitrates for whatever subset is on screen — a league, a
    team, a rung — without the page re-deriving anything from text.

    EVERY graded lane on the card counts, playable or not — the counters
    answer "how did the tips do on what I am looking at", which is not
    the tiles' question (those keep the playable standard, because that
    is the subset a bettor acts on). The bettor asked for this after a
    J1 filter showed tip 1 at 0/2: six cards on screen, only two of them
    above the bar. The final pick is the ★ lane's own grade.
    """
    if not f.settled:
        return ""
    def mark(which):
        src = f.status if which == 1 else (f.tip2 if which == 2 else f.tip3)
        m = src.lstrip()[:1]
        return m if m in ("✅", "❌", "◦") else None

    out = []
    for which, key in ((1, "g1"), (2, "g2"), (3, "g3")):
        cell = f.tip1 if which == 1 else (f.tip2 if which == 2 else f.tip3)
        if not cell.strip() or cell.lstrip("✅❌◦ ").startswith("—"):
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


# How far a DNB must out-claim tip 1 before it takes the star. Fitted and
# validated 1 Sep over 29,953 graded bank cards: on the 426 where a DNB
# clears tip 1 by more than this, the DNB grades 94.13% against tip 1's
# 82.16%. It survives both time windows independently (+7.44 older,
# +16.59 newer) and survives stripping every DNB push out of the sample
# (+9.58 on 355 cards), so it is not a settlement artefact.
DNB_GATE = 2.0


def _claim(cell: str) -> float | None:
    """A lane's own probability — the FIRST percentage in its cell, which
    is the claim; what follows is the edge and the margin."""
    c = (cell or "").strip()
    if not c or c.startswith("—"):
        return None
    m = re.search(r"(\d+(?:\.\d+)?)%", c)
    return float(m.group(1)) if m else None


def _is_dnb(cell: str) -> bool:
    return bool(re.search(r"(?:^|[^A-Za-z])DNB[12]", cell or ""))


def _edge(cell: str) -> float | None:
    """A lane's printed EDGE — the signed percentage after the claim."""
    m = re.search(r"%\s*\*{0,2}([+−-]\d+(?:\.\d+)?)%", cell or "")
    return float(m.group(1).replace("−", "-")) if m else None


def _struck(rung: str) -> str:
    """The line a printed rung is actually bought at — Rule 6's safer
    neighbour, which is exactly two rungs: U3.0 struck as U3.5 and U4.25
    as U4.5. Every other rung is bought as printed. The quote and the
    verdict must both speak about the line that will appear on the slip,
    not the notation Athena prints. See odds_api.bought for why the
    substitutions are enumerated rather than derived."""
    if not rung or rung[0] not in ("O", "U"):
        return rung
    from scripts.odds_api import bought
    try:
        return bought(rung)
    except Exception:
        return rung


def _rung(cell: str) -> str:
    m = re.search(r"(?:^|[^A-Za-z])((?:[OU]\d+(?:\.\d+)?)|1X|X2|12|DNB[12])",
                  cell or "")
    return m.group(1) if m else ""


_SLICES = None

# What each label graded over 62,528 replayed picks, in both time
# windows. Registered in docs/confluence-guard.md — these are the numbers
# the live period is being graded against, so they are not tuned.
SAYS = {"super green": 0.8956, "green": 0.8740, "orange": 0.8342,
        "red": 0.7796, "super red": 0.7705}

# How far above break-even a quote must sit before the card says PLAY.
# Swept on 8,121 priced picks: taking everything returned -1.67%, and the
# gradient crossed zero at about +5% — +0.83% at this bar and +1.49% at
# +8%, on a hit rate FALLING from 81.8% to 72.7%. Six is the registered
# choice, not the best cell in the table.
DECLINE_MARGIN = 0.06

# WATCH: a card whose starred lane is NOT red and whose best quote sits
# UNDER the bar by no more than this. Not a play — but the feed is 26
# books and the bettor's are not among the sharpest, so a card the panel
# prices a few percent short is one a book of his own may clear: West
# Brom v Watford read 1.26 against a 1.2707 bar on the feed and 1.27 at
# Unibet (2 Sep). Beyond five percent the panel's best is already far
# from the bar and no reachable book is likely to beat it.
WATCH_BAND = 0.05

# STRONG: a play that also carries a top-quartile confluence score, in
# Europe where the score means anything. Measured on the 1,008 bets the
# bar fires on: strong grades 81.0% and +8.87% (+9.91 / +7.83 across the
# two windows) while everything else grades 72.8% and -0.67%. So the
# split is not cosmetic — one half carries the entire return and the
# other has no measured edge at all.
STRONG_SCORE = 0.71

FORWARD = ROOT / "config" / "forward_log.tsv"
_LOGGED: set | None = None


def _stamp(f, best: int, lane: str, lab: str, sc, claim: float,
           need: float, q: dict) -> None:
    """Record what the card said, at the moment it said it.

    The retro record is what it is; the only way this stops being a
    replay is a forward one. Every labelled card with a live quote is
    written once, on FIRST sight, because that is the honest stand-in for
    "what the board offered when you looked at it" — a later re-render
    catches a moved price and would flatter or damn the rule by accident.
    Append-only: scripts/forward_settle.py grades it once results land.
    """
    global _LOGGED
    key = (f.kickoff.split(" ")[0], f.teams, lane)
    if _LOGGED is None:
        # Read the existing log ONCE per run, not once per card: the board
        # re-renders many times a day and the log only grows, so a scan
        # per stamp is quadratic in a file that never shrinks.
        _LOGGED = set()
        if FORWARD.exists():
            for ln in FORWARD.read_text().splitlines():
                if ln.startswith("#"):
                    continue
                p = ln.split("\t")
                if len(p) > 5:
                    _LOGGED.add((p[1], p[3], p[5]))
    if key in _LOGGED:
        return
    if not FORWARD.exists():
        FORWARD.write_text(
            "# What the card said, stamped when it said it. Append-only,\n"
            "# written by scripts/webapp.py at render and graded by\n"
            "# scripts/forward_settle.py. One row per fixture-lane, kept\n"
            "# from FIRST sight so a later re-render cannot re-price it.\n"
            "# stamped\tdate\tleague\tfixture\ttip\tlane\tclaim\tlabel"
            "\tscore\tneeds\tconsensus\tbest\tbook\n")
    _LOGGED.add(key)
    with FORWARD.open("a") as fh:
        fh.write("\t".join([
            dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M"),
            key[0], f.code, f.teams, str(best), lane, f"{claim:.1f}",
            lab, ("" if sc is None else f"{sc:+.2f}"), f"{need:.3f}",
            q.get("consensus") or "", q.get("best") or "",
            q.get("book") or ""]) + "\n")


def _label_of(f, best: int, full: bool = False):
    """The guard label for the starred lane, computed once.

    Split out of _guard because the card now needs the label BEFORE it
    renders any lane: only the starred lane's price bar is backed by a
    validated hit rate, and every other lane has to say that it is not.
    """
    global _SLICES
    if f.settled or not best:
        return None
    cell = f.tip3 if best == 3 else f.tip1
    p = _claim(cell)
    if p is None:
        return None
    e = _edge(cell)
    dnb = best == 3 and _is_dnb(f.tip3)
    # The shipped tier — one definition, shared with the bank
    # (guard_slices.tier_of): a gated DNB or a high, modest-edge tip 1 is
    # green; a low claim, or a middling claim on an OVER, is red.
    from scripts import guard_slices
    side = _rung(cell)[:1] if _rung(cell)[:1] in ("O", "U") else ""
    tier = guard_slices.tier_of(p, e, side, dnb)
    if _SLICES is None:
        _SLICES = guard_slices.read_table()
    sc = None
    if _SLICES and " v " in f.teams:
        h, a = [t.strip() for t in f.teams.split(" v ")]
        sc = guard_slices.score(f.code, h, a, _rung(cell), p / 100.0, _SLICES)
    lab = guard_slices.label(f.code, tier, sc, dnb)
    return (lab, sc, cell, p) if full else lab


def _star(f) -> int:
    """The chooser: which lane the card stars. Extracted because the TABS
    now need it too — a tab that decides playability from a different
    lane than the card marks would be two rules pretending to be one.

    Tip 1 by default; a DNB that out-claims it by DNB_GATE takes it.
    Tip 2 is never starred (it graded 12.7 points below tip 1 on the same
    fixtures) and double chance is never gated in (a DC switch lost 3.53
    points where the DNB gained 9.08).
    """
    if f.settled:
        return 0
    p1, p3 = _claim(f.tip1), _claim(f.tip3)
    if p1 is not None:
        if _is_dnb(f.tip3) and p3 is not None and p3 - p1 > DNB_GATE:
            return 3
        return 1
    return 3 if f.tip3.strip() else 0


def verdict(f, best: int) -> dict | None:
    """PLAY (normal or strong) or NO PLAY, decided once.

    The tabs and the card must never disagree about whether something is
    playable, so both read this rather than each applying the rule.
    """
    got = _label_of(f, best, full=True)
    if not got:
        return None
    lab, sc, cell, p = got
    hit = SAYS[lab]
    need = (1 / hit) * (1 + DECLINE_MARGIN)
    lane = _struck(_rung(cell))
    q = quotes().get((f.teams, lane))
    try:
        got_odds = float(q["best"] or q["consensus"]) if q else None
    except (TypeError, ValueError):
        got_odds = None
    strong = (region_silent(f.code) is False and sc is not None
              and sc >= STRONG_SCORE)
    # A match that has kicked off is not playable, whatever the file says.
    # odds_api stops QUOTING a started fixture, but the quote file is a
    # file: it outlives the moment it was written, and a render an hour
    # later would go on offering a price from a game now in progress. The
    # bar belongs on the decision, not only on the fetch.
    live = odds_api.started(f.kickoff)
    play = (not lab.endswith("red")) and got_odds is not None \
        and got_odds >= need and not live
    # The watch list: same lane, same bar, the panel's best a little short
    # of it. Decided here beside PLAY, so the tab and the verify cannot
    # hold two definitions of "just shy".
    watch = (not play) and (not lab.endswith("red")) and got_odds is not None \
        and got_odds >= need * (1 - WATCH_BAND) and not live
    return dict(label=lab, score=sc, cell=cell, claim=p, lane=lane,
                need=need, odds=got_odds, book=(q or {}).get("book"),
                play=play, watch=watch, strong=play and strong,
                mark=("strong" if play and strong else
                      "normal" if play else "no play"))


_FROZEN = None


def frozen() -> dict:
    """(date, fixture) -> the card's LAST published state before kickoff.

    A kicked-off card has no live quote: odds_api stops quoting a fixture
    the moment it starts, so the price that made it a PLAY is gone from
    config/odds_quotes.tsv by the time anyone looks. The forward log kept
    every state the card passed through, append-only, and quoting stops
    at kickoff — so the last row for a fixture-date IS the card as it
    stood when the whistle went.

    LAST, not first, and the two differ. Başakşehir v Galatasaray was
    logged on 1 Sep starring O1.5 at 1.26 (a watch) and on 2 Sep starring
    U4.5 at 1.33 (a play): the star moved to a different lane as the
    repricing came in. Freezing the first sighting made Running say
    "was watch O1.5" about a card whose Playable entry had said "PLAY
    U4.5" an hour earlier, which is the exact contradiction the freeze
    exists to remove (the bettor's ask, 3 Sep: frozen at kickoff).

    This is display only. The forward log's own measurement — the NORMAL
    and STRONG tiles, through forward_settle.rows() — still counts the
    FIRST sighting per fixture-date, because that is the decide-at-first-
    sight rule the record is kept under and it must not be re-priced into
    a better story.

    Read from the raw file rather than forward_settle.rows(), which only
    yields rows it can already GRADE — a match still in progress has no
    result, so every card this tab exists for would be filtered out.
    """
    global _FROZEN
    if _FROZEN is None:
        from scripts import forward_settle
        _FROZEN = {}
        if forward_settle.FORWARD.exists():
            for ln in forward_settle.FORWARD.read_text().splitlines():
                if ln.startswith("#") or not ln.strip():
                    continue
                p = ln.split("\t")
                if len(p) < 13 or forward_settle._artefact(p[5]):
                    continue
                key = (p[1], p[3])
                try:
                    need, best = float(p[9]), float(p[11] or p[10])
                except ValueError:
                    continue
                try:
                    score = float(p[8])
                except ValueError:
                    score = None
                _FROZEN[key] = dict(d=p[1], code=p[2], fixture=p[3],
                                    tip=p[4], lane=p[5], label=p[7],
                                    score=score, need=need, best=best,
                                    book=p[12])
    return _FROZEN


def was_called(f) -> dict | None:
    """What the board said about this card while it could still be bought.

    The same bars verdict() applies, read against the frozen price rather
    than a live one, and the mark is the same vocabulary: strong, normal,
    watch, no play. Once the whistle goes this is the ONLY honest verdict
    available — the live one has no price to work from — and it is what
    the card keeps from kickoff, through Running, to Completed. A decline
    stays a decline, so nothing here can flatter the record.
    """
    r = frozen().get((f.kickoff.split(" ")[0], f.teams))
    if not r:
        return None
    best, need = r.get("best"), r.get("need")
    if str(r["label"]).endswith("red") or not best or not need:
        return dict(row=r, mark="no play")
    if best >= need:
        sc = r.get("score")
        strong = (not region_silent(r.get("code") or f.code)
                  and sc is not None and sc >= STRONG_SCORE)
        return dict(row=r, mark="strong" if strong else "normal")
    if best >= need * (1 - WATCH_BAND):
        return dict(row=r, mark="watch")
    return dict(row=r, mark="no play")


PLAYED_MARKS = ("strong", "normal", "watch")


def running_call(f) -> dict | None:
    """The frozen call on a match in progress that the board had offered.

    Only the three acted-on marks qualify for the Running tab; a card the
    board declined belongs in Athena lanes whether or not it has started.
    """
    if f.settled or not odds_api.started(f.kickoff):
        return None
    call = was_called(f)
    return call if call and call["mark"] in PLAYED_MARKS else None


def _frozen_guard(f, call: dict) -> str:
    """Badge and verdict line for a card past kickoff, from the log.

    One renderer for Running and Completed alike, so a card cannot change
    its story on the way between them.
    """
    r = call["row"]
    lab, sc = r["label"], r.get("score")
    hit = SAYS.get(lab)
    tip = (f"Guard: {lab}. Cards labelled this way graded "
           f"{hit*100:.1f}% over 62,528 replayed picks, in both time "
           f"windows. " if hit else f"Guard: {lab}. ")
    tip += ("The tier says avoid. " if lab.endswith("red") else "")
    tip += ("Score silent outside Europe."
            if region_silent(r.get("code") or f.code)
            else f"Confluence score {sc:+.1f}." if sc is not None else "")
    badge = (f'<div class="guard g-{lab.replace(" ", "-")}" '
             f'title="{html.escape(tip)}">{lab}</div>')

    word = {"strong": "★ STRONG · PLAY", "normal": "PLAY",
            "watch": "watch", "no play": "no play"}[call["mark"]]
    cls = {"strong": "strong", "normal": "yes",
           "watch": "dimv", "no play": "no"}[call["mark"]]
    when = "was" if f.settled else "running · was"
    who = f'Tip {r.get("tip") or ""} {html.escape(str(r.get("lane") or ""))}'
    tail = ""
    if r.get("need") and r.get("best"):
        tail = (f'<span class="dim"> · needed {r["need"]:.2f}, '
                f'{html.escape(str(r.get("book") or "market"))} paid</span> '
                f'<b>{r["best"]:.2f}</b>')
    return (badge + f'<div class="verdict {cls}">{when} {word} '
            f'<span class="dim">· {who.strip()}</span>{tail}'
            f'<span class="dim"> · price at first sight, '
            f'{html.escape(r["d"])}</span></div>')


def _guard(f, best: int) -> str:
    """The card's risk label: five bands, registered in docs/.

    Two layers, and this is the second. The chooser upstream may flip
    tip 1 to a gated DNB; this one only labels what it starred, and its
    action space is play or no play. There is no lane to flip to — on
    condemned cards, standing grades 82.22% against tip 3's 78.17% even
    where a tip 3 exists.

    The score is silent outside Europe, where it measured -0.06.

    Past kickoff the label and the mark both come from the FROZEN call
    instead. A settled card has no starred lane and no quote, so the live
    path renders nothing at all — which silently erased whether a
    completed card had been strong, normal or watched. The card now looks
    the same from kickoff through Running to Completed, because after the
    whistle there is only one verdict left that was ever true.
    """
    call = (was_called(f)
            if f.settled or odds_api.started(f.kickoff) else None)
    if call:
        return _frozen_guard(f, call)
    v = verdict(f, best)
    if not v:
        return ""
    lab, sc = v["label"], v["score"]
    hit = SAYS[lab]
    tip = (f"Guard: {lab}. Cards labelled this way graded {hit*100:.1f}% "
           f"over 62,528 replayed picks, in both time windows. "
           + ("The tier says avoid. " if lab.endswith("red") else "")
           + ("Score silent outside Europe." if region_silent(f.code)
              else f"Confluence score {sc:+.1f}." if sc is not None else ""))
    badge = (f'<div class="guard g-{lab.replace(" ", "-")}" '
             f'title="{html.escape(tip)}">{lab}</div>')

    # THE MARK, one of three, and the tabs read the same verdict() so the
    # board can never say playable while the card says decline.
    #
    # STRONG is not decoration. On the 1,008 bets the bar fires on, a
    # top-quartile score in Europe grades 81.0% and +8.87% (+9.91/+7.83
    # by window) while everything else grades 72.8% and -0.67%. One half
    # carries the entire return; the other has no measured edge at all.
    who = f'Tip {best} {html.escape(v["lane"])}' if v["lane"] else f'Tip {best}'
    need, odds, book = v["need"], v["odds"], v["book"] or "market"
    if v["strong"]:
        line = (f'<div class="verdict strong">★ STRONG · PLAY {who} '
                f'<span class="dim">· needs {need:.2f}, '
                f'{html.escape(book)} pays</span> <b>{odds:.2f}</b>'
                f'<span class="dim"> · score {sc:+.1f}</span></div>')
    elif v["play"]:
        line = (f'<div class="verdict yes">PLAY {who} '
                f'<span class="dim">· needs {need:.2f}, '
                f'{html.escape(book)} pays</span> <b>{odds:.2f}</b></div>')
    elif lab.endswith("red"):
        line = (f'<div class="verdict no">no play <span class="dim">· '
                f'{who} · the tier says avoid</span></div>')
    elif odds is None:
        line = (f'<div class="verdict dimv">{who} needs <b>{need:.2f}</b> '
                f'<span class="dim">· nothing quoted yet</span></div>')
    else:
        line = (f'<div class="verdict no">no play <span class="dim">· {who} '
                f'needs {need:.2f}, best anywhere is</span> '
                f'<b>{odds:.2f}</b></div>')
    if odds is not None:
        _stamp(f, best, v["lane"], lab, sc, v["claim"], need,
               quotes().get((f.teams, v["lane"])) or {})
    return badge + line


def region_silent(code: str) -> bool:
    from scripts.confluence import region
    return region(code) != "Europe"


def _needs(cell: str, starred_label: str | None) -> tuple:
    """(required price, hit rate used, whether that rate is VALIDATED).

    The starred lane gets its bar from the guard label, whose hit rate
    survived two windows over 62,528 replayed picks. Every other lane has
    only its own printed claim, which has never been tested as a price
    input — so the card says so rather than dressing the two up alike.
    """
    if starred_label:
        return (1 / SAYS[starred_label]) * (1 + DECLINE_MARGIN), \
            SAYS[starred_label], True
    c = _claim(cell)
    if c is None:
        return None, None, False
    return (1 / (c / 100.0)) * (1 + DECLINE_MARGIN), c / 100.0, False


def _past_kick(f) -> bool:
    """Is this card beyond the point where a price can still be taken?

    Settled or merely started, the answer for the layout is the same: no
    live bar, no no-play pills, no injected quote — the frozen verdict
    line carries the whole price story, and the card stops changing shape
    on its way to the archive.
    """
    return bool(f.settled or odds_api.started(f.kickoff))


def _lanebar(f, cell: str, starred_label: str | None) -> str:
    """PASS or DECLINE for ONE lane, whether or not it is the star.

    The bettor's point: a card whose starred lane fails on price is not
    the same as a card with nothing on it, and the reader should be able
    to see which lanes cleared without doing the arithmetic per lane.
    """
    need, _hit, solid = _needs(cell, starred_label)
    if need is None or _past_kick(f):
        return ""
    lane = _struck(_rung(cell))
    q = quotes().get((f.teams, lane)) if lane else None
    soft = "" if solid else (' <span class="dim" title="This bar comes from '
                             'the lane&#39;s own printed claim, not from a '
                             'validated label — only the starred lane has '
                             'one">·&nbsp;claim-based</span>')
    if not q:
        return (f'<div class="lanebar dimv">needs {need:.2f}'
                f'<span class="dim"> · not quoted</span>{soft}</div>')
    try:
        got = float(q["best"] or q["consensus"])
    except (TypeError, ValueError):
        return ""
    if got >= need:
        return (f'<div class="lanebar yes">PASS <b>{got:.2f}</b>'
                f'<span class="dim"> · needs {need:.2f}</span>{soft}</div>')
    gap = 100 * (got / need - 1)
    return (f'<div class="lanebar no">DECLINE <b>{got:.2f}</b>'
            f'<span class="dim"> · needs {need:.2f}, {gap:+.1f}%</span>'
            f'{soft}</div>')


def _goals(f) -> int | None:
    """Total goals in the FINAL result, or None while it can still move.

    A live score is deliberately not offered to the goal filter: "goal >3"
    is a question about how a match ended, and answering it from a score
    that is still climbing would give a different answer on every reload.
    """
    st = (f.status or "").strip()
    if not (f.settled or st.startswith("FT")):
        return None
    m = re.search(r"(\d+)\s*-\s*(\d+)", st)
    return int(m.group(1)) + int(m.group(2)) if m else None


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
    # Reading guidance only, so a card past kickoff drops it — the star
    # points at what to buy, and there is nothing left to buy.
    past = _past_kick(f)
    best = 0
    if not past:
        # Athena marks exactly ONE preferred lane per card (the bettor's
        # rule, 30 Aug), and the order is what the measurement supports.
        # Tip 2 is never starred: it graded 12.7 points BELOW what tip 1
        # would have done on the same fixtures.
        #
        # The star is TIP 1 by default, and moves only for a DNB that
        # out-claims it by DNB_GATE. The rule this replaced dropped to
        # tip 3 whenever tip 1 was not playable, which traded down by
        # construction — tip 3's baseline sits about five points under
        # tip 1's — and measured 82.08% against always-tip-1's 83.49%
        # across 57 leagues, worse in 36 of them. A thin edge on tip 1
        # means the league baseline is already high, not that the tip is
        # weak, so an unplayable tip 1 is no reason to leave it.
        #
        # Double chance is deliberately NOT gated in: on the same test a
        # DC switch LOST 3.53 points where the DNB gained 9.08. The star
        # means "read this first", never "this is the better bet" — the
        # buy≥ bracket decides that.
        best = _star(f)
    star = ('<span class="best-tag" title="The lane to read first on this '
            'card — not a claim that it is the better bet; the buy≥ '
            'bracket decides that">★ read first</span>')

    def lane(which, cell, lab=None, noplay=False):
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
        tail = " <span class=\"dim\">· result lane</span>" if which == 3 else ""
        if noplay:
            tail += ('<span class="noplay" title="Shown for the record. '
                     'This lane is not what the guard would stake — either '
                     'the price never cleared its bar or it is not the '
                     'starred lane.">no play</span>')
        return (f'<div class="lane{pl}"><span class="which">Tip {which}'
                f"</span> {_fmt(cell, f.teams, quoted=not past)}{tail}"
                f"{star if which == best else ''}"
                f"{_lanebar(f, cell, lab)}{live}</div>")

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
    # ORDER: the starred lane, then the lane that would inherit the card
    # if the star fails on price, then the last one. Tip 2 is always last
    # — it is never starred and grades about fourteen points under the
    # final pick — so the fallback is whichever of tip 1 and tip 3 the
    # star is not. Settled cards keep the tip 1 order so the grading
    # column reads consistently down the page.
    cells = {1: f.tip1, 2: f.tip2, 3: f.tip3}
    if f.settled or not best:
        seq = [1, 3, 2]
    else:
        seq = [best, 3 if best != 3 else 1, 2]
        seq = [w for i, w in enumerate(seq) if w not in seq[:i]]
    # Only the STARRED lane carries a validated bar; the others are
    # priced off their own claim and say so.
    lab = _label_of(f, best) if not past else None
    v = verdict(f, best) if not past else None
    playing = bool(v and v["play"])
    # The card leads with the ONE line that would be staked, and nothing
    # else. Everything the engine also published sits behind the fold,
    # each lane marked no play — still readable, still carrying its own
    # arithmetic, but never mistakable for an instruction. On a card the
    # guard refuses outright, every lane is marked.
    # A card past kickoff is a record, not an instruction, so it carries
    # no no-play marks at all — the frozen verdict above already says what
    # the board called, and a "no play" pill under a line reading
    # "was PLAY" is the card arguing with itself.
    mark = not past
    face = lane(seq[0], cells[seq[0]], lab, noplay=mark and not playing)
    rest = "".join(lane(w, cells[w], None, noplay=mark) for w in seq[1:])
    top = (f'<div class="teams">{html.escape(f.teams)}'
           f'<span class="more">more ▾</span></div>'
           f'<div class="meta">{head} · {league}</div>{kw}'
           f"{_guard(f, best)}{face}")
    body = rest + tie_html
    if read:
        body += f'<div class="read">{read[1]}</div>'
    if not body:
        body = '<div class="read dim">nothing more on this one</div>'
    return (f'<details class="card {kind}" '
            f'data-fx="{html.escape(f.teams)}" '
            f'data-t="{_haystack(f)}" '
            f'data-lg="{html.escape(_fold(f.league.lower()))}" '
            f"{_sortkeys(f)}{_probkeys(f)}{_gradekeys(f)}>"
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


def _learn(playable: list, waiting: list, reads: dict) -> str:
    """The teaching block: three LIVE cards from today's board \u2014 a normal
    play, a strong play, and a card with no play on it \u2014 each rendered
    exactly as it is on its tab, with the reading rules beside them.

    Live rather than hand-written, so the example can never drift from
    what the card actually shows: when the verdict line changes, the
    lesson changes with it.
    """
    def pick(cards, want):
        for f in sorted(cards, key=lambda x: x.kickoff):
            v = verdict(f, _star(f))
            if v and v["mark"] == want:
                return f
        return None

    strong = pick(playable, "strong")
    normal = pick(playable, "normal")
    none = next((f for f in sorted(waiting, key=lambda x: x.kickoff)
                 if (v := verdict(f, _star(f))) and v["odds"] is not None
                 and not v["play"]), None)

    def show(f, kind, caption, lesson):
        if f is None:
            return (f'<div class="learncard"><h3>{caption}</h3>'
                    '<p class="dim">no card of this kind on the board right '
                    'now \u2014 it will appear here when one is.</p></div>')
        return (f'<div class="learncard"><h3>{caption}</h3>'
                f'<div class="grid" style="max-width:420px">'
                f'{_card(f, kind, reads)}</div>'
                f'<div class="lesson">{lesson}</div></div>')

    blocks = show(normal, "play", "1 \u00b7 A normal play",
        "The card is green-bordered and the verdict line says <b>PLAY</b> "
        "with the lane, the price it needs and the book that pays it. Read "
        "it in this order: the <b>label</b> (green, orange, super green \u2014 the "
        "guard's read of this kind of card; orange lands 83.4% of the time, "
        "green 87.4%), then <b>needs</b> (the break-even that label implies "
        "plus 6%), then the <b>price</b>. The price cleared the bar, so this "
        "is a bet: 4% of the bankroll at that price or better. If the book "
        "you use is short of the bar, it is not a bet there.") + show(
        strong, "play", "2 \u00b7 A strong play",
        "Same as a normal play, plus <b>\u2605 STRONG</b>: the confluence score "
        "\u2014 the card run back through the board's own searches, league, each "
        "club, the side, club-and-side, all as-of \u2014 sits in the top quarter "
        "in a European league. On 1,008 replayed bets the strong ones landed "
        "81.0% at +8.87% while the rest landed 72.8% at \u22120.67%. In the "
        "bankroll replay this lane is the only thing that compounds: play "
        "these first, and never skip one for price if any book you hold "
        "clears the bar.") + show(
        none, "pend", "3 \u00b7 A card with no play",
        "The lane bars say <b>DECLINE</b> or <b>needs</b> and the verdict "
        "says <b>no play</b>: either the tier is red (the guard says avoid, "
        "whatever the price) or no book clears the bar. Nothing here is a "
        "bet. Do not buy a declined card because it is a good read \u2014 the "
        "market disagrees with Athena most exactly where Athena is most "
        "wrong, and every attempt to bet those cards has lost. And do not "
        "come back later hoping the price drifts out: a card that clears "
        "only because the price moved late lost in both seasons at both "
        "books.")

    rows = [
        ("The rule in one line", "Bet only what the verdict line says "
         "PLAY, at 4% of the bankroll, at or above the price it needs. "
         "Everything else on the board is graded and banked, not played."),
        ("Label", "the guard's read of this KIND of card, from the card's "
         "own tier and its confluence score. Five: super green, green, "
         "orange, red, super red. Each carries one measured hit rate; red "
         "and super red are never played."),
        ("needs", "the price the label's hit rate needs to break even, "
         "plus 6%. The bar is the same for every card with that label \u2014 "
         "that is deliberate: a bar built per card finds exactly the cards "
         "the market is right about."),
        ("PASS / DECLINE", "every lane on the card carries its own bar: "
         "PASS means the best quote clears it, DECLINE means it does not, "
         "needs x.xx means nothing is quoted yet. Only the starred lane's "
         "bar is backed by a validated hit rate; the others are shown for "
         "honesty."),
        ("\u2605 the star", "the lane the card is read from: tip 1 unless a "
         "draw-no-bet on tip 3 out-claims it by two points. Tip 2 is never "
         "starred and never a play."),
        ("The struck line", "the board quotes the line that reaches the "
         "slip, not the rung Athena prints: U3.0 is bought as U3.5 and "
         "U4.25 as U4.5, because the engine cannot tell those apart and "
         "the safer line pays more on settlement. Everything else as "
         "printed."),
        ("👀 Watch lanes", "the starred lane is not red and the panel's "
         "best sits under its bar by five percent or less. Not a play on "
         "the feed's prices — but your own book may clear it: check the "
         "offer, take it only at or above the needs price on the card."),
        ("When to decide", "at first sight, two or three days out. A card "
         "that clears then may be bought later if its price has drifted "
         "out. A card that does not clear then is not a play on Saturday "
         "either."),
        ("Kickoff", "a match that has kicked off is not playable. Quotes "
         "stop at kickoff and the verdict checks the clock."),
        ("\U0001f534 Running", "what the board called before a kickoff that "
         "has since happened. Not buyable any more, so not a play — the "
         "price shown is the one from first sight, kept in the forward "
         "log. A card the board declined never appears here."),
        ("Tip 2 \u00b7 (team)", "a TEAM total \u2014 one side alone to score. "
         "Printed and graded, never played: it landed 12.7 points below "
         "tip 1 on the same fixtures."),
        ("Tip 3", "the result lane \u2014 double chance or draw no bet. Only a "
         "gated DNB is ever the star; double chance is never played."),
        ("\U0001f9e0 the read", "what Athena measured in this matchup; tap "
         "the card for the full story. Every phrase maps to something "
         "measured, never invented."),
    ]
    items = "".join(f'<tr><td class="mk"><b>{k}</b></td><td>{v}</td></tr>'
                    for k, v in rows)
    return ('<style>.learngrid{display:grid;gap:14px;grid-template-columns:'
            'repeat(auto-fit,minmax(300px,1fr))}.learncard h3{margin:6px 0}'
            '.lesson{font-size:.93em;line-height:1.45;margin-top:8px;'
            'padding:8px 10px;border-left:3px solid #888;opacity:.92}</style>'
            f'<div id="learn"><h2>\U0001f393 Learn Athena \u2014 how the '
            f'board is played</h2>'
            f'<p class="dim">Three cards from today\'s board, live: the '
            f'lesson is whatever they show right now.</p>'
            f'<div class="learngrid">{blocks}</div>'
            f'<div class="wrap" style="margin-top:10px">'
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
    # PLAYABLE now means what it says: cards the guard would actually
    # stake. It used to mean "a lane cleared the engine's edge bar", which
    # is a different and much looser question — 8,121 priced cards cleared
    # that and returned -1.67%, while the 1,008 clearing THIS one returned
    # +1.71%. A tab called playable that lists cards nobody should back was
    # the wrong promise.
    #
    # Everything else moves to Athena lanes, which is now the data tab:
    # still published, still graded, still feeding the bank and the
    # forward log, but marked no play.
    def _v(f):
        return verdict(f, _star(f))
    playable = [f for f in pending if (v := _v(f)) and v["play"]]
    strong_n = sum(1 for f in playable if (v := _v(f)) and v["strong"])
    # Three-way, not two: PLAY, WATCH (the bettor's list, 2 Sep — a
    # starred lane a few percent short of its bar on the panel, worth
    # checking at his own books), and the rest.
    watch = [f for f in pending if (v := _v(f)) and v["watch"]]
    # RUNNING (the bettor's ask, 3 Sep): cards the board offered before
    # kickoff and that are now in progress. They cannot be PLAY or watch —
    # both bars require an unstarted match — but dropping them straight
    # into Athena lanes filed the night's calls next to genuine declines
    # with nothing to tell them apart. They keep the price from the
    # forward log, not a live quote, because there is no live quote.
    running = [f for f in pending if running_call(f)]
    waiting = [f for f in pending if f not in playable and f not in watch
               and f not in running]
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
    # The four lane records, fused into ONE bar in the baseline bar's
    # style (the bettor's layout, 2 Sep): still this session's numbers,
    # read as one line rather than four tiles.
    def sc(name, h, n, extra=""):
        v = f"<b>{h / n * 100:.1f}%</b>" if n else "<b>—</b>"
        return f'{name} {v}<span class="dim"> {h}/{n}{extra}</span>'
    sessbar = (
        f'<div class="basebar">Session #{SESSION_NO} — hit vs claimed: '
        + " · ".join([
            sc("final pick", fh, fn,
               (f" · claims {sum(fsays)/len(fsays):.1f}" if fsays else "")),
            sc("tip 1", pb1, pq1, claims(1)),
            sc("tip 2", pb2, pq2, claims(2)),
            sc("tip 3", h3, n3, claims(3) + (f" · {hs3} hindsight" if hs3 else "")
               + " · probation"),
        ])
        + ' <span class="dim">— the ★ lane, then each family\'s PLAYABLE '
          'lanes, graded on this session\'s completed cards</span></div>')

    # NORMAL and STRONG: the record of the cards the board itself marked
    # PLAY, by kind, from the forward log — stamped at first sight, so a
    # card counts as the play it was when it was offered, at the price it
    # was offered at. This is the guard graded on cards it had never
    # seen, split the way the rules split it: STRONG first.
    from scripts import forward_settle as _fs
    from scripts.confluence import region as _region
    final = {}
    for f in fixtures:
        if f.settled and "—" in f.status:
            sc_ = f.status.split("—")[-1].strip().split(" ")[0]
            if "-" in sc_:
                try:
                    hg, ag = sc_.split("-")
                    final[(f.kickoff.split(" ")[0], f.teams)] = (int(hg), int(ag))
                except ValueError:
                    pass
    kinds = {"normal": [0, 0], "strong": [0, 0]}
    seen: set = set()
    if FORWARD.exists():
        for ln in FORWARD.read_text().splitlines():
            if ln.startswith("#") or not ln.strip():
                continue
            p = ln.split("\t")
            if len(p) < 13 or p[1] < SESSION_DATE or _fs._artefact(p[5]):
                continue
            key = (p[1], p[3])
            if key in seen:
                continue
            seen.add(key)
            try:
                need, best = float(p[9]), float(p[11] or p[10])
                score = float(p[8]) if p[8] else None
            except ValueError:
                continue
            if p[7].endswith("red") or best < need or key not in final:
                continue
            got = _fs._settle(p[5], *final[key])
            if got is None:
                continue
            kind = ("strong" if (score is not None and score >= STRONG_SCORE
                                 and _region(p[2]) == "Europe") else "normal")
            kinds[kind][1] += 1
            kinds[kind][0] += got[1]
    (nh, nn), (sh, sn) = kinds["normal"], kinds["strong"]
    tiles = "".join([
        tile("taken bets", f"{bh / bn * 100:.1f}%" if bn else "—",
             f"your lanes · {bh}/{bn} hits"),
        tile("roi", f"{roi:+.1f}%", f"flat stakes · {bn} settled"),
        tile("normal", f"{nh / nn * 100:.1f}%" if nn else "—",
             f"PLAY cards · {nh}/{nn}" if nn else "PLAY cards · none settled yet"),
        tile("★ strong", f"{sh / sn * 100:.1f}%" if sn else "—",
             f"STRONG cards · {sh}/{sn}" if sn else "STRONG cards · none settled yet"),
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

    # A bet's fixture, so a row can be filtered by league, country and
    # lane kind exactly like a card is.
    _fxmap = {f.teams: f for f in fixtures}

    def _short(note: str, cap: int = 64) -> str:
        """The Found bets cell gets the book and the first clause; the
        full note stays in bets.tsv, the README and the row's hover
        (the bettor's ask, 2 Sep: shorter there, longer anywhere else)."""
        book, _, rest = note.partition(" — ")
        if not rest or len(book) > 14:
            # No book prefix (older notes open with the reasoning): the
            # whole note is the clause to shorten.
            book, rest = "", note
        first = re.split(r"\s*[;:]\s+|\s+—\s+|\.\s+", rest.strip(), 1)[0]
        if len(first) > cap:
            first = first[:cap].rsplit(" ", 1)[0] + "…"
        return f"{book} · {first}" if book and first else (book or first)

    def _bet_tr(b):
        # Every field is searchable through the same bar the cards use —
        # the row carries its own lowercase haystack in data-t.
        prob = f'{b["prob"]*100:.1f}%' if b["prob"] else "—"
        bits = [b["name"], b["lane"], b["align"], b["note"],
                b["mark"], f'{b["odds"]:.2f}', prob]
        f = _fxmap.get(b["name"])
        if f is not None:
            bits += [f.league, f.code, _country(f.code)]
        rung = b["lane"].split()[0]
        if rung in ("1X", "X2", "12"):
            bits.append("double chance result lane")
        elif rung == "DNB":
            bits += ["draw no bet", "dnb", "result lane"]
        else:
            side = "over" if rung.startswith("O") else "under"
            kind = "team" if "(" in b["lane"] else "match"
            bits += [side, f"{kind} {side}"]
            if kind == "match":
                bits += [f"ft {side}", f"fulltime {side}"]
        bits.append({"✅": "hit won", "❌": "miss lost", "◦": "push",
                     "open": "open pending"}.get(b["mark"], ""))
        raw = " ".join(x for x in bits if x).lower()
        folded = _fold(raw)
        hay = raw if folded == raw else raw + " " + folded
        # Settled rows carry their grade so the bets counter can total
        # whatever the filter leaves on screen.
        g = ("" if b["mark"] == "open"
             else f' data-g="{0 if b["mark"].startswith("❌") else 1}"')
        # A taken bet is one lane, not three, so it carries a single
        # probability — a bare "<80" reaches it, a lane-prefixed
        # "tip 2 <80" does not, because the row has no such lane.
        p = f' data-p="{b["prob"]*100:.1f}"' if b["prob"] else ""
        if f is not None and _goals(f) is not None:
            p += f' data-goals="{_goals(f)}"'
        # Kickoff, shown compact but sorted on the full stamp: "31-08"
        # ahead of "01-09" is right by the calendar and wrong by string
        # order, so the cell carries the ISO value for the sorter.
        kick = (f'<td class="dim" data-v="{f.kickoff}">{board._stamp(f)}</td>'
                if f is not None else '<td class="dim">—</td>')
        return (f'<tr data-t="{html.escape(hay)}"{g}{p}'
                + (f' data-lg="{html.escape(_fold(f.league.lower()))}"'
                   if f is not None else "") + ">"
                f'<td class="mk">{b["mark"]}</td>{kick}'
                # The fixture opens its own card — the same element the
                # board renders, not a second copy of it, so a position
                # can always be read back against the lanes it was struck
                # from. A bet whose fixture is no longer on the board
                # stays plain text rather than offering a dead link.
                + (f'<td><button class="fxopen" onclick="showCard(this)">'
                   f'{html.escape(b["name"])}</button></td>'
                   if f is not None else
                   f'<td>{html.escape(b["name"])}</td>')
                + f'<td>{html.escape(b["lane"])}</td>'
                f'<td class="dim">{prob}</td>'
                f'<td>{b["odds"]:.2f}</td><td>{b["ret"]}</td>'
                f'<td><span class="align {_align_cls(b["align"])}">'
                f'{html.escape(b["align"])}</span></td>'
                f'<td class="note" title="{html.escape(b["note"])}">'
                f'{html.escape(_short(b["note"]))}</td></tr>')

    # The page opens on kickoff, earliest first; a bet whose fixture has
    # left the board has no kickoff and sits at the bottom, in log order.
    # Any column clicked after that sorts the table its own way.
    def _kick(b):
        f = _fxmap.get(b["name"])
        return (f is None, f.kickoff if f is not None else "")
    bets_html = "".join(_bet_tr(b) for b in sorted(bet_rows, key=_kick))
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
        # Tip 2 and tip 3 as the retro bank stores them: the lane text in
        # t2/t3 and its grade in m2/m3, NOT a glyph buried in the text.
        # The board copy used to inline the mark and drop tip 3 entirely,
        # so a board card in Ask Athena showed two lanes where the fixture
        # file had three, and contributed nothing to the tip 2 and tip 3
        # counters (found from the Casa Pia card, 31 Aug).
        def _lane(cell):
            c = re.sub(r"\*\*(.+?)\*\*", r"\1", (cell or "").strip())
            if c in ("", "—", "— none") or c.startswith("—"):
                return None, ""
            m = re.match(r"^([✅❌◦])\s*", c)
            return (c[m.end():], m.group(1)) if m else (c, "")

        for key, cell in (("2", f.tip2), ("3", f.tip3)):
            text, glyph = _lane(cell)
            if text is None:
                continue
            entry["t" + key] = text
            if glyph:
                entry["m" + key] = glyph
        rd = reads.get(f"{f.code}|{f.teams}|{f.kickoff.split(' ')[0]}")
        if rd:
            entry["kw"] = rd[0]
        # The guard on a board card, in the bank's own fields, so Ask
        # Athena reads a live card and a past one with the same words:
        # label, score, strong, the starred lane, and the verdict at the
        # live quote (a settled card keeps the verdict it was offered at
        # only through the forward log; here it shows its label).
        if not f.settled:
            v = verdict(f, _star(f))
            if v:
                entry["g"], entry["pk"] = v["label"], _star(f)
                if v["score"] is not None:
                    entry["cs"] = round(v["score"], 1)
                entry["st"] = int(bool(v["strong"]))
                if v["odds"] is not None:
                    entry["bp"], entry["need"] = round(v["odds"], 2), round(v["need"], 2)
                    entry["v"] = v["mark"]
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
    # Only the league names that are CONTAINED IN another league's name
    # need exact matching — "laliga" inside "laliga 2", "brasileirão"
    # inside "brasileirão série b". Everything else stays a plain
    # substring search, so "serie a" still finds the Italian and the
    # Brazilian and the country term separates them.
    # Exact matching is reserved for collisions the SAME COUNTRY cannot
    # break: "laliga" inside "laliga 2", "bundesliga" inside "2.
    # bundesliga", "brasileirao" inside "brasileirao serie b". Where the
    # two leagues sit in different countries — Italy's Serie B inside
    # Brazil's Brasileirão Série B — the country word already separates
    # them, so "brazil, serie b" and "italy, serie b" both work and the
    # name stays a plain substring (the bettor's rule, 31 Aug).
    _lands: dict[str, set] = {}
    for f in fixtures:
        _lands.setdefault(_fold(f.league.lower()), set()).add(_country(f.code))
    # Country per competition code, for the bank cards' own filter.
    _codes = {f.code for f in fixtures}
    _bankfile = ROOT / "web" / "matchbank.json"
    if _bankfile.exists():
        _codes |= set(_json.loads(_bankfile.read_text())["comps"])
    countries_js = _json.dumps({c: _country(c) for c in sorted(_codes)
                                if _country(c)})
    leagues_js = _json.dumps(sorted(
        a for a in _lands
        if any(a != b and a in b and _lands[a] & _lands[b] for b in _lands)))
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
    # A board fixture whose result has since been merged into the results
    # store is ALREADY in the retro bank — replayed as-of and graded on
    # all three lanes — so keeping the board copy too double-counts it.
    # 288 rows after the 31 Aug ingest, which pulled the hero window down
    # twice as far as the week deserved. The retro row wins; it carries
    # every lane, where the board row carries at most two.
    for comp in bank.values():
        seen, keep = set(), []
        for m in sorted(comp["matches"], key=lambda x: x.get("src") == "board"):
            k = (m["d"], m.get("kh"), m.get("ka"))
            if k in seen:
                continue
            seen.add(k)
            keep.append(m)
        comp["matches"] = sorted(keep, key=lambda x: x["d"])

    # The headline number is what a reader FOLLOWING THE STAR actually
    # gets, so it scores the chosen lane rather than tip 1 — the same
    # chooser the cards apply (_card: tip 1 unless a DNB out-claims it by
    # DNB_GATE). The population is unchanged: still the cards whose tip 1
    # cleared the playable bar, which is what the site offers, so the
    # number stays directly comparable to the tip-1 figure it replaces.
    graded = []
    for comp in bank.values():
        for m in comp["matches"]:
            if m.get("mark") not in ("✅", "✅½", "◦", "❌"):
                continue
            e = re.search(r"([+\-−]\d+(?:\.\d+)?)%\s*(?:\(|·|$)",
                          m.get("tip", ""))
            if not e or float(e.group(1).replace("−", "-")) <= 1.0:
                continue
            p1, p3 = _claim(m.get("tip")), _claim(m.get("t3"))
            mark = m["mark"]
            if (_is_dnb(m.get("t3")) and p1 is not None and p3 is not None
                    and p3 - p1 > DNB_GATE
                    and m.get("m3") in ("✅", "◦", "❌")):
                mark = m["m3"]
            graded.append((m["d"], mark))
    graded.sort(reverse=True)
    window = graded[:300]
    # A push counts as a hit, same as everywhere on the board: the
    # standing offset plays the rung a notch softer, which wins there,
    # and a DNB on a draw returns the stake.
    hero_rate = (sum(1 for _d, mk in window
                     if mk.startswith("✅") or mk == "◦")
                 / len(window) * 100) if len(window) >= 100 else None
    hero_sub = f" — {hero_rate:.1f}% hitrate" if hero_rate else ""
    hero_fine = ("The final pick · the 300 most recent graded "
                 "playable cards")

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
    # SORTED, because `live` is a set and Python randomises string hashing
    # per process: iterating it wrote the same 8.7 MB of JSON in a
    # different key order on every render, so `git status` came back dirty
    # after a no-op re-render and every board commit carried a phantom
    # diff. `alias` is sorted for the same reason — it is fed partly from
    # set iteration further up. Same content either way; this only makes
    # the render reproducible.
    names = {g: prefer.get(g, g) for g in sorted(live)}
    (OUT.parent / "matchbank.json").write_text(
        _json.dumps(dict(comps=bank, alias=dict(sorted(alias.items())),
                         names=names), ensure_ascii=False))

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
.fcap {{ font-size:11px; margin:-8px 0 12px; }}
.fxopen {{ background:none; border:0; padding:0; font:inherit; color:inherit;
  cursor:pointer; text-align:left; border-bottom:1px dotted #55607a; }}
.fxopen:hover {{ color:var(--gold); border-bottom-color:var(--gold); }}
.fxmodal {{ position:fixed; inset:0; z-index:60; display:none;
  background:rgba(6,8,12,.82); padding:24px 14px; overflow:auto; }}
.fxmodal.on {{ display:block; }}
.fxbox {{ max-width:520px; margin:0 auto; }}
.fxbox .card {{ margin:0; }}
.fxshut {{ display:block; margin:12px auto 0; background:#181d27;
  border:1px solid #2b3242; color:#c8d0e0; border-radius:6px;
  padding:7px 16px; font:inherit; cursor:pointer; }}
.fxshut:hover {{ border-color:var(--gold); color:var(--gold); }}
.buyat {{ color:#cfd6e4; }}
.buyat b {{ color:var(--gold); }}
.fhelp {{ font-size:12px; margin:-4px 0 14px; color:#8a93a6; }}
.fhelp summary {{ cursor:pointer; }}
.fhelp div {{ padding:8px 0 0 2px; line-height:1.7; }}
.fhelp code {{ background:rgba(255,255,255,.06); border-radius:3px;
  padding:1px 5px; color:#cfd6e4; }}
.askq {{ width:100%; margin:10px 0 8px; }}
.chip {{ display:inline-block; margin-left:4px; font-size:10px;
  background:#1b2233; border:1px solid var(--edge);
  border-radius:5px; padding:0 4px; color:var(--dim); }}
#ask-out .card summary {{ cursor:pointer; }}
#ask-out .grid {{ max-height:70vh; overflow-y:auto; }}
#ask-counts {{ grid-template-columns:repeat(5,1fr); }}
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
.card.watch {{ border-left-color:var(--gold); }}
.card.run {{ border-left-color:#ff5d5d; }}
.tabs a.on.amber {{ background:var(--gold); color:#111; }}
.tabs a.on.red {{ background:#ff5d5d; color:#111; }}
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
.guard {{ display:inline-block; margin:6px 0 2px; padding:2px 8px;
  border-radius:999px; font-size:10px; text-transform:uppercase;
  letter-spacing:.11em; border:1px solid transparent; cursor:help; }}
.g-super-green {{ color:#8fe3a8; border-color:#2f6b45;
  background:rgba(47,107,69,.16); }}
.g-green {{ color:#7fc79a; border-color:#27523a; }}
.g-orange {{ color:#d9b46a; border-color:#5b4a24; }}
.g-red {{ color:#e08b7a; border-color:#6b3129; }}
.g-super-red {{ color:#f0a08e; border-color:#8a3a2e;
  background:rgba(138,58,46,.18); font-weight:600; }}
.verdict {{ font-size:12px; margin:0 0 4px; letter-spacing:.03em; }}
.play {{ color:#cfe6ff; }}
.rung {{ font-size:10px; color:var(--dim); margin-left:6px;
  border:1px solid #2a3346; border-radius:4px; padding:0 5px;
  letter-spacing:.05em; cursor:help; white-space:nowrap; }}
.lanebar {{ font-size:11px; margin-top:5px; letter-spacing:.04em; }}
.lanebar.yes {{ color:#8fe3a8; }}
.lanebar.no {{ color:#e08b7a; }}
.lanebar.dimv {{ color:var(--dim); }}
.verdict.yes {{ color:#8fe3a8; }}
.verdict.yes b {{ color:#b8f0c8; }}
.verdict.no {{ color:#e08b7a; }}
.verdict.dimv {{ color:var(--dim); }}
.verdict.strong {{ color:#ffd875; background:rgba(230,195,92,.10);
  border:1px solid rgba(230,195,92,.45); border-radius:6px;
  padding:4px 8px; font-weight:600; }}
.verdict.strong b {{ color:#fff0c2; }}
.panenote {{ font-size:12px; color:var(--dim); line-height:1.55;
  border-left:2px solid #2a3346; padding:2px 0 2px 10px; margin:2px 0 12px; }}
.panenote b {{ color:var(--tx); }}
.panenote .sgm {{ color:#ffd875; }}
.card.pend .lane {{ opacity:.72; }}
.noplay {{ font-size:10px; text-transform:uppercase; letter-spacing:.12em;
  color:#e08b7a; border:1px solid #6b3129; border-radius:4px;
  padding:1px 6px; margin-left:8px; white-space:nowrap; }}
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
 {sessbar}
 <div class="session">SESSION #{SESSION_NO} · {SESSION_START} – {session_end}{
    f" · longest hit streak <b>{best_streak}</b>" if best_streak else ""}</div>
 <div class="tiles">{tiles}</div>
 <div class="ask">
  <b>🔎 Ask Athena</b> <span class="dim">— look up any matchup it has run;
  past matches are a retrosim (each competition's last ~200 games).</span>
  <div class="askrow">
   <input type="date" id="ask-d">
   <select id="ask-lg" onfocus="ensureBank()" onmousedown="ensureBank()">
    <option value="">League…</option></select>
   <div class="combo"><input id="ask-a" placeholder="Team A"
    autocomplete="off" onfocus="ensureBank()"><div class="sug"
    id="sug-a"></div></div>
   <div class="combo"><input id="ask-b" placeholder="Team B"
    autocomplete="off" onfocus="ensureBank()"><div class="sug"
    id="sug-b"></div></div>
   <button class="btn askbtn" onclick="askAthena()">Enter</button>
  </div>
  <input id="ask-q" class="askq" oninput="askFilter()" style="display:none"
   placeholder="narrow these results — league, country, lane, hit/miss, tip 2 none… commas narrow">
  <div class="fcounts" id="ask-counts" style="display:none"></div>
  <div id="ask-out"></div>
 </div>
 <div class="tabs">
  <a href="#home/playable" data-t="playable">🟢 Playing now
   <span class="dim">{len(playable)}{f" · ★{strong_n}" if strong_n else ""}</span></a>
  <a href="#home/watch" data-t="watch" class="amber">👀 Watch lanes
   <span class="dim">{len(watch)}</span></a>
  <a href="#home/running" data-t="running" class="red">🔴 Running
   <span class="dim">{len(running)}</span></a>
  <a href="#home/bets" data-t="bets" class="gold">🟡 Found bets
   <span class="dim">{bh}/{bn}</span></a>
  <a href="#home/lanes" data-t="lanes" class="blue">🔵 Athena lanes
   <span class="dim">{len(waiting)} · no play</span></a>
  <a href="#home/done" data-t="done" class="grey">⚪ Completed
   <span class="dim">{len(done)}</span></a>
 </div>
 <button class="btn" onclick="document.getElementById('learn')
  .scrollIntoView({{behavior:'smooth'}})">🎓 Learn Athena — how to read
  these blocks</button>
 <input id="q" oninput="applyFilter(this.value)"
  placeholder="filter — team, league, code, lane, tip 2 none… commas narrow: real madrid, team over">
 <div class="fcounts" id="fcounts"></div>
 <div class="fcap dim" id="fcap"></div>
 <details class="fhelp"><summary>what can I type in the filter?</summary>
  <div><b>anything on the card</b> — a team, a league, a country, a code,
  a rung (<code>o2.5</code>), a mark (<code>hit</code>, <code>miss</code>,
  <code>push</code>, <code>open</code>).<br>
  <b>lane kinds</b> — <code>team over</code>, <code>match under</code>
  (also <code>ft under</code>),
  <code>double chance</code>, <code>draw no bet</code>,
  <code>result lane</code>, <code>playable</code>. Tie one to a lane with
  <code>tip 1 under</code>, <code>tip 2 over</code>.<br>
  <b>the guard</b> — <code>green</code>, <code>orange</code>,
  <code>red</code>, <code>super green</code>, <code>super red</code>: the
  label on the card. Type <code>guard red</code> or <code>label red</code>
  to keep clubs called Red out of it. Also <code>strong</code>,
  <code>verdict play</code>, <code>verdict no play</code>.<br>
  <b>an absent lane</b> — <code>tip 2 none</code> (also
  <code>no tip 3</code>): the cards where that lane never printed.<br>
  <b>a probability threshold</b> — <code>tip 2 &lt;80</code>,
  <code>tip 1 80&gt;</code>, <code>tip 3 &lt;=70</code>, or
  <code>prob &lt;60</code> for the card's headline number. The arrow points
  at the side it keeps and can sit either way round; the % is optional.<br>
  <b>a goal threshold</b> — <code>goal &gt;3</code>, <code>goal &lt;3</code>:
  how the match actually finished, on settled cards only.<br>
  <b>commas narrow</b> — <code>brazil, serie b, tip 1 under, 80&gt;</code>
  is all four at once.</div></details>
 <div class="fxmodal" id="fxmodal" onclick="hideCard(event)">
  <div class="fxbox" id="fxbox"></div>
  <button class="fxshut" onclick="hideCard()">close</button>
 </div>
 <div class="tabpane" id="t-playable">
  <div class="panenote">Only what the guard would actually stake: the
  starred lane cleared its label's break-even by {DECLINE_MARGIN*100:.0f}%
  and the tier is not red. <b class="sgm">★ STRONG</b> adds a top-quartile
  confluence score in Europe — on 1,008 replayed bets those graded 81.0%
  and +8.87%, against 72.8% and −0.67% for the rest.</div>
  {_grid(playable, "play", reads)}</div>
 <div class="tabpane" id="t-watch">
  <div class="panenote">Not plays — yet. The starred lane is not red and
  the panel's best quote sits under its bar by no more than
  {WATCH_BAND*100:.0f}%. The feed is 26 books and yours are not the
  sharpest, so a card the panel prices a little short is one your own book
  may clear: check the offer, and take it only at or above the
  <b>needs</b> price on the card. Under that it is still a decline.</div>
  {_grid(watch, "watch", reads)}</div>
 <div class="tabpane" id="t-running">
  <div class="panenote">Cards the board called before kickoff, now in
  progress. They are no longer buyable, so they are no longer PLAY or
  watch — but they were, and the price shown is the one from
  <b>first sight</b> in the forward log, not a live quote. A card the
  board declined never appears here: this tab is the night's calls
  playing out, not a second chance at them.</div>
  {_grid(running, "run", reads)}</div>
 <div class="tabpane" id="t-bets">{bets_meta}<div class="wrap">
  <table id="t-betstable" class="sortable">
  <tr><th data-sort="s">·</th>
  <th data-sort="s" data-dir="asc" class="asc">Kickoff</th>
  <th data-sort="s">Fixture</th><th data-sort="s">Lane</th>
  <th data-sort="n">Prob</th><th data-sort="n">Odds</th>
  <th data-sort="n">Return</th><th data-sort="s">Athena says</th>
  <th data-sort="s">Note</th></tr>
  {bets_html}</table></div></div>
 <div class="tabpane" id="t-lanes">
  <div class="panenote">Everything Athena published that is <b>not</b>
  being played — the price never cleared, or the tier said avoid. Kept
  in full because it is graded, banked and fed back into the record;
  it is data, not a shortlist. Open a card to see why it was refused.</div>
  {_grid(waiting, "pend", reads)}</div>
 <div class="tabpane" id="t-done">{_grid(done, "done", reads)}</div>
 {_learn(playable, waiting, reads)}
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

 <h3>How the board is played — the rules of PRE-ALFA 2</h3>
 <p>Written down 2 Sep at the reset, from what two runs and the odds
 layer measured. Session #6 plays these and changes nothing while it
 runs. Six rules, in order:</p>
 <p><b>1. The verdict line is the whole decision.</b> Every card marks
 one lane — tip 1, or a draw-no-bet on tip 3 that out-claims it by two
 points — and the guard reads that lane into one of five labels from the
 card's own tier and its confluence score, the card run back through the
 board's searches as-of. Each label carries one measured hit rate, frozen
 on two windows over 62,528 replayed picks. The card says <b>PLAY</b> only
 when the best live quote clears that label's break-even by 6% and the
 tier is not red. That is a bet. Anything else on the board is graded and
 banked, not played.</p>
 <p><b>2. Flat 4% of the bankroll as it stands.</b> Measured again this
 session through two seasons at real closing prices: the whole book at
 4% is a coin flip with a near-halving drawdown; the STRONG lane compounds
 (€50 to €106, every year-long start above €50). So the stake is flat,
 and the volume is low — the target is a few plays a week, not fifteen.</p>
 <p><b>3. STRONG first.</b> A play whose confluence score sits in the top
 quarter in a European league landed 81.0% at +8.87% on the 1,008 bets
 the bar fires on, against 72.8% and −0.67% for the rest. Never skip a
 strong card for price if any book you hold clears its bar.</p>
 <p><b>4. Decide at first sight, buy later if it drifts out.</b> On the
 same 8,506 cards the early line is lower than the close 61% of the time
 — the market drifts away from Athena's side toward kickoff — but cards
 that clear the bar only because the price drifted out late lost in both
 seasons at both books. A card that does not clear on Thursday is not a
 play on Saturday; a card that did may be bought at Saturday's longer
 price.</p>
 <p><b>5. The bar is a category, not the card.</b> Every attempt to bet
 on Athena's own number — the claim, a blend of claim and searches, an
 inverse play, per-league bars — lost at real prices, because the market
 disagrees with Athena most exactly where Athena is most wrong. The five
 coarse labels are the only bar that has stayed positive in both seasons,
 and they stay coarse on purpose.</p>
 <p><b>6. The line that reaches the slip.</b> U3.0 is bought as U3.5 and
 U4.25 as U4.5 — the engine cannot tell those apart and the safer line
 pays more on settlement; everything else as printed. A match that has
 kicked off is not playable. Tip 2 and double chance are never played.
 Where no feed carries a league (Swiss, Polish, Algerian) the score is
 typed by hand through the same grader as the sweep.</p>
 <p class="dim">The result lane's own record, kept because tip 3 does not
 inherit a league's tip 1 weakness:</p>
 {read_tiers}

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
 <p>Five runs so far, and the arc between them is the whole story:</p>
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
  <div class="run"><b>Sessions #4–5 <span class="when">· 28 Aug – 1 Sep
   2026 · closed</span></b>The first full run on the calibrated floors,
   and then the two build days that changed how the board is read: live
   prices from 26 books, the guard's five labels, the decline rule — the
   first rule in this project with a positive return at real prices — and
   the finding underneath it, that the edge lives in the panel and the
   STRONG lane rather than in volume. Seven missing seasons filled, 552
   club spellings folded, five drifted guards closed. Closed at Tip 1
   <b>80.9%</b> on 267 settled, found bets <b>−0.3%</b> ROI on 143 — flat,
   the best a book has done here, and still not the number.</div>
  <div class="run"><b>Session #{SESSION_NO} <span class="when">· {SESSION_START}
   2026 – running</span></b><b>PRE-ALFA 2, the long run.</b> Nothing in the
   engine or its rules is touched while it runs: the board plays the six
   rules above exactly as written, at 4% flat, STRONG first, decided at
   first sight, and the forward log grades the guard on cards it has never
   seen. The only sample that can settle whether the odds layer is real is
   this one. {live_line}</div>
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
  const tab = ["playable","watch","running","bets","lanes","done"]
    .includes(h[1])
    ? h[1] : "playable";
  for (const s of document.querySelectorAll(".page"))
    s.classList.toggle("on", s.id === "p-" + page);
  for (const a of document.querySelectorAll("nav a"))
    a.classList.toggle("on", a.dataset.p === page);
  for (const s of document.querySelectorAll(".tabpane"))
    s.classList.toggle("on", s.id === "t-" + tab);
  for (const a of document.querySelectorAll(".tabs a"))
    a.classList.toggle("on", a.dataset.t === tab);
  if (typeof recount === "function") recount();
}}
// Commas narrow rather than widen: every term must be present, so
// "real madrid, team over" is the Madrid cards that offer a team over.
// A card's haystack carries its lanes' kinds as words, not just their
// printed rungs — see _haystack.
// A term that IS a league name matches that league exactly, because one
// league's name can sit inside another's: "laliga" would otherwise drag
// in every LaLiga 2 card, and no amount of typing could ask for the top
// flight alone (the bettor's catch, 31 Aug). Everything else stays a
// plain substring match, so partial words keep working.
const LEAGUES = new Set({leagues_js});
const COUNTRY = {countries_js};

// Typed accents are folded away too, so "brasileirão" and
// "brasileirao" are the same search — the card carries both spellings.
const fold = s => s.normalize("NFD").replace(/[\\u0300-\\u036f]/g, "");

// "tip 2 none" / "no tip 3" — cards where that lane never printed. The
// absent lane carries a sentinel token rather than any "tip 2" text,
// because matching is plain substring and a bare "tip 2" search must not
// come back with the cards that have no tip 2. Both bars read their
// query through here, so both speak the same vocabulary.
const NONEQ = /^(?:no[ -]?tip ?([123])|tip ?([123]) ?(?:none|empty|missing|absent))$/;

// A threshold on a lane's probability: "tip 2 <80", "tip 1 80>",
// "tip 3 <=70", or a bare "<80" for the headline number (tip 1 on a
// card, the ticket's own probability on a taken bet). The arrow decides
// the side it keeps and may sit before or after the number — "<80" and
// "80<" both mean below 80 — because either reads naturally when you are
// typing fast. The % is optional.
const CMPQ = /^(?:tip ?([123])|prob|p)? *(?:(<=|>=|<|>) *(\d+(?:\.\d+)?)|(\d+(?:\.\d+)?) *(<=|>=|<|>))$/;
// "goal >3" — how the match FINISHED, a different number space from the
// probabilities, so it carries its own keyword. Without one, "3" would
// have to be read as either three goals or three percent.
const GOALQ = /^(?:goals?|g) *(?:(<=|>=|<|>) *(\d+)|(\d+) *(<=|>=|<|>))$/;
function parseCmp(s) {{
  const t = s.replace(/[%≤≥]/g, x => x === "≤" ? "<=" : x === "≥" ? ">=" : "")
             .replace(/\\s+/g, " ").trim();
  const g = GOALQ.exec(t);
  if (g) return {{what: "goals", op: g[1] || g[4],
                 val: parseFloat(g[2] !== undefined ? g[2] : g[3])}};
  const m = CMPQ.exec(t);
  if (!m) return null;
  return {{what: "prob", lane: m[1] || null, op: m[2] || m[5],
          val: parseFloat(m[3] !== undefined ? m[3] : m[4])}};
}}
function cmpOk(el, c) {{
  const d = el.dataset;
  const raw = c.what === "goals" ? d.goals
            : c.lane ? d["p" + c.lane]
            : (d.p1 !== undefined ? d.p1 : d.p);
  const v = parseFloat(raw);
  // No such lane, no number, no match — an absent lane is not "below 80".
  // The card's headline data-p doubles as a sort key and parks a card
  // with no numbers at −1, which must not read as a low probability.
  if (!isFinite(v) || v < 0) return false;
  return c.op === "<" ? v < c.val : c.op === "<=" ? v <= c.val
       : c.op === ">" ? v > c.val : v >= c.val;
}}

function qterms(q) {{
  return fold((q || "").toLowerCase()).split(",").map(s => s.trim())
    .filter(Boolean).map(s => {{
      const flat = s.replace(/\\s+/g, " ");
      const n = NONEQ.exec(flat);
      if (n) return "~none" + (n[1] || n[2]);
      return parseCmp(flat) || s;
    }});
}}
// A position opens the card it was struck from. The card is CLONED out of
// the board rather than rebuilt, so what the modal shows is the same lanes,
// numbers and marks the board is showing — a second renderer would drift
// from the first the day either one changed.
function showCard(btn) {{
  const name = btn.textContent.trim();
  const src = document.querySelector('.card[data-fx="' + name.replace(/"/g, "") + '"]');
  const box = document.getElementById("fxbox");
  if (!src || !box) return;
  const copy = src.cloneNode(true);
  copy.open = true;                 // the modal always shows the full card
  copy.style.display = "";          // never inherit a filter's hidden state
  const more = copy.querySelector(".more");
  if (more) more.remove();          // nothing left to expand in here
  box.innerHTML = "";
  box.appendChild(copy);
  document.getElementById("fxmodal").classList.add("on");
}}
function hideCard(ev) {{
  // A click on the card itself must not close the sheet under the cursor.
  if (ev && ev.target.closest && ev.target.closest(".fxbox")) return;
  document.getElementById("fxmodal").classList.remove("on");
}}
document.addEventListener("keydown", e => {{
  if (e.key === "Escape") hideCard();
}});

// One term against one element: a threshold, an exact league, or text.
function termOk(t, el, hay, lg) {{
  if (typeof t !== "string") return cmpOk(el, t);
  if (LEAGUES.has(t) && lg !== undefined) return lg === t;
  return hay.includes(t);
}}

function applyFilter(q) {{
  const terms = qterms(q);
  for (const c of document.querySelectorAll(".card,#t-bets tr[data-t]")) {{
    // Ask Athena renders bank cards with the same class into #ask-out,
    // and it has a search bar of its own. The two bars are separate
    // instruments: the board's filters the board, the ask bar filters
    // the bank answer. Without this guard the board bar reached across
    // and hid rows of an ask result that had nothing to do with it.
    // #fxbox holds a CLONE of a board card while the modal is open —
    // filtering it would hide the card the reader just asked for, and
    // counting it would score the same fixture twice.
    if (c.closest("#ask-out") || c.closest("#fxbox")) continue;
    const hay = c.dataset.t || "";
    const lg = c.dataset.lg;
    c.style.display = terms.every(t => termOk(t, c, hay, lg)) ? "" : "none";
  }}
  recount();
}}

// The filter bar's own scoreboard: every settled card carries its four
// grades, so whatever the filter leaves on screen — one league, one
// team, one rung — gets counted live. No filter means the whole run.
function recount() {{
  const box = document.getElementById("fcounts");
  if (!box) return;
  const t = {{gf: [0, 0], g1: [0, 0], g2: [0, 0], g3: [0, 0]}};
  for (const c of document.querySelectorAll(".card.done")) {{
    if (c.style.display === "none" || c.closest("#ask-out")
        || c.closest("#fxbox")) continue;
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
  // Which counter belongs to the tab in view: the four lane records on
  // Completed, the book's own record on Found bets, and nothing on the
  // two pending tabs, where there is nothing yet to score.
  const tab = (location.hash.split("/")[1] || "playable");
  const cap = document.getElementById("fcap");
  if (tab === "bets") {{
    let h = 0, n = 0;
    for (const r of document.querySelectorAll("#t-bets tr[data-g]")) {{
      if (r.style.display === "none") continue;
      n++; h += r.dataset.g === "1" ? 1 : 0;
    }}
    box.style.display = "";
    box.style.gridTemplateColumns = "1fr";
    box.innerHTML = '<div class="fc"><div class="v">' +
      (n ? (h / n * 100).toFixed(1) + "%" : "—") +
      '</div><div class="l">taken bets</div><div class="s">' +
      (n ? h + "/" + n + " settled" : "nothing settled in this filter") +
      "</div></div>";
    if (cap) cap.textContent = "your own positions, pushes counted as hits";
    return;
  }}
  if (tab !== "done") {{
    box.style.display = "none";
    if (cap) cap.textContent = "";
    return;
  }}
  box.style.display = "";
  box.style.gridTemplateColumns = "repeat(4,1fr)";
  box.innerHTML = cell("final pick", "gf") + cell("tip 1", "g1") +
                  cell("tip 2", "g2") + cell("tip 3", "g3");
  if (cap) cap.textContent = t.g1[1] + t.g2[1] + t.g3[1] === 0
    ? "no completed matches in this filter"
    : "every graded lane on screen, playable or not — the tiles above "
      + "keep the playable standard";
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
        const c = tr.cells[i];
        if (!c) return th.dataset.sort === "n" ? 0 : "";
        // A cell may carry its own sort value when what it SHOWS does not
        // order correctly — a kickoff reads "31-08" but must sort behind
        // "01-09", so the cell keeps the full stamp in data-v.
        const s = (c.dataset.v !== undefined ? c.dataset.v : c.textContent)
          .trim().replace(/[−–]/g, "-");
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
let BANKING = null;
async function ensureBank() {{
  if (BANK) return;
  // The bank is several megabytes, so it is fetched on first use rather
  // than on page load — and only once, however many controls ask for it.
  if (BANKING) return BANKING;
  const sel = document.getElementById("ask-lg");
  if (sel && sel.options.length <= 1) sel.options[0].textContent = "loading…";
  BANKING = (async () => {{
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
  if (sel) sel.options[0].textContent = "League…";
  refreshLeagues();
  }})();
  return BANKING;
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

// The same vocabulary the board's filter uses, so a bank result can be
// narrowed by league, country, lane kind, date or mark — and counted.
function askHay(m, comp) {{
  const bits = [m.h, m.a, comp.name, COUNTRY[comp.code] || "", m.d,
                m.tip, m.t2 || "", m.kw || ""];
  // The guard's words, the same ones the board bar takes: the label
  // (green / orange / red / super …), "guard x" and "label x" for the
  // case where a bare "red" also finds NY Red Bulls, "strong", and the
  // verdict where a closing price exists ("verdict play" / "verdict no
  // play"); "unpriced" for the cards no price reached.
  if (m.g) {{
    bits.push(m.g, "guard " + m.g, "label " + m.g);
    if (m.g.startsWith("super ")) bits.push("guard " + m.g.slice(6), "label " + m.g.slice(6));
  }}
  if (m.st) bits.push("strong");
  if (m.v) {{
    bits.push("verdict " + m.v);
    bits.push(m.v === "no play" ? "verdict no play" : "verdict play");
  }} else if (m.g) bits.push("unpriced");
  const rung = /(?:^|[^A-Za-z])([OU])(\d+(?:\.\d+)?)/.exec(m.tip);
  if (rung) {{
    const side = rung[1] === "O" ? "over" : "under";
    bits.push(side, "match " + side, "ft " + side, "fulltime " + side,
              "tip1 " + side, "tip 1 " + side,
              "tip1 " + rung[0].toLowerCase(), "tip 1 " + rung[0].toLowerCase());
  }}
  if (m.t2) {{
    const r2 = /(?:^|[^A-Za-z])([OU])(\d+(?:\.\d+)?)/.exec(m.t2);
    if (r2) {{
      const s2 = r2[1] === "O" ? "over" : "under";
      const k2 = m.t2.includes("(team)") ? "team" : "match";
      bits.push(k2 + " " + s2, "tip2 " + s2, "tip 2 " + s2);
      if (k2 === "match") bits.push("ft " + s2, "fulltime " + s2);
    }}
  }} else bits.push("~none2");    // see NONEQ: "tip 2 none"
  if (!m.tip) bits.push("~none1");
  if (m.t3) {{
    const l3 = /(1X|X2|12|DNB[12])/.exec(m.t3);
    bits.push("result lane");
    if (l3) bits.push(l3[1].startsWith("DNB") ? "draw no bet dnb"
                                             : "double chance",
                      "tip3 " + l3[1].toLowerCase(),
                      "tip 3 " + l3[1].toLowerCase());
  }} else bits.push("~none3");
  // "playable" is a board word; the bank rows carry the same edge in
  // their tip text, so the word means the same thing on both bars.
  const pe = /([+\-−]\d+(?:\.\d+)?)%\s*(?:\(|·|$)/.exec(m.tip || "");
  if (pe && parseFloat(pe[1].replace("−", "-")) > 1.0) bits.push("playable");
  bits.push(m.mark === "✅" ? "hit won" : m.mark === "❌" ? "miss lost"
            : m.mark === "◦" ? "push" : "pending");
  const raw = bits.join(" ").toLowerCase();
  const f = fold(raw);
  return (f === raw ? raw : raw + " " + f).replace(/"/g, "");
}}

function askCard(m, comp, note, open) {{
  const gm = k => ("✅◦❌".includes(m[k] || "") && m[k])
    ? (m[k] === "❌" ? "0" : "1") : null;
  const head = m.mark ? m.mark + " " + (m.score || "") :
    (m.src === "board" ? "🕑 on the board" : "");
  let body = (m.kw ? '<div class="kw">🧠 ' + m.kw + "</div>" : "")
    + '<div class="lane pl"><span class="which">Tip 1</span> '
    + m.tip.replaceAll(" · ", "<br>") + "</div>";
  if (m.t2) body += '<div class="lane"><span class="which">Tip 2</span> '
    + (gm("m2") !== null ? m.m2 + " " : "")
    + m.t2.replaceAll(" · ", "<br>") + "</div>";
  if (m.t3) body += '<div class="lane"><span class="which">Tip 3</span> '
    + (gm("m3") !== null ? m.m3 + " " : "")
    + m.t3.replaceAll(" · ", "<br>") + "</div>";
  let g = "";
  for (const [k, key] of [["mark", "g1"], ["m2", "g2"], ["m3", "g3"]])
    if (gm(k) !== null) g += " data-" + key + '="' + gm(k) + '"';
  // The guard on a past card: its label as a badge, and where a closing
  // price exists the verdict line a live card shows. NORMAL and STRONG
  // are counted on the STARRED lane's mark (pk: tip 1, or a gated DNB on
  // tip 3), only on cards the verdict said PLAY — so the two tiles read
  // "what following the board's plays would have scored", by kind.
  if (m.g) {{
    const star = m.pk === 3 ? gm("m3") : gm("mark");
    if (m.v && m.v !== "no play" && star !== null)
      g += ' data-g' + (m.v === "strong" ? "s" : "n") + '="' + star + '"';
    body = '<div class="guard g-' + m.g.replaceAll(" ", "-") + '">' + m.g
      + (m.st ? " · ★ strong" : "") + "</div>"
      + (m.v ? '<div class="verdict ' + (m.v === "no play" ? "no" : m.v === "strong" ? "strong" : "yes") + '">'
          + (m.v === "no play" ? "no play" : (m.v === "strong" ? "★ STRONG · PLAY" : "PLAY"))
          + ' <span class="dim">· tip ' + (m.pk || 1) + " needs " + m.need.toFixed(2)
          + ", closed at</span> <b>" + m.bp.toFixed(2) + "</b></div>"
        : '<div class="verdict dimv">unpriced <span class="dim">· no closing price for this league</span></div>')
      + body;
  }}
  // Each lane's claim, for the threshold terms ("tip 2 <80"). First
  // percentage in the cell is the probability; the rest is edge/margin.
  for (const [k, key] of [["tip", "p1"], ["t2", "p2"], ["t3", "p3"]]) {{
    const pm = /(\d+(?:\.\d+)?)%/.exec(m[k] || "");
    if (pm) g += " data-" + key + '="' + pm[1] + '"';
  }}
  // Total goals in the final result, for "goal >3".
  const sc = /^(\d+)\s*-\s*(\d+)$/.exec((m.score || "").trim());
  if (sc) g += ' data-goals="' + (+sc[1] + +sc[2]) + '"';
  // The lane marks on the summary line, so a long league list reads as a
  // scoreboard and only the card you open costs any space.
  const chips = [["1", m.mark], ["2", m.m2], ["3", m.m3]]
    .filter(x => x[1] && "✅◦❌".includes(x[1]))
    .map(x => '<span class="chip">' + x[1] + x[0] + "</span>").join("");
  return '<details class="card play"' + (open ? " open" : "")
    + ' data-t="' + askHay(m, comp) + '"' + g + "><summary>"
    + '<div class="teams">' + m.h + " v " + m.a
    + '<span class="more">more ▾</span></div>'
    + '<div class="meta">' + head + " · " + m.d + " · " + comp.name
    + (note ? " · " + note : "") + " " + chips + "</div>"
    + "</summary>" + body + "</details>";
}}

// Narrow what the bank returned, and score it. Only tip 1 is graded in
// the bank — the retrosim rows carry its mark and nothing else — so this
// counts the one lane it can honestly count.
function askFilter() {{
  const q = document.getElementById("ask-q");
  const terms = qterms(q ? q.value : "");
  const t = {{g1: [0, 0], g2: [0, 0], g3: [0, 0], gn: [0, 0], gs: [0, 0]}};
  let shown = 0;
  for (const c of document.querySelectorAll("#ask-out .card")) {{
    const hay = c.dataset.t || "";
    const vis = terms.every(x => termOk(x, c, hay, undefined));
    c.style.display = vis ? "" : "none";
    if (!vis) continue;
    shown++;
    for (const k of ["g1", "g2", "g3", "gn", "gs"]) {{
      const v = c.dataset[k];
      if (v === undefined) continue;
      t[k][1]++; t[k][0] += v === "1" ? 1 : 0;
    }}
  }}
  const box = document.getElementById("ask-counts");
  if (!box) return;
  const any = document.querySelector("#ask-out .card");
  box.style.display = any ? "" : "none";
  if (q) q.style.display = any ? "" : "none";
  const cell = (label, k) => {{
    const [hh, nn] = t[k];
    return '<div class="fc"><div class="v">' +
      (nn ? (hh / nn * 100).toFixed(1) + "%" : "—") +
      '</div><div class="l">' + label + '</div><div class="s">' +
      (nn ? hh + "/" + nn : "not in this slice") + "</div></div>";
  }};
  // NORMAL and STRONG: the starred lane on the cards the guard would
  // have marked PLAY at the closing price, by kind — the bettor's tiles
  // (2 Sep), in place of the bare match count.
  box.innerHTML = cell("tip 1", "g1") + cell("tip 2", "g2") +
    cell("tip 3", "g3") + cell("normal", "gn") + cell("★ strong", "gs");
}}
async function askAthena() {{
  await ensureBank();
  const out = document.getElementById("ask-out");
  const code = document.getElementById("ask-lg").value;
  const A = document.getElementById("ask-a").value.trim();
  const B = document.getElementById("ask-b").value.trim();
  const D = document.getElementById("ask-d").value;
  const wrap = h => '<div class="grid">' + h + "</div>";

  // A league alone answers with its whole bank; adding a date narrows to
  // that day. Either way the filter bar below can cut it further, which
  // is the point — the list is a working set, not a wall (the bettor's
  // ask, 31 Aug).
  if (!A && !B) {{
    if (!code) {{
      out.innerHTML = '<div class="askerr">Pick a league — on its own it '
        + "lists everything Athena has run there; add a date to narrow to "
        + "one day, or fill in both teams for a head-to-head.</div>";
      askFilter();
      return;
    }}
    const list = D ? BANK[code].matches.filter(m => m.d === D)
                   : BANK[code].matches.slice().sort(
                       (x, y) => y.d.localeCompare(x.d));
    out.innerHTML = list.length
      ? '<div class="dim" style="margin-bottom:6px">' + list.length
        + " match" + (list.length > 1 ? "es" : "") + " — " + BANK[code].name
        + (D ? " on " + D : ", newest first") + ":</div>"
        + wrap(list.map(m => askCard(m, BANK[code], "", list.length <= 6))
               .join(""))
      : '<div class="askerr">Athena has nothing for ' + BANK[code].name
        + (D ? " on " + D : "") + ".</div>";
    askFilter();
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
      out.innerHTML = wrap(exact.map(x => askCard(x[0], x[1], "", true))
                           .join(""));
      askFilter();
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
    + wrap(all.map(x => askCard(x[0], x[1], "", all.length <= 6))
             .join(""));
  askFilter();
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
