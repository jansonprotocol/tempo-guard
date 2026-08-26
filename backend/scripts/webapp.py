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
STAGE = "BETA STAGE 2"        # bumped at each stage transition, deliberately
SESSION_NO = 3                # bumped when a run closes and a new one opens
SESSION_START = "24 Aug"      # the reset date of the current run

# The archived eras: frozen history, recorded once (the numbers live in
# archive/*/log.md and the README's archive section; they never change).
SESSIONS = [
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


def _bets_rows() -> list[dict]:
    fixtures = ledger.read_fixtures()
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
        if len(parts) > 4 and parts[4] == "1":
            out.append(dict(mark="◦", name=name, lane=lane, odds=odds,
                            ret="1.00x", note=note))
            continue
        fx = fixtures.get(name)
        if fx is None or fx["hg"] is None:
            out.append(dict(mark="open", name=name, lane=lane, odds=odds,
                            ret="—", note=note))
            continue
        if rung == "DNB":
            gf, ga = ((fx["hg"], fx["ag"]) if side == "H"
                      else (fx["ag"], fx["hg"]))
            s = 1.0 if gf > ga else 0.0 if gf == ga else -1.0
        else:
            goals = (fx["hg"] + fx["ag"]) if side == "-" else (
                fx["hg"] if side == "H" else fx["ag"])
            s = ledger.pricing.settle_fraction(rung, goals)
        ret = max(s, 0.0) * odds + (1 - abs(s))
        out.append(dict(mark=MARK[s], name=name, lane=lane, odds=odds,
                        ret=f"{ret:.2f}x", note=note))
    return out


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

    def lane(which, cell):
        if cell.strip() in ("", "—", "— none"):
            return ""
        pl = " pl" if (not f.settled and f.lane(which)) else ""
        return (f'<div class="lane{pl}"><span class="which">Tip {which}'
                f"</span> {_fmt(cell)}</div>")

    read = reads.get(f"{f.code}|{f.teams}|{f.kickoff.split(' ')[0]}")
    kw = (f'<div class="kw">🧠 {html.escape(read[0])}</div>' if read else "")
    top = (f'<div class="teams">{html.escape(f.teams)}'
           f'<span class="more">more ▾</span></div>'
           f'<div class="meta">{head} · {league}</div>{kw}'
           f"{lane(1, f.tip1)}")
    body = lane(2, f.tip2)
    if read:
        body += f'<div class="read">{read[1]}</div>'
    if not body:
        body = '<div class="read dim">nothing more on this one</div>'
    return (f'<details class="card {kind}" '
            f'data-t="{html.escape(f.teams.lower())} '
            f'{html.escape(f.league.lower())} {f.code.lower()}">'
            f"<summary>{top}</summary>{body}</details>")


def _grid(cards, kind, reads):
    return ('<div class="grid">'
            + "".join(_card(f, kind, reads) for f in cards)
            + "</div>") if cards \
        else '<p class="dim">nothing here right now</p>'


def _hitrates_rows() -> str:
    rows = []
    for ln in (ROOT / "config" / "league_hitrates.tsv").read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        lg, n, hit, gap = ln.split("\t")
        cls = "pos" if not gap.startswith("-") and not gap.startswith("−") \
            else "neg"
        rows.append(f"<tr><td>{html.escape(lg)}</td><td>{hit}%</td>"
                    f'<td class="{cls}">{gap}</td><td class="dim">{n}</td>'
                    f"</tr>")
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


def _learn() -> str:
    """The teaching block: one example card, each part explained in a line."""
    card = """<details class="card play" open>
<summary><div class="teams">Real Madrid v Real Sociedad<span class="more">more \u25be</span></div>
<div class="meta">\U0001f551 26-08 21:00 \u00b7 LaLiga <span class="badge">(80.5 \u22120.1)</span></div>
<div class="kw">\U0001f9e0 elite attack vs leaky defence</div>
<div class="lane pl"><span class="which">Tip 1</span> O1.5 81.2% +6.8%<br>buy\u22651.33</div></summary>
<div class="lane pl"><span class="which">Tip 2</span> <b>Real Madrid O1.5</b> 67.0% +24.7% (team)<br>buy\u22651.62</div>
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
        ("(80.5 \u22120.1)", "the league's proven record: hitrate over its "
         "last 200 matches, replayed as-of, and the gap between what Athena "
         "claimed and what actually landed. Near zero = the engine tells "
         "the truth here."),
        ("\U0001f9e0 the read", "what Athena measured in this matchup, in "
         "keywords \u2014 tap the card for the full story in sentences."),
        ("Tip 1", "the engine's best market. O1.5 = over 1.5 goals in the "
         "match; 81.2% = claimed probability; +6.8% = edge over a typical "
         "match."),
        ("buy\u22651.33", "the minimum odds that make this tip worth "
         "money \u2014 margin included. Never buy below it; the margin IS "
         "the edge."),
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
            f'<button class="btn" onclick="scrollTo({{top:0,'
            f'behavior:\'smooth\'}})">\u2191 Back to top</button></div>')


def main() -> None:
    fixtures = board.load()
    t, p = board._tallies(fixtures)
    (h1, n1), _ = t[1], t[2]
    (p1, q1), _ = p[1], p[2]
    bh, bn, roi = headline.bets()

    reads = _reads(fixtures)
    pending = [f for f in fixtures if not f.settled]
    playable = [f for f in pending if f.lane(1) or f.lane(2)]
    waiting = [f for f in pending if f not in playable]
    done = [f for f in fixtures if f.settled][::-1]

    def tile(label, value, sub):
        return (f'<div class="tile"><div class="v">{value}</div>'
                f'<div class="l">{label}</div><div class="s">{sub}</div></div>')

    tiles = "".join([
        tile("confirmed hitrate", f"{h1 / n1 * 100:.1f}%" if n1 else "—",
             f"Tip 1 · {h1}/{n1} settled"),
        tile("played lanes &gt;+1%", f"{p1 / q1 * 100:.1f}%" if q1 else "—",
             f"Tip 1 · {p1}/{q1}"),
        tile("found bets", f"{roi:+.1f}%", f"ROI · {bh}/{bn} settled"),
        tile("pending", str(len(pending)),
             f"{len(playable)} playable"),
    ])

    bets_html = "".join(
        f'<tr><td class="mk">{b["mark"]}</td><td>{html.escape(b["name"])}'
        f'</td><td>{html.escape(b["lane"])}</td><td>{b["odds"]:.2f}</td>'
        f'<td>{b["ret"]}</td><td class="note">{html.escape(b["note"])}</td>'
        f"</tr>" for b in _bets_rows())

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
    for comp in bank.values():
        comp["teams"] = sorted(set(comp["teams"]))
        comp["matches"].sort(key=lambda m: m["d"])
    (OUT.parent / "matchbank.json").write_text(
        _json.dumps(bank, ensure_ascii=False))

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{TITLE}</title>
<link rel="icon" href="athena-logo.png">
<style>
:root {{ --bg:#0e1116; --card:#161b24; --edge:#232a36; --tx:#dbe2ee;
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
.ask {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:12px 14px; margin:0 0 12px; font-size:13px; }}
.askrow {{ display:grid; gap:8px; margin-top:10px;
  grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); }}
.askrow input, .askrow select {{ background:#111622; border:1px solid
  var(--edge); border-radius:8px; color:var(--tx); padding:8px 10px;
  font:inherit; font-size:13px; }}
.askbtn {{ margin:0; }}
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
.lane .which {{ color:var(--dim); font-size:10px; text-transform:uppercase;
  letter-spacing:.1em; margin-right:6px; }}
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
.session {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:14px 16px; margin-bottom:12px; }}
.session ul {{ margin:4px 0 2px 18px; color:var(--dim); }}
.about p {{ margin:10px 0; max-width:74ch; }}
.about .mission {{ font-size:18px; font-weight:700; color:var(--gold);
  margin:14px 0; }}
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
 <div class="session">SESSION #{SESSION_NO} · {SESSION_START} – {session_end}</div>
 <div class="tiles">{tiles}</div>
 <div class="ask">
  <b>🔎 Ask Athena</b> <span class="dim">— look up any matchup it has run;
  past matches are a retrosim (each competition's last ~200 games).</span>
  <div class="askrow">
   <input type="date" id="ask-d">
   <select id="ask-lg"><option value="">League…</option></select>
   <input id="ask-a" list="dl-a" placeholder="Team A (home)" disabled>
   <input id="ask-b" list="dl-b" placeholder="Team B (away)" disabled>
   <button class="btn askbtn" onclick="askAthena()">Enter</button>
  </div>
  <datalist id="dl-a"></datalist><datalist id="dl-b"></datalist>
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
  oninput="for(const c of document.querySelectorAll('.card'))
  c.style.display=c.dataset.t.includes(this.value.toLowerCase())?'':'none'">
 <div class="tabpane" id="t-playable">{_grid(playable, "play", reads)}</div>
 <div class="tabpane" id="t-bets"><div class="wrap"><table>
  <tr><th>·</th><th>Fixture</th><th>Lane</th><th>Odds</th><th>Return</th>
  <th>Note</th></tr>{bets_html}</table></div></div>
 <div class="tabpane" id="t-lanes">{_grid(waiting, "pend", reads)}</div>
 <div class="tabpane" id="t-done">{_grid(done, "done", reads)}</div>
 {_learn()}
</section>

<section class="page" id="p-sessions">
 <h2>Past sessions</h2>
 <p class="dim">Every era is archived whole and never edited — the numbers
 below are how each run actually ended.</p>
 {sessions_html}
</section>

<section class="page" id="p-retrosim">
 <h2>Retrosim confirmed hitrates</h2>
 <p class="dim">Every league's Tip 1, replayed as-of over its 200 most
 recent matches on the current build. <b>hit</b> is what landed;
 <b>gap</b> is hit minus what the engine claimed — near zero means the
 engine tells the truth about itself. Cup lanes use debited
 probabilities.</p>
 <div class="wrap"><table><tr><th>League</th><th>Hit</th><th>Gap</th>
 <th>n</th></tr>{_hitrates_rows()}</table></div>
</section>

<section class="page" id="p-patches">
 <h2>Patchlist &amp; notes</h2>
 <p class="dim">What changed and why, one line each. The full evidence
 behind every line lives in the repository's README and scripts.</p>
 <div class="wrap"><table><tr><th>Date</th><th>Area</th><th>Change</th>
 </tr>{_patch_rows()}</table></div>
</section>

<section class="page about" id="p-about">
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
 <p><b>How it stays honest:</b> every constant in the engine must prove
 itself on two separate time windows before it ships; every era is
 archived untouched, including the ones that lost money; new lanes (like
 the international cups) run <b>probationary</b> until live results
 confirm the backtests. When the engine doesn't know, it says nothing —
 an abstained match is an answer, not a failure.</p>
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
addEventListener("hashchange", route); route();

let BANK = null, LOOKUP = null;
const norm = s => s.normalize("NFD").replace(/[\u0300-\u036f]/g, "")
  .toLowerCase().replace(/[.\-'()\/]/g, " ").split(/\s+/)
  .filter(w => w && !["fc","fk","cf","sc","ac","afc","bk","if","sk",
                      "club","cp"].includes(w)).join(" ");
let DATES = null;
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
  if (sel.value !== keep) sel.dispatchEvent(new Event("change"));
}}
async function ensureBank() {{
  if (BANK) return;
  BANK = await (await fetch("matchbank.json")).json();
  LOOKUP = {{}}; DATES = {{}};
  for (const [code, comp] of Object.entries(BANK)) {{
    DATES[code] = new Set();
    for (const m of comp.matches) {{
      const k = code + "|" + norm(m.h) + "|" + norm(m.a);
      (LOOKUP[k] = LOOKUP[k] || []).push(m);
      DATES[code].add(m.d);
    }}
  }}
  refreshLeagues();
}}
document.getElementById("ask-d").addEventListener("change",
  async () => {{ await ensureBank(); refreshLeagues(); }});
document.getElementById("ask-lg").addEventListener("focus", ensureBank);
document.getElementById("ask-lg").addEventListener("change", e => {{
  const comp = BANK && BANK[e.target.value];
  for (const id of ["dl-a", "dl-b"]) {{
    const dl = document.getElementById(id); dl.innerHTML = "";
    if (comp) for (const tm of comp.teams) {{
      const o = document.createElement("option");
      o.value = tm; dl.appendChild(o);
    }}
  }}
  for (const id of ["ask-a", "ask-b"]) {{
    const inp = document.getElementById(id);
    inp.disabled = !comp; inp.value = "";
  }}
}});
function askCard(m, comp, note) {{
  const mark = m.mark ? m.mark + " " + (m.score || "") :
    (m.src === "board" ? "🕑 on the board" : "");
  let body = "";
  if (m.t2) body += '<div class="lane"><span class="which">Tip 2</span> '
    + m.t2.replaceAll(" · ", "<br>") + "</div>";
  return '<div class="grid"><div class="card play"><div class="teams">'
    + m.h + " v " + m.a + '</div><div class="meta">' + mark + " · "
    + m.d + " · " + comp.name + (note ? " · " + note : "") + "</div>"
    + (m.kw ? '<div class="kw">🧠 ' + m.kw + "</div>" : "")
    + '<div class="lane pl"><span class="which">Tip 1</span> '
    + m.tip.replaceAll(" · ", "<br>") + "</div>" + body + "</div></div>";
}}
async function askAthena() {{
  await ensureBank();
  const out = document.getElementById("ask-out");
  const code = document.getElementById("ask-lg").value;
  const A = document.getElementById("ask-a").value.trim();
  const B = document.getElementById("ask-b").value.trim();
  const D = document.getElementById("ask-d").value;
  if (!code || !A || !B) {{
    out.innerHTML = '<div class="askerr">Pick a league and both teams '
      + "first.</div>"; return;
  }}
  const comp = BANK[code];
  let hits = LOOKUP[code + "|" + norm(A) + "|" + norm(B)] || [];
  let note = "";
  if (!hits.length) {{
    hits = LOOKUP[code + "|" + norm(B) + "|" + norm(A)] || [];
    if (hits.length) note = "shown home-first, as it was played";
  }}
  if (!hits.length) {{
    out.innerHTML = '<div class="askerr">Athena has not run this matchup. '
      + "The board carries what the operator feeds in, and past matches "
      + "cover roughly each competition's last 200 games — try the "
      + "suggestions while typing, or the reverse fixture.</div>";
    return;
  }}
  let show = hits;
  if (D) {{
    const exact = hits.filter(m => m.d === D);
    if (exact.length) show = exact;
    else note = "not on " + D + " — showing the date(s) Athena ran it";
  }}
  out.innerHTML = show.slice(-3).map(m => askCard(m, comp, note)).join("");
}}
</script>
</body></html>"""
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(page)
    print(f"web app rendered: {OUT.relative_to(OUT.parents[2])} "
          f"({len(page) // 1024}KB)")


if __name__ == "__main__":
    main()
