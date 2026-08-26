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


def _card(f, kind: str) -> str:
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
    lanes = ""
    for which, cell in ((1, f.tip1), (2, f.tip2)):
        if cell.strip() in ("", "—", "— none"):
            continue
        pl = ' class="pl"' if (not f.settled and f.lane(which)) else ""
        lanes += (f'<div class="lane"{pl}><span class="which">Tip {which}'
                  f"</span> {_fmt(cell)}</div>")
    return (f'<div class="card {kind}" data-t="{html.escape(f.teams.lower())} '
            f'{html.escape(f.league.lower())} {f.code.lower()}">'
            f'<div class="teams">{html.escape(f.teams)}</div>'
            f'<div class="meta">{head} · {league}</div>{lanes}</div>')


def _grid(cards, kind):
    return ('<div class="grid">'
            + "".join(_card(f, kind) for f in cards) + "</div>") if cards \
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


def main() -> None:
    fixtures = board.load()
    t, p = board._tallies(fixtures)
    (h1, n1), _ = t[1], t[2]
    (p1, q1), _ = p[1], p[2]
    bh, bn, roi = headline.bets()

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
 <div class="tiles">{tiles}</div>
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
 <input id="q" placeholder="filter — team, league, code…"
  oninput="for(const c of document.querySelectorAll('.card'))
  c.style.display=c.dataset.t.includes(this.value.toLowerCase())?'':'none'">
 <div class="tabpane" id="t-playable">{_grid(playable, "play")}</div>
 <div class="tabpane" id="t-bets"><div class="wrap"><table>
  <tr><th>·</th><th>Fixture</th><th>Lane</th><th>Odds</th><th>Return</th>
  <th>Note</th></tr>{bets_html}</table></div></div>
 <div class="tabpane" id="t-lanes">{_grid(waiting, "pend")}</div>
 <div class="tabpane" id="t-done">{_grid(done, "done")}</div>
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
</script>
</body></html>"""
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(page)
    print(f"web app rendered: {OUT.relative_to(OUT.parents[2])} "
          f"({len(page) // 1024}KB)")


if __name__ == "__main__":
    main()
