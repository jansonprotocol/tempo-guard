"""
The board as a web app: one self-contained page, derived like everything.

Same doctrine as the README — fixtures.tsv, bets.tsv and league_hitrates.tsv
are the only typed sources, and this renders them; nothing on the page is
hand-written. `board.py` calls this at the end of every render, so the app
and the README can never disagree. The output is a single static file with
inline CSS/JS: no build step, no framework, no server — any static host
(Vercel with root directory `web/`, GitHub Pages) serves it as-is.

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

OUT = Path(__file__).resolve().parents[2] / "web" / "index.html"
TITLE = "ATHENA — TEMPO GUARD"
STAGE = "BETA STAGE 2"        # bumped at each stage transition, deliberately


def _fmt(cell: str) -> str:
    """A tip cell to HTML: escape, bold, and break at the price dot."""
    s = html.escape(cell)
    s = re.sub(r"\*\*(.+?)\*\*", r"<b>\1</b>", s)
    return s.replace(" · ", "<br>")


def _bets_rows() -> list[dict]:
    """The placed-bets table, settled exactly as board.render_bets does."""
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


def main() -> None:
    fixtures = board.load()
    t, p = board._tallies(fixtures)
    (h1, n1), (h2, n2) = t[1], t[2]
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
        tile("placed bets", f"{roi:+.1f}%", f"ROI · {bh}/{bn} settled"),
        tile("pending", str(len(pending)),
             f"{len(playable)} playable lanes"),
    ])

    def section(title, glyph, cards, kind):
        if not cards:
            return ""
        inner = "".join(_card(f, kind) for f in cards)
        return (f'<h2>{glyph} {title} <span class="n">{len(cards)}</span>'
                f'</h2><div class="grid">{inner}</div>')

    bets_html = "".join(
        f'<tr><td class="mk">{b["mark"]}</td><td>{html.escape(b["name"])}'
        f'</td><td>{html.escape(b["lane"])}</td><td>{b["odds"]:.2f}</td>'
        f'<td>{b["ret"]}</td><td class="note">{html.escape(b["note"])}</td>'
        f"</tr>" for b in _bets_rows())

    badges = sorted(((k, v) for k, v in rates().items()),
                    key=lambda kv: -float(kv[1].strip("()").split()[0]))
    badge_html = "".join(
        f"<tr><td>{html.escape(k)}</td><td>{html.escape(v.strip('()'))}"
        f"</td></tr>" for k, v in badges)

    page = f"""<!doctype html>
<html lang="en"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>{TITLE}</title>
<style>
:root {{ --bg:#0e1116; --card:#161b24; --edge:#232a36; --tx:#dbe2ee;
  --dim:#8b95a7; --green:#37c26b; --blue:#4f8ef7; --gold:#e8b93c; }}
* {{ box-sizing:border-box; margin:0; }}
body {{ background:var(--bg); color:var(--tx); font:15px/1.45 system-ui,
  -apple-system,"Segoe UI",Roboto,sans-serif; padding:16px; max-width:1200px;
  margin:0 auto; }}
header h1 {{ font-size:20px; letter-spacing:.06em; }}
header .stage {{ color:var(--gold); font-size:12px; letter-spacing:.2em; }}
.tiles {{ display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr));
  gap:10px; margin:16px 0; }}
.tile {{ background:var(--card); border:1px solid var(--edge);
  border-radius:10px; padding:12px 14px; }}
.tile .v {{ font-size:24px; font-weight:700; }}
.tile .l {{ color:var(--dim); font-size:11px; text-transform:uppercase;
  letter-spacing:.08em; margin-top:2px; }}
.tile .s {{ color:var(--dim); font-size:12px; }}
input#q {{ width:100%; background:var(--card); border:1px solid var(--edge);
  border-radius:8px; color:var(--tx); padding:9px 12px; margin:4px 0 8px; }}
h2 {{ font-size:15px; margin:20px 0 10px; letter-spacing:.03em; }}
h2 .n {{ color:var(--dim); font-weight:400; }}
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
td {{ padding:7px 9px; border-top:1px solid var(--edge);
  vertical-align:top; }}
td.mk {{ white-space:nowrap; }}
td.note {{ color:var(--dim); }}
details {{ margin-top:18px; }} summary {{ cursor:pointer; }}
.wrap {{ overflow-x:auto; }}
footer {{ color:var(--dim); font-size:12px; margin:26px 0 8px; }}
</style></head><body>
<header><h1>{TITLE}</h1><div class="stage">{STAGE}</div></header>
<div class="tiles">{tiles}</div>
<input id="q" placeholder="filter — team, league, code…"
 oninput="for(const c of document.querySelectorAll('.card'))
 c.style.display=c.dataset.t.includes(this.value.toLowerCase())?'':'none'">
{section("Playable lanes — edge above +1%", "🟢", playable, "play")}
{section("Pending", "🔵", waiting, "pend")}
{section("Completed", "⚪", done, "done")}
<h2>🟡 Placed bets <span class="n">{bh}/{bn} settled · ROI {roi:+.1f}%</span></h2>
<div class="wrap"><table><tr><td class="mk"><b>·</b></td><td><b>Fixture</b>
</td><td><b>Lane</b></td><td><b>Odds</b></td><td><b>Return</b></td>
<td><b>Note</b></td></tr>{bets_html}</table></div>
<details><summary>League track records — 200-match retrosim (hit, gap)
</summary><div class="wrap"><table>{badge_html}</table></div></details>
<footer>Derived from config/fixtures.tsv · bets.tsv · league_hitrates.tsv —
nothing on this page is typed by hand. Cup lanes are probationary.</footer>
</body></html>"""
    OUT.parent.mkdir(exist_ok=True)
    OUT.write_text(page)
    print(f"web app rendered: {OUT.relative_to(OUT.parents[2])} "
          f"({len(page) // 1024}KB)")


if __name__ == "__main__":
    main()
