"""
One update is never one match: sweep every unsettled fixture, then render.

The failure this exists to prevent is a human one. A score arrives for one
match, that row gets updated, and three others that finished in the same
hour sit stale on the board — settled bets still showing as open, an
`O1.5` that landed still reading "needs 1 more". So there is no
per-fixture update any more: any refresh sweeps the whole board.

For every fixture not already graded, this asks ESPN for its state and:

    in play    writes `LIVE 63' 2-1`, which drives the live lane states
    finished   grades it — Tip 1 into the status, Tip 2 into its own cell,
               using the ledger's settlement so a push is a push
    untipped   records `FT 2-1 (no tip)` so the row stops looking pending

Extra time is handled where it matters: every market here settles on the
90, so a match ending AET is graded from its goal timeline, counting only
goals scored inside regulation. Sabah 5-2 was 3-2 at the whistle, and the
board says 3-2. And a match still IN extra time settles immediately — the
90 is already history the moment ET kicks off, so the row moves to
completed rather than sitting live over a decided bet.

Nothing is guessed. A fixture ESPN does not carry is left exactly as it
was and named in the summary, so it can be graded by hand.

Usage:  python scripts/sweep.py            sweep, then render the board
        python scripts/sweep.py --dry      show what would change
        python scripts/sweep.py --set "Home v Away" 2-1
                                           grade ONE kicked-off fixture by
                                           hand, for the leagues no feed
                                           carries (Swiss, Polish, Algerian)
"""
from __future__ import annotations

import json
import re
import sys
import urllib.request
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import liveline
from scripts.board import FIXTURES, load
from scripts.live_scores import ESPN

# Cup codes ESPN serves under their own slugs; the qualifiers share the
# board's own codes, so both live here rather than in two places.
#
# ALG-L1 is deliberately absent: ESPN carries no Algerian league at any
# slug (400 on every one tried), so a mapping there would only pretend to
# have looked. Its fixtures come back named for grading by hand.
SLUGS = dict(ESPN, **{
    "UEL-Q": "uefa.europa_qual", "UECL-Q": "uefa.europa.conf_qual",
    "UCL": "uefa.champions", "UEL": "uefa.europa",
    "UECL": "uefa.europa.conf", "NOR-EL": "nor.1",
    "SCO-PL": "sco.1", "GER-BL": "ger.1", "BEL-PL": "bel.1",
    "NED-ED": "ned.1", "ARG-PD": "arg.1", "MEX-LMX": "mex.1",
})


def _get(url: str):
    # One retry after a short pause: on a busy Saturday ESPN drops the odd
    # request, and a single failed fetch used to silently blank a whole
    # league for the pass — the 16:00 Championship wall one sweep, the
    # Bundesliga the next — misread as "not on ESPN".
    import time
    for attempt in (0, 1):
        try:
            with urllib.request.urlopen(url, timeout=15) as r:
                return json.load(r)
        except Exception:
            if attempt == 0:
                time.sleep(1.5)
    return None


def board_day(code: str, day: str) -> list[dict]:
    """ESPN's events for one competition around one board day.

    ESPN files a fixture under the COMPETITION's local calendar day; the
    board keeps kickoffs in European time. For Europe the two agree, but a
    Chilean 18:00 is our next midnight and a Saudi late game is our
    previous night — Coquimbo v U. Católica sat on `dates=20260826` while
    the board called it the 27th, and the sweep saw nothing there. So the
    neighbouring days are asked for too and the answers merged; a fixture
    is then found by its clubs, whichever bucket ESPN filed it in.
    """
    slug = SLUGS.get(code)
    if not slug:
        return []
    d0 = datetime.strptime(day, "%Y-%m-%d").date()
    out, seen = [], set()
    for off in (-1, 0, 1):
        d = (d0 + timedelta(days=off)).strftime("%Y%m%d")
        data = _get(f"https://site.api.espn.com/apis/site/v2/sports/soccer/"
                    f"{slug}/scoreboard?dates={d}")
        for ev in (data or {}).get("events", []):
            if ev["id"] not in seen:
                seen.add(ev["id"])
                out.append(ev)
    return out


def regulation(slug: str, ev: dict) -> tuple[int, int] | None:
    """The 90-minute score of a match that went to extra time.

    Every market on this board settles on the 90 (stoppage time
    included), so a 5-1 after extra time is a 4-1 for our purposes.

    Read from the RUNNING SCORE in the goal narration, never by counting
    goal events: ESPN's keyEvents feed drops events (Plzen's 2-1 and 4-1
    goals were simply absent), and a count of an incomplete list graded
    Plzen 2-1 when the 90 ended 4-1 — flipping two ledger positions the
    wrong way. Each "Goal! Home X, Away Y." line carries the cumulative
    score, so the last such line at minute <= 90 IS the regulation score
    even when earlier lines are missing. The commentary feed (complete)
    is preferred; keyEvents is the fallback. None when neither carries a
    goal line — a reason to abstain, not to guess.
    """
    data = _get(f"https://site.api.espn.com/apis/site/v2/sports/soccer/"
                f"{slug}/summary?event={ev['id']}")
    if not data:
        return None
    import re as _re

    def _score(items, minute_of, text_of):
        best, best_min = None, -1
        for it in items or []:
            text = text_of(it) or ""
            if not text.startswith("Goal!"):
                continue
            clock = minute_of(it) or ""
            try:
                minute = int(clock.split("'")[0].split("+")[0] or 0)
            except ValueError:
                continue
            if minute > 90:      # "90'+5'" parses as 90 and stays IN
                continue
            m = _re.search(r"(\d+), .*?(\d+)\.", text)
            if m and minute >= best_min:
                best, best_min = (int(m.group(1)), int(m.group(2))), minute
        return best

    got = _score(data.get("commentary"),
                 lambda c: (c.get("time") or {}).get("displayValue"),
                 lambda c: c.get("text"))
    if got is None:
        got = _score(data.get("keyEvents"),
                     lambda k: (k.get("clock") or {}).get("displayValue"),
                     lambda k: k.get("text"))
    if got is None:
        # A feed with entries but no pre-90 goal line is a goalless
        # regulation (Inter Escaldes 0-0, decided in ET), not an unknown.
        if data.get("commentary") or data.get("keyEvents"):
            return 0, 0
        return None
    # The narration names the HOME side first on ESPN soccer feeds.
    return got


def settle(cell: str, teams: str, hg: int, ag: int):
    """(mark, fraction) for one lane at a final score, or None."""
    from app.engine import pricing
    got = liveline._goals_for(cell, teams, hg, ag)
    if got is None:
        return None
    market, goals = got
    try:
        s = pricing.settle_fraction(market, goals)
    except (ValueError, IndexError):
        return None
    return ("✅" if s > 0 else "◦" if s == 0 else "❌"), s


def grade_cells(f, hg: int, ag: int, note: str = "") -> tuple[str, str, str]:
    """(tip2, status, tip3) for one fixture at a final score.

    The ONE place a result becomes marks on a row, whether the score came
    off ESPN or was typed by hand — the same settle() for the totals and
    the same result_market for the result lane, so a hand-graded row can
    never carry a mark the sweep would not have written.
    """
    from app.engine import result_market
    got1 = settle(f.tip1, f.teams, hg, ag)
    tip2, tip3 = f.tip2, f.tip3
    got2 = settle(f.tip2, f.teams, hg, ag)
    if got2 and not tip2.lstrip().startswith(("✅", "❌", "◦")):
        tip2 = f"{got2[0]} {tip2}"
    # Tip 3 settles on the RESULT, not the total — a DNB draw is
    # the one push in the family.
    if tip3.strip() and not tip3.lstrip().startswith(("✅", "❌", "◦")):
        try:
            g3 = result_market.won(tip3.split()[0], hg, ag)
            tip3 = ("◦ " if g3 is None else "✅ " if g3 else "❌ ") + tip3
        except (ValueError, IndexError):
            pass
    if got1 is None:
        status = f"FT {hg}-{ag} (no tip)"
    else:
        word = ("HIT" if got1[1] > 0 else "PUSH" if got1[1] == 0 else "MISS")
        status = f"{got1[0]} {word} — {hg}-{ag}{note}"
    return tip2, status, tip3


def _write_row(lines: list[str], f, tip2: str, status: str, tip3: str) -> bool:
    for i, ln in enumerate(lines):
        parts = ln.split("\t")
        if len(parts) in (7, 8) and parts[0] == f.kickoff and parts[3] == f.teams:
            parts[5], parts[6] = tip2, status
            if len(parts) == 8:
                parts[7] = tip3
            lines[i] = "\t".join(parts)
            return True
    return False


def set_result(teams: str, score: str) -> None:
    """Grade ONE unsettled fixture by hand: --set "Home v Away" 2-4.

    For the leagues no feed carries. ESPN's Swiss slug answers with the
    league's name and zero events on every date asked; Poland and
    Algeria have no slug at all. Sixteen pending cards sit in those
    three, and the board's verify step refuses a card four hours past
    kickoff with no result — correctly — which until now meant a
    scratchpad script or a mark typed into the row by hand. Same
    grader as the sweep, same render, same verify.
    """
    m = re.fullmatch(r"\s*(\d+)\s*-\s*(\d+)\s*", score)
    if not m:
        raise SystemExit(f"score must look like 2-1, got {score!r}")
    hg, ag = int(m.group(1)), int(m.group(2))
    hits = [f for f in load() if f.teams == teams and not f.settled]
    if len(hits) != 1:
        raise SystemExit(f"{len(hits)} unsettled rows match {teams!r}; "
                         "type the fixture exactly as the board prints it")
    f = hits[0]
    if not odds_started(f.kickoff):
        raise SystemExit(f"{teams} has not kicked off yet ({f.kickoff})")
    tip2, status, tip3 = grade_cells(f, hg, ag)
    lines = FIXTURES.read_text().split("\n")
    if not _write_row(lines, f, tip2, status, tip3):
        raise SystemExit(f"row for {teams} not found in {FIXTURES}")
    FIXTURES.write_text("\n".join(lines))
    print(f"set {f.teams}: {status}")
    from scripts import board
    board.main()


def odds_started(kickoff: str) -> bool:
    from scripts.odds_api import started
    return started(kickoff)


def main() -> None:
    if "--set" in sys.argv:
        i = sys.argv.index("--set")
        try:
            set_result(sys.argv[i + 1], sys.argv[i + 2])
        except IndexError:
            raise SystemExit('usage: sweep.py --set "Home v Away" 2-1')
        return
    dry = "--dry" in sys.argv
    fixtures = load()
    todo = [f for f in fixtures if not f.settled]
    if not todo:
        print("nothing unsettled on the board")
        return

    cache: dict[tuple[str, str], list] = {}
    lines = FIXTURES.read_text().split("\n")
    changed, missing, still = [], [], []

    # Rows settled mid-ET carry "(90'; to extra time)" with no final —
    # once ESPN marks the match finished, the note gains the AET score,
    # so the board tells the whole story: the 90 it settled on AND how
    # the tie actually ended. The MARK never changes here; only the note.
    finals = [f for f in fixtures
              if f.settled and f.status.endswith("(90'; to extra time)")]
    for f in finals:
        day = f.kickoff.split(" ")[0]
        key = (f.code, day)
        if key not in cache:
            cache[key] = board_day(f.code, day)
        home, away = (x.strip() for x in f.teams.split(" v ", 1))
        for cand in cache[key]:
            comp = cand["competitions"][0]
            ch = next(x for x in comp["competitors"] if x["homeAway"] == "home")
            ca = next(x for x in comp["competitors"] if x["homeAway"] == "away")
            if not (liveline.same_club(home, ch["team"]["displayName"])
                    and liveline.same_club(away, ca["team"]["displayName"])):
                continue
            if cand["status"]["type"].get("state") == "post":
                fin = f"{int(ch.get('score') or 0)}-{int(ca.get('score') or 0)}"
                status = f.status.replace("(90'; to extra time)",
                                          f"(90'; {fin} aet)")
                for i, ln in enumerate(lines):
                    parts = ln.split("\t")
                    if len(parts) in (7, 8) and parts[0] == f.kickoff \
                            and parts[3] == f.teams:
                        parts[6] = status
                        lines[i] = "\t".join(parts)
                        break
                changed.append(f"{f.teams}: {status}")
            break

    for f in todo:
        day = f.kickoff.split(" ")[0]
        key = (f.code, day)
        if key not in cache:
            cache[key] = board_day(f.code, day)
        home, away = ((x.strip() for x in f.teams.split(" v ", 1))
                      if " v " in f.teams else (f.teams, ""))
        ev = None
        for cand in cache[key]:
            comp = cand["competitions"][0]
            ch = next(x for x in comp["competitors"] if x["homeAway"] == "home")
            ca = next(x for x in comp["competitors"] if x["homeAway"] == "away")
            if (liveline.same_club(home, ch["team"]["displayName"])
                    and liveline.same_club(away, ca["team"]["displayName"])):
                ev = cand
                break
        if ev is None:
            missing.append(f.teams)
            continue

        st = ev["status"]["type"]
        comp = ev["competitions"][0]
        ch = next(x for x in comp["competitors"] if x["homeAway"] == "home")
        ca = next(x for x in comp["competitors"] if x["homeAway"] == "away")
        hg, ag = int(ch.get("score") or 0), int(ca.get("score") or 0)
        detail = st.get("shortDetail", "")
        tip3 = f.tip3

        if st.get("state") == "pre":
            still.append(f.teams)
            continue

        in_play = st.get("state") == "in"
        # HARDCODED: a match in extra time is settled NOW. Every market on
        # this board settles on the 90, and once ET has kicked off the 90
        # is history — holding the row open only makes a decided bet look
        # live. ESPN marks ET as STATUS_OVERTIME / period 3+, and the
        # regulation score comes from the goal timeline, same as AET.
        in_et = in_play and ((ev["status"].get("period") or 0) >= 3
                             or "OVERTIME" in (st.get("name") or ""))
        if in_play and not in_et:
            status = f"LIVE {detail} {hg}-{ag}"
            tip2 = f.tip2
        else:
            # Finished (or in extra time, which settles the same way).
            # Markets settle on the 90, so anything after it is peeled
            # back off the scoreline before grading.
            note = ""
            if in_et:
                reg = regulation(SLUGS[f.code], ev)
                if reg is None:
                    # no timeline yet — leave it visibly live instead
                    status = f"LIVE {detail} {hg}-{ag}"
                    tip2 = f.tip2
                    if status != f.status:
                        for i, ln in enumerate(lines):
                            parts = ln.split("\t")
                            if len(parts) in (7, 8) and parts[0] == f.kickoff \
                                    and parts[3] == f.teams:
                                parts[6] = status
                                lines[i] = "\t".join(parts)
                                break
                        changed.append(f"{f.teams}: {status}")
                    continue
                note = " (90'; to extra time)"
                hg, ag = reg
            elif "AET" in detail or "PEN" in detail or "ET" == detail:
                reg = regulation(SLUGS[f.code], ev)
                if reg is None:
                    missing.append(f"{f.teams} (finished AET, no timeline)")
                    continue
                note = f" (90'; {hg}-{ag} aet)"
                hg, ag = reg
            tip2, status, tip3 = grade_cells(f, hg, ag, note)

        if status == f.status and tip2 == f.tip2 and tip3 == f.tip3:
            continue
        _write_row(lines, f, tip2, status, tip3)
        changed.append(f"{f.teams}: {status}")

    for c in changed:
        print(("would set " if dry else "set ") + c)
    if missing:
        print("not on ESPN (left untouched): " + ", ".join(missing))
    print(f"{len(changed)} updated · {len(still)} not started · "
          f"{len(todo)} unsettled checked")

    if changed and not dry:
        FIXTURES.write_text("\n".join(lines))
        from scripts import board
        board.main()


if __name__ == "__main__":
    main()
