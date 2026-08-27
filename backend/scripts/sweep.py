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
"""
from __future__ import annotations

import json
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
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return json.load(r)
    except Exception:
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

    Every market on this board settles on the 90, so a 5-2 after extra
    time is a 3-2 for our purposes. Counted from the goal timeline; None
    when the timeline is unavailable, which is a reason to abstain rather
    than to guess.
    """
    data = _get(f"https://site.api.espn.com/apis/site/v2/sports/soccer/"
                f"{slug}/summary?event={ev['id']}")
    if not data:
        return None
    comp = ev["competitions"][0]
    home = next(x for x in comp["competitors"] if x["homeAway"] == "home")
    away = next(x for x in comp["competitors"] if x["homeAway"] == "away")
    hg = ag = 0
    for k in data.get("keyEvents") or []:
        if "Goal" not in (k.get("type") or {}).get("text", ""):
            continue
        clock = (k.get("clock") or {}).get("displayValue", "")
        minute = int(clock.split("'")[0].split("+")[0] or 0)
        if minute > 90:
            continue
        team = (k.get("team") or {}).get("displayName", "")
        if liveline.same_club(team, home["team"]["displayName"]):
            hg += 1
        elif liveline.same_club(team, away["team"]["displayName"]):
            ag += 1
    return hg, ag


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


def main() -> None:
    dry = "--dry" in sys.argv
    fixtures = load()
    todo = [f for f in fixtures if not f.settled]
    if not todo:
        print("nothing unsettled on the board")
        return

    cache: dict[tuple[str, str], list] = {}
    lines = FIXTURES.read_text().split("\n")
    changed, missing, still = [], [], []

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
                            if len(parts) == 7 and parts[0] == f.kickoff \
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
            got1 = settle(f.tip1, f.teams, hg, ag)
            tip2 = f.tip2
            got2 = settle(f.tip2, f.teams, hg, ag)
            if got2 and not tip2.lstrip().startswith(("✅", "❌", "◦")):
                tip2 = f"{got2[0]} {tip2}"
            if got1 is None:
                status = f"FT {hg}-{ag} (no tip)"
            else:
                word = ("HIT" if got1[1] > 0 else
                        "PUSH" if got1[1] == 0 else "MISS")
                status = f"{got1[0]} {word} — {hg}-{ag}{note}"

        if status == f.status and tip2 == f.tip2:
            continue
        for i, ln in enumerate(lines):
            parts = ln.split("\t")
            if len(parts) == 7 and parts[0] == f.kickoff and parts[3] == f.teams:
                parts[5], parts[6] = tip2, status
                lines[i] = "\t".join(parts)
                break
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
