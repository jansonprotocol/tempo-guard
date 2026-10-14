"""
Clean up the cards no livescore ever reached, so none of them stays stuck.

WHY THIS EXISTS (the bettor, 14 Oct: "make an automatic workflow that
cleans up cards that never showed livescores and therefore stayed
stuck"). The sweep asks ESPN for the board day and the day either side of
it, by the fixture's own clubs. A card it cannot find there is left
exactly as it was — correctly, nothing is guessed — and it then sits
unsettled for ever: it keeps a frozen call on the Running tab days after
the whistle, it trips board.verify's four-hour rot check so every other
job has to render with --allow-stale, and the only way out was a person
reading the stale list and typing a score. Three times this session that
person was an assistant hand-grading a dozen rows.

Two reasons a card never shows a livescore, and they need different
answers:

  THE FEED HAD IT, THE SWEEP MISSED IT. A kickoff that moved by more than
  a day, or a competition whose local calendar runs further from Europe's
  than the sweep's one-day window reaches. So PASS ONE asks again, WIDE:
  the same slugs, the same club resolver, WIDE_DAYS either side. A final
  found there is graded through grade_cells — the same grader the sweep
  and the hand path use, so a rescued row can carry no mark the sweep
  would not have written. A fixture ESPN says was postponed or cancelled
  takes the OFF mark instead, and one found SCHEDULED at a later date is
  LEFT ALONE and named: the match has not been played, a result is still
  coming, and the row is not the board's mistake.

  NOTHING CARRIES IT AT ALL. Poland, Algeria, the friendlies, Greece —
  nine board codes have no ESPN slug, and a card the odds feed listed but
  no scoreboard holds will never grade itself. PASS TWO parks it: the
  status becomes PARK_STATUS, which board.Fixture.settled reads as
  finished-with-nothing-to-grade, so the row leaves pending, leaves
  Running, stops rotting, and feeds no tally, hit rate or ROI. The card
  is NOT lost: every park is appended to config/unreached.tsv with its
  kickoff and whether a bet is held, and `sweep.py --set "Home v Away"
  2-1` still finishes a parked row by hand, grading it properly.

A parked row is re-asked for RECHECK_DAYS, up to RECHECK_MAX a run, in
case the scoreboard publishes it late.

NOTHING IS EVER GRADED FROM A GUESS. A score comes from the feed, by the
same name resolver and the same final-status test the sweep uses, or it
does not come at all.

Usage:  python scripts/stuck.py              report, write nothing
        python scripts/stuck.py --write      rescue, mark and park
        python scripts/stuck.py --hours 24   a different parking window
        python scripts/stuck.py --max 5      stop after this many cards
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, liveline, sweep  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
UNREACHED = ROOT / "config" / "unreached.tsv"

# Hours past kickoff before a card with no result is parked. Twelve is
# past any delay a match comes back from and well past the sweep's own
# four-hour rot bar, so a card the feed is merely slow about is never
# taken off the board by this.
PARK_AFTER_H = 12
WIDE_DAYS = 7           # days either side of the board date pass one asks
PARK_STATUS = "FT — no source"
RECHECK_DAYS = 10       # a parked row is re-asked for this long
RECHECK_MAX = 10        # parked rows re-asked in one run
MAX_CARDS = 25          # stuck cards handled in one run, newest first


def _bet_names() -> set[str]:
    from scripts import ledger
    try:
        lines = ledger.BETS.read_text().splitlines()
    except OSError:
        return set()
    return {ln.split("\t")[0] for ln in lines
            if ln.strip() and not ln.startswith("#")}


def parked(status: str) -> bool:
    return (status or "").startswith(PARK_STATUS)


def stuck_cards(fixtures, hours: int) -> list:
    """Unsettled cards hours past kickoff with no result in sight."""
    now = dt.datetime.now()
    out = []
    for f in fixtures:
        if f.settled:
            continue
        # A postponed, cancelled or abandoned fixture produces no result
        # at this kickoff and the board already says so (board.OFF_MARK).
        # It waits for a person to re-date it, which is not this job.
        if board.is_off(f.status):
            continue
        try:
            ko = dt.datetime.strptime(f.kickoff, "%Y-%m-%d %H:%M")
        except ValueError:
            continue
        if ko > now - dt.timedelta(hours=hours):
            continue
        out.append(f)
    return sorted(out, key=lambda f: f.kickoff, reverse=True)


def recheck_cards(fixtures) -> list:
    """Rows this job has already answered, young enough to ask again.

    Both of its answers are provisional on purpose: a parked card may be
    published late, and a card marked MOVED is a card whose real kickoff
    is still to come — the pass that finds its final is this one, since
    the sweep only ever looks a day either side of the board's date.
    """
    cut = (dt.datetime.now() - dt.timedelta(days=RECHECK_DAYS)) \
        .strftime("%Y-%m-%d %H:%M")
    out = [f for f in fixtures if f.kickoff >= cut
           and (parked(f.status)
                or (not f.settled and board.is_off(f.status)))]
    return sorted(out, key=lambda f: f.kickoff, reverse=True)[:RECHECK_MAX]


def _event(f, cache: dict):
    """The fixture's own event anywhere in the widened window, or None.

    Nearest day first, so a kickoff that drifted by two days costs two
    extra calls rather than fourteen, and the first real match wins.
    """
    home, away = ((x.strip() for x in f.teams.split(" v ", 1))
                  if " v " in f.teams else (f.teams, ""))
    day = dt.datetime.strptime(f.kickoff.split(" ")[0], "%Y-%m-%d").date()
    for off in sorted(range(-WIDE_DAYS, WIDE_DAYS + 1), key=abs):
        d = (day + dt.timedelta(days=off)).strftime("%Y-%m-%d")
        key = (f.code, d)
        if key not in cache:
            # board_day already asks the day either side of the one it is
            # given, so the window is covered with a third of the calls.
            cache[key] = sweep.board_day(f.code, d)
        for cand in cache[key]:
            comp = cand["competitions"][0]
            ch = next(x for x in comp["competitors"] if x["homeAway"] == "home")
            ca = next(x for x in comp["competitors"] if x["homeAway"] == "away")
            if liveline.same_club(home, ch["team"]["displayName"]) \
                    and liveline.same_club(away, ca["team"]["displayName"]):
                return cand, ch, ca
    return None


def _graded(f, ev, ch, ca) -> tuple[str, str, str] | None:
    """(tip2, status, tip3) for an event that is a real final, else None."""
    st = ev["status"]["type"]
    name, detail = st.get("name") or "", st.get("shortDetail") or ""
    if not sweep._is_final(name, detail):
        return None
    hg, ag = int(ch.get("score") or 0), int(ca.get("score") or 0)
    note = ""
    # Every market here settles on the 90, so extra time comes back off
    # the scoreline first — the sweep's own rule, read the same way.
    if "AET" in detail or "PEN" in detail or detail == "ET" \
            or name in ("STATUS_FINAL_AET", "STATUS_FINAL_PEN",
                        "STATUS_END_OF_EXTRATIME"):
        reg = sweep.regulation(ev.get("_slug")
                               or sweep.SLUGS.get(f.code, ""), ev)
        if reg is None:
            return None     # no timeline to read: leave it for a person
        note = f" (90'; {hg}-{ag} aet)"
        hg, ag = reg
    # THE REASON IT WAS STUCK, kept on the row — but only for a real
    # drift. ESPN dates an event in UTC and the board in European time,
    # so a late kickoff sits on the next day in the feed with nothing
    # wrong: a day either side is the sweep's own window and is normal.
    played = (ev.get("date") or "")[:10]
    try:
        drift = abs((dt.date.fromisoformat(played)
                     - dt.date.fromisoformat(f.kickoff.split(" ")[0])).days)
    except ValueError:
        drift = 0
    if drift > 1:
        note += f" [played {played}]"
    return sweep.grade_cells(f, hg, ag, note)


def main() -> None:
    write = "--write" in sys.argv
    hours = PARK_AFTER_H
    cap = MAX_CARDS
    for flag, setter in (("--hours", "h"), ("--max", "m")):
        if flag in sys.argv:
            i = sys.argv.index(flag)
            try:
                v = int(sys.argv[i + 1])
            except (IndexError, ValueError):
                raise SystemExit(f"{flag} needs a number")
            if setter == "h":
                hours = v
            else:
                cap = v
    fixtures = board.load()
    todo = stuck_cards(fixtures, hours)[:cap]
    again = recheck_cards(fixtures)
    if not todo and not again:
        print("nothing stuck: every card past kickoff carries a result "
              "or a mark")
        return
    bets = _bet_names()
    lines = board.FIXTURES.read_text().split("\n")
    cache: dict = {}
    fixed, offed, moved, park, held = [], [], [], [], []
    for f in again + todo:
        got = _event(f, cache)
        if got:
            ev, ch, ca = got
            cells = _graded(f, ev, ch, ca)
            if cells:
                if write:
                    sweep._write_row(lines, f, *cells)
                fixed.append(f"{f.teams}: {cells[1]}")
                continue
            name = ev["status"]["type"].get("name") or ""
            if name in sweep.OFF_NAMES:
                off = (f"{board.OFF_MARK} "
                       f"{name.removeprefix('STATUS_').title()}")
                if write and not parked(f.status):
                    sweep._write_row(lines, f, f.tip2, off, f.tip3)
                offed.append(f"{f.teams}: {off}")
                continue
            # FOUND, AND NOT PLAYED: the match moved. No result is coming
            # at the kickoff the board carries, which is exactly what the
            # OFF mark says (board.OFF_MARK) — so the row is marked and
            # held, unsettled, not rotting, until a person re-dates it,
            # and the recheck pass above grades it once it is played. The
            # kickoff column is never rewritten here: a card's first-sight
            # stamp is keyed on (date, fixture), and moving the date would
            # orphan its locked lane, tier and band.
            when = (ev.get("date") or "")[:10]
            off = f"{board.OFF_MARK} Moved to {when or '?'}"
            if write and f.status.strip() != off:
                sweep._write_row(lines, f, f.tip2, off, f.tip3)
            moved.append(f"{f.teams}: {off} ({name or '?'})")
            continue
        if parked(f.status):
            continue        # already parked, still nowhere: nothing to do
        why = ("no ESPN slug for the league" if not sweep.slugs_for(f.code)
               else f"not on ESPN within {WIDE_DAYS} days")
        was = liveline.score_of(f.status) if f.status else None
        note = f" ({why}" + (f", last seen {f.status.strip()}" if was else "")
        if write:
            sweep._write_row(lines, f, f.tip2, PARK_STATUS + note + ")",
                             f.tip3)
        park.append((f, why))
        if f.teams in bets:
            held.append(f.teams)

    for c in fixed:
        print(("graded " if write else "would grade ") + c)
    for c in offed:
        print(("marked " if write else "would mark ") + c)
    for g, why in park:
        print(("parked " if write else "would park ") + f"{g.teams}: {why}")
    if moved:
        print(("moved, not played yet" if write else "would mark moved")
              + ":\n  " + "\n  ".join(moved))
    if held:
        print("::warning::parked with a BET held, so the bet stays open "
              "until a score is typed: " + ", ".join(held))
    print(f"{len(fixed)} graded · {len(offed)} marked off · {len(park)} parked "
          f"· {len(moved)} moved · {len(todo)} stuck checked "
          f"({len(again)} parked rows re-asked)")

    if not write or not (fixed or offed or park or moved):
        return
    board.FIXTURES.write_text("\n".join(lines))
    if park:
        new = not UNREACHED.exists()
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
        with UNREACHED.open("a") as fh:
            if new:
                fh.write("# Cards no livescore ever reached, parked by "
                         "scripts/stuck.py so they stop sitting stuck.\n"
                         "# The row reads 'FT — no source': settled for the "
                         "board, graded nothing, counted nowhere.\n"
                         "# A score can still be typed — sweep.py --set "
                         '"Home v Away" 2-1 — and grades it properly.\n'
                         "# parked_utc\tkickoff\tcode\tleague\tfixture\t"
                         "why\tbet held\n")
            for g, why in park:
                fh.write(f"{stamp}\t{g.kickoff}\t{g.code}\t{g.league}\t"
                         f"{g.teams}\t{why}\t"
                         f"{'yes' if g.teams in bets else ''}\n")
    board.main()


if __name__ == "__main__":
    main()
