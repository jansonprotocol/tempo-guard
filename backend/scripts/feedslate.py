"""
Fill the board from the odds feed — the slate nobody has to type.

Until 21 Sep every card on the board arrived through a slate somebody
pasted by hand (screenshots of a fixture list, retyped as
kickoff<TAB>CODE<TAB>League<TAB>Home v Away, then futurematch.py). The
bettor, that day, on seeing the Playable tab fill after an odds refresh:
"is it now automatically whenever odds refresh pulls matches from its
refresh it also grades it automatically?" It was not: the refresh only
priced cards already on the board. But the feed's answer to one league
call IS a fixture list — every upcoming match in the competition with
its kickoff — so the listing the refresh already pays for can be the
slate. "make this", he said.

What this does, per league the ENGINE carries and the FEED carries right
now (odds_api.carried — a club key always, a national-team key only in
its window):

  1. ask the feed for the league's upcoming fixtures (cached 90 minutes
     like every other call, 2 credits a league when not);
  2. keep those kicking off inside the window (--days, default 7),
     converted from the feed's UTC to the board's Amsterdam clock;
  3. drop any already on the board — same league, same two clubs by the
     sweep's own matcher, kickoff within a day — so a re-run adds
     nothing twice and a hand slate is never duplicated;
  4. REFUSE any whose names the engine cannot resolve against its own
     store (aliases first, then the resolver, then a sibling division by
     exact spelling — ingest_board._resolve, the same three steps). A
     name miss is printed with its league so the alias can be typed;
     it is never guessed and never lands on the board as an
     "unresolved name" abstention;
  5. hand the rest to futurematch.add_slate, exactly as a typed slate
     would be — priced through the current engine, an abstention on
     thin history still added as an answer — then pull the quotes and
     render + verify.

What it does NOT do: reach the leagues the feed lacks (Algeria,
Colombia, Morocco, Peru, the Dutch second division, the friendlies, the
CAF and AFC qualifiers). Those still come in by hand. The feed's own
spelling goes on the card — it is what the sweep will meet on ESPN —
and the engine reads it through the same alias table it reads a typed
slate with.

Usage:  python scripts/feedslate.py                 print the slate it would add
        python scripts/feedslate.py --write         add it to the board, quote, render
        python scripts/feedslate.py --days 10       widen the window
        python scripts/feedslate.py --only ENG-PL,MLS
        python scripts/feedslate.py --clock         name every pending card whose
                                                    kickoff is off the feed's
        (--allow-stale passes through to board.verify, as on the two-day job)
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path
from zoneinfo import ZoneInfo

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import odds_api as oa

ROOT = Path(__file__).resolve().parents[2]
NAMES = ROOT / "config" / "league_names.tsv"
AMS = ZoneInfo("Europe/Amsterdam")
DAYS = 7


def league_names() -> dict[str, str]:
    out = {}
    for ln in NAMES.read_text().splitlines():
        if ln.startswith("#") or "\t" not in ln:
            continue
        code, name = ln.split("\t")[:2]
        out[code] = name
    return out


def roster() -> list[str]:
    """Every league the engine has a store for AND the feed has a key for,
    in the menu's order. Whether the key is live today is carried()'s
    call, made per run."""
    from app.data import store
    have = set(store.available_leagues())
    return [c for c in league_names() if c in have and (c in oa.SPORT or c in oa.INTL)]


def _kickoff(commence: str) -> dt.datetime:
    """The feed's UTC ISO stamp as an aware Amsterdam datetime."""
    utc = dt.datetime.fromisoformat(commence.replace("Z", "+00:00"))
    return utc.astimezone(AMS)


def _on_board(code: str, home: str, away: str, day: dt.date, fixtures) -> bool:
    from scripts.liveline import same_club
    for f in fixtures:
        if f.code != code or " v " not in f.teams:
            continue
        fd = dt.date.fromisoformat(f.kickoff.split(" ")[0])
        if abs((fd - day).days) > 1:
            continue
        h, a = (x.strip() for x in f.teams.split(" v ", 1))
        if same_club(h, home) and same_club(a, away):
            return True
    return False


def build(days: int = DAYS, now: dt.datetime | None = None,
          only: list[str] | None = None) -> tuple[list[tuple], list[tuple]]:
    """(rows, skipped): rows are (kickoff, code, league, "Home v Away")
    ready for a slate file; skipped are (code, "Home v Away", why)."""
    from app.data import store
    from scripts.board import load
    from scripts.ingest_board import _resolve
    now = now or dt.datetime.now(dt.timezone.utc)
    until = now + dt.timedelta(days=days)
    names = league_names()
    fixtures = load()
    rows, skipped, seen = [], [], set()
    for code in roster():
        if only and code not in only:
            continue
        if not oa.carried(code):
            continue
        events = oa.fetch_league(code)
        if not events:
            continue
        df = store.load_results(code)
        known = sorted(set(df["home"]) | set(df["away"])) if not df.empty else []
        for ev in sorted(events, key=lambda e: e.get("commence_time", "")):
            when = ev.get("commence_time")
            home, away = ev.get("home_team"), ev.get("away_team")
            if not (when and home and away):
                continue
            ko = _kickoff(when)
            if ko < now or ko > until:
                continue
            teams = f"{home} v {away}"
            key = (code, ko.date(), teams)
            if key in seen:
                continue
            seen.add(key)
            if _on_board(code, home, away, ko.date(), fixtures):
                continue
            rh = _resolve(code, df, known, home)
            ra = _resolve(code, df, known, away)
            if rh is None or ra is None:
                skipped.append((code, teams,
                                f"{'home' if rh is None else 'away'} name unresolved"))
                continue
            rows.append((ko.strftime("%Y-%m-%d %H:%M"), code, names[code], teams))
    rows.sort()
    return rows, skipped


def clock(fixtures=None) -> list[tuple]:
    """Every pending card whose kickoff disagrees with the feed's, on the
    board's Amsterdam clock: (code, teams, board kickoff, feed kickoff).

    The bettor, 21 Sep: "make sure all kickoff times are synched to
    Amsterdam local time." Measured that day: 99 of 99 feed-carried cards
    and 52 of 52 ESPN-carried ones agreed to the minute; the ten left
    (Algeria, Morocco, Peru, one Danish match) have no source to check
    against. A card that stops agreeing is either a typo in a hand slate
    or a match the broadcaster moved — both worth a line in the daily
    job's log. Nothing is rewritten here: the kickoff is the row's key
    in the forward log and the live log, so a move is a hand-set."""
    from scripts.board import load
    out = []
    for f in (fixtures if fixtures is not None else load()):
        if f.settled or f.status or not oa.carried(f.code):
            continue
        day = f.kickoff.split(" ")[0]
        ev = oa.find(f.code, f.teams, day)
        if not ev:
            continue
        ko = _kickoff(ev["commence_time"]).strftime("%Y-%m-%d %H:%M")
        if ko != f.kickoff:
            out.append((f.code, f.teams, f.kickoff, ko))
    return out


def main() -> None:
    args = sys.argv[1:]
    days = int(args[args.index("--days") + 1]) if "--days" in args else DAYS
    only = args[args.index("--only") + 1].split(",") if "--only" in args else None
    if not oa._key():
        print("ODDS_API_KEY not set — the feed cannot be listed", file=sys.stderr)
        sys.exit(1)
    if "--clock" in args:
        off = clock()
        for code, teams, ours, theirs in off:
            print(f"::warning::kickoff differs from the feed — {code} {teams}: "
                  f"board {ours}, feed {theirs} (Amsterdam)")
        print(f"{len(off)} pending cards off the feed's clock", file=sys.stderr)
        return
    rows, skipped = build(days, only=only)
    for r in rows:
        print("\t".join(r))
    print(f"{len(rows)} fixtures the board does not have, next {days} days, "
          f"{len({r[1] for r in rows})} leagues", file=sys.stderr)
    if skipped:
        print("skipped (name unresolved — never guessed; add the alias to "
              "config/team_aliases.json):", file=sys.stderr)
        for code, teams, why in skipped:
            print(f"  {code:9} {teams} — {why}", file=sys.stderr)
    if "--write" not in args:
        return
    if not rows:
        print("nothing to add", file=sys.stderr)
        return
    from scripts import board, futurematch
    slate = ROOT / "config" / "feedslate.tmp.tsv"
    slate.write_text("\n".join("\t".join(r) for r in rows) + "\n")
    try:
        futurematch.add_slate(slate)
    finally:
        slate.unlink(missing_ok=True)
    futurematch.pull_quotes()
    board.main()


if __name__ == "__main__":
    main()
