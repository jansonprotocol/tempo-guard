"""Is a sweep worth running right now? Answers in a few milliseconds.

The scheduler this serves cannot decide for itself when football happens,
so it wakes every few minutes and asks. Almost every time the answer is
no, and that has to be cheap: this module imports nothing but the standard
library — no pandas, no engine — so the "no" path costs a checkout and an
interpreter start rather than a dependency install.

    python scripts/livecheck.py          prints the reason, exit 0 = sweep
    python scripts/livecheck.py --quiet  exit code only

A sweep is worth running when either:

  LIVE      a fixture has kicked off and carries no result yet. That is
            the whole point — those rows are what a sweep updates, and
            they keep being worth updating until they settle.
  IMMINENT  a fixture kicks off within LEAD minutes. Catching the first
            whistle promptly matters more than catching it exactly, and
            it means the run that follows already has the match live.

Kickoffs are stored in the board's own clock, Amsterdam local, so the
comparison is made there rather than in whatever timezone the runner
happens to hold.
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path
from zoneinfo import ZoneInfo

FIXTURES = Path(__file__).resolve().parents[2] / "config" / "fixtures.tsv"
BOARD_TZ = ZoneInfo("Europe/Amsterdam")

# How far ahead of a kickoff to start sweeping.
LEAD = timedelta(minutes=6)
# How long after a kickoff to keep asking about a fixture that never
# settled. Ninety minutes plus stoppage, half time and a generous margin;
# past this the row needs a hand, not another sweep, and a no-feed league
# would otherwise keep the scheduler awake for ever.
STALE = timedelta(hours=5)

SETTLED = ("✅", "❌", "◦")     # hit, miss, push


def _rows():
    if not FIXTURES.exists():
        return
    for ln in FIXTURES.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 7:
            continue
        yield p[0], p[3], p[6]                # kickoff, teams, status


def reason(now: datetime | None = None) -> str | None:
    """Why a sweep should run, or None when it should not."""
    now = now or datetime.now(BOARD_TZ)
    live, soon = [], []
    for kickoff, teams, status in _rows():
        if status[:1] in SETTLED or status.startswith("FT"):
            continue
        try:
            ko = datetime.strptime(kickoff, "%Y-%m-%d %H:%M").replace(
                tzinfo=BOARD_TZ)
        except ValueError:
            continue
        if ko <= now:
            if now - ko < STALE:
                live.append(teams)
        elif ko - now <= LEAD:
            soon.append((ko, teams))
    if live:
        return (f"{len(live)} live and ungraded: "
                + ", ".join(live[:3]) + ("…" if len(live) > 3 else ""))
    if soon:
        ko, teams = min(soon)
        return f"{teams} kicks off at {ko:%H:%M}"
    return None


def next_kickoff(now: datetime | None = None):
    """The next unstarted fixture, for the idle message."""
    now = now or datetime.now(BOARD_TZ)
    best = None
    for kickoff, teams, status in _rows():
        if status[:1] in SETTLED or status.startswith("FT"):
            continue
        try:
            ko = datetime.strptime(kickoff, "%Y-%m-%d %H:%M").replace(
                tzinfo=BOARD_TZ)
        except ValueError:
            continue
        if ko > now and (best is None or ko < best[0]):
            best = (ko, teams)
    return best


def main() -> None:
    quiet = "--quiet" in sys.argv
    why = reason()
    if why:
        if not quiet:
            print(f"SWEEP: {why}")
        raise SystemExit(0)
    if not quiet:
        nxt = next_kickoff()
        if nxt:
            ko, teams = nxt
            mins = (ko - datetime.now(BOARD_TZ)).total_seconds() / 60
            print(f"idle: nothing live · next is {teams} at {ko:%d-%m %H:%M} "
                  f"({mins/60:.1f}h away)")
        else:
            print("idle: nothing live and nothing else on the board")
    raise SystemExit(1)


if __name__ == "__main__":
    main()
