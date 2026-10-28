"""
Is the board the bettor can SEE the board this repository has?

Every fault this project has had was invisible while it was happening.
The board fill deadlocked on one verify line and was discarded daily for
six days. The renderer raised ZeroDivisionError on a 0.0% claim and no
page was written for thirty hours. The host stopped deploying on 24 Oct
and served a four-day-old page until somebody read it by eye. The live
sweep left 4-to-7 hour holes whenever it handed the chain back to a cron
that does not come. Each time the workflows were green, because each
time what broke was downstream of everything that checks itself, and
nothing compared the END of the pipe with the start of it.

This does. It is the only check that looks at what the reader looks at:

  DEPLOY    the page the host serves, against the page the `site` branch
            holds. A branch newer than the served page means the deploy
            is stuck — the one fault no amount of green CI can show.
  GRADES    cards hours past kickoff still carrying no result. That is
            the sweep failing, and it is what "many cards not graded"
            looks like from here.
  HORIZON   how far ahead the board has fixtures. A horizon that stops
            growing is the fill failing.
  LIVE      the age of web/live.json, which the page polls from main's
            raw URL. Stale means no sweep is running.

Standard library only, like livecheck: a watchdog that needs a
dependency install is a watchdog that fails for its own reasons.

    python scripts/health.py            report, exit 1 if anything is sick
    python scripts/health.py --json     the same as a JSON object
    python scripts/health.py --quiet    exit code only

Thresholds are deliberately slack. This is a smoke alarm, not a metric:
it should stay silent through a normal quiet Tuesday night and go off
before the bettor notices by eye.
"""
from __future__ import annotations

import datetime as dt
import json
import re
import sys
import urllib.request
from pathlib import Path
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "config" / "fixtures.tsv"
LIVE_JSON = ROOT / "web" / "live.json"
SITE_FILE = ROOT / "config" / "site_url.txt"


def site_url() -> str:
    """Where the board is published. From the environment first, then
    config/site_url.txt — NOT hardcoded here, because a Render static
    site that is recreated gets a new hostname, and a watchdog still
    polling the old one would cry wolf for ever while the real page sat
    healthy and unwatched."""
    import os
    env = os.environ.get("ATHENA_SITE_URL", "").strip()
    if env:
        return env
    if SITE_FILE.exists():
        for ln in SITE_FILE.read_text(encoding="utf-8").splitlines():
            ln = ln.strip()
            if ln and not ln.startswith("#"):
                return ln
    raise SystemExit("no site URL: set ATHENA_SITE_URL or write "
                     "config/site_url.txt")
BOARD_TZ = ZoneInfo("Europe/Amsterdam")

SETTLED = ("✅", "❌", "◦")
# A card is only "ungraded" once it is well clear of full time: 90
# minutes, half time, stoppage and a margin for a feed that lags.
GRADE_GRACE_H = 4
# The board should always carry a couple of days of fixtures; the fill
# runs daily and fills a week, so two days means two fills have failed.
HORIZON_MIN_H = 36
# live.json is rewritten every sweep pass while anything is live, and at
# least whenever a sweep runs. Six hours of silence means no sweep.
LIVE_MAX_H = 6
# How far the served page may lag the branch before the deploy is sick.
DEPLOY_LAG_MIN = 25


def _rows():
    """(kickoff, code, league, teams, tip1, tip2, status) per board row."""
    if not FIXTURES.exists():
        return []
    out = []
    for ln in FIXTURES.read_text(encoding="utf-8").splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        c = ln.split("\t")
        if len(c) >= 7:
            out.append(c)
    return out


def _settled(status: str) -> bool:
    s = (status or "").strip()
    return s[:1] in SETTLED or s.startswith("FT")


def _off(status: str) -> bool:
    """Postponed, cancelled or moved: no result is coming at this
    kickoff and the board already says so, so it is not a missing grade."""
    s = (status or "").strip()
    return s.startswith("PP") or "Moved to" in s


def _ko(row):
    try:
        return dt.datetime.strptime(row[0], "%Y-%m-%d %H:%M")
    except (ValueError, IndexError):
        return None


def _built_at(text: str) -> int | None:
    """The render stamp the page carries, or None if it predates it."""
    m = re.search(r"const BUILT_AT = (\d+)", text or "")
    return int(m.group(1)) if m else None


def _get(url: str, timeout: int = 45) -> str:
    req = urllib.request.Request(url, headers={"Cache-Control": "no-cache"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", "replace")


def check(now: dt.datetime | None = None, served: str | None = None,
          branch_page: str | None = None) -> dict:
    """Every check, as {name: {ok, detail, heal}}. `served` and
    `branch_page` are injectable so this is testable without a network."""
    now = now or dt.datetime.now(BOARD_TZ).replace(tzinfo=None)
    rows = _rows()
    out: dict[str, dict] = {}

    # ── GRADES ──────────────────────────────────────────────────────────
    late = []
    for r in rows:
        if _settled(r[6]) or _off(r[6]):
            continue
        ko = _ko(r)
        if ko and ko < now - dt.timedelta(hours=GRADE_GRACE_H):
            late.append((r[0], r[3]))
    late.sort()
    out["grades"] = dict(
        ok=not late,
        detail=(f"{len(late)} card(s) more than {GRADE_GRACE_H}h past "
                f"kickoff with no result"
                + (f"; oldest {late[0][0]} {late[0][1]}" if late else "")
                if late else "every card past kickoff carries a result"),
        heal="live-sweep.yml")

    # ── HORIZON ─────────────────────────────────────────────────────────
    future = [k for k in (_ko(r) for r in rows if not _settled(r[6]))
              if k and k > now]
    horizon = max(future) if future else None
    hrs = (horizon - now).total_seconds() / 3600 if horizon else 0
    out["horizon"] = dict(
        ok=bool(horizon) and hrs >= HORIZON_MIN_H,
        detail=(f"board runs {hrs:.0f}h ahead "
                f"(to {horizon:%Y-%m-%d %H:%M}), {len(future)} pending"
                if horizon else "NO pending fixture on the board at all"),
        heal="board-fill.yml")

    # ── LIVE ────────────────────────────────────────────────────────────
    age_h = None
    if LIVE_JSON.exists():
        try:
            at = json.loads(LIVE_JSON.read_text(encoding="utf-8"))["at"]
            age_h = (dt.datetime.now(dt.timezone.utc).timestamp() - at) / 3600
        except Exception:                                   # noqa: BLE001
            age_h = None
    out["live"] = dict(
        ok=age_h is not None and age_h <= LIVE_MAX_H,
        detail=(f"web/live.json is {age_h:.1f}h old" if age_h is not None
                else "web/live.json missing or unreadable"),
        heal="live-sweep.yml")

    # ── DEPLOY ──────────────────────────────────────────────────────────
    # The only check that leaves the repository. The served page against
    # the page the branch holds: if the branch is newer, the host is not
    # building, and nothing inside CI can see that.
    if served is None:
        try:
            served = _get(site_url())
        except Exception as exc:                            # noqa: BLE001
            out["deploy"] = dict(ok=False, heal="publish",
                                 detail=f"the site did not answer: {exc}")
            return out
    sb, bb = _built_at(served), _built_at(branch_page or "")
    if sb is None:
        detail = ("the served page carries no build stamp — it predates "
                  "the staleness banner, so the deploy is at least that old")
        ok = False
    elif bb is None:
        gap = (dt.datetime.now(dt.timezone.utc).timestamp() - sb) / 60
        ok = gap < 24 * 60
        detail = f"served page built {gap/60:.1f}h ago (no branch page to compare)"
    else:
        gap = (bb - sb) / 60
        ok = gap <= DEPLOY_LAG_MIN
        detail = (f"served page is {gap:.0f} min behind the site branch"
                  if gap > 0 else "served page matches the site branch")
    out["deploy"] = dict(ok=ok, detail=detail, heal="publish")
    return out


def main() -> None:
    args = sys.argv[1:]
    branch_page = None
    p = ROOT / "web" / "index.html"
    if p.exists():
        branch_page = p.read_text(encoding="utf-8", errors="replace")
    res = check(branch_page=branch_page)
    sick = [k for k, v in res.items() if not v["ok"]]
    if "--json" in args:
        print(json.dumps(dict(ok=not sick, sick=sick, checks=res), indent=1))
    elif "--quiet" not in args:
        for name, v in res.items():
            print(f"  {'ok  ' if v['ok'] else 'SICK'}  {name:8} {v['detail']}")
        print(("HEALTHY" if not sick
               else "UNHEALTHY: " + ", ".join(sick)))
    sys.exit(1 if sick else 0)


if __name__ == "__main__":
    main()
