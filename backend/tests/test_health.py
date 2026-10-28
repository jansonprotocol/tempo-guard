"""
The watchdog, and the two things that made one necessary.

Every fault this project has had was invisible while it happened and was
found by eye days later, with the workflows green throughout. The common
shape: each job checked its own step, and what broke was downstream of
the last thing that checks itself. scripts/health.py is the only check
that leaves the repository and compares the END of the pipe — the page
the host actually serves — with the start of it.

These pin its four answers against hand-built inputs rather than the
live board, so they say the same thing in January as they do today.
"""
import datetime as dt
import json

import pytest

from scripts import health as h


NOW = dt.datetime(2026, 10, 28, 12, 0)


def _board(tmp_path, rows):
    """A fixtures.tsv of (kickoff, status) pairs, as the board writes it."""
    f = tmp_path / "fixtures.tsv"
    f.write_text("# header\n" + "".join(
        f"{ko}\tENG-PL\tPremier League\tHome{i} v Away{i}\t"
        f"U4.5 85.0% +1.0%\t\t{st}\n"
        for i, (ko, st) in enumerate(rows)), encoding="utf-8")
    return f


def _page(built_at: int | None) -> str:
    if built_at is None:
        return "<html>a page from before the build stamp existed</html>"
    return f"<html><script>const BUILT_AT = {built_at};</script></html>"


@pytest.fixture
def board(tmp_path, monkeypatch):
    def use(rows, live_age_h=0.2):
        monkeypatch.setattr(h, "FIXTURES", _board(tmp_path, rows))
        lj = tmp_path / "live.json"
        at = dt.datetime.now(dt.timezone.utc).timestamp() - live_age_h * 3600
        lj.write_text(json.dumps({"at": at, "cards": {}}), encoding="utf-8")
        monkeypatch.setattr(h, "LIVE_JSON", lj)
    return use


# ── GRADES: "many cards not graded on final score" ──────────────────────────

def test_a_card_long_past_kickoff_with_no_result_is_sick(board):
    board([("2026-10-28 06:00", "")])                  # 6h ago, nothing
    r = h.check(now=NOW, served=_page(1), branch_page=_page(1))
    assert not r["grades"]["ok"]
    assert "1 card" in r["grades"]["detail"]
    assert r["grades"]["heal"] == "live-sweep.yml"


def test_a_card_just_past_kickoff_is_not_yet_sick(board):
    """Ninety minutes, half time, stoppage and a lagging feed: a card is
    not a missing grade until it is well clear of full time."""
    board([("2026-10-28 10:30", "")])                  # 1.5h ago
    assert h.check(now=NOW, served=_page(1), branch_page=_page(1))["grades"]["ok"]


def test_a_graded_card_is_not_a_missing_grade(board):
    board([("2026-10-28 06:00", "✅ HIT — 2-1"),
           ("2026-10-28 06:00", "❌ MISS — 3-3"),
           ("2026-10-28 06:00", "◦ PUSH — 1-0"),
           ("2026-10-28 06:00", "FT — no source (no ESPN slug for the league)")])
    assert h.check(now=NOW, served=_page(1), branch_page=_page(1))["grades"]["ok"]


def test_a_postponed_card_is_not_a_missing_grade(board):
    """No result is coming at this kickoff and the board already says
    so; it waits for a person to re-date it, which is not a fault."""
    board([("2026-10-28 06:00", "PP Moved to 2026-11-02")])
    assert h.check(now=NOW, served=_page(1), branch_page=_page(1))["grades"]["ok"]


# ── HORIZON: "new cards not on the board" ───────────────────────────────────

def test_a_board_that_runs_out_is_sick(board):
    board([("2026-10-28 20:00", "")])                  # 8h of board left
    r = h.check(now=NOW, served=_page(1), branch_page=_page(1))
    assert not r["horizon"]["ok"]
    assert r["horizon"]["heal"] == "board-fill.yml"


def test_an_empty_board_is_sick_and_says_so(board):
    board([("2026-10-20 20:00", "✅ HIT — 1-0")])       # all settled
    r = h.check(now=NOW, served=_page(1), branch_page=_page(1))
    assert not r["horizon"]["ok"]
    assert "NO pending fixture" in r["horizon"]["detail"]


def test_a_week_of_fixtures_is_healthy(board):
    board([("2026-11-03 20:00", "")])
    assert h.check(now=NOW, served=_page(1), branch_page=_page(1))["horizon"]["ok"]


# ── LIVE: "live scores stuck and updated slow" ──────────────────────────────

def test_a_stale_live_json_is_sick(board):
    board([("2026-11-03 20:00", "")], live_age_h=9)
    r = h.check(now=NOW, served=_page(1), branch_page=_page(1))
    assert not r["live"]["ok"] and "9.0h old" in r["live"]["detail"]


def test_a_fresh_live_json_is_healthy(board):
    board([("2026-11-03 20:00", "")], live_age_h=0.1)
    assert h.check(now=NOW, served=_page(1), branch_page=_page(1))["live"]["ok"]


# ── DEPLOY: the fault that four days of green CI could not show ─────────────

def test_a_branch_newer_than_the_served_page_is_sick(board):
    """24-28 Oct exactly: every publish landed on `site`, the data was
    right, and the host served a four-day-old page."""
    board([("2026-11-03 20:00", "")])
    now = int(dt.datetime(2026, 10, 28, 11, 22, tzinfo=dt.timezone.utc).timestamp())
    r = h.check(now=NOW, served=_page(now - 4 * 86400), branch_page=_page(now))
    assert not r["deploy"]["ok"]
    assert "behind the site branch" in r["deploy"]["detail"]
    assert r["deploy"]["heal"] == "publish"


def test_a_served_page_that_matches_the_branch_is_healthy(board):
    board([("2026-11-03 20:00", "")])
    t = 1793187567
    assert h.check(now=NOW, served=_page(t), branch_page=_page(t))["deploy"]["ok"]


def test_a_few_minutes_of_lag_is_allowed(board):
    """A publish, a build and a CDN are not instant; only a real hole
    counts."""
    board([("2026-11-03 20:00", "")])
    t = 1793187567
    r = h.check(now=NOW, served=_page(t), branch_page=_page(t + 10 * 60))
    assert r["deploy"]["ok"], r["deploy"]["detail"]


def test_a_page_with_no_build_stamp_is_sick(board):
    """What the live site returned on 28 Oct: so old it predates the
    stamp itself."""
    board([("2026-11-03 20:00", "")])
    r = h.check(now=NOW, served=_page(None), branch_page=_page(1793187567))
    assert not r["deploy"]["ok"] and "no build stamp" in r["deploy"]["detail"]


def test_a_site_that_does_not_answer_is_sick(board, monkeypatch):
    board([("2026-11-03 20:00", "")])
    monkeypatch.setattr(h, "_get", lambda *a, **k: (_ for _ in ()).throw(
        OSError("connection refused")))
    r = h.check(now=NOW, branch_page=_page(1))
    assert not r["deploy"]["ok"] and "did not answer" in r["deploy"]["detail"]


# ── the whole thing ─────────────────────────────────────────────────────────

def test_a_healthy_board_reports_every_check_ok(board):
    board([("2026-11-03 20:00", ""), ("2026-10-28 06:00", "✅ HIT — 1-0")])
    t = 1793187567
    r = h.check(now=NOW, served=_page(t), branch_page=_page(t))
    assert all(v["ok"] for v in r.values()), \
        [k for k, v in r.items() if not v["ok"]]
    assert set(r) == {"grades", "horizon", "live", "deploy"}


def test_every_check_names_a_job_that_fixes_it(board):
    """The workflow heals by dispatching what a check names, so a check
    with no healer is a check that only ever shouts."""
    board([("2026-10-28 06:00", "")])
    r = h.check(now=NOW, served=_page(1), branch_page=_page(1))
    for name, v in r.items():
        assert v["heal"] in ("live-sweep.yml", "board-fill.yml", "publish"), \
            f"{name} names no healer"


def test_the_watchdog_imports_nothing_heavy():
    """A watchdog that needs a dependency install is a watchdog that
    fails for its own reasons — and the health workflow deliberately
    runs it before any pip."""
    import pathlib
    src = pathlib.Path(h.__file__).read_text(encoding="utf-8")
    for heavy in ("import pandas", "import requests", "from app",
                  "import pyarrow", "from scripts import board"):
        assert heavy not in src, heavy


# ── the watchdog's own workflow ─────────────────────────────────────────────

def _health_yml() -> str:
    import pathlib
    return (pathlib.Path(__file__).resolve().parents[2] / ".github"
            / "workflows" / "health.yml").read_text(encoding="utf-8")


def test_the_alarm_reads_the_real_exit_code():
    """`python health.py | tee f` then `$?` reads TEE's exit code, which
    is always 0 — so the first version of this workflow reported healthy
    while the check said SICK and raised no alarm at all. A watchdog
    that cannot fail is worse than no watchdog."""
    y = _health_yml()
    import re
    piped = re.search(r"python scripts/health\.py[^\n|]*\|[^\n]*tee[^\n]*\n"
                      r"\s*(?:RC=)?\$\?|"
                      r"\|\s*tee[^\n]*\n\s*echo \"sick=\$\?\"", y)
    assert piped is None, "the exit code is being read from a pipeline"
    assert "RC=$?" in y and "sick=$RC" in y, \
        "the exit code must come from the unpiped run"


def test_the_alarm_raises_something_a_person_receives():
    """Going red is not telling anyone: nobody reads a green tick and
    nobody reads a red one either."""
    y = _health_yml()
    assert "gh issue create" in y and "gh issue comment" in y, \
        "a sick board must reach the bettor, not just the Actions tab"
    assert "gh issue close" in y, \
        "an open issue must mean something is wrong RIGHT NOW"


def test_the_watchdog_runs_before_any_dependency_install():
    y = _health_yml()
    assert y.index("scripts/health.py") < y.index("pip install"), \
        "health.py must answer before pip can fail"
