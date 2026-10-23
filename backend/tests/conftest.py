"""
Test isolation: webapp's caches do not survive a test.

scripts/webapp.py holds its expensive reads in module-level dicts —
MARKET_BAR, LIVE_BAR, BAND_STATS, the frozen forward log and the rest —
filled once per render and never cleared, which is exactly right in a
job that renders once and exits.

In a test session it is a trap. Several tests point webapp.FORWARD at a
small temp log and read through it; monkeypatch faithfully restores the
PATH afterwards, but nothing restores the caches those reads filled, so
every later test in the process is answering off a forward log of four
rows. The tests that depend on the real record then fail — not on what
they assert, but on the order they ran in. They pass alone and fail in
the suite, which is the signature.

Eight tests were failing that way, long enough that a permanently red
suite had stopped being read at all — and it was a red suite that let a
ZeroDivisionError in the renderer go unnoticed for thirty hours on
22 Oct. So the caches are cleared between tests. Nothing in the
application changes: a job still fills each cache once and keeps it.
"""
import pytest


# (attribute, value to reset it to). A dict is emptied in place rather
# than rebound, because callers hold references to these objects.
_EMPTY = ("MARKET_BAR", "LIVE_BAR", "BAND_STATS", "FROM_STATS",
          "BAND_MOVES", "MOVE_STATS", "_SEVEN_DONE", "_LIVE_HIST",
          "_FIRST", "_LOCKS")
_NONE = ("_QUOTES", "_SLICES", "_LOGGED", "_FROZEN", "_REVIEW", "_BOOK",
         "_COUNTRIES", "_LEAGUE_NAMES")


@pytest.fixture(autouse=True)
def _clear_webapp_caches():
    """Every test starts with webapp's caches cold."""
    try:
        from scripts import webapp as w
    except Exception:                                     # noqa: BLE001
        yield
        return

    def reset():
        for name in _EMPTY:
            got = getattr(w, name, None)
            if isinstance(got, dict):
                got.clear()
            elif isinstance(got, list):
                del got[:]
        for name in _NONE:
            if hasattr(w, name):
                setattr(w, name, None)
        locks_read = getattr(w, "_LOCKS_READ", None)
        if isinstance(locks_read, list):
            del locks_read[:]

    reset()
    yield
    reset()
