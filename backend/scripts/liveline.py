"""
How a lane stands while the match is still running.

A pre-match probability priced ninety minutes; once the ball is rolling
the only honest statement is what the CURRENT score has done to the bet.
This says it in the shortest possible form — "✓ landed", "needs 1 more",
"room for 3" — and it says it from `pricing.settle_fraction`, the same
settlement the ledger pays out on, rather than a second opinion about
what a line means.

Over rungs can be WON in play and never given back; under rungs can only
be LOST in play, so they get headroom instead of a verdict. Quarter lines
carry their half states through unchanged.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing

# "**Al-Nassr O1.5** 84.0% …" or "U4.25 86.5% …" — the market, and the
# team it belongs to when the lane is a team total.
_MARKET = re.compile(r"\*\*\s*(?:(?P<team>.+?)\s+)?(?P<mk>[OU]\d+(?:\.\d+)?)"
                     r"\s*\*\*|(?P<mk2>[OU]\d+(?:\.\d+)?)")
_SCORE = re.compile(r"(\d+)\s*-\s*(\d+)")


def score_of(status: str) -> Optional[tuple[int, int]]:
    """(home, away) from a live status line, or None when it carries no
    score yet."""
    m = _SCORE.search(status or "")
    return (int(m.group(1)), int(m.group(2))) if m else None


def _goals_for(cell: str, teams: str, hg: int, ag: int) -> Optional[tuple]:
    m = _MARKET.search(cell)
    if not m:
        return None
    market = m.group("mk") or m.group("mk2")
    team = (m.group("team") or "").strip()
    if not team or " v " not in teams:
        return market, hg + ag
    home, away = (x.strip() for x in teams.split(" v ", 1))
    low = team.lower()
    if low in home.lower() or home.lower() in low:
        return market, hg
    if low in away.lower() or away.lower() in low:
        return market, ag
    return market, hg + ag


def progress(cell: str, teams: str, status: str) -> str:
    """A short live state for one lane, or "" when it cannot be read."""
    sc = score_of(status)
    if sc is None:
        return ""
    got = _goals_for(cell, teams, *sc)
    if got is None:
        return ""
    market, goals = got
    try:
        now = pricing.settle_fraction(market, goals)
    except (ValueError, IndexError):
        return ""

    if market.startswith("O"):
        # An over, once cleared, cannot be taken back.
        if now >= 1.0:
            return "✓ landed"
        for k in range(1, 8):
            if pricing.settle_fraction(market, goals + k) >= 1.0:
                half = " (half in)" if now > 0 else ""
                return f"needs {k} more{half}"
        return ""
    # An under can only be lost from here. A quarter line settles in
    # halves and a whole line can push, so the state comes first and
    # headroom only when the lane is still whole.
    if now <= -1.0:
        return "✗ gone"
    if now < 0.0:
        return "half gone"
    if now == 0.0:
        return "push as it stands"
    if now < 1.0:
        return "half safe"
    # Two thresholds, because a quarter line has a middle band: `full` is
    # how many more goals leave the lane untouched, `safe` how many leave
    # it still winning something. U4.25 at one goal is full through three
    # and half-wins at four — "room for 2" undersold it.
    full = safe = None
    for k in range(1, 9):
        s = pricing.settle_fraction(market, goals + k)
        if full is None and s < 1.0:
            full = k - 1
        if safe is None and s <= 0.0:
            safe = k - 1
            break
    if full is None:
        return "room to spare"
    if safe is None or safe == full:
        return "next goal hurts" if full == 0 else f"room for {full}"
    return (f"room for {safe} · half from the {full + 1}"
            + ("st" if full + 1 == 1 else "nd" if full + 1 == 2
               else "rd" if full + 1 == 3 else "th"))


_LEGS = Path(__file__).resolve().parents[2] / "config" / "first_legs.tsv"
_LEG_LINE = re.compile(r"^(?P<h>.+?)\s+(?P<hg>\d+)-(?P<ag>\d+)\s+(?P<a>.+)$")


def _legs() -> dict:
    if not _LEGS.exists():
        return {}
    out = {}
    for ln in _LEGS.read_text().splitlines():
        if ln.strip() and not ln.startswith("#") and "\t" in ln:
            fixture, leg = (x.strip() for x in ln.split("\t", 1))
            out[fixture] = leg
    return out


_DROP = {"fc", "fk", "cf", "sc", "ac", "afc", "bk", "if", "sk", "club",
         "cp", "of", "the", "ri"}


# Letters that carry no accent to strip: NFD leaves ø, đ, ł and ß whole,
# so "Lillestrøm" and ESPN's "Lillestrom" came out as different clubs.
_FOLD = str.maketrans({"ø": "o", "Ø": "o", "đ": "d", "Đ": "d", "ł": "l",
                       "Ł": "l", "ð": "d", "þ": "th", "ı": "i"})
_SPELL = {"æ": "ae", "Æ": "ae", "œ": "oe", "ß": "ss"}


def _words(s: str) -> set:
    import unicodedata
    for ch, rep in _SPELL.items():
        s = s.replace(ch, rep)
    s = s.translate(_FOLD)
    s = unicodedata.normalize("NFD", s)
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    for ch in ".-'()/":
        s = s.replace(ch, " ")
    return {w for w in s.split() if w not in _DROP}


def _aliases() -> dict:
    """Alias -> display name, from config/club_nicknames.tsv.

    Spelling tolerance cannot reach a TRANSLATED name: ESPN's "Red Star
    Belgrade" and the board's "Crvena zvezda" share no letters, so no
    fuzzy rule will ever pair them. Those are culture, not data, and they
    already have a home — the same hand-typed file the Ask Athena form
    reads, so a club keeps one identity across the whole project.
    """
    global _ALIAS
    if _ALIAS is None:
        _ALIAS = {}
        path = _LEGS.parent / "club_nicknames.tsv"
        if path.exists():
            for ln in path.read_text().splitlines():
                if ln.strip() and not ln.startswith("#") and "\t" in ln:
                    a, d = (x.strip() for x in ln.split("\t", 1))
                    _ALIAS[frozenset(_words(a))] = d
    return _ALIAS


_ALIAS: dict | None = None


def _identity(s: str) -> set:
    """Every word a name answers to, its display name included."""
    w = _words(s)
    display = _aliases().get(frozenset(w))
    return w | _words(display) if display else w


def same_club(a: str, b: str) -> bool:
    """Loose club identity across sources: accents and Nordic letters
    folded, club words dropped, translated names resolved through the
    nickname file, and one token allowed to be a prefix of another so
    "Hearts" finds "Heart of Midlothian"."""
    x, y = _identity(a), _identity(b)
    if not x or not y:
        return False
    for p in x:
        for q in y:
            if len(p) >= 4 and len(q) >= 4 and (p.startswith(q)
                                                or q.startswith(p)):
                return True
    return bool(x & y)


def _side_goals(leg: str, home: str, away: str):
    """(this fixture's home goals, away goals) in the first leg."""
    m = _LEG_LINE.match(leg)
    if not m:
        return None
    lh, la = m.group("h").lower(), m.group("a").lower()
    hg1, ag1 = int(m.group("hg")), int(m.group("ag"))

    # The first leg was the reverse fixture, so today's home side was away.
    if same_club(home, la) and same_club(away, lh):
        return ag1, hg1
    if same_club(home, lh) and same_club(away, la):
        return hg1, ag1
    return None


def tie_note(teams: str, status: str) -> str:
    """The aggregate picture in one sentence, or "" when this is not a
    known two-legged tie. Context only — Athena prices the match total
    and cannot see the tie."""
    if " v " not in teams:
        return ""
    leg = _legs().get(teams)
    if not leg:
        return ""
    home, away = (x.strip() for x in teams.split(" v ", 1))
    got = _side_goals(leg, home, away)
    if got is None:
        return ""
    h1, a1 = got
    hg, ag = score_of(status) or (0, 0)
    ah, aa = h1 + hg, a1 + ag
    started = bool(score_of(status))
    lead = "lead" if started else "carry"
    if ah > aa:
        need = ah - aa
        tail = (f"{away} need {need} to level"
                + (" it" if need == 1 else ""))
        core = f"{home} {lead} {ah}-{aa} on aggregate"
    elif aa > ah:
        need = aa - ah
        tail = f"{home} need {need} to level" + (" it" if need == 1 else "")
        core = f"{away} {lead} {aa}-{ah} on aggregate"
    else:
        core = f"level {ah}-{aa} on aggregate"
        tail = "as it stands this goes to extra time"
    return f"1st leg {leg} · {core} — {tail}."
