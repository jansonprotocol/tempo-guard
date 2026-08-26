"""
What Athena reads in a fixture, said in human words.

The engine's per-team tags (app/data/tags.py: attack, defence, form,
table stakes, possession — each z-scored against the league, as-of the
match date) and the cup lane's Elo strengths already contain the story
behind every tip. This module just says it out loud: a keyword line for
the collapsed card ("elite attack vs leaky defence") and a sentence or
two for the expanded one. Nothing here is invented — every phrase maps
to a measured label, and a fixture the engine can't read gets no story
rather than a made-up one.
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path
from typing import Optional

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import club_elo, tags

_ELITE = 1700


def _dom(code: str, home: str, away: str, when: date):
    # Board rows carry display names; resolve them to store names the same
    # way the engine does, or the tags see a stranger and stay silent.
    from app.data import features, store
    df = store.load_results(code)
    if df is not None and not df.empty:
        home = features._resolve_in_frame(
            df, features._aliased(code, df, home)) or home
        away = features._resolve_in_frame(
            df, features._aliased(code, df, away)) or away
    h, a = tags.for_fixture(code, home, away, when)
    if not h.labels() and not a.labels():
        return None

    # The collapsed keywords: the sharpest contrast on the card.
    if h.table and a.table and h.table.startswith("top") \
            and a.table.startswith("top"):
        kw = "top-of-table clash"
    elif h.attack and a.defence and "attack" in h.attack \
            and a.defence.split()[0] in ("leaky", "porous"):
        kw = f"{h.attack} vs {a.defence}"
    elif a.attack and h.defence and h.defence.split()[0] in ("leaky",
                                                            "porous"):
        kw = f"{a.attack} (away) vs {h.defence}"
    elif h.defence and a.defence and all(
            t.split()[0] in ("elite", "solid") for t in (h.defence,
                                                         a.defence)):
        kw = "two organised defences"
    else:
        kw = (h.labels() or a.labels())[0]

    def side(t, name):
        return f"<b>{name}</b>: " + (", ".join(t.labels())
                                     if t.labels() else "an average side")

    s = f"{side(h, home)} — against {side(a, away)}."
    if kw == "top-of-table clash":
        s += (" Two sides at the top meeting head-on; Athena prices these "
              "0.15 goals lower — big matches tighten.")
    elif "attack" in kw and ("leaky" in kw or "porous" in kw):
        s += " That pairing — firepower against a defence that leaks — is where the goals in this tip come from."
    elif kw == "two organised defences":
        s += " Two organised defences meeting is unders territory."
    stakes = [t.table for t in (h, a) if t.table and not
              t.table.startswith("top")]
    if stakes:
        s += f" Stakes on the table: {' / '.join(stakes)}."
    return kw, s


def _cup(code: str, home: str, away: str, when: date):
    eh = club_elo.elo_asof(home, club_elo._cutoff(when))
    ea = club_elo.elo_asof(away, club_elo._cutoff(when))
    if eh is None or ea is None:
        return None
    gap = eh - ea
    if abs(gap) >= 250:
        side = home if gap > 0 else away
        phrase, kw = (f"a mismatch — <b>{side}</b> is the far stronger club",
                      "Elo mismatch")
    elif abs(gap) >= 120:
        side = home if gap > 0 else away
        phrase, kw = (f"a clear edge to <b>{side}</b>",
                      f"Elo edge {'home' if gap > 0 else 'away'}")
    else:
        phrase, kw = "an even tie on club strength", "even tie on Elo"
    if min(eh, ea) >= _ELITE:
        kw += " · elite level"
        phrase += " — and both are elite sides, where big games tighten"
    s = (f"Athena prices cups from Club Elo — strength measured across "
         f"every competition, not just the domestic league. It reads "
         f"<b>{home}</b> at {eh:.0f} against <b>{away}</b> at {ea:.0f}: "
         f"{phrase}.")
    return f"Elo {eh:.0f} v {ea:.0f} · {kw}", s


def fixture_read(code: str, teams: str, kickoff: str
                 ) -> Optional[tuple[str, str]]:
    """(keywords, sentence) for a board fixture, or None — no story is an
    answer, exactly like an abstained tip."""
    if " v " not in teams:
        return None
    home, away = (x.strip() for x in teams.split(" v ", 1))
    when = date.fromisoformat(kickoff.split(" ")[0])
    try:
        if code in club_elo.CUPS:
            return _cup(code, home, away, when)
        return _dom(code, home, away, when)
    except Exception:
        return None
