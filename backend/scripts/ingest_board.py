"""
Merge the board's completed matches into the results bank.

Until now the store ended where the last provider snapshot ended (22-24
Aug), and the fixtures this project itself graded — scores read off
ESPN or the bettor's own screen — never fed back. The bettor asked for
the merge (30 Aug) so the retrosim windows include the freshest week.
As-of discipline is untouched: every replay still reads only results
dated before the fixture it prices, so nothing can train on the match
it is being scored against.

Guard rails, in order:
  - only domestic leagues the store already carries (cups keep their
    own provider conventions and are skipped);
  - only rows with a real final score (the FT — no source mark stays
    out);
  - both team names must resolve against the store's own names through
    the engine's resolver — an unresolved name is printed and skipped,
    never guessed, so no duplicate identity can be created;
  - store.save() drops (date, home, away) duplicates, so re-running
    after the next sweep is safe and idempotent.

Usage:  python scripts/ingest_board.py            merge and report
"""
from __future__ import annotations

import re
import sys
from functools import lru_cache
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.data.features import _match_team

FIXTURES = Path(__file__).resolve().parents[2] / "config" / "fixtures.tsv"
SCORE = re.compile(r"(\d+)-(\d+)")
# The season LABEL is the league's own: "2026" for a calendar-year league
# (Brazil, MLS, Argentina...), "2026-27" for an autumn-to-spring one. A
# single hard-coded label put every board result of a calendar-year
# league into a second season file beside the provider's, and the exact
# (date, home, away) check in that file could not see the provider's row
# in the other — 27 matches counted twice by 12 Sep.
def _season(code: str) -> str:
    from app.data import sources
    return sources.get(code).default_seasons()[-1]


def _already_stored(have, date, home, away, hg, ag) -> bool:
    """Exact match, or the same pairing with the same score within a day
    of the date — the board keys by the Amsterdam kickoff, providers by
    the local date, and a 01:30 kickoff in Brazil is a day apart."""
    for d, h, a, g1, g2 in have:
        if h == home and a == away and abs((d - date).days) <= 1 and (d == date or (g1 == hg and g2 == ag)):
            return True
    return False
SKIP_PREFIX = ("UCL", "UEL", "UECL")


def _resolve(code: str, df, names: list[str], team: str):
    """The store's name for a board team, or None — never a guess.

    Three steps, each stricter than a fuzzy match:

    1. THE ALIAS TABLE FIRST, as the engine itself reads it. Until 21 Sep
       this script called the raw resolver and never opened
       config/team_aliases.json, so "AGF", "København", "LA Galaxy",
       "Urawa Reds", "Atl. Nacional" and a dozen more that the table had
       mapped for weeks still came back unresolved here — 68 completed
       board fixtures that never reached the bank. features._aliased is
       the one reader, with its exact-membership guard, so a stale line
       degrades to the resolver rather than misfiring.
    2. The league's own resolver on that name.
    3. A PROMOTED CLUB, by its own spelling only. A club up from the
       second division has no rows in this league's store until a
       provider adds them, but it has rows in its old division's store
       under the store's own name for it — Elversberg is in GER-B2 and
       not in GER-BL. If the board name matches a sibling division of the
       same country EXACTLY (accent-insensitive or canonical, never
       fuzzy), that spelling is the store's and using it creates no new
       identity; it is the same club one division up. Fuzzy is refused
       across divisions on purpose: the failure this guards against is
       Yokohama F. Marinos resolving onto Yokohama FC.
    """
    from app.data.features import _aliased, _canonical, _norm_accent
    got = _match_team(_aliased(code, df, team), names)
    if got is not None:
        return got
    country = code.split("-")[0]
    sibs = [s for s in store.available_leagues()
            if s != code and s.split("-")[0] == country]
    # 4. A CUP ENTRANT, by its own spelling only (the bettor, 14 Oct, two
    #    Champions League cards missing from the board: "just along those
    #    matches come in correctly and give its true tip"). A code with no
    #    sibling sharing its prefix is a continental cup, and there the
    #    whole domestic world is the sibling: a club playing its first tie
    #    in a competition has no rows in that competition's store and
    #    cannot have any until it plays them — Como, LASK and Real Betis
    #    are not in the UCL store, and Crystal Palace, Sunderland,
    #    Bournemouth, Monaco, Getafe, Brighton, Copenhagen, Twente and
    #    Freiburg are not in the UEL's or the UECL's. Twenty-one cup
    #    fixtures in one week were refused for it. The engine was built
    #    for exactly this case: cup_composite's club strength is
    #    league_rating + 0.8 x (domestic PPG - 1.45) + 0.3 x own_cup_form,
    #    and own_cup_form is OMITTED under six cup matches — so the card
    #    prices from the club's domestic record, which is what it should
    #    do. The name check in front of it was the only thing stopping it.
    #    Exact spelling only, accent-insensitive or canonical, never
    #    fuzzy: the same guard the promoted-club step carries.
    if code in CUPS:
        # every domestic store is the sibling here, and each one is asked
        # through ITS OWN alias table first — the same two steps the club's
        # domestic card already resolves by, so "Como" finds "Como 1907"
        # and "Olympiakos Piraeus" finds "Olympiakos" without a guess.
        for sib in store.available_leagues():
            if sib in CUPS or sib.startswith("INT-") or sib == code:
                continue
            sdf = store.load_results(sib)
            if sdf is None or sdf.empty:
                continue
            # NO FUZZY PASS ACROSS BORDERS. _aliased ends with a fuzzy
            # match against the spellings a league's fold consumed, which
            # is safe inside that league and lethal outside it: asked
            # about "Inter Club d'Escaldes" the Italian store answered
            # "FC Internazionale Milano". So only the two statements that
            # cannot guess are used — the hand-written alias table and
            # the store's own fold — and the comparison stays exact.
            from app.data import aliases as _aliases
            cands = {team, _aliases.get(sib, team),
                     store.folded_name(sib, team)}
            for cand in filter(None, cands):
                for n in _store_names(sib):
                    if _norm_accent(n) == _norm_accent(cand) \
                            or _canonical(n) == _canonical(cand):
                        return n
        return None
    for sib in sibs:
        for n in _store_names(sib):
            if _norm_accent(n) == _norm_accent(team) or _canonical(n) == _canonical(team):
                return n
    return None


# The continental cups: codes whose entrants are CLUBS with domestic
# records elsewhere. INT-* is deliberately not here — those are national
# teams, and no domestic club store can speak for them.
CUPS = frozenset({"UCL", "UCL-Q", "UEL", "UEL-Q", "UECL", "UECL-Q"})


@lru_cache(maxsize=None)
def _store_names(code: str) -> tuple[str, ...]:
    """Every club name a league's store carries, read once per process."""
    sdf = store.load_results(code)
    if sdf is None or sdf.empty:
        return ()
    return tuple(sorted(set(sdf["home"]) | set(sdf["away"])))


def main() -> None:
    stored = set(store.available_leagues())
    added: dict[str, int] = {}
    skipped: list[str] = []
    rows: dict[str, list[dict]] = {}

    for ln in FIXTURES.read_text().splitlines():
        c = ln.split("\t")
        if ln.startswith("#") or len(c) < 7:
            continue
        code, teams, status = c[1], c[3], c[6]
        if code not in stored or code.startswith(SKIP_PREFIX):
            continue
        if not status or status.startswith("LIVE") or "no source" in status:
            continue
        m = SCORE.search(status)
        if not m or " v " not in teams:
            continue
        h, a = teams.split(" v ", 1)
        df = store.load_results(code)
        names = sorted(set(df["home"]) | set(df["away"]))
        rh, ra = _resolve(code, df, names, h), _resolve(code, df, names, a)
        if rh is None or ra is None:
            skipped.append(f"{code}: {teams} — "
                           f"{'home' if rh is None else 'away'} unresolved")
            continue
        rows.setdefault(code, []).append(dict(
            date=pd.Timestamp(c[0].split(" ")[0]), home=rh, away=ra,
            hg=int(m.group(1)), ag=int(m.group(2)), season=_season(code),
            league_code=code, country="", status="result"))

    for code, new in sorted(rows.items()):
        season = _season(code)
        cur = store.load(code, season)
        # Against EVERY season file the store holds for the league, not
        # only the one being written to, and with the day-shift rule.
        allr = store.load_results(code)
        have = list(zip(allr["date"], allr["home"], allr["away"], allr["hg"], allr["ag"])) \
            if not allr.empty else []
        fresh = [r for r in new
                 if not _already_stored(have, r["date"], r["home"], r["away"], r["hg"], r["ag"])]
        if not fresh:
            continue
        add = pd.DataFrame(fresh)
        merged = pd.concat([cur, add], ignore_index=True) \
            if not cur.empty else add
        store.save(code, season, merged)
        added[code] = len(fresh)

    for code, n in sorted(added.items()):
        print(f"  {code:9} +{n}")
    print(f"merged {sum(added.values())} results into "
          f"{len(added)} leagues")
    if skipped:
        print("skipped (name unresolved — never guessed):")
        for s in skipped:
            print(f"  {s}")


if __name__ == "__main__":
    main()
