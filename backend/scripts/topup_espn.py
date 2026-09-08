"""Top up the results store from ESPN for the week the other sources missed.

    python scripts/topup_espn.py             every league with an ESPN slug
    python scripts/topup_espn.py ENG-CH SWE-AL   just these
    python scripts/topup_espn.py --dry       report, write nothing

WHY. On 7 Sep the store stopped at 30 Aug in every league on the board:
openfootball trails, football-data.co.uk was returning 503 site-wide, and
the board ingest only carries the fixtures the board itself graded. So a
week's rounds were missing under every card being priced. ESPN's season
scoreboard, which the sweep already reads day by day, has the week — this
fetches it once per league and appends what the store does not have.

GUARDS, in the board-ingest's order:
  * domestic leagues only — cups keep their own provider conventions;
  * only rows dated AFTER the store's last stored result, so nothing a
    provider already wrote is touched or duplicated;
  * both names must resolve against the store's own names through the
    engine's resolver, else the row is printed and skipped — an ESPN
    spelling must never open a second history for a club;
  * no odds: espn.fetch_season runs the odds guard before returning.

A later football-data or openfootball load merges over these rows by
date the way it always has, so this is a top-up, not a fork.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import aliases, espn, sources, store
from app.data.features import _match_team
from scripts import liveline
from scripts.sweep import SLUGS

SKIP_PREFIX = ("UCL", "UEL", "UECL")
WINDOW_DAYS = 14        # how far back of the last stored result to look


def _resolve(code: str, name: str, names: list[str]) -> str | None:
    """An ESPN spelling to the store's own, or None. Three layers, in
    the order the engine itself applies them: the typed alias table,
    the resolver's fuzzy match, and the sweep's club identity (nicknames
    and identity words — the layer that already pairs ESPN's
    "Wolverhampton Wanderers" with the board's "Wolves" every pass). A
    match must be UNIQUE; two store names for one ESPN name is a skip."""
    want = aliases.get(code, name) or name
    got = _match_team(want, names)
    if got is not None:
        return got
    hits = [n for n in names if liveline.same_club(name, n)]
    return hits[0] if len(hits) == 1 else None


def topup(code: str, dry: bool = False) -> tuple[int, list[str]]:
    src = sources.get(code)
    season = src.default_seasons()[-1]
    year = int(str(season)[:4])
    have = store.load_results(code)
    if have.empty:
        return 0, [f"{code}: store empty, not touched"]
    last = have["date"].max()
    live = espn.fetch_season(SLUGS[code], year, code,
                             calendar_year=getattr(src, "calendar_year", True))
    if live is None or live.empty:
        return 0, [f"{code}: ESPN returned nothing"]
    # A window, not a cut at the last stored date: the board ingest writes
    # a handful of matches per round and pushes the last date forward,
    # which would hide the REST of that round. The (date, home, away)
    # check below keeps anything already stored from being written twice.
    full = live
    live = live[live["date"] > last - pd.Timedelta(days=WINDOW_DAYS)]
    names = sorted(set(have["home"]) | set(have["away"]))
    rows, skipped = [], []
    for r in live.itertuples():
        rh, ra = _resolve(code, r.home, names), _resolve(code, r.away, names)
        if rh is None or ra is None:
            skipped.append(f"{code}: {r.home} v {r.away} — "
                           f"{'home' if rh is None else 'away'} unresolved")
            continue
        ht = {}
        if getattr(r, "hthg", None) is not None and not pd.isna(r.hthg):
            ht = dict(hthg=int(r.hthg), htag=int(r.htag))
        rows.append(dict(date=pd.Timestamp(r.date), home=rh, away=ra,
                         hg=int(r.hg), ag=int(r.ag), season=season,
                         league_code=code, country="", status="result", **ht))
    cur = store.load(code, season)
    seen = set(zip(cur["date"], cur["home"], cur["away"])) if not cur.empty else set()
    # Also against every season file, not only the current one: a round
    # the board ingest already wrote may sit under either label.
    seen |= set(zip(have["date"], have["home"], have["away"]))
    fresh = [x for x in rows if (x["date"], x["home"], x["away"]) not in seen]
    # Rows the store already holds but without a half-time score — every
    # ESPN-sourced row before 8 Sep, and everything the board ingest
    # wrote — get it filled from the timeline now. Scores are never
    # overwritten; only an empty half-time cell is written.
    filled = 0
    if not cur.empty and "hthg" not in cur:
        # A season file from a provider that never carried half-time
        # scores (ESPN before 8 Sep) has no column to fill; give it one.
        cur = cur.assign(hthg=float("nan"), htag=float("nan"))
    if not cur.empty:
        # The fill reads the WHOLE fetched season, not the window: the
        # data is already in hand and a calendar-year league has rows
        # back to January with no half-time score.
        ht = {}
        for r in full.itertuples():
            if getattr(r, "hthg", None) is None or pd.isna(r.hthg):
                continue
            rh, ra = _resolve(code, r.home, names), _resolve(code, r.away, names)
            if rh and ra:
                ht[(pd.Timestamp(r.date), rh, ra)] = (int(r.hthg), int(r.htag))
        for i, r in cur.iterrows():
            k = (r["date"], r["home"], r["away"])
            if k in ht and pd.isna(r["hthg"]):
                cur.at[i, "hthg"], cur.at[i, "htag"] = ht[k]
                filled += 1
    if (fresh or filled) and not dry:
        out = pd.concat([cur, pd.DataFrame(fresh)], ignore_index=True) \
            if fresh and not cur.empty else (pd.DataFrame(fresh) if fresh else cur)
        store.save(code, season, out)
    if filled:
        skipped.append(f"{code}: half-time filled on {filled} stored rows")
    return len(fresh), skipped


def main() -> None:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    dry = "--dry" in sys.argv
    codes = args or sorted(c for c in SLUGS if not c.startswith(SKIP_PREFIX)
                           and c in set(store.available_leagues()))
    total, skipped = 0, []
    for code in codes:
        try:
            n, sk = topup(code, dry)
        except Exception as e:                       # one league must not stop the rest
            skipped.append(f"{code}: {type(e).__name__}: {e}")
            continue
        if n:
            print(f"  {code:9} {'would add' if dry else '+'}{n}")
        total += n
        skipped.extend(sk)
    print(f"{'would add' if dry else 'added'} {total} results across {len(codes)} leagues")
    if skipped:
        print("skipped (never guessed):")
        for s in skipped:
            print(f"  {s}")


if __name__ == "__main__":
    main()
