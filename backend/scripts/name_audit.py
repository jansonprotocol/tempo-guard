"""
How many fixtures does the engine lose to names rather than to missing data?

Every abstain looks the same from the outside — "insufficient history" — but
there are two very different causes hiding behind that message:

    GENUINE     the club really is new to the store. Wisla Plock has 3 rows
                because it was promoted, and no amount of name work invents a
                fourth. Abstaining is correct.

    NAME        the club has hundreds of rows filed under a spelling the
                resolver cannot reach. Kobenhavn has 68 matches under `FC
                Copenhagen`; Basaksehir has 629 under `Buyuksehyr`. The data is
                there and the tip is withheld anyway.

Only the second kind is recoverable, and it is worth knowing which is which
before deciding whether name work buys any volume at all.

Two passes:

**Unresolved fixtures.** Every upcoming fixture in the store is run through the
real resolver. A name that comes back None is a fixture that cannot be priced.
This is the direct measurement — no proxy, no modelling — because the fixture
feed and the results store are the two halves the resolver actually bridges.

**Split clubs.** Two stored spellings of one club fragment its history, so each
half looks thinner than the club really is. Candidates are found by canonical
similarity and reported with the row count on each side; a group where one side
sits under the history gate is a club the engine is currently mispricing rather
than merely failing to price. These are CANDIDATES for `config/team_merges.json`
and are deliberately not applied automatically — the resolver's own history
includes a loose match once folding Yokohama F. Marinos onto Yokohama FC.

Usage:  python scripts/name_audit.py [--leagues A,B] [--split-only]
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from rapidfuzz import fuzz

from app.data import store
from app.data.features import (MIN_MATCHES, _aliased, _canonical, _match_team,
                               _resolve_in_frame, _team_names)

# Two stored names this similar are probably one club. Deliberately high: this
# reports candidates for a human to confirm, and a false positive here would
# propose merging two real clubs into one.
SPLIT_CUTOFF = 88


def rows_for(df, name: str) -> int:
    return int(((df["home"] == name) | (df["away"] == name)).sum())


def unresolved(code: str) -> tuple[list, int]:
    """(unresolvable names with their fixture count, total upcoming fixtures)."""
    fx = store.load_fixtures(code)
    res = store.load_results(code)
    if fx is None or fx.empty or res is None or res.empty:
        return [], 0
    # `_aliased` then `_resolve_in_frame`, which is exactly the path
    # build_request takes. An earlier version called `_match_team` directly and
    # reported 19.3% of fixtures blocked — it was measuring the resolver with
    # the alias table switched off, so every name the table already fixes was
    # counted as a failure.
    seen: dict[str, int] = defaultdict(int)
    for _, r in fx.iterrows():
        for side in ("home", "away"):
            n = str(r[side])
            if _resolve_in_frame(res, _aliased(code, res, n)) is None:
                seen[n] += 1
    return sorted(seen.items(), key=lambda kv: -kv[1]), len(fx)


def elsewhere(code: str, name: str) -> list:
    """Leagues OTHER than `code` whose store carries this club.

    A promoted club is not a name failure — the spelling is fine, the history is
    just filed one division down. Le Mans has 328 rows in FRA-L2 and a Ligue 1
    fixture list; Elversberg has 102 in GER-B2. The resolver is right to return
    None, because the club genuinely is not in that league's results, and no
    alias can fix it: the fix is to look one division down.
    """
    hits = []
    canon = _canonical(name)
    for other in store.available_leagues():
        if other == code:
            continue
        df = store.load_results(other)
        if df is None or df.empty:
            continue
        for nm in set(df["home"]) | set(df["away"]):
            if _canonical(str(nm)) == canon:
                r = df[(df["home"] == nm) | (df["away"] == nm)]
                hits.append((other, str(nm), len(r), str(r["date"].max())[:10]))
    return hits


def splits(code: str) -> list:
    res = store.load_results(code)
    if res is None or res.empty:
        return []
    names = _team_names(res)
    canon = [(n, _canonical(n)) for n in names]
    out, used = [], set()
    for i, (a, ca) in enumerate(canon):
        if a in used:
            continue
        group = [a]
        for b, cb in canon[i + 1:]:
            if b in used or ca == cb:
                continue
            if fuzz.token_set_ratio(ca, cb) >= SPLIT_CUTOFF:
                group.append(b)
        if len(group) > 1:
            counts = [(n, rows_for(res, n)) for n in group]
            # A group is only interesting when merging would change something:
            # some side has to be under the gate the engine actually applies.
            if min(c for _, c in counts) < MIN_MATCHES * 4:
                out.append(sorted(counts, key=lambda kv: -kv[1]))
                used.update(group)
    return out


def main() -> None:
    args = sys.argv[1:]
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    split_only = "--split-only" in args

    if not split_only:
        print("UNRESOLVED FIXTURE NAMES — data may exist under another spelling\n")
        print(f"{'league':9}{'fixtures':>10}{'blocked':>9}  names")
        tot_fx = tot_bad = 0
        for code in codes:
            try:
                bad, n_fx = unresolved(code)
            except Exception as exc:
                print(f"{code:9} FAILED {exc}", file=sys.stderr)
                continue
            if not n_fx:
                continue
            blocked = sum(c for _, c in bad)
            tot_fx += n_fx
            tot_bad += blocked
            if bad:
                shown = ", ".join(f"{n} ({c})" for n, c in bad[:6])
                print(f"{code:9}{n_fx:10}{blocked:9}  {shown}")
                for n, c in bad:
                    for lg, nm, rows, last in elsewhere(code, n):
                        print(f"{'':28}{n} -> {lg} as {nm!r}, "
                              f"{rows} rows to {last}  (PROMOTED, not a name)")
        print(f"\n{tot_bad} of {tot_fx} upcoming fixtures blocked by an "
              f"unresolved name "
              f"({tot_bad / tot_fx * 100:.1f}%)" if tot_fx else "")

    print("\n\nSPLIT-CLUB CANDIDATES — one club under two stored spellings\n")
    n_groups = 0
    for code in codes:
        try:
            g = splits(code)
        except Exception as exc:
            print(f"{code:9} FAILED {exc}", file=sys.stderr)
            continue
        for grp in g:
            n_groups += 1
            print(f"  {code:9}" + "   ".join(f"{n} [{c}]" for n, c in grp))
    print(f"\n{n_groups} candidate groups. Confirm each before adding to "
          f"config/team_merges.json.")


if __name__ == "__main__":
    main()
