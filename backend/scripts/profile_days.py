"""
Each profile's record ONE DATE AT A TIME.

The bettor, 21 Oct, on STRONG's hitrate falling over the weekend: "we
need to investigate this and see where it goes wrong. Check the daily
hitrate of the profiles, only exactly per date, what was played and
resolve on each date (14-10, 15-10, 16-10, etc), maybe this gives some
insight, as we now only look at an overall picture."

The Profiles page answers for the session. A tier at 86% over seven
weeks says nothing about whether it is losing a point a week or lost
four cards on one Sunday, and those are different problems: the first is
the tier, the second is the weekend. So this prints the same population
the Profiles page counts, cut by the date the match was PLAYED.

THE POPULATION IS THE RECORD'S OWN, not a re-derivation: the FIRST-SIGHT
row of config/forward_log.tsv (everything is locked at first sight,
4 Oct), classified by webapp.profile_name — the one classifier the
Profiles page and scripts/tier_optimize.py also read — joined to the
settled score, with red labels and declined cards out, exactly as
webapp.build_market builds it. ROI is at the feed's BEST price and so is
optimistic for him by 5 to 10 points a band (27 Sep); it is the market's
ROI, never his.

ONE MATCH, ONE ROW. A pair of clubs can hold two board rows — a
postponed card keeps its kickoff for ever while the replay arrives at
its new date — and a match graded onto both counts twice. --dups names
every one of those it can see, because a phantom row moves a tier's
whole rate: on 21 Oct two were in the record, a MISS doubled in
★ STRONG and a HIT doubled in LIVE INPLAY GOOD. The daily table counts
the record AS IT STANDS, so what it prints is what the board prints;
--dedupe prints it again with the phantoms dropped, so the size of the
contamination is visible rather than argued about.

Usage:  python scripts/profile_days.py              every date, every profile
        python scripts/profile_days.py --from 2026-10-14
        python scripts/profile_days.py --profile "★ STRONG"   one profile, carded
        python scripts/profile_days.py --dups       the double-graded matches
        python scripts/profile_days.py --dedupe     the table without them
"""
from __future__ import annotations

import collections
import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, forward_settle as fs, webapp as w  # noqa: E402


def record() -> list[dict]:
    """Every settled counted card at its first-sight call, newest last."""
    fixtures = board.load()
    byday = {(f.kickoff.split(" ")[0], f.teams): f for f in fixtures}
    final = {}
    for f in fixtures:
        if not (f.settled and "—" in f.status):
            continue
        sc = f.status.split("—")[-1].strip().split(" ")[0]
        if "-" not in sc:
            continue
        try:
            hg, ag = sc.split("-")
            final[(f.kickoff.split(" ")[0], f.teams)] = (int(hg), int(ag))
        except ValueError:
            pass

    rows, seen = [], set()
    if not w.FORWARD.exists():
        return rows
    for ln in w.FORWARD.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 13 or p[1] < w.SESSION_DATE or fs._artefact(p[5]):
            continue
        key = (p[1], p[3])
        if key in seen:          # FIRST sight only; a re-render cannot re-price
            continue
        seen.add(key)
        f = byday.get(key)
        if f is None or key not in final or p[7].endswith("red") \
                or w.is_declined(f):
            continue
        try:
            need, best, claim = float(p[9]), float(p[11] or p[10]), float(p[6])
        except ValueError:
            continue
        got = fs._settle(p[5], *final[key])
        if got is None or need <= 0:
            continue
        money = got[0]
        rows.append(dict(
            date=p[1], league=p[2], fixture=p[3], lane=p[5], claim=claim,
            best=best, gap=(best / need - 1) * 100, score=final[key],
            hit=bool(got[1]),
            # the repo's own money convention: a stake fraction, paid at
            # the price, and a loss is the whole stake
            pl=money * (best - 1) if money > 0 else money,
            profile=w.profile_name(p[5], p[7], claim,
                                   (best / need - 1) * 100, best,
                                   w.all_seven(f, p[7])),
            side=("OVER" if p[5].startswith("O") else
                  "UNDER" if p[5].startswith("U") else "result"),
            goals=final[key][0] + final[key][1]))
    rows.sort(key=lambda r: (r["date"], r["fixture"]))
    return rows


def phantoms(rows: list[dict]) -> list[dict]:
    """Rows that are a second grading of a match another row already
    holds — same competition, same clubs, same score, a different date.
    The earliest row keeps the match; the rest are the phantoms."""
    held, out = set(), []
    for r in rows:
        key = (r["league"], r["fixture"], r["score"])
        if key in held:
            out.append(r)
            continue
        held.add(key)
    return out


def agg(rows: list[dict]) -> tuple[int, int, float, float]:
    """(n, hits, hit%, ROI%) — ROI at the feed's best price."""
    n = len(rows)
    if not n:
        return 0, 0, 0.0, 0.0
    h = sum(r["hit"] for r in rows)
    return n, h, 100 * h / n, 100 * sum(r["pl"] for r in rows) / n


def _cell(rows: list[dict], width: int = 22) -> str:
    n, h, rate, roi = agg(rows)
    return f"{h:>3}/{n:<3} {rate:5.1f}% {roi:+6.1f}%" if n else " " * width


def daily(rows: list[dict], cols: list[str]) -> None:
    """One line per date: the whole board, then a column per profile."""
    head = f"{'date':11} {'dow':4} {'every card':>22}"
    for c in cols:
        head += "  | " + f"{c[:20]:>22}"
    print(head)
    for day in sorted({r["date"] for r in rows}):
        rs = [r for r in rows if r["date"] == day]
        dow = dt.date.fromisoformat(day).strftime("%a")
        line = f"{day:11} {dow:4} {_cell(rs)}"
        for c in cols:
            line += "  | " + _cell([r for r in rs if r["profile"] == c])
        print(line)


def carded(rows: list[dict], profile: str) -> None:
    """Every card of one profile, so a bad day can be read rather than
    inferred — the lane, the claim, the price, the gap and the score."""
    rs = [r for r in rows if r["profile"] == profile]
    if not rs:
        print(f"no settled counted cards for {profile!r}")
        return
    print(f"{profile} — {_cell(rs).strip()}")
    for r in rs:
        print(f"  {r['date']} {'HIT ' if r['hit'] else 'MISS'} "
              f"{r['league']:8} {r['lane']:5} claim {r['claim']:5.1f}% "
              f"best {r['best']:.2f} gap {r['gap']:+5.1f}% "
              f"{r['score'][0]}-{r['score'][1]}  {r['fixture']}")


def by(rows: list[dict], key: str, label: str) -> None:
    groups = collections.defaultdict(list)
    for r in rows:
        groups[r[key]].append(r)
    print(f"  by {label}:")
    for k, v in sorted(groups.items(), key=lambda kv: -len(kv[1])):
        print(f"    {str(k)[:46]:48} {_cell(v).strip()}")


def main() -> None:
    args = sys.argv[1:]
    rows = record()
    if not rows:
        print("no settled counted cards in the record yet")
        return
    since = ""
    if "--from" in args:
        since = args[args.index("--from") + 1]
        rows = [r for r in rows if r["date"] >= since]
    if not rows:
        print(f"no settled counted cards on or after {since}")
        return
    ghosts = phantoms(rows)
    if "--dedupe" in args:
        drop = {id(g) for g in ghosts}
        rows = [r for r in rows if id(r) not in drop]

    if "--dups" in args:
        if not ghosts:
            print("no match is graded onto two rows")
            return
        print(f"{len(ghosts)} phantom row(s) — one match graded twice:")
        for g in ghosts:
            print(f"  {g['date']} {g['profile']:18} {g['lane']:5} "
                  f"{g['score'][0]}-{g['score'][1]}  {g['fixture']} "
                  f"[{g['league']}]")
        print("\nwhat dropping them does to each profile:")
        drop = {id(g) for g in ghosts}
        clean = [r for r in rows if id(r) not in drop]
        for p in sorted({g["profile"] for g in ghosts}):
            print(f"  {p:20} as counted {_cell([r for r in rows if r['profile'] == p]).strip()}"
                  f"   ->   {_cell([r for r in clean if r['profile'] == p]).strip()}")
        return

    if "--profile" in args:
        carded(rows, args[args.index("--profile") + 1])
        return

    order = [p for _, p in sorted(
        ((-len([r for r in rows if r["profile"] == p]), p)
         for p in {r["profile"] for r in rows}))]
    print("EVERY SETTLED COUNTED CARD BY THE DATE IT WAS PLAYED")
    print("hits/n, hit rate, and ROI at the feed's BEST price — which is "
          "5-10 points\nbetter per band than the books he can reach, so "
          "it is the market's ROI, not his.\n")
    # Four profile columns a line keeps it readable in a terminal.
    for i in range(0, len(order), 4):
        daily(rows, order[i:i + 4])
        print()
    print(f"FROM {since} ON" if since else "WHOLE SESSION")
    for p in order:
        print(f"  {p:62} {_cell([r for r in rows if r['profile'] == p]).strip()}")
    print()
    by(rows, "side", "side of the card")
    if ghosts:
        print(f"\n::warning::{len(ghosts)} match(es) are graded onto two "
              f"board rows and counted twice — python scripts/"
              f"profile_days.py --dups")


if __name__ == "__main__":
    main()
