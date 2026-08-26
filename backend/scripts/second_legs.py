"""
Does the tie change the game? Second legs, by scenario.

Athena prices one match and cannot see the aggregate — the board says so.
This asks whether that blind spot costs anything: do second legs deviate
from expectation once you know the tie state, the strength profile, and
which side hosted first?

Four questions, each cut the same way — mean deviation of the actual
total from the competition's as-of rolling baseline, with a standard
error, and split into an older and a newer half:

  LEAD      how far ahead the leg-2 HOME side is on aggregate (from -3
            to +3). The hypothesis: a big lead means a dead rubber and
            fewer goals; a deficit means a chase and more.
  PROFILE   the Elo shape (home stronger / even / home weaker), and the
            lead crossed with it — the user's scenario: an even tie
            where the stronger side hosts the decider.
  SCHEDULE  did the stronger club host the first leg or the second? The
            order is fixed before a ball is kicked, so if it matters it
            is knowable at tip time.
  HALF      for the frames carrying half-time scores, what the leg-2
            first half does to the second: when the side that needs
            nothing goes ahead, do the goals dry up?

Two-legged ties are found by pairing reversed fixtures of the same clubs
in the same season and competition, 3-28 days apart, excluding group and
league phases (those reverse fixtures are not a tie). Elo cuts cover
2023-03 onward, where the ratings exist; the lead cut runs on everything.

Usage:  python scripts/second_legs.py
"""
from __future__ import annotations

import math
import sys
from collections import defaultdict
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import club_elo, store
from scripts.cup_asof import CUPS

GROUPY = ("group", "gruppe", "league", "final")   # not two-legged ties
ELO_FROM = pd.Timestamp("2023-04-01")


def ties():
    """Every second leg in the store, with its first leg attached."""
    out = []
    for code in CUPS:
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        df = df.dropna(subset=["hg", "ag"]).sort_values("date")
        has_ht = "hthg" in df.columns
        by = defaultdict(list)
        for r in df.itertuples():
            stage = str(getattr(r, "stage", "") or "").lower()
            if any(g in stage for g in GROUPY):
                continue
            key = (getattr(r, "season", ""), frozenset((str(r.home),
                                                        str(r.away))))
            by[key].append(r)
        for rows in by.values():
            if len(rows) != 2:
                continue
            one, two = rows
            gap = (two.date - one.date).days
            if not (3 <= gap <= 28) or str(one.home) != str(two.away):
                continue
            base_df = df[df.date < two.date]
            w = base_df[base_df.date >= two.date - pd.Timedelta(days=1095)]
            if len(w) < 40:
                w = base_df
            if len(w) < 30:
                continue
            out.append(dict(
                code=code, date=two.date,
                home=str(two.home), away=str(two.away),
                # aggregate entering leg 2, from the leg-2 home side's view
                lead=int(one.ag) - int(one.hg),
                total=int(two.hg) + int(two.ag),
                base=float((w.hg + w.ag).mean()),
                hth=(int(two.hthg) if has_ht and not pd.isna(two.hthg)
                     else None),
                hta=(int(two.htag) if has_ht and not pd.isna(two.htag)
                     else None)))
    return sorted(out, key=lambda r: r["date"])


def add_elo(rows):
    for r in rows:
        r["eh"] = r["ea"] = None
        if r["date"] < ELO_FROM:
            continue
        eh = club_elo.elo_asof(r["home"], r["date"])
        ea = club_elo.elo_asof(r["away"], r["date"])
        if eh and ea:
            r["eh"], r["ea"] = eh, ea
    return rows


def cell(label, rows, key="dev"):
    n = len(rows)
    if n < 25:
        print(f"  {label:34} n {n:4}  —")
        return
    vals = [r[key] for r in rows]
    m = sum(vals) / n
    sd = math.sqrt(sum((v - m) ** 2 for v in vals) / (n - 1))
    se = sd / math.sqrt(n)
    t = m / se if se else 0.0
    flag = "  <--" if abs(t) >= 2 else ""
    print(f"  {label:34} n {n:4}  {m:+.3f} goals ± {se:.3f}   t {t:+5.2f}"
          f"{flag}")


def windows(label, rows, key="dev"):
    """The two-window bar: an effect that only exists in one half is a
    story about that half."""
    if len(rows) < 60:
        cell(label, rows, key)
        return
    mid = sorted(r["date"] for r in rows)[len(rows) // 2]
    cell(label + " (older)", [r for r in rows if r["date"] < mid], key)
    cell(label + " (newer)", [r for r in rows if r["date"] >= mid], key)


def main() -> None:
    rows = add_elo(ties())
    for r in rows:
        r["dev"] = r["total"] - r["base"]
    print(f"{len(rows)} second legs found "
          f"({rows[0]['date'].date()} – {rows[-1]['date'].date()}); "
          f"{sum(1 for r in rows if r['eh'])} carry Elo\n")

    print("LEAD — aggregate margin of the leg-2 HOME side")
    for lo, hi, lab in ((-9, -3, "home trails by 3+"), (-2, -2, "trails by 2"),
                        (-1, -1, "trails by 1"), (0, 0, "level"),
                        (1, 1, "leads by 1"), (2, 2, "leads by 2"),
                        (3, 9, "leads by 3+")):
        cell(lab, [r for r in rows if lo <= r["lead"] <= hi])
    print()
    big = [r for r in rows if abs(r["lead"]) >= 2]
    windows("either side 2+ ahead", big)
    windows("home side 2+ ahead", [r for r in rows if r["lead"] >= 2])
    windows("home side 2+ behind", [r for r in rows if r["lead"] <= -2])

    elo = [r for r in rows if r["eh"]]
    if len(elo) >= 100:
        print("\nPROFILE — Elo shape of the tie (leg-2 home side first)")
        def prof(r):
            g = r["eh"] - r["ea"]
            return ("home stronger" if g >= 100 else
                    "home weaker" if g <= -100 else "even")
        for p in ("home stronger", "even", "home weaker"):
            cell(p, [r for r in elo if prof(r) == p])
        print("\n  the scenario asked for: an even tie, decider at home")
        for lab, f in (("even Elo · level aggregate",
                        lambda r: prof(r) == "even" and r["lead"] == 0),
                       ("even Elo · home leads",
                        lambda r: prof(r) == "even" and r["lead"] > 0),
                       ("even Elo · home trails",
                        lambda r: prof(r) == "even" and r["lead"] < 0),
                       ("stronger home · level",
                        lambda r: prof(r) == "home stronger"
                        and r["lead"] == 0),
                       ("stronger home · leads",
                        lambda r: prof(r) == "home stronger"
                        and r["lead"] > 0),
                       ("stronger home · trails",
                        lambda r: prof(r) == "home stronger"
                        and r["lead"] < 0)):
            cell(lab, [r for r in elo if f(r)])

        print("\nSCHEDULE — where the stronger club hosted the decider")
        cell("stronger club home in leg 2",
             [r for r in elo if r["eh"] > r["ea"]])
        cell("stronger club home in leg 1",
             [r for r in elo if r["ea"] > r["eh"]])
        windows("stronger club home in leg 2",
                [r for r in elo if r["eh"] > r["ea"]])

    # THE DECISIVE CUT. Everything above is measured against the plain
    # competition baseline, which knows nothing about who is playing —
    # so a "lead" effect can simply be the strength gap that produced
    # the lead, and the engine already prices strength through Elo. Only
    # deviation from the ENGINE'S OWN mu can justify a new knob.
    if len(elo) >= 100:
        print("\nAGAINST THE ENGINE'S MU — does the tie add anything Elo "
              "has not already priced?")
        priced = []
        for r in elo:
            got = club_elo.cup_mu(r["code"], r["home"], r["away"],
                                  r["date"].date(), None)
            if got:
                r["dev_mu"] = r["total"] - got[0]
                priced.append(r)
        print(f"  ({len(priced)} of {len(elo)} priceable by the live path)")
        for lo, hi, lab in ((-9, -2, "home 2+ behind"), (-1, 1, "tie tight"),
                            (2, 9, "home 2+ ahead")):
            cell(lab, [r for r in priced if lo <= r["lead"] <= hi], "dev_mu")
        cell("stronger club home in leg 2",
             [r for r in priced if r["eh"] > r["ea"]], "dev_mu")
        cell("stronger club home in leg 1",
             [r for r in priced if r["ea"] > r["eh"]], "dev_mu")
        windows("home 2+ ahead", [r for r in priced if r["lead"] >= 2],
                "dev_mu")
        windows("either side 2+ ahead",
                [r for r in priced if abs(r["lead"]) >= 2], "dev_mu")

    ht = [r for r in rows if r["hth"] is not None]
    if len(ht) >= 100:
        print(f"\nHALF — second-half goals in leg 2 ({len(ht)} with a "
              f"half-time score)")
        for r in ht:
            r["sh"] = r["total"] - (r["hth"] + r["hta"])
        print("  (deviation is second-half goals against a 0.55 share of "
              "the baseline)")
        for r in ht:
            r["dev2"] = r["sh"] - 0.55 * r["base"]
        cell("all second halves", ht, "dev2")
        lead_ht = [r for r in ht if r["hth"] > r["hta"]]
        cell("leg-2 home side ahead at HT", lead_ht, "dev2")
        cell("leg-2 away side ahead at HT",
             [r for r in ht if r["hta"] > r["hth"]], "dev2")
        cell("level at HT", [r for r in ht if r["hth"] == r["hta"]], "dev2")
        settled = [r for r in ht
                   if (r["hth"] - r["hta"]) + r["lead"] >= 2]
        cell("tie effectively settled by HT", settled, "dev2")
        windows("tie effectively settled by HT", settled, "dev2")


if __name__ == "__main__":
    main()
