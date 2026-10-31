"""
Does the LEAGUE still say anything once the profile has spoken?

The bettor, 31 Oct: "Is there some merit in have a look at league level
performance inside those profiles. For example: Laliga matches in strong
watch hits X% or Premier league inside strong hits x%. Not band
specific, just profile overall."

The question has a sharp form, because league is ALREADY an engine
input — config/league_hitrates.tsv feeds the card before any profile
exists. So a league that still runs under its profile's rate is not a
discovery that some leagues are harder; it is the engine UNDER-
CORRECTING for one, which is a different and more useful thing.

Measuring it needs care that eyeballing a grid does not give:

  STRATIFY BY PROFILE. Leagues are not spread evenly across profiles —
  MLS is thick in the watch families, BRA-SB in the play ones — so a
  raw league rate mostly reports which profiles that league lands in.
  Every league is compared against what its OWN profile mix predicts,
  leave-one-out so a league never helps set the rate it is judged by.

  THEN MAKE IT SURVIVE TWO TESTS. Roughly 35 leagues clear the volume
  bar, so at the 5% level about two will look significant for nothing.
  A survivor has to run the same direction in SEVERAL profiles (which
  are near enough independent samples of that league) AND in both
  halves of the record. One spike in one profile in one month is the
  trap this exists to avoid — on the first run SUI-SL had the largest
  z of any league, +2.63, entirely from thirteen cards in a single
  profile, all of them before 10 Oct.

    python scripts/league_profile.py              the whole report
    python scripts/league_profile.py --table      just profile x league
    python scripts/league_profile.py --min 20     raise the volume bar

READ-ONLY. Nothing here moves a card, a tier or a band.
"""
from __future__ import annotations

import collections
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import profile_mine as pm, webapp as w  # noqa: E402

MIN_LEAGUE_N = 15     # cards before a league is tested at all
MIN_CELL_N = 8        # ...before a profile x league cell is printed
MIN_STRATUM = 5       # ...before a league counts inside one profile
Z_FLAG = 2.0          # the ~5% level, before any correction


def play_profiles() -> set:
    """The profiles bought at a price the feed printed. Everywhere else
    the pill is bought live at a price the feed never saw, so its feed
    ROI says nothing (CLAUDE.md, 14 Oct) and this prints none."""
    return {w.PROFILES[0], w.PROFILES[1]}


def rate(rs: list[dict]) -> tuple[int, int, float]:
    n = len(rs)
    h = sum(r["hit"] for r in rs)
    return n, h, (100 * h / n if n else 0.0)


def expected(rows: list[dict], code: str,
             by_profile: dict[str, list[dict]]) -> tuple[float, float, int]:
    """(expected hits, variance, cards) for one league, stratified by
    profile and leave-one-out."""
    E = V = 0.0
    n = 0
    for prof, rs in by_profile.items():
        mine = [r for r in rs if r["code"] == code]
        if not mine:
            continue
        others = [r for r in rs if r["code"] != code]
        if len(others) < 10:
            continue
        q = sum(r["hit"] for r in others) / len(others)
        E += len(mine) * q
        V += len(mine) * q * (1 - q)
        n += len(mine)
    return E, V, n


def survivors(rows: list[dict], by_profile: dict) -> list[dict]:
    """Leagues whose direction holds across profiles AND across time."""
    rows = sorted(rows, key=lambda r: r["date"])
    # THE SPLIT MUST LEAVE TWO HALVES. Taking the card-count median date
    # outright can land on the LAST date in the record — every card is
    # then "first half", the second is empty, and over_time can never be
    # true for anything. So: the date that divides the cards most evenly
    # among those that are not the last one.
    dates = sorted({r["date"] for r in rows})
    if len(dates) < 2:
        return []
    mid = min(dates[:-1],
              key=lambda d: abs(sum(1 for r in rows if r["date"] <= d)
                                - len(rows) / 2))
    out = []
    for code in sorted({r["code"] for r in rows}):
        mine = [r for r in rows if r["code"] == code]
        if len(mine) < MIN_LEAGUE_N:
            continue
        E, V, n = expected(rows, code, by_profile)
        if V <= 0 or not n:
            continue
        O = sum(r["hit"] for r in mine)
        z = (O - E) / math.sqrt(V)
        # direction inside each profile it appears in, with 5+ cards
        dirs = []
        for prof, rs in by_profile.items():
            k = [r for r in rs if r["code"] == code]
            others = [r for r in rs if r["code"] != code]
            if len(k) < MIN_STRATUM or len(others) < 10:
                continue
            q = 100 * sum(r["hit"] for r in others) / len(others)
            dirs.append(rate(k)[2] - q)
        # ...and in each half of the record
        halves = []
        for sel in (lambda r: r["date"] <= mid, lambda r: r["date"] > mid):
            k = [r for r in mine if sel(r)]
            if len(k) < MIN_STRATUM:
                halves.append(None)
                continue
            e, _v, _n = expected([r for r in rows if sel(r)], code,
                                 {p: [r for r in rs if sel(r)]
                                  for p, rs in by_profile.items()})
            halves.append(100 * (sum(r["hit"] for r in k) - e) / len(k))
        same = (len(dirs) >= 2 and (all(d < 0 for d in dirs)
                                    or all(d > 0 for d in dirs)))
        both = (all(h is not None for h in halves)
                and (all(h < 0 for h in halves) or all(h > 0 for h in halves)))
        out.append(dict(code=code, n=len(mine), hit=100 * O / len(mine),
                        exp=100 * E / len(mine), z=z, profiles=len(dirs),
                        across=same, over_time=both, halves=halves,
                        survives=same and both))
    out.sort(key=lambda d: d["z"])
    return out


def main() -> None:
    args = sys.argv[1:]
    rows = pm.cards()
    if not rows:
        print("no settled counted cards in the record yet")
        return
    minc = int(args[args.index("--min") + 1]) if "--min" in args else MIN_CELL_N
    by_profile = collections.defaultdict(list)
    for r in rows:
        by_profile[r["profile"]].append(r)
    PLAY = play_profiles()

    print(f"{len(rows)} settled counted cards · "
          f"{len({r['code'] for r in rows})} competitions · "
          f"{len(by_profile)} profiles\n")

    print("PROFILE x LEAGUE")
    for prof in sorted(by_profile, key=lambda k: -len(by_profile[k])):
        rs = by_profile[prof]
        n, h, base = rate(rs)
        if n < 40:
            continue
        note = ("feed ROI is real here: bought pre-match" if prof in PLAY
                else "bought live — the feed price was never paid")
        print(f"\n  {prof}   {h}/{n} {base:.1f}%   [{note}]")
        cells = [(rate([r for r in rs if r["code"] == c]), c)
                 for c in {r["code"] for r in rs}]
        for (cn, ch, chr_), c in sorted(
                [(x, c) for x, c in cells if x[0] >= minc],
                key=lambda t: -t[0][2]):
            lo, hi = pm.wilson(ch, cn)
            roi = 100 * sum(r["pl"] for r in rs if r["code"] == c) / cn
            roitxt = f"{roi:+6.1f}%" if prof in PLAY else "      "
            out = " clear" if (lo > base or hi < base) else ""
            print(f"     {c:10} {ch:>3}/{cn:<3} {chr_:5.1f}%  "
                  f"{chr_ - base:+5.1f} vs profile   CI {lo:3.0f}-{hi:3.0f}%  "
                  f"{roitxt}{out}")
    if "--table" in args:
        return

    res = survivors(rows, by_profile)
    print("\n\nLEAGUE AFTER PROFILE — stratified, leave-one-out")
    print(f"  {'league':9} {'n':>4} {'hit':>7} {'expect':>7} {'z':>6} "
          f"{'profiles':>8} {'same way':>9} {'both halves':>11}")
    for d in res:
        flag = "  <<" if abs(d["z"]) >= Z_FLAG else ""
        print(f"  {d['code']:9} {d['n']:>4} {d['hit']:6.1f}% "
              f"{d['exp']:6.1f}% {d['z']:+6.2f} {d['profiles']:>8} "
              f"{'yes' if d['across'] else 'no':>9} "
              f"{'yes' if d['over_time'] else 'no':>11}{flag}")
    n_tested = len(res)
    print(f"\n  {n_tested} competitions with {MIN_LEAGUE_N}+ cards. "
          f"At the {Z_FLAG:.0f}-sigma level about "
          f"{n_tested * 0.05:.1f} will look real for nothing, so z alone "
          f"decides nothing.")

    keep = [d for d in res if d["survives"]]
    print("\nSURVIVES BOTH TESTS — same direction in 2+ profiles AND in "
          "both halves")
    if not keep:
        print("  none. Every apparent league effect is one profile or one "
              "stretch.")
    for d in keep:
        way = "UNDER" if d["hit"] < d["exp"] else "OVER"
        print(f"  {d['code']:9} {d['n']:>4} cards  {d['hit']:.1f}% against "
              f"{d['exp']:.1f}% expected  — runs {way} in "
              f"{d['profiles']} profiles, halves "
              f"{d['halves'][0]:+.1f} / {d['halves'][1]:+.1f}")
    print("\n  League is already an engine input (config/league_hitrates.tsv),")
    print("  so a survivor is the engine under-correcting for that league,")
    print("  not news that some leagues are harder. READ-ONLY: nothing here")
    print("  moves a card, a tier or a band.")


if __name__ == "__main__":
    main()
