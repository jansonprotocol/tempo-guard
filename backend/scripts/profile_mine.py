"""
Look inside a profile bucket for structure it is hiding.

The bettor, 31 Oct, on the unlabelled plain-watch row — 161 cards,
80.7%, −4.9%: "These are not labeled but do have some volume. Can we
filter it down more and perhaps find some profiles unknown yet.
Hopefully to strengthen bands that are still thin?"

A bucket that size can hide two things, and they want different
answers:

  A SUB-POPULATION THAT DOES NOT BELONG. Usually a rule whose test is
  narrower than the thing it was written about. Worth acting on, because
  it is not a pattern found by looking — it is an existing rule with a
  hole, and the record only confirms it.

  A SUB-SLICE THAT LOOKS GOOD. Almost always noise. Twenty slices of a
  bucket at an 80% base rate will hand you one at 85% for nothing, and
  the eye goes straight to it. So every candidate here is printed with
  a Wilson interval, a first-half/second-half split, and its overlap
  with the other candidates — because four "findings" that share their
  cards are one finding, and usually not even that.

THE NUMBER THAT MATTERS MOST is at the foot of the report: how many
settled cards a cell needs before 85% can be told apart from 80% at
all. It is about 300. Every cell on the Profiles page is far short of
it, which is why he stopped betting the tips and started watching the
table, and why the band mover's MOVE_MIN_N of 30 decides on far less
than it appears to.

    python scripts/profile_mine.py                  the unlabelled bucket
    python scripts/profile_mine.py --profile MILD   any profile, by name
    python scripts/profile_mine.py --min 25         only slices this big
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, forward_settle as fs, webapp as w  # noqa: E402

CAND_HIT = 85.0      # a slice has to beat this to be worth a second look
HOLD_HIT = 82.0      # ...in BOTH halves of the record, not just overall
MIN_N = 20           # and be at least this big to be printed at all
DRAG_MARGIN = 2.5    # points under the bucket before a slice is a drag


def wilson(hits: int, n: int, z: float = 1.96) -> tuple[float, float]:
    """95% interval on a hit rate. Wilson, not normal: at n=30 and 87%
    the normal approximation runs off the end of the scale."""
    if not n:
        return (0.0, 0.0)
    p = hits / n
    d = 1 + z * z / n
    c = (p + z * z / (2 * n)) / d
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n)) / d
    return (100 * (c - m), 100 * (c + m))


def cards() -> list[dict]:
    """Every settled counted card with the features worth slicing on —
    the Profiles page's own population, read at first sight."""
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

    out, seen = [], set()
    for ln in w.FORWARD.read_text(encoding="utf-8").splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 13 or p[1] < w.SESSION_DATE or fs._artefact(p[5]):
            continue
        key = (p[1], p[3])
        if key in seen:
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
        gap = (best / need - 1) * 100
        seven = w.all_seven(f, p[7])
        money = got[0]
        out.append(dict(
            date=p[1], code=p[2], fixture=p[3], lane=p[5], claim=claim,
            label=p[7], best=best, gap=gap, band=w.gap_band(gap), seven=seven,
            side=("O" if p[5].startswith("O")
                  else "U" if p[5].startswith("U") else "R"),
            hit=bool(got[1]),
            pl=money * (best - 1) if money > 0 else money,
            profile=w.profile_name(p[5], p[7], claim, gap, best, seven)))
    out.sort(key=lambda r: r["date"])
    return out


def agg(rs: list[dict]) -> tuple | None:
    n = len(rs)
    if not n:
        return None
    h = sum(r["hit"] for r in rs)
    hr = 100 * h / n
    return (n, h, hr, 100 * sum(r["pl"] for r in rs) / n,
            100 / hr if hr else 0.0, sum(r["best"] for r in rs) / n)


def slices(rs: list[dict]) -> dict[str, list[dict]]:
    """The dimensions worth asking about, fixed in advance. Fishing one
    more out of the data after seeing the answer is how a bucket this
    size produces a profile that is not there."""
    out: dict[str, list[dict]] = {}
    for key in ("band", "side", "lane", "label", "code"):
        for v in sorted({r[key] for r in rs}):
            out[f"{key} = {v}"] = [r for r in rs if r[key] == v]
    out["all seven rates"] = [r for r in rs if r["seven"]]
    out["fewer than seven"] = [r for r in rs if not r["seven"]]
    for lo, hi in ((0, 83), (83, 86), (86, 101)):
        out[f"claim {lo}-{hi}"] = [r for r in rs if lo <= r["claim"] < hi]
    for lo, hi in ((0, 1.15), (1.15, 1.25), (1.25, 9)):
        out[f"price {lo}-{hi}"] = [r for r in rs if lo <= r["best"] < hi]
    return out


def main() -> None:
    args = sys.argv[1:]
    want = (args[args.index("--profile") + 1] if "--profile" in args
            else w.PROFILES[9])
    minn = int(args[args.index("--min") + 1]) if "--min" in args else MIN_N

    rows = cards()
    names = sorted({r["profile"] for r in rows})
    # EXACT FIRST. A prefix match quietly folded ★ STRONG FROM into
    # ★ STRONG — 48 extra cards and a different answer — because one
    # profile's name is the start of another's.
    sub = [r for r in rows if r["profile"] == want]
    if not sub:
        near = sorted({r["profile"] for r in rows
                       if r["profile"].lower().startswith(want.lower())})
        if len(near) == 1:
            sub = [r for r in rows if r["profile"] == near[0]]
        elif len(near) > 1:
            print(f"{want!r} matches more than one profile — name one:")
            for n in near:
                print(f"   {n}")
            return
    if not sub:
        print(f"no settled cards in {want!r}. Profiles present:")
        for n in names:
            print(f"   {n}")
        return

    base = agg(sub)
    mid = sub[len(sub) // 2]["date"]
    lo, hi = wilson(base[1], base[0])
    print(f"{sub[0]['profile']}")
    print(f"  {base[1]}/{base[0]}  {base[2]:.1f}%  ROI {base[3]:+.1f}%  "
          f"break-even {base[4]:.2f}  avg price {base[5]:.2f}")
    print(f"  95% interval {lo:.1f}% - {hi:.1f}%   ·   halves split at {mid}")
    print("\n  ROI is at the FEED's best price and runs 5-10 points better "
          "than a book he can reach.\n")

    cand, tested = {}, 0
    print(f"  {'slice':34} {'n':>4} {'hit':>7} {'ROI':>8} {'b/e':>5} | "
          f"{'1st':>11} {'2nd':>11}")
    for name, rs in sorted(slices(sub).items(), key=lambda kv: -len(kv[1])):
        a = agg(rs)
        if not a or a[0] < minn:
            continue
        tested += 1
        e, l = (agg([r for r in rs if r["date"] <= mid]),
                agg([r for r in rs if r["date"] > mid]))
        good = (a[2] >= CAND_HIT and e and l
                and e[2] >= HOLD_HIT and l[2] >= HOLD_HIT)
        # A DRAG: materially under the bucket and under it in BOTH
        # halves. The margin is 2.5 points because a slice inside one
        # point of the bucket is the bucket; the both-halves test is
        # what stops a single bad fortnight printing as structure.
        bad = (a[2] <= base[2] - DRAG_MARGIN and e and l
               and e[2] <= base[2] and l[2] <= base[2])
        if good:
            cand[name] = rs
        print(f"  {'*' if good else '!' if bad else ' '} "
              f"{name[:32]:32} {a[0]:>4} {a[2]:6.1f}% {a[3]:+7.1f}% "
              f"{a[4]:5.2f} | "
              f"{(f'{e[1]}/{e[0]} {e[2]:.0f}%' if e else '-'):>11} "
              f"{(f'{l[1]}/{l[0]} {l[2]:.0f}%' if l else '-'):>11}")
    print(f"\n  * clears {CAND_HIT:.0f}% and holds {HOLD_HIT:.0f}%+ in both "
          f"halves   ·   ! sits {DRAG_MARGIN:.1f}+ points under the bucket, "
          f"in both halves")
    print(f"  {tested} slices of {minn}+ cards were tested. At a "
          f"{base[2]:.0f}% base rate, roughly 1 in 20 clears "
          f"{CAND_HIT:.0f}% by chance.")

    if cand:
        print("\nCANDIDATES, with the interval the sample actually supports")
        for name, rs in cand.items():
            a = agg(rs)
            lo, hi = wilson(a[1], a[0])
            sep = "separates" if lo > base[2] else "OVERLAPS the bucket"
            print(f"  {name[:34]:34} {a[1]:>3}/{a[0]:<3} {a[2]:5.1f}%   "
                  f"{lo:5.1f}% - {hi:5.1f}%   {sep}")
        if len(cand) > 1:
            print("\n  and how much they share — candidates that overlap are "
                  "one finding, not several:")
            ks = list(cand)
            ids = {k: {(r["date"], r["fixture"]) for r in v}
                   for k, v in cand.items()}
            for i in range(len(ks)):
                for j in range(i + 1, len(ks)):
                    a_, b_ = ids[ks[i]], ids[ks[j]]
                    print(f"    {ks[i][:26]:26} vs {ks[j][:26]:26} "
                          f"{len(a_ & b_):>3} shared of {len(a_ | b_):>3}")
            union = set().union(*ids.values())
            print(f"    all of them together: {len(union)} distinct cards "
                  f"of {base[0]}")

    print("\nHOW MUCH VOLUME A CELL NEEDS before 85% can be told from 80%")
    for n in (30, 50, 100, 200, 300, 400):
        lo, hi = wilson(round(0.85 * n), n)
        print(f"    n={n:>3}   85% reads {lo:5.1f}% - {hi:5.1f}%   "
              f"{'SEPARATES' if lo > 80 else 'overlaps 80%'}")
    print("    The band mover acts at MOVE_MIN_N="
          f"{_move_min()}, which is inside the noise.")


def _move_min() -> str:
    try:
        from scripts import tier_optimize
        return str(tier_optimize.MOVE_MIN_N)
    except Exception:                                       # noqa: BLE001
        return "?"


if __name__ == "__main__":
    main()
