"""Does the board's own search history predict which tips fail?

The bettor's idea, from the Arsenal card that lost: after Athena prints a
tip, run it back through the searches a human would run by hand — the
league's tip-1 record, then the record filtered to each club, then to the
tip's SIDE, then to club-and-side — and count how many of those slices sit
above or below the league baseline. Each slice that reads worse is a flag
against the tip; each that reads better is a flag for it. Sum the flags and
the card gets a colour: super green through super red.

It is a conditional base-rate rule, and the honest way to test it is to
build every slice STRICTLY AS-OF. The board's search bar looks backwards
over completed matches, so a slice it shows today includes the very fixture
you would have been betting on. Here every counter is fed only by cards
dated strictly EARLIER than the one being scored, in one forward pass over
the whole dump, so a score could actually have been computed before
kickoff.

Two scorers are measured side by side:

  sign     the bettor's rule exactly — +1 per slice above the league
           baseline, -1 per slice below it, outside a deadband
  shrunk   the same slices, but each one pulled toward the baseline by an
           empirical-Bayes prior before it is allowed to vote, so a
           49-match slice cannot shout as loudly as a 400-match one

Reports the tip-1 hit rate per score, split into two time windows, plus
what tip 2 and tip 3 did on the cards the score condemns — because the
proposal is not only to flag those cards but to reroute them.

    python scripts/confluence.py --dump cf.pkl
    python scripts/confluence.py --dump cf.pkl --dead 3 --k 40

The dump comes from:  scripts/final_pick.py --n 600 --dump cf.pkl
"""
from __future__ import annotations

import math
import pickle
import re
import sys
from collections import defaultdict

MIN_SLICE = 15        # a slice below this many cards does not get a vote
MIN_LEAGUE = 100      # a league below this many cards is not scored at all
DEAD = 2.0            # points of deadband around the league baseline
KPRIOR = 25.0         # empirical-Bayes prior, in cards, for the shrunk mode

SIDE = re.compile(r"^([OU])\d")


def _side(mk: str | None) -> str | None:
    m = SIDE.match(mk or "")
    return m.group(1) if m else None


class Counter:
    """As-of hits and n, keyed by whatever tuple is handed in."""

    def __init__(self) -> None:
        self.d: dict = defaultdict(lambda: [0, 0])

    def rate(self, key, floor: int):
        h, n = self.d[key]
        return (h / n, n) if n >= floor else (None, n)

    def add(self, key, hit: bool) -> None:
        c = self.d[key]
        c[0] += bool(hit)
        c[1] += 1


# Which field carries the grade, the claim and the market, per lane. Tip
# 1 is a MATCH total so both clubs are in it; tip 2 is a TEAM total, so
# only the club it names is — TA is the home side, TB the away one.
LANE = {1: ("hit_t1", "p1", "mk1"), 2: ("hit_t2", "p2", "mk2")}


def teams(r: dict, lane: int) -> list:
    if lane == 1:
        return [r["h"], r["a"]]
    mk = r.get("mk2") or ""
    return [r["h"]] if mk.startswith("TA") else [r["a"]] if mk.startswith("TB") \
        else [r["h"], r["a"]]


def slices(r: dict, lane: int = 1) -> list:
    """The searches a human would run on this card, as counter keys.

    Deliberately the SAME six the bettor ran by hand on the Arsenal card:
    the league, each club, the side, and each club crossed with the side.
    """
    L = r["code"]
    mk = (r.get(LANE[lane][2]) or "").replace("TA ", "").replace("TB ", "")
    s = _side(mk)
    out = [("team", (L, t)) for t in teams(r, lane)]
    if s:
        out += [("side", (L, s))]
        out += [("ts", (L, t, s)) for t in teams(r, lane)]
    return out


def score_card(r: dict, C: dict, mode: str, dead: float, k: float,
               lane: int = 1):
    """(score, votes) for one card from strictly-earlier history."""
    _hf, claimf, _mf = LANE[lane]
    base, bn = C["lg"].rate((r["code"],), MIN_LEAGUE)
    if base is None:
        return None, 0
    votes = 0
    total = 0.0

    def vote(rate: float, n: int) -> None:
        nonlocal votes, total
        if mode in ("shrunk", "noclaim"):
            rate = (rate * n + base * k) / (n + k)
        d = (rate - base) * 100
        if abs(d) <= dead:
            return
        votes += 1
        total += 1.0 if d > 0 else -1.0

    # Flag zero is the card's own CLAIM against the league's proven record
    # — the bettor's first check, and the only one that needs no history
    # beyond the league itself.
    if mode != "noclaim" and r.get(claimf) is not None:
        d = (r[claimf] - base) * 100
        if abs(d) > dead:
            votes += 1
            total += 1.0 if d > 0 else -1.0
    for kind, key in slices(r, lane):
        rate, n = C[kind].rate(key, MIN_SLICE)
        if rate is not None:
            vote(rate, n)
    return (total if votes else 0.0), votes


def walk(rows: list, mode: str, dead: float, k: float, lane: int = 1) -> list:
    """One forward pass: score each card, then let it feed the counters."""
    hitf = LANE[lane][0]
    C = {kind: Counter() for kind in ("lg", "team", "side", "ts")}
    rows = sorted(rows, key=lambda r: (r["d"], r["code"]))
    out = []
    i = 0
    while i < len(rows):
        # Everything on the same DATE is scored before any of it is
        # counted, so a card can never inform its own slate.
        j = i
        while j < len(rows) and rows[j]["d"] == rows[i]["d"]:
            j += 1
        day = rows[i:j]
        for r in day:
            if r.get(hitf) is None:
                continue
            sc, votes = score_card(r, C, mode, dead, k, lane)
            if sc is None:
                continue
            out.append(dict(r, score=int(sc), votes=votes))
        for r in day:
            if r.get(hitf) is None:
                continue
            C["lg"].add((r["code"],), r[hitf])
            for kind, key in slices(r, lane):
                C[kind].add(key, r[hitf])
        i = j
    return out


def rate(sel: list, field: str = "hit_t1"):
    g = [x for x in sel if x.get(field) is not None]
    if not g:
        return None, 0
    return sum(bool(x[field]) for x in g) / len(g), len(g)


def se(p: float, n: int) -> float:
    return 100 * math.sqrt(max(p * (1 - p), 1e-9) / n)


def report(scored: list, label: str, lane: int = 1) -> None:
    hitf = LANE[lane][0]
    scored.sort(key=lambda r: r["d"])
    half = len(scored) // 2
    windows = (("older", scored[:half]), ("newer", scored[half:]))
    lo = min(r["score"] for r in scored)
    hi = max(r["score"] for r in scored)
    allp, alln = rate(scored, hitf)
    print(f"\n  {label}   {alln} cards, tip {lane} overall "
          f"{allp*100:.2f}%")
    print(f"   score      n     tip{lane}      vs all     older"
          "     newer      tip2      tip3")
    for s in range(hi, lo - 1, -1):
        sel = [r for r in scored if r["score"] == s]
        p, n = rate(sel, hitf)
        if n < 40:
            continue
        o = rate([r for r in windows[0][1] if r["score"] == s], hitf)
        w = rate([r for r in windows[1][1] if r["score"] == s], hitf)
        p2, n2 = rate(sel, "hit_t2")
        p3, n3 = rate(sel, "hit_t3")
        print(f"  {s:+3d}   {n:6}   {p*100:6.2f}%  {(p-allp)*100:+7.2f}"
              f"   {(o[0] or 0)*100:7.2f}   {(w[0] or 0)*100:7.2f}"
              f"   {(p2 or 0)*100:7.2f}   {(p3 or 0)*100:7.2f}"
              f"   ±{se(p, n):.2f}")

    # The proposal's actual payload: a cut, not a colour. What does the
    # book look like if the worst scores never get played?
    print("\n   cut               kept        tip1     lift    older    newer")
    for thr in range(lo, hi):
        keep = [r for r in scored if r["score"] > thr]
        drop = [r for r in scored if r["score"] <= thr]
        if len(drop) < 100 or len(keep) < 500:
            continue
        p, n = rate(keep, hitf)
        o = rate([r for r in windows[0][1] if r["score"] > thr], hitf)
        w = rate([r for r in windows[1][1] if r["score"] > thr], hitf)
        print(f"   drop score <= {thr:+d}   {n:6} ({100*n/alln:4.1f}%)"
              f"  {p*100:6.2f}%  {(p-allp)*100:+6.2f}"
              f"  {(o[0] or 0)*100:7.2f} {(w[0] or 0)*100:7.2f}"
              f"   dropped {len(drop)} at "
              f"{(rate(drop, hitf)[0] or 0)*100:.2f}%")


BANDS = ((0.0, 0.76), (0.76, 0.80), (0.80, 0.84), (0.84, 1.01))
BUCKETS = (("<=-2", lambda s: s <= -2), ("-1", lambda s: s == -1),
           ("0", lambda s: s == 0), ("+1", lambda s: s == 1),
           (">=+2", lambda s: s >= 2))


def stratify(scored: list, lane: int = 1) -> None:
    """The question that decides whether this is a NEW signal.

    The shipped guard is already a claim threshold, so a score that only
    works because low scores land on low-claim cards has added nothing.
    Hold the claim roughly fixed and see whether the score still moves the
    hit rate INSIDE each band.
    """
    hitf, claimf, _ = LANE[lane]
    print("\n   the score INSIDE claim bands — is it more than a proxy "
          f"for p{lane}?")
    print("   claim band        n" + "".join(f"{b:>11}" for b, _ in BUCKETS))
    for lo, hi in BANDS:
        band = [r for r in scored
                if r.get(claimf) is not None and lo <= r[claimf] < hi]
        if len(band) < 200:
            continue
        bp, bn = rate(band, hitf)
        cells = []
        for _lbl, f in BUCKETS:
            p, n = rate([r for r in band if f(r["score"])], hitf)
            cells.append(f"{p*100:5.1f}/{n:<5}" if n >= 60 else f"{'—':>11}")
        print(f"   {lo:.2f}-{hi:.2f} ({bp*100:5.2f}%) {bn:6}"
              + "".join(f"{c:>11}" for c in cells))


def tier(r: dict) -> str:
    """The shipped risk guard, so the two can be cross-tabulated.

    Kept in step with the version measured on 1 Sep: a gated DNB or a
    high, modest-edge tip 1 is green; a low claim, or a middling claim on
    an OVER, is red; everything else orange.
    """
    if r.get("pick") == 3:
        return "green"
    p, e = r.get("p1"), r.get("e1")
    if p is None:
        return "orange"
    if p >= 0.84 and (e is None or e < 0.01):
        return "green"
    if p < 0.76 or (p < 0.80 and _side(r.get("mk1")) == "O"):
        return "red"
    return "orange"


def crosstab(scored: list) -> None:
    """Can the score rescue cards the guard condemns, or condemn cards it
    passes? That is the bettor's actual proposal, so it gets its own
    table rather than being inferred from two separate ones."""
    print("\n   guard tier x confluence score  (tip 1 hit rate / n)")
    print("   tier      overall" + "".join(f"{b:>12}" for b, _ in BUCKETS))
    for t in ("green", "orange", "red"):
        sel = [r for r in scored if tier(r) == t]
        if len(sel) < 200:
            continue
        p, n = rate(sel, "hit_t1")
        cells = []
        for _lbl, f in BUCKETS:
            q, m = rate([r for r in sel if f(r["score"])], "hit_t1")
            cells.append(f"{q*100:5.1f}/{m:<6}" if m >= 60 else f"{'—':>12}")
        print(f"   {t:8} {p*100:5.2f}/{n:<6}" + "".join(f"{c:>12}"
                                                        for c in cells))


def main() -> None:
    args = sys.argv[1:]

    def opt(flag, cast, default):
        return cast(args[args.index(flag) + 1]) if flag in args else default

    path = opt("--dump", str, "cf.pkl")
    dead = opt("--dead", float, DEAD)
    k = opt("--k", float, KPRIOR)
    rows = pickle.loads(open(path, "rb").read())
    rows = [r for r in rows if r.get("h")]
    print(f"{len(rows)} cards carrying both clubs")
    for lane, modes in ((1, ("sign", "shrunk", "noclaim")), (2, ("shrunk",))):
        for mode in modes:
            scored = walk(rows, mode, dead, k, lane)
            if len(scored) < 500:
                continue
            report(scored, f"TIP {lane} · {mode}  (deadband {dead:g}pt"
                           + (f", prior {k:g})" if mode != "sign" else ")"),
                   lane)
            stratify(scored, lane)
            if lane == 1:
                crosstab(scored)


if __name__ == "__main__":
    main()
