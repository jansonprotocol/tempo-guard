"""
Do shot-based ratings predict goals better than goal-based ones?

The measurement that prompted this: scored as forecasters over 7,613
fixtures, the market beats Athena by 2.3% of Brier, and only about a third
of that gap is reachable by team news (open-to-close is worth 0.6% while
the gap to the OPENING line is 1.70%). The rest sits in the base model —
in how team strength is estimated before anyone knows the lineup.

The suspicion this tests: the engine's goal expectation is built mainly
from goals, and goals are the noisy realisation. Shots on target regress
far better between matches, which is why shot-based ratings beat
goals-only ones in every published comparison. The store already holds
`hst`/`ast` for every fixture, so this costs a replay and nothing else.

The test is deliberately a FAIR one. Both models share the same functional
form, the same rolling window, the same shrinkage toward the league mean
and the same Poisson scoring. The ONLY difference is the statistic each
one regresses: goals, or shots on target converted at the league's own
fitted rate. Anything that beats the other therefore beats it because of
the statistic, not because of tuning.

Strictly as-of: every fixture is predicted from matches STRICTLY EARLIER
than its own date, in one forward pass per league, so nothing can leak.

    python scripts/shot_ratings.py                 every league
    python scripts/shot_ratings.py --leagues E0,I1
    python scripts/shot_ratings.py --window 40     matches of memory per team

Reports Brier and log loss on U1.5/U2.5/U3.5/U4.5 — four lines neither
model selects — split into two time windows, because a result that does
not hold in both halves is not a result.
"""
from __future__ import annotations

import math
import sys
from collections import defaultdict, deque
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store

LINES = (1.5, 2.5, 3.5, 4.5)
DEFAULT_WINDOW = 40          # matches of memory per team, per venue
MIN_SEEN = 8                 # a team predicts nothing until it has this many
PRIOR = 6.0                  # matches' worth of shrinkage toward the league


def _pois_le(mu: float, k: int) -> float:
    """P(total <= k) for a Poisson — the under side of a whole line."""
    t = math.exp(-mu)
    s = t
    for i in range(1, k + 1):
        t *= mu / i
        s += t
    return min(max(s, 1e-9), 1 - 1e-9)


class Roll:
    """Rolling per-team attack and defence, kept separately by venue."""

    def __init__(self, window: int):
        self.w = window
        self.f = defaultdict(lambda: deque(maxlen=window))   # for
        self.a = defaultdict(lambda: deque(maxlen=window))   # against

    def rate(self, team: str, venue: str, lg_for: float) -> tuple:
        """(attack, defence) as multipliers of the league mean, shrunk."""
        F, A = self.f[(team, venue)], self.a[(team, venue)]
        if len(F) < MIN_SEEN:
            return None
        af = (sum(F) + PRIOR * lg_for) / (len(F) + PRIOR)
        ad = (sum(A) + PRIOR * lg_for) / (len(A) + PRIOR)
        return (af / lg_for if lg_for else 1.0,
                ad / lg_for if lg_for else 1.0)

    def push(self, team: str, venue: str, for_: float, against: float) -> None:
        self.f[(team, venue)].append(for_)
        self.a[(team, venue)].append(against)


def replay(code: str, window: int) -> list:
    """One forward pass: predict each fixture from strictly earlier ones."""
    df = store.load_results(code)
    if df is None or len(df) < 200:
        return []
    need = ("hst", "ast")
    if not all(c in df.columns for c in need):
        return []
    rows = df.dropna(subset=["hg", "ag", "hst", "ast"]).sort_values("date")
    if len(rows) < 200:
        return []

    goals = Roll(window)
    shots = Roll(window)
    # League baselines, as-of AND rolling. An all-time cumulative mean was
    # the first version and it quietly favoured goals: both models share the
    # goal means, but only the shots model also depends on the shots-to-goals
    # CONVERSION, so a baseline stale by fifteen years of changing recording
    # standards penalised one side of the comparison and not the other.
    LGW = 500
    lg = {k: deque(maxlen=LGW) for k in ("hg", "ag", "hst", "ast")}
    lg["cg"] = deque(maxlen=LGW)
    lg["cs"] = deque(maxlen=LGW)
    out = []

    for _i, r in rows.iterrows():
        h, a = str(r["home"]), str(r["away"])
        hg, ag = float(r["hg"]), float(r["ag"])
        hs, as_ = float(r["hst"]), float(r["ast"])

        n = len(lg["hg"])
        if n >= 60:
            mh, ma = sum(lg["hg"]) / n, sum(lg["ag"]) / n
            sh, sa = sum(lg["hst"]) / n, sum(lg["ast"]) / n
            tot_s = sum(lg["cs"])
            conv = (sum(lg["cg"]) / tot_s) if tot_s else 0.0
            # Each side's ATTACK is measured where it is playing and meets
            # the opponent's DEFENCE measured where THEY are playing: the
            # home team's home attack against the away team's away defence,
            # and the mirror. Both models use this identical form, so only
            # the statistic differs.
            gh, ga = goals.rate(h, "H", mh), goals.rate(a, "A", ma)
            sh_, sa_ = shots.rate(h, "H", sh), shots.rate(a, "A", sa)
            if gh and ga and sh_ and sa_ and conv > 0:
                mu_goals = mh * gh[0] * ga[1] + ma * ga[0] * gh[1]
                exp_sot = sh * sh_[0] * sa_[1] + sa * sa_[0] * sh_[1]
                mu_shots = exp_sot * conv
                out.append((r["date"], hg + ag, mu_goals, mu_shots, h, a))

        lg["hg"].append(hg); lg["ag"].append(ag)
        lg["hst"].append(hs); lg["ast"].append(as_)
        lg["cg"].append(hg + ag); lg["cs"].append(hs + as_)
        goals.push(h, "H", hg, ag); goals.push(a, "A", ag, hg)
        shots.push(h, "H", hs, as_); shots.push(a, "A", as_, hs)
    return out


def score(rows: list) -> dict:
    """Brier and log loss for both models over the four lines."""
    acc = {"goals": [0.0, 0.0], "shots": [0.0, 0.0]}
    n = 0
    for _d, total, mg, ms, *_ in rows:
        for L in LINES:
            y = 1.0 if total <= L else 0.0
            for tag, mu in (("goals", mg), ("shots", ms)):
                p = _pois_le(mu, int(L))
                acc[tag][0] += (p - y) ** 2
                acc[tag][1] -= y * math.log(p) + (1 - y) * math.log(1 - p)
            n += 1
    if not n:
        return {}
    return {k: (v[0] / n, v[1] / n) for k, v in acc.items()} | {"n": n}


def main() -> None:
    args = sys.argv[1:]
    window = int(args[args.index("--window") + 1]) if "--window" in args \
        else DEFAULT_WINDOW
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))

    allrows = []
    for c in codes:
        try:
            rows = replay(c, window)
        except Exception as exc:
            print(f"{c:9} FAILED {exc}", file=sys.stderr)
            continue
        if len(rows) < 100:
            continue
        s = score(rows)
        allrows += rows
        d = (s["goals"][0] - s["shots"][0]) / s["goals"][0] * 100
        print(f"  {c:9} {s['n']//len(LINES):5} fixtures   goals "
              f"{s['goals'][0]:.5f}   shots {s['shots'][0]:.5f}   "
              f"{d:+6.2f}%{'  shots' if d > 0 else ''}", flush=True)

    if not allrows:
        print("nothing to score")
        return
    allrows.sort(key=lambda r: r[0])
    half = len(allrows) // 2
    print(f"\n  window {window} matches per team per venue\n")
    print("  slice                n        Brier(goals)  Brier(shots)   "
          "logloss(g)  logloss(s)")
    for tag, sel in (("all", allrows), ("older half", allrows[:half]),
                     ("newer half", allrows[half:])):
        s = score(sel)
        d = (s["goals"][0] - s["shots"][0]) / s["goals"][0] * 100
        print(f"  {tag:18} {s['n']//len(LINES):6}      {s['goals'][0]:.5f}"
              f"       {s['shots'][0]:.5f}      {s['goals'][1]:.5f}"
              f"     {s['shots'][1]:.5f}   {d:+.2f}%")
    print("\n  a positive percentage means SHOTS beat goals; the result only")
    print("  counts if both halves agree.\n")

    # Neither statistic is the whole story: shots carry the signal, goals
    # carry finishing quality. Sweep the mixture and let both windows vote.
    def bscore(rows, w):
        t = 0.0
        n = 0
        for _d, total, mg, ms, *_ in rows:
            mu = w * ms + (1 - w) * mg
            for L in LINES:
                y = 1.0 if total <= L else 0.0
                p = _pois_le(mu, int(L))
                t += (p - y) ** 2
                n += 1
        return t / n
    print("  blend of the two goal expectations (w = weight on SHOTS)")
    print("   w      all         older half   newer half")
    base = bscore(allrows, 0.0)
    best = None
    for w in [i / 10 for i in range(11)]:
        a = bscore(allrows, w)
        o = bscore(allrows[:half], w)
        nn = bscore(allrows[half:], w)
        if best is None or a < best[1]:
            best = (w, a, o, nn)
        print(f"  {w:.1f}   {a:.5f}      {o:.5f}     {nn:.5f}"
              f"   {100 * (base - a) / base:+.2f}%")
    print(f"\n  best mixture w={best[0]:.1f} -> {best[1]:.5f}, "
          f"{100 * (base - best[1]) / base:+.2f}% against goals alone")


if __name__ == "__main__":
    main()
