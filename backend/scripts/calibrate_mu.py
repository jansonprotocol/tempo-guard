"""
Where does the overconfidence come from, and what is the single fix?

The dispersion test settled one thing already: Poisson describes real total-goal
counts to within half a point at every rung traded (272,857 matches,
`scripts/dispersion.py`). So `p_win(market, mu)` is not wrong ABOUT mu — if mu
were right, the probability would be right.

That leaves mu. Two ways it can be wrong:

    BIAS         mu is systematically too high or too low. Easy to see, easy to
                 fix, and it does NOT explain the symptom: the engine
                 over-predicts goals by ~0.16, which would make Unders look
                 harder than they are and therefore OVER-perform. They
                 under-perform.

    OVER-SPREAD  mu is unbiased on average but too extreme per fixture — a
                 low-tempo read is called lower than it turns out, a high one
                 higher. Every tip is selected precisely on those extremes,
                 because that is where the edge is, so the tip book inherits
                 the whole error while the league average looks fine.

Over-spread is the hypothesis this script tests, by regressing what actually
happened on what was predicted:

    actual_total = a + b * mu_pred

    b = 1    the engine's spread is right
    b < 1    mu is too extreme; shrink it toward the league mean by b
    b > 1    mu is too timid

If b < 1 the correction is one line — replace mu with
`league_mu + b * (mu - league_mu)` — and it is applied at the point where every
probability is derived, so it fixes the tips, the break-even prices and the
buy-from thresholds at once.

Usage:  python scripts/calibrate_mu.py [--n 200] [--leagues MLS,ENG-CH]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.predict import build_request

DEFAULT_N = 200


def collect(league: str, n: int) -> list[tuple[float, float, int]]:
    """(mu predicted, league mu, actual total) for the last n results."""
    df = store.load_results(league)
    if df is None or len(df) < 200:
        return []
    out = []
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if req is None:
            continue
        out.append((req.mu_total, req.league_mu, int(r["hg"]) + int(r["ag"])))
    return out


def ols(xs: list[float], ys: list[float]) -> tuple[float, float]:
    n = len(xs)
    mx = sum(xs) / n
    my = sum(ys) / n
    sxx = sum((x - mx) ** 2 for x in xs)
    if sxx == 0:
        return (my, 0.0)
    b = sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / sxx
    return (my - b * mx, b)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else DEFAULT_N
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))

    pooled: list[tuple[float, float, int]] = []
    print(f"{'league':9}{'n':>5}{'mean mu':>9}{'mean act':>10}"
          f"{'bias':>8}{'slope b':>9}   shrink to")
    for lg in codes:
        rows = collect(lg, n)
        if len(rows) < 50:
            continue
        pooled += rows
        mu = [r[0] for r in rows]
        act = [float(r[2]) for r in rows]
        _, b = ols(mu, act)
        print(f"{lg:9}{len(rows):5}{sum(mu)/len(mu):9.3f}{sum(act)/len(act):10.3f}"
              f"{(sum(act)-sum(mu))/len(mu):+8.3f}{b:9.3f}   {b*100:.0f}%",
              flush=True)

    if not pooled:
        return
    mu = [r[0] for r in pooled]
    act = [float(r[2]) for r in pooled]
    a, b = ols(mu, act)
    print(f"\nPOOLED  n={len(pooled)}")
    print(f"  mean mu {sum(mu)/len(mu):.3f}   mean actual {sum(act)/len(act):.3f}"
          f"   bias {(sum(act)-sum(mu))/len(mu):+.3f}")
    print(f"  actual = {a:.3f} + {b:.3f} * mu")
    print(f"  -> the engine's spread should be scaled to {b*100:.0f}% "
          f"of what it currently claims")

    # What the correction does to the extremes, which is where tips live.
    lo = sorted(pooled, key=lambda r: r[0])[: len(pooled) // 5]
    hi = sorted(pooled, key=lambda r: r[0])[-len(pooled) // 5:]
    for lab, grp in (("lowest mu quintile", lo), ("highest mu quintile", hi)):
        m = sum(r[0] for r in grp) / len(grp)
        t = sum(r[2] for r in grp) / len(grp)
        print(f"  {lab:22} says {m:.2f}   actual {t:.2f}   miss {t-m:+.2f}")


if __name__ == "__main__":
    main()
