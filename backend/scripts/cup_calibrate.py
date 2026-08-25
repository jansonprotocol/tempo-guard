"""
Can any constant save the cup path? The gap, decomposed and swept.

The 25 Aug replay measured the cup family at −11.4 and the by-market cut
localised the disease: the base-rate rung (`U4.25`, −1.6) is fine while every
rung that spends per-fixture information is broken (`O2.25` −23.0). That is
the signature of a mu whose DEVIATIONS from the competition baseline carry
far less signal than domestic deviations do — the spread is mostly noise, and
MU_SHRINK = 0.35 (tuned on domestic leagues) keeps far too much of it.

Two questions, answered from one expensive pass and one free sweep:

  SLOPE   regress actual totals on the raw (un-shrunk) cup mu. The slope is
          the fraction of the spread that is real. Domestic leagues measured
          0.42 and earned k = 0.35. If cups measure near zero, no shrink can
          help and only a cross-league strength model (a build, not a
          constant) reopens the board. Negative would mean anti-signal, the
          IRL-PD disease at continental scale.

  SWEEP   re-shrink the captured raw mu at k from 0.35 down to 0.0, re-select
          the tip offline exactly as market_select would (modules are inert,
          measured 0.00 contribution, so the approximation is stated and
          small), and re-grade. Two windows come free: the older and newer
          half of each competition's fixtures, split by date. A k that
          calibrates BOTH windows with a non-degenerate market mix is a
          candidate to re-enable cups; anything less and the switch stays off.

The capture back-solves the raw mu from the served one — mu_served =
base + k·(raw − base) is invertible while k > 0 — so the expensive pass runs
the engine exactly as live, fallback search and all.

Usage:  python scripts/cup_calibrate.py [--n 1200]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, features, store
from app.engine import market_select
from app.predict import build_request
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.team_shrink_sweep import wilson

MAIN = ("UCL", "UEL", "UECL")
KS = (0.35, 0.25, 0.15, 0.10, 0.05, 0.0)


def collect(n: int) -> list[tuple]:
    features.CUP_TIPS_ENABLED = True
    out = []
    for code in MAIN:
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        base = features.INTL_GOAL_AVERAGES[code]
        cfg = config.get(code)
        rows = df.sort_values("date").tail(n)
        half = rows["date"].iloc[len(rows) // 2]
        for _, r in rows.iterrows():
            d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
            try:
                req = build_request(code, str(r["home"]), str(r["away"]), d)
            except Exception:
                continue
            if req is None or not req.mu_total:
                continue
            k = features.MU_SHRINK
            raw = base + (req.mu_total - base) / k
            window = "new" if r["date"] >= half else "old"
            out.append((code, window, raw, base,
                        int(r["hg"]), int(r["ag"]),
                        cfg.max_under_line, cfg.min_over_line))
    return out


def choose(mu: float, base: float, max_u, min_o):
    """The selector, offline: highest-probability playable rung over the
    floor, ties to the softer settlement — market_select's behaviour with the
    inert modules omitted."""
    best = None
    for m, _e, p, _q in market_select.score_markets(mu, base):
        if not market_select.playable(m, max_u, min_o):
            continue
        if p < market_select.MIN_WIN_PROB:
            continue
        if best is None or p > best[1] + 1e-9:
            best = (m, p)
    return best


def grade(rows, k) -> dict:
    got = []
    for code, window, raw, base, hg, ag, mx, mn in rows:
        mu = base + k * (raw - base) if k > 0 else base
        pick = choose(mu, base, mx, mn)
        if pick is None:
            continue
        m, p = pick
        res = evaluate_market(m, hg, ag)
        if res is None:
            continue
        got.append((window, m, p, hit_weight(res) >= 1.0))
    return got


def show(label, rows):
    if len(rows) < 30:
        print(f"  {label:12} {len(rows):5} tips — too few")
        return
    kk = sum(1 for r in rows if r[3])
    hit, says = kk / len(rows), sum(r[2] for r in rows) / len(rows)
    w = wilson(kk, len(rows))
    print(f"  {label:12} {len(rows):5} tips   says {says*100:5.1f}%   "
          f"hit {hit*100:5.1f}%   gap {(hit-says)*100:+5.1f}   "
          f"[{w[0]*100:.0f}-{w[1]*100:.0f}]")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 1200

    rows = collect(n)
    print(f"{len(rows)} cup fixtures captured\n")

    # The slope: how much of the raw spread is real.
    xs = [r[2] - r[3] for r in rows]              # raw mu minus baseline
    ys = [(r[4] + r[5]) - r[3] for r in rows]     # actual minus baseline
    mx, my = sum(xs) / len(xs), sum(ys) / len(ys)
    sxx = sum((x - mx) ** 2 for x in xs)
    sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
    slope = sxy / sxx
    se = math.sqrt(max(0.0, (sum((y - my) ** 2 for y in ys) / sxx - slope ** 2)
                        / (len(xs) - 2)))
    print(f"actual = {my - slope * mx:+.3f} + {slope:.3f} x raw-mu deviation"
          f"   (se {se:.3f}, domestic leagues measured 0.42)\n")

    for k in KS:
        got = grade(rows, k)
        top = {}
        for _w, m, _p, _h in got:
            top[m] = top.get(m, 0) + 1
        mix = "  ".join(f"{m}:{c}" for m, c in
                        sorted(top.items(), key=lambda x: -x[1])[:3])
        print(f"CUP_MU_SHRINK = {k:.2f}   mix {mix}")
        for w in ("old", "new"):
            show(w, [r for r in got if r[0] == w])


if __name__ == "__main__":
    main()
