"""
Does a flat 4% stake fit what this engine actually produces?

Staking is the one part of the system that has never been measured here, and it
is not a detail: the same set of tips can compound or bleed depending only on
what is risked per bet. Two properties of ATHENA's output make the question
sharper than usual.

**The lanes are not the same bet.** Tip 1 strikes ~84% at prices near 1.27; the
team lanes strike ~64% at prices near 1.65. A flat stake treats those as
interchangeable. They are not — the second loses a third of the time, so a run
of five losers is ordinary rather than alarming, and the stake has to survive it.

**The edge is known imprecisely, and that asymmetry is brutal.** Kelly sizing
assumes the probability is right. Ours is measured at -1.5 points out of sample
overall and -2.5 in the high-edge band, and over-betting a Kelly fraction
computed from an optimistic p does not merely reduce growth, it reverses it.
So the simulation is run across three PRICE regimes rather than one. Stressing
the probability alone would have proved nothing about a flat stake, which never
reads the probability — the returns come from real results at real prices and
come out identical. What actually gives way in practice is the price: the
market declines to offer `buy from`, and the margin gets surrendered to get a
bet on. That is the stress that matters.

Method — no distributional assumptions:

1. Replay leagues as-of and record every tip the engine issues, with the goal
   total that actually followed. Each becomes a real per-bet return, settled
   through `pricing.settle_fraction` at that tip's own `buy from` price, so
   pushes and half-wins land as the fractions they really are.
2. Bootstrap sequences of bets from that pool.
3. Run each staking rule down the same sequences and record what happens to a
   bankroll.

Reported per rule: median growth, the bad tail (5th percentile), the worst
drawdown along the way, and how often the bankroll halves — which is the number
that actually ends betting careers.

Usage:  python scripts/staking.py [--n 100] [--paths 4000] [--bets 200]
"""
from __future__ import annotations

import random
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select, pricing
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture

# A spread of leagues rather than all 62: the pool only needs to represent the
# mix of rungs and prices the engine emits, and replaying everything costs an
# hour for a distribution that stops moving after a few thousand tips.
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "GER-B2", "ESP-LL", "ESP-L2",
           "ITA-SA", "ITA-SB", "FRA-L1", "FRA-L2", "NED-ED", "POR-PL",
           "BEL-PL", "TUR-SL", "SCO-PL", "DEN-SL", "POL-EK", "JPN-J1",
           "BRA-SA", "MEX-LMX"]

# The measured out-of-sample calibration gap, applied to the win probability so
# the Kelly rules size off an honest number rather than the published one.
CALIB_GAP = 0.015

RULES = [
    ("flat 1%", ("flat", 0.01)),
    ("flat 2%", ("flat", 0.02)),
    ("flat 4%", ("flat", 0.04)),
    ("flat 8%", ("flat", 0.08)),
    ("quarter Kelly", ("kelly", 0.25)),
    ("half Kelly", ("kelly", 0.50)),
    ("full Kelly", ("kelly", 1.00)),
]


def pool(n: int, gap: float, mult: float = 1.0) -> list[tuple[float, float, float]]:
    """(return per unit staked, win probability, decimal price) for each tip.

    `mult` scales the price paid away from `buy from`. Shrinking the win
    probability alone does NOT stress a flat rule — a flat stake never reads the
    probability, so the returns, which come from real results at real prices,
    come out identical. The honest stress is on the PRICE, because that is the
    thing that actually gives way in practice: mult=1.0 pays break-even exactly
    (the whole 5% margin surrendered), and mult below that is the Radomiak case,
    buying under the zero line to get a bet on.
    """
    out = []
    for lg in LEAGUES:
        df = store.load_results(lg)
        if df is None or len(df) < 200:
            continue
        cfg = config.get(lg)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for _, r in df.sort_values("date").tail(n).iterrows():
            d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
            try:
                req = build_request(lg, str(r["home"]), str(r["away"]), d)
                if req is None:
                    continue
                mk = predict_fixture(
                    req, cfg, module_flags=flags).translated_play.market
                if not mk:
                    continue
                price = pricing.buy_from(mk, req.mu_total) * mult
                p = market_select.p_win(mk, req.mu_total)
            except Exception:
                continue
            total = int(r["hg"]) + int(r["ag"])
            s = pricing.settle_fraction(mk, total)
            # Return per unit staked: the won part pays at the price, the
            # pushed part comes back at evens.
            ret = max(s, 0.0) * price + (1 - abs(s))
            out.append((ret - 1.0, max(0.01, p - gap), price))
    return out


def same_day_correlation() -> None:
    """Do results on one match-day move together?

    This is the assumption every staking rule leans on and nobody checks. Ten
    bets at 4% is only 40% at risk in the sense that matters if the ten are
    roughly independent; if a high-scoring Saturday sinks every `U4.25` at once,
    they are closer to one 40% bet and the rule has to shrink accordingly.

    Measured by comparing the observed variance of a day's hit rate against the
    binomial variance independence would produce. A ratio near 1.0 means
    independent; a large ratio means the day, not the fixture, is the unit of
    risk.
    """
    import statistics as st

    from app.util.asian_lines import evaluate_market

    print("same-day correlation — is a slate one bet or many?\n")
    for mkt in ("U4.25", "O1.5"):
        fracs, ns = [], []
        for lg in LEAGUES:
            df = store.load_results(lg)
            if df is None or df.empty:
                continue
            for _, g in df.groupby(df["date"].dt.date):
                if len(g) < 6:
                    continue
                res = [evaluate_market(mkt, int(h), int(a))
                       for h, a in zip(g["hg"], g["ag"])]
                w = [r is True or r == "half_win" for r in res if r is not None]
                if len(w) < 6:
                    continue
                fracs.append(sum(w) / len(w))
                ns.append(len(w))
        if not fracs:
            continue
        p = sum(f * n for f, n in zip(fracs, ns)) / sum(ns)
        nbar = sum(ns) / len(ns)
        obs = st.pvariance(fracs)
        exp = p * (1 - p) / nbar
        print(f"  {mkt:7}{len(fracs):6} match-days   base {p*100:.1f}%   "
              f"observed var {obs:.5f}   independent {exp:.5f}   "
              f"ratio {obs/exp:.2f}")
    print()


def fraction(rule, p: float, price: float) -> float:
    kind, k = rule
    if kind == "flat":
        return k
    # Kelly on a decimal price: edge over the price, per unit of profit offered.
    b = price - 1.0
    if b <= 0:
        return 0.0
    f = (p * b - (1 - p)) / b
    return max(0.0, min(0.5, f * k))


def run(pl: list, rule, paths: int, bets: int, seed: int = 7):
    rnd = random.Random(seed)
    finals, drawdowns, halved = [], [], 0
    for _ in range(paths):
        bank, peak, worst = 1.0, 1.0, 0.0
        broke = False
        for _ in range(bets):
            ret, p, price = pl[rnd.randrange(len(pl))]
            f = fraction(rule, p, price)
            bank *= 1 + f * ret
            peak = max(peak, bank)
            worst = max(worst, 1 - bank / peak)
            if bank <= 0.5:
                broke = True
        finals.append(bank)
        drawdowns.append(worst)
        halved += broke
    finals.sort()
    drawdowns.sort()
    return (finals[len(finals) // 2], finals[int(len(finals) * 0.05)],
            drawdowns[len(drawdowns) // 2], halved / paths)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 100
    paths = int(args[args.index("--paths") + 1]) if "--paths" in args else 4000
    bets = int(args[args.index("--bets") + 1]) if "--bets" in args else 200

    same_day_correlation()

    # Three price regimes, because the price is what gives way in practice.
    ARMS = [
        ("bought at buy-from (break-even + 5%)", 0.0, 1.000),
        ("bought at break-even exactly (margin surrendered)", CALIB_GAP, 1 / 1.05),
        ("bought 2% UNDER break-even (the Radomiak case)", CALIB_GAP, 0.98 / 1.05),
    ]
    for label, gap, mult in ARMS:
        pl = pool(n, gap, mult)
        if not pl:
            print("no tips in pool")
            return
        edge = sum(r for r, _, _ in pl) / len(pl)
        avg_p = sum(p for _, p, _ in pl) / len(pl)
        avg_p2 = sum(pr for _, _, pr in pl) / len(pl)
        print(f"\n=== {label} ===")
        print(f"{len(pl)} tips   mean return/bet {edge*100:+.2f}%   "
              f"mean p {avg_p*100:.1f}%   mean price {avg_p2:.3f}")
        print(f"\n  {'rule':16}{'median':>9}{'5th pct':>10}{'max DD':>9}"
              f"{'halved':>9}")
        for name, rule in RULES:
            med, p5, dd, hv = run(pl, rule, paths, bets)
            print(f"  {name:16}{med:8.2f}x{p5:9.2f}x{dd*100:8.0f}%"
                  f"{hv*100:8.0f}%")
        print(f"\n  over {bets} bets, {paths} simulated sequences, "
              f"bankroll starts at 1.00")


if __name__ == "__main__":
    main()
