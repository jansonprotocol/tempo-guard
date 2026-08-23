"""
Is the EDGE number a usable sorting key, or just decoration?

Every tip is published with an edge: the probability the engine attaches, minus
the base rate that market wins in that league regardless of fixture. On the
current slate it ranges from +0.1% to +5.8%, and the volume problem is really a
question about that spread — if I can only afford five bets, are the +4% tips
actually better than the +0.5% ones, or is the ordering noise?

That is testable without a single bookmaker price. Replay each league as-of,
bucket every tip by its OWN stated edge, and compare what each band claimed to
what it delivered:

    says      mean probability the engine attached to tips in this band
    hit       what landed, push counted as a hit
    gap       hit - says. Calibration. Near zero in every band means the
              probabilities are honest across the range.
    base      base rate of the markets this band actually chose
    REAL      hit - base. The edge that was actually there.

The band structure is the whole point. If REAL rises with the stated edge, the
number sorts and low-edge tips should be skipped rather than bought cheap. If
REAL is flat across bands, the engine cannot tell its good tips from its
mediocre ones and the ranking is worthless — in which case volume should be
bought by price alone.

Also reported per band: the break-even price the band's own hit rate implies.
That is what a bet in this band has to be bought at to make money, computed
from realised settlement rather than from the claimed probability.

Usage:  python scripts/edge_bands.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

DEFAULT_N = 150
BANDS = [(-9, 1.0), (1.0, 2.0), (2.0, 3.5), (3.5, 99)]
BAND_NAMES = ["under +1%", "+1 to +2%", "+2 to +3.5%", "over +3.5%"]

MARKETS = ("O1.5", "O1.75", "O2.25", "O2.5", "O2.75",
           "U2.5", "U2.75", "U3.25", "U3.5", "U3.75", "U4.25", "U3.0", "U4.5")


def base_rates(code: str) -> dict[str, float]:
    """How often each market wins in this league regardless of fixture."""
    df = store.load_results(code)
    if df is None or df.empty:
        return {}
    out = {}
    for m in MARKETS:
        w = [hit_weight(evaluate_market(m, int(h), int(a)))
             for h, a in zip(df["hg"], df["ag"])]
        w = [x for x in w if x >= 0]
        out[m] = sum(1 for x in w if x >= 1.0) / len(w) if w else 0.0
    return out


def collect(league: str, n: int) -> list[tuple[float, float, bool, float, str]]:
    """(stated edge, p, hit, base rate, market) for every tip the league issues."""
    df = store.load_results(league)
    if df is None or len(df) < 200:
        return []
    rates = base_rates(league)
    cfg = config.get(league)
    flags = ModuleFlags(**(cfg.module_overrides or {}))

    rows = []
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(league, str(r["home"]), str(r["away"]), d)
            if req is None:
                continue
            mk = predict_fixture(req, cfg, module_flags=flags).translated_play.market
        except Exception:
            continue
        if not mk:
            continue
        res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
        if res is None:
            continue
        p = market_select.p_win(mk, req.mu_total)
        base = rates.get(mk)
        if base is None:
            continue
        rows.append(((p - base) * 100, p, res is True or res == "half_win",
                     base, mk))
    return rows


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if not n:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - m) / d, (c + m) / d)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else DEFAULT_N
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))

    rows = []
    for lg in codes:
        try:
            rows += collect(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    print(f"{len(rows)} tips across {len(codes)} leagues\n")

    print(f"{'stated edge':14}{'n':>6}{'says':>8}{'hit':>8}{'gap':>7}"
          f"{'base':>8}{'REAL':>8}{'95% CI':>13}{'needs':>8}")
    for (lo, hi), name in zip(BANDS, BAND_NAMES):
        b = [r for r in rows if lo <= r[0] < hi]
        if len(b) < 30:
            continue
        k = sum(1 for r in b if r[2])
        hit = k / len(b)
        says = sum(r[1] for r in b) / len(b)
        base = sum(r[3] for r in b) / len(b)
        w = wilson(k, len(b))
        print(f"{name:14}{len(b):6}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+7.1f}{base*100:7.1f}%{(hit-base)*100:+7.1f}"
              f"   [{w[0]*100:.0f}-{w[1]*100:.0f}]{1/hit:8.3f}")

    k = sum(1 for r in rows if r[2])
    hit = k / len(rows)
    base = sum(r[3] for r in rows) / len(rows)
    print(f"\n{'ALL':14}{len(rows):6}"
          f"{sum(r[1] for r in rows)/len(rows)*100:7.1f}%{hit*100:7.1f}%"
          f"{(hit-sum(r[1] for r in rows)/len(rows))*100:+7.1f}"
          f"{base*100:7.1f}%{(hit-base)*100:+7.1f}")


if __name__ == "__main__":
    main()
