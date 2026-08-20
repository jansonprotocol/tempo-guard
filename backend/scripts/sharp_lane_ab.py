"""
What is the sharp lane actually worth, and can the probability model do better?

The sharp lane has never been examined. It fires when a fixture sits 0.7 sigma
from its league's scoring norm and the lean agrees, then hands out one of three
hardcoded markets — O2.5, U2.75 or U3.25 — chosen by an if/else on the under
guard rather than by anything the goal model says. That is the same disconnect
just removed from the safe lane: a signal computes a goal expectation and the
market choice ignores it.

The replacement costs nothing to build, because the safe lane already proved
the machinery. `choose()` with a HIGH probability floor picks safe lines; the
same call with a LOW floor maximises edge and takes volatile ones. That was the
first version's behaviour — 60% strike at +2.53% edge — which is a poor safe
lane and a perfectly sensible aggressive one.

So the two lanes become one mechanism at two settings, which is what the
original two-lane brief described.

Measured here, for the current lane and for probability floors:

    fire rate   how often a sharp play is offered at all. A lane that fires on
                every fixture is not sharp, and one that never fires is not a
                lane.
    strike      hit rate when it does fire.
    edge        against the base rate of the markets it picks, since value is
                the sharp lane's job even though the safe lane is judged on
                strike.

Nothing is written. The engine is untouched until the numbers justify it.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "FRA-L2",
    "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL", "SCO-PL",
    "SUI-SL", "CZE-FL", "FIN-VL", "IRL-PD", "ENG-CH", "ENG-L2",
    "SCO-CH", "BRA-SA", "ARG-PD", "COL-PA", "MEX-LMX", "MLS",
    "JPN-J1", "CHN-SL",
]
LIMIT = 500
SHARP_FLOORS = [0.50, 0.55, 0.60, 0.65, 0.70]

# A sharp play is only worth offering when it differs from the safe one and
# carries meaningfully more edge; otherwise it is the safe lane with a louder
# name.
MIN_EXTRA_EDGE = 0.02


def won(market: str, total: int) -> bool:
    return hit_weight(evaluate_market(market, total, 0)) >= 1.0


def base_of(markets, totals) -> float:
    if not markets or not totals:
        return 0.0
    return sum(
        sum(1 for t in totals if won(m, t)) / len(totals) for m in markets
    ) / len(markets)


def main() -> None:
    rows = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            pred = predict_fixture(req, cfg, module_flags=flags)
            L = getattr(pred, "lanes", None)
            rows.append({
                "code": code,
                "total": hg + ag,
                "safe": pred.translated_play.market,
                "cur_sharp": L.sharp.market if (L and L.sharp) else None,
                "mu": req.mu_total,
                "lmu": req.league_mu,
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return
    print(f"\n{len(rows)} fixtures\n")

    def report(label, picks):
        """picks: list of (market or None, total)"""
        fired = [(m, t) for m, t in picks if m]
        if not fired:
            print(f"  {label:22s} never fires")
            return
        markets = [m for m, _ in fired]
        totals = [t for _, t in picks]
        hits = sum(1 for m, t in fired if won(m, t))
        base = base_of(markets, totals)
        strike = hits / len(fired)
        mk = Counter(markets).most_common(3)
        mix = " ".join(f"{m}:{c * 100 // len(fired)}%" for m, c in mk)
        print(f"  {label:22s} fires {len(fired) / len(picks):5.1%}  "
              f"strike {strike:6.1%}  edge {strike - base:+6.2%}   {mix}")

    print(f"  {'lane':22s} {'fire':>11}  {'strike':>13}  {'edge':>12}   mix")
    print("  " + "-" * 84)

    # Current lane
    report("current (z-score)", [(r["cur_sharp"], r["total"]) for r in rows])

    # Probability lane at several floors
    for fl in SHARP_FLOORS:
        picks = []
        for r in rows:
            got = market_select.choose(r["mu"], r["lmu"], min_win_prob=fl)
            if got is None:
                picks.append((None, r["total"]))
                continue
            market, edge, _p = got
            # Only offer it when it is genuinely sharper than the safe call and
            # the extra risk buys extra edge.
            safe_edge = None
            for m, e, _h, _t in market_select.score_markets(r["mu"], r["lmu"]):
                if m == r["safe"]:
                    safe_edge = e
                    break
            if market == r["safe"] or safe_edge is None:
                picks.append((None, r["total"]))
            elif edge - safe_edge >= MIN_EXTRA_EDGE:
                picks.append((market, r["total"]))
            else:
                picks.append((None, r["total"]))
        report(f"probability @ {fl:.2f}", picks)

    # For reference: what the safe lane did on the same fixtures.
    print()
    report("safe lane (0.79)", [(r["safe"], r["total"]) for r in rows])


if __name__ == "__main__":
    main()
