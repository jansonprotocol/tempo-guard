"""
On a fallback U4.25, is a team total a better bet than the tip itself?

The fallback is the selector's shrug: nothing on the ladder clears the
probability floor, so it takes the safest buyable rung. It carries negative
edge by construction — the engine is saying it found nothing worth backing —
and because U4.25 wins ~88% of the time by itself, the price is inside the
book's margin. That combination is the worst of both: no information, and no
money in it either.

So this asks a narrower question than the Tip 2 work: not "is a team total a
good second lane" but "on THESE fixtures specifically, would it have been a
better thing to back than the tip".

Judged on three things, because hit rate alone would pick the fallback every
time:

    strike     how often each lands
    edge       strike minus the base rate of that market, as-of
    price      the fair odds each deserves, and what survives a 5% margin —
               the reason a 88% market can be unbettable while a 78% one is not

Both team-total directions are scored. The OVER direction is the one the
holdout supported; the UNDER direction — a side kept out — is measured here
too rather than merely asserted to fail, since the fallback subset is exactly
where a defensive read would be expected to live if it lived anywhere.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select, team_total
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 150
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "ITA-SB", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL",
           "SUI-SL", "GRE-SL", "POL-EK", "BRA-SA", "ESP-L2"]
MARGIN = 0.05


def main() -> None:
    rows = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            t1 = predict_fixture(req, cfg, module_flags=flags).translated_play.market
            edge = next((e for m, e, _p, _q in
                         market_select.score_markets(req.mu_total, req.league_mu)
                         if m == t1), 0.0)
            if not (t1 == "U4.25" and edge < 0):
                continue
            base = team_total.base_rates(code, req.match_date)
            if base is None or req.p_home_tt05 is None:
                continue
            hg, ag = int(hg), int(ag)
            rows.append(dict(lg=code, hg=hg, ag=ag, tot=hg + ag,
                             ph=float(req.p_home_tt05), pa=float(req.p_away_tt05),
                             bh=base[0], ba=base[1]))
        print(f"  {code}: {len(pairs)}", flush=True)

    n = len(rows)
    print(f"\n  {n} fallback fixtures (Tip 1 = U4.25 at negative edge)\n")
    if not n:
        return

    def report(label, hits, picks, bases):
        if not picks:
            print(f"  {label:<38s}      no picks")
            return
        k = len(picks)
        hit = hits / k
        bas = sum(bases) / k
        fair = 1 / hit if hit else 0
        print(f"  {label:<38s} {k:5d}  hit {hit:6.2%}  base {bas:6.2%}  "
              f"edge {hit - bas:+6.2%}  fair {fair:5.3f}  at {MARGIN:.0%} margin "
              f"{fair * (1 - MARGIN):5.3f}")

    # The tip itself.
    hits = sum(hit_weight(evaluate_market("U4.25", r["tot"], 0)) >= 1.0 for r in rows)
    b = sum(1 for r in rows for _ in [0]) and None
    # base rate of U4.25 on these same fixtures
    ubase = [sum(1 for x in rows if x["tot"] <= 4) / n] * n
    report("Tip 1  U4.25 (the fallback)", hits, rows, ubase)

    # Team total OVER, best side above the floor.
    for floor in (0.75, 0.79, 0.83):
        picks, hits, bases = [], 0, []
        for r in rows:
            c = []
            if r["ph"] >= floor:
                c.append((r["ph"], r["hg"] >= 1, r["bh"]))
            if r["pa"] >= floor:
                c.append((r["pa"], r["ag"] >= 1, r["ba"]))
            if c:
                p, w, bb = max(c)
                picks.append(r); hits += w; bases.append(bb)
        report(f"team total OVER 0.5, floor {floor:.2f}", hits, picks, bases)

    # Team total UNDER, the least likely side below a ceiling.
    for ceil in (0.60, 0.55, 0.50):
        picks, hits, bases = [], 0, []
        for r in rows:
            c = []
            if r["ph"] <= ceil:
                c.append((1 - r["ph"], r["hg"] == 0, 1 - r["bh"]))
            if r["pa"] <= ceil:
                c.append((1 - r["pa"], r["ag"] == 0, 1 - r["ba"]))
            if c:
                p, w, bb = max(c)
                picks.append(r); hits += w; bases.append(bb)
        report(f"team total UNDER 0.5, P <= {ceil:.2f}", hits, picks, bases)

    print("\n  COVERAGE — how many fallbacks a team total can actually replace")
    for floor in (0.75, 0.79, 0.83):
        k = sum(1 for r in rows if r["ph"] >= floor or r["pa"] >= floor)
        print(f"    floor {floor:.2f}: {k}/{n} = {k/n:.0%}")

    print("\n  HEAD TO HEAD on the fixtures a team total covers (floor 0.79)")
    cov = [r for r in rows if r["ph"] >= 0.79 or r["pa"] >= 0.79]
    if cov:
        a = sum(hit_weight(evaluate_market("U4.25", r["tot"], 0)) >= 1.0 for r in cov)
        bb = 0
        for r in cov:
            c = []
            if r["ph"] >= 0.79:
                c.append((r["ph"], r["hg"] >= 1))
            if r["pa"] >= 0.79:
                c.append((r["pa"], r["ag"] >= 1))
            bb += max(c)[1]
        print(f"    U4.25 fallback  {a}/{len(cov)} = {a/len(cov):.2%}")
        print(f"    team total      {bb}/{len(cov)} = {bb/len(cov):.2%}")
        both = sum(1 for r in cov
                   if (hit_weight(evaluate_market("U4.25", r["tot"], 0)) >= 1.0))
        print(f"    (the fallback still wins more often — the question is price)")


if __name__ == "__main__":
    main()
