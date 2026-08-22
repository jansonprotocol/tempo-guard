"""
"On a fallback, take the team lane instead" — what does that rule actually cost?

The argument for it is asymmetric and correct as far as it goes: when Tip 1
loses, swapping can only help or be neutral; the risk is confined to fixtures
where Tip 1 would have won and the lane does not.

That makes marginal hit rates the wrong tool. Tip 1 hits 88% on fallbacks and a
team lane hits 76%, which looks decisive — but the two are not independent bets
on independent events. Both are cut from the same goal expectation, so they win
and lose together far more often than their marginals suggest, and the only
number that decides the rule is the JOINT outcome:

    Tip 1 won,  lane won    swapping changes nothing
    Tip 1 won,  lane lost   swapping COSTS a bet
    Tip 1 lost, lane won    swapping GAINS a bet
    Tip 1 lost, lane lost   swapping changes nothing

    net = gains - costs

Reported for fallbacks (Tip 1 negative edge) and, as a control, for every other
fixture — because a rule that helps on fallbacks and is applied everywhere is a
different rule, and this repository has been fooled by that shape before.
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
            e1 = next((e for m, e, _p, _q in
                       market_select.score_markets(req.mu_total, req.league_mu)
                       if m == t1), 0.0)
            tt = team_total.candidates(code, req.match_date,
                                       req.p_home_tt05, req.p_away_tt05)
            if not tt:
                continue
            hg, ag = int(hg), int(ag)
            rows.append(dict(
                fallback=e1 < 0,
                t1_won=hit_weight(evaluate_market(t1, hg + ag, 0)) >= 1.0,
                lane_won=team_total.won(tt[0][0], hg, ag),
                lane=tt[0][0].split()[1]))
        print(f"  {code}: {len(pairs)}", flush=True)

    print(f"\n  {len(rows)} fixtures with a team lane available\n")

    def block(label, sel):
        if not sel:
            print(f"  {label}: none")
            return
        n = len(sel)
        ww = sum(1 for r in sel if r["t1_won"] and r["lane_won"])
        wl = sum(1 for r in sel if r["t1_won"] and not r["lane_won"])
        lw = sum(1 for r in sel if not r["t1_won"] and r["lane_won"])
        ll = sum(1 for r in sel if not r["t1_won"] and not r["lane_won"])
        print(f"  {label}  (n = {n})")
        print(f"    Tip 1 won,  lane won    {ww:5d}  {ww/n:6.1%}   no change")
        print(f"    Tip 1 won,  lane lost   {wl:5d}  {wl/n:6.1%}   COSTS a bet")
        print(f"    Tip 1 lost, lane won    {lw:5d}  {lw/n:6.1%}   GAINS a bet")
        print(f"    Tip 1 lost, lane lost   {ll:5d}  {ll/n:6.1%}   no change")
        print(f"    Tip 1 alone  {(ww+wl)/n:6.2%}      lane alone  {(ww+lw)/n:6.2%}")
        print(f"    net of swapping  {lw - wl:+5d} bets  = {(lw - wl)/n:+.2%}\n")

    block("FALLBACKS (Tip 1 negative edge)", [r for r in rows if r["fallback"]])
    block("EVERYTHING ELSE (control)", [r for r in rows if not r["fallback"]])

    print("  FALLBACKS, split by which lane the swap would take")
    for lane in ("U1.5", "O1.5", "O0.5"):
        block(f"  lane {lane}", [r for r in rows if r["fallback"] and r["lane"] == lane])


if __name__ == "__main__":
    main()
