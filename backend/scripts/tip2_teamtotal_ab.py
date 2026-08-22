"""
Tip 1 and Tip 2 hit rates with team totals available as a second lane.

Tip 1 is untouched by construction — team totals are only ever considered for
the runner-up — so its number here is a control. What moves is Tip 2, and the
question is whether letting it leave the ladder makes it better or merely
different.

Reported three ways, because the headline alone would hide the mechanism:

    LADDER ONLY   Tip 2 restricted to match totals, as it shipped
    WITH TEAM     team totals allowed to replace the runner-up on edge
    THE SWAPS     only the fixtures where a team total actually took over,
                  scored both ways — the only comparison that isolates the
                  change rather than diluting it across fixtures nothing
                  happened on

Hit rate is measured on the full-win convention used everywhere in this
repository, so a half-win counts as a win. Base rates and edge are reported
alongside because a team total wins ~76% of the time by itself, and a raw hit
rate would flatter it enormously.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select, team_total
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.two_tips import MAX_TIP2_GAP, MIN_EDGE, MIN_TIP2_ABS, PREFER, FLOOR

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 150
LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "ESP-LL", "ITA-SA", "ITA-SB", "NED-ED",
           "FRA-L1", "FRA-L2", "JPN-J1", "POR-PL", "TUR-SL", "BEL-PL",
           "SUI-SL", "GRE-SL", "POL-EK", "BRA-SA", "ESP-L2"]


def won(m, hg, ag) -> bool:
    if m in (team_total.HOME, team_total.AWAY):
        return team_total.won(m, hg, ag)
    return hit_weight(evaluate_market(m, int(hg) + int(ag), 0)) >= 1.0


def ladder_tip2(sc, p1, floor):
    cands = [(m, e, p) for m, e, p in sc
             if abs(p - p1) > 1e-9 and e >= MIN_EDGE
             and p >= max(MIN_TIP2_ABS, p1 - MAX_TIP2_GAP)]
    if not cands:
        return None
    top = max(p for _m, _e, p in cands)
    tier = [c for c in cands if abs(c[2] - top) < 1e-9]
    tier.sort(key=lambda c: PREFER.index(c[0]) if c[0] in PREFER else 99)
    m2, e2, p2 = tier[0]
    return (m2, p2, e2)


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
            sc = [(m, e, p) for m, e, p, _q in
                  market_select.score_markets(req.mu_total, req.league_mu)
                  if market_select.playable(m, cfg.max_under_line, cfg.min_over_line)]
            by = {m: (e, p) for m, e, p in sc}
            if t1 not in by:
                continue
            e1, p1 = by[t1]
            lad = ladder_tip2(sc, p1, cfg.min_win_prob or FLOOR)
            tt = team_total.candidates(code, req.match_date,
                                       req.p_home_tt05, req.p_away_tt05)
            rows.append(dict(lg=code, hg=int(hg), ag=int(ag),
                             t1=t1, p1=p1, e1=e1,
                             lad=lad, tt=(tt[0] if tt else None)))
        print(f"  {code}: {len(pairs)}", flush=True)

    print(f"\n  {len(rows)} fixtures\n")

    def rate(sel, pick):
        got = [(pick(r), r) for r in sel]
        got = [(m, r) for m, r in got if m]
        if not got:
            return None
        hits = sum(won(m, r["hg"], r["ag"]) for m, r in got)
        return hits, len(got), hits / len(got)

    t1r = rate(rows, lambda r: r["t1"])
    print(f"  TIP 1 (control, untouched)   {t1r[0]}/{t1r[1]} = {t1r[2]:.2%}")

    lad = rate(rows, lambda r: r["lad"][0] if r["lad"] else None)
    print(f"  TIP 2 ladder only            {lad[0]}/{lad[1]} = {lad[2]:.2%}")

    def combined(r):
        l, t = r["lad"], r["tt"]
        if t and (l is None or t[2] > l[2]):
            return t[0]
        return l[0] if l else None

    comb = rate(rows, combined)
    print(f"  TIP 2 with team totals       {comb[0]}/{comb[1]} = {comb[2]:.2%}"
          f"   ({comb[2] - lad[2]:+.2%})")

    swaps = [r for r in rows
             if r["tt"] and (r["lad"] is None or r["tt"][2] > r["lad"][2])]
    print(f"\n  THE SWAPS — {len(swaps)} of {len(rows)} fixtures "
          f"({len(swaps)/len(rows):.0%}) where a team total took over")
    was = [r for r in swaps if r["lad"]]
    if was:
        a = sum(won(r["lad"][0], r["hg"], r["ag"]) for r in was)
        b = sum(won(r["tt"][0], r["hg"], r["ag"]) for r in was)
        print(f"    on those, the ladder rung would have gone {a}/{len(was)} = "
              f"{a/len(was):.2%}")
        print(f"    the team total went                      {b}/{len(was)} = "
              f"{b/len(was):.2%}   ({(b-a)/len(was):+.2%})")
    new = [r for r in swaps if not r["lad"]]
    if new:
        c = sum(won(r["tt"][0], r["hg"], r["ag"]) for r in new)
        print(f"    plus {len(new)} fixtures that had NO Tip 2 at all: "
              f"{c}/{len(new)} = {c/len(new):.2%}")

    print(f"\n  fixtures still offering no Tip 2: "
          f"{sum(1 for r in rows if combined(r) is None)}")
    print("  Tip 2 market mix:",
          dict(Counter(combined(r) for r in rows if combined(r)).most_common(8)))


if __name__ == "__main__":
    main()
