"""
Two diagnostics that decide whether a new class of toggle is worth building.

A. WHERE DO MISSES COME FROM?
   The engine picks a side (over/under) and a rung on the Asian ladder. A miss
   can therefore mean two very different things:

     wrong rung   the side was right but the line was too tight. U2.75 on a
                  match that ended 3-0 is this: every looser under would have
                  won. Fixable by picking rungs more conservatively.

     wrong side   no rung on the chosen side could have won. U3.5 on a 4-0 is
                  borderline; U4.25 on a 6-1 is not. Fixable only by a better
                  goal signal, not by rung selection.

   The split matters because the two have completely different remedies, and
   the answer determines whether market selection is worth touching at all.

B. DOES TEAM TENDENCY SURVIVE THE ROLLING FORM MODEL?
   Raw team over/under tendency correlates at about r=0.25 pooled — but the
   engine already models scoring through 10-match rolling rates, so much of
   that is presumably already in mu. The honest question is whether a residual
   remains.

   Test: residual = actual_total - mu for every match, attributed to both
   teams. Split each team's matches chronologically in half, and correlate the
   first-half mean residual against the second-half mean residual across teams.
   A team-level bias that is real and unmodelled shows up as a positive
   split-half correlation. Noise refit to itself does not — that is the whole
   point of splitting rather than fitting the same matches twice.

   If r is near zero, populating `team_nudges` would be fitting noise and the
   field should stay empty.
"""
from __future__ import annotations

import math
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.data.features import TEMPO_BASE, TEMPO_SPAN
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1",
    "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL",
    "SCO-PL", "AUT-BL", "SUI-SL", "DEN-SL", "SWE-AL",
    "NOR-EL", "POL-EK", "CZE-FL", "BRA-SA", "ARG-PD",
    "MEX-LMX", "MLS", "JPN-J1", "CHN-SL", "COL-PA",
]
LIMIT = 400

# The loosest rung the engine can reach on each side. A miss that even these
# would not have saved is a wrong side, not a wrong rung.
LOOSEST_OVER = "O1.5"      # wins at 2+
LOOSEST_UNDER = "U4.25"    # wins at <=4

# One rung looser than each market the engine emits.
ONE_LOOSER = {
    "O2.75": "O2.5", "O2.5": "O2.25", "O2.25": "O1.75", "O1.75": "O1.5",
    "U2.5": "U2.75", "U2.75": "U3.25", "U3.25": "U3.5",
    "U3.5": "U3.75", "U3.75": "U4.25",
}


def won(market: str, total: int) -> bool:
    """Grade a market against a total under the full-win convention."""
    return hit_weight(evaluate_market(market, total, 0)) >= 1.0


def main() -> None:
    # ── Collected across every league ────────────────────────────────
    n_all = n_hit = 0
    miss_rung = miss_side = 0
    saved_by_one = 0
    market_counts: dict[str, list[int]] = defaultdict(lambda: [0, 0])
    residual_by_team: dict[tuple[str, str], list[tuple[object, float]]] = defaultdict(list)
    per_league: dict[str, list[int]] = {}
    looser_all_hit = 0

    for code in LEAGUES:
        try:
            cfg = config.get(code)
            flags = ModuleFlags(**(cfg.module_overrides or {}))
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 60:
            print(f"{code}: skipped (only {len(pairs)} replayable)", flush=True)
            continue

        l_all = l_hit = l_rung = l_side = 0

        for req, (hg, ag) in pairs:
            total = hg + ag
            pred = predict_fixture(req, cfg, module_flags=flags)
            market = pred.translated_play.market
            w = hit_weight(evaluate_market(market, hg, ag))
            if w < 0:
                continue

            hit = w >= 1.0
            n_all += 1
            l_all += 1
            slot = market_counts[market]
            slot[1] += 1

            if hit:
                n_hit += 1
                l_hit += 1
                slot[0] += 1
            else:
                # A: is a looser rung on the same side enough?
                loosest = LOOSEST_OVER if market.startswith("O") else LOOSEST_UNDER
                if won(loosest, total):
                    miss_rung += 1
                    l_rung += 1
                else:
                    miss_side += 1
                    l_side += 1
                nxt = ONE_LOOSER.get(market)
                if nxt and won(nxt, total):
                    saved_by_one += 1

            # What would the whole book look like one rung looser?
            nxt = ONE_LOOSER.get(market, market)
            looser_all_hit += int(won(nxt, total))

            # B: residual against the engine's expected total
            mu = TEMPO_BASE + req.tempo_index * TEMPO_SPAN
            resid = total - mu
            residual_by_team[(code, req.home_team)].append((req.match_date, resid))
            residual_by_team[(code, req.away_team)].append((req.match_date, resid))

        per_league[code] = [l_hit, l_all, l_rung, l_side]
        print(
            f"{code:8s} n={l_all:4d}  hit {l_hit / l_all:6.1%}   "
            f"misses: rung {l_rung:3d}  side {l_side:3d}",
            flush=True,
        )

    if not n_all:
        print("no data")
        return

    misses = n_all - n_hit
    print("\n" + "=" * 68)
    print("A. MISS ANATOMY")
    print("=" * 68)
    print(f"  graded            {n_all}")
    print(f"  hit               {n_hit}  ({n_hit / n_all:.1%})")
    print(f"  missed            {misses}")
    if misses:
        print(f"    wrong rung      {miss_rung:4d}  ({miss_rung / misses:.1%} of misses) "
              f"— a looser line on the same side would have won")
        print(f"    wrong side      {miss_side:4d}  ({miss_side / misses:.1%} of misses) "
              f"— no line on that side could have won")
        print(f"    saved by one    {saved_by_one:4d}  ({saved_by_one / misses:.1%} of misses) "
              f"— exactly one rung looser was enough")
    print(f"\n  ceiling if the side is always right: {(n_hit + miss_rung) / n_all:.1%}")
    print(f"  book shifted one rung looser:        {looser_all_hit / n_all:.1%}  "
          f"(vs {n_hit / n_all:.1%} now)")

    print("\n  by market:")
    for m, (h, s) in sorted(market_counts.items(), key=lambda x: -x[1][1]):
        print(f"    {m:8s} {h:4d}/{s:4d}  {h / s:6.1%}")

    # ── B ────────────────────────────────────────────────────────────
    print("\n" + "=" * 68)
    print("B. RESIDUAL TEAM BIAS (does tendency survive rolling form?)")
    print("=" * 68)

    MIN_PER_TEAM = 24
    firsts, seconds, weights = [], [], []
    per_league_pairs: dict[str, list[tuple[float, float]]] = defaultdict(list)

    for (code, team), rows in residual_by_team.items():
        if len(rows) < MIN_PER_TEAM:
            continue
        rows.sort(key=lambda r: r[0])
        half = len(rows) // 2
        a = float(np.mean([r[1] for r in rows[:half]]))
        b = float(np.mean([r[1] for r in rows[half:]]))
        firsts.append(a)
        seconds.append(b)
        weights.append(len(rows))
        per_league_pairs[code].append((a, b))

    if len(firsts) < 10:
        print(f"  only {len(firsts)} teams with >={MIN_PER_TEAM} matches — inconclusive")
        return

    r = float(np.corrcoef(firsts, seconds)[0, 1])
    n = len(firsts)
    # Fisher z 95% interval
    z = 0.5 * math.log((1 + r) / (1 - r)) if abs(r) < 1 else 0.0
    se = 1 / math.sqrt(n - 3)
    lo = math.tanh(z - 1.96 * se)
    hi = math.tanh(z + 1.96 * se)

    print(f"  teams with >={MIN_PER_TEAM} matches: {n}")
    print(f"  split-half correlation of residual: r = {r:+.3f}  "
          f"(95% CI {lo:+.3f} .. {hi:+.3f})")
    print(f"  spread of first-half means:  sd = {np.std(firsts):.3f} goals")
    print(f"  spread of second-half means: sd = {np.std(seconds):.3f} goals")

    if hi < 0.10:
        verdict = "no usable residual — team_nudges would fit noise"
    elif r < 0.15:
        verdict = "marginal — any nudge must be shrunk hard toward zero"
    else:
        verdict = "real residual — a shrunk team nudge is worth testing"
    print(f"  verdict: {verdict}")

    print("\n  per league (teams, r):")
    for code, prs in sorted(per_league_pairs.items()):
        if len(prs) < 8:
            continue
        a = [p[0] for p in prs]
        b = [p[1] for p in prs]
        rr = float(np.corrcoef(a, b)[0, 1]) if np.std(a) and np.std(b) else float("nan")
        print(f"    {code:8s} {len(prs):3d} teams   r = {rr:+.3f}")


if __name__ == "__main__":
    main()
