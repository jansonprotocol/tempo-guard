"""
Do the disconnected modules know anything about goals? A small first test.

burst_sentinel, det, ulr, deg and mfr change zero markets out of 998, because
they move the old flowchart's lean scores and the flowchart no longer picks the
market. They are not weak, they are unplugged.

`module_mu_scale` is the one wire that could reconnect them. Their net opinion —
positive when they argue for more goals, negative for fewer — is converted from
lean-score units into goals and added to mu before the market is chosen. At
scale 0.0 nothing changes, which is the current engine exactly.

The question this answers is narrow and worth stating precisely: does that
combined opinion carry information about total goals that mu does not already
have? If it does, five features come back at once. If it does not, the honest
conclusion is that the modules were re-describing the same signal all along and
their disconnection cost nothing.

DELIBERATELY SMALL
==================
Three leagues, not sixteen. A sweep this cheap should be run before anything
wide, and the last two searches here were forty-minute jobs that answered
questions a three-league slice would have answered in three. If the tilt does
nothing across a high-scoring league, a low-scoring one and a noisy one, it will
not start doing something at league fifteen.

Judged on the net flip ledger rather than strike, since a few dozen changed
markets move a strike rate by rounding.

RESULT: THE MODULES CARRY NOTHING. IT FAILS IN BOTH DIRECTIONS.
==============================================================
896 fixtures, ENG-PL / JPN-J1 / FRA-L2.

Applied as designed, the tilt is actively harmful, and monotonically so:

    scale  0.25   changed  12   rescued  0   broken  4   NET  -4
    scale  0.50   changed  27   rescued  1   broken  4   NET  -3
    scale  1.00   changed  57   rescued  1   broken  7   NET  -6
    scale  1.50   changed 110   rescued  2   broken 18   NET -16
    scale  2.00   changed 161   rescued  2   broken 24   NET -22

At scale 2.0 that is 2 rescues against 24 breaks, better than four standard
deviations from a coin flip. The modules' combined opinion is not merely
uninformative about total goals, it points the wrong way — which fits what mu
already contains: ULR reads low tempo and mu already reflects low tempo, so
adding it double-counts and overshoots.

Inverting it looks like a win and is not one:

    scale -1.00   NET  +5   strike 82.9%   edge +1.57%   U4.25 58%
    scale -3.00   NET +35   strike 86.3%   edge +1.64%   U4.25 82%

Strike climbs nearly four points while edge FALLS from the baseline +2.13%, and
the market mix collapses — U3.0 goes from 29% of picks to zero and U4.25 takes
82%. A large negative scale multiplied against a mostly-negative tilt raises mu
on under-leaning fixtures, which pushes them up the ladder to the safest rung.
That is not the modules being right when inverted. It is the fifth appearance of
buying certainty in this session, after the floor sweep, the O1.0 tail, the
abstention lane and the league cull.

So module_mu_scale stays at 0.0 and the modules stay disconnected. The finding
is worth the wire: their disconnection has cost nothing, and the ablation table
that once valued them was measuring a flowchart that no longer decides anything.
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300

# One high-scoring, one low-scoring, one noisy — enough to see a real effect
# and cheap enough to run before committing to a wide sweep.
LEAGUES = ["ENG-PL", "JPN-J1", "FRA-L2"]

# Tilt spans roughly -0.28 to +0.24 in lean-score units, so a scale of 1.0 moves
# mu by up to a quarter of a goal — the same order as possession (+/-0.35) and
# season stage (+0.15), both of which are known quantities here.
SCALES = [0.0, -0.25, -0.5, -0.75, -1.0, -1.5, -2.0, -3.0]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(mk, tt) -> float:
    if not len(mk) or not len(tt):
        return 0.0
    n = len(tt)
    return sum(c * sum(1 for t in tt if won(m, t)) / n
               for m, c in Counter(mk).items()) / len(mk)


def main() -> None:
    data = {}
    for code in LEAGUES:
        pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        if len(pairs) < 100:
            continue
        data[code] = [(r, int(hg) + int(ag)) for r, (hg, ag) in pairs]
        print(f"  {code}: {len(pairs)}", flush=True)

    n = sum(len(v) for v in data.values())
    print(f"\n{n} fixtures across {len(data)} leagues\n")

    def run(scale):
        picks = []
        for code, pairs in data.items():
            cfg = deepcopy(config.get(code))
            cfg.module_mu_scale = scale
            flags = ModuleFlags(**(cfg.module_overrides or {}))
            for req, total in pairs:
                m = predict_fixture(req, cfg,
                                    module_flags=flags).translated_play.market
                picks.append((code, m, total))
        return picks

    baseline = run(0.0)
    b_by_key = {i: p for i, p in enumerate(baseline)}
    b_hits = sum(1 for _c, m, t in baseline if won(m, t))
    print(f"  {'scale':>6} {'strike':>16} {'edge':>8}  "
          f"{'changed':>8} {'resc':>5} {'brok':>5} {'NET':>5}")
    print("  " + "-" * 62)

    for s in SCALES:
        picks = run(s)
        mk = [m for _c, m, _t in picks]
        tt = [t for _c, _m, t in picks]
        hits = sum(1 for _c, m, t in picks if won(m, t))
        changed = resc = brok = 0
        for i, (_c, m, t) in enumerate(picks):
            bm = b_by_key[i][1]
            if m == bm:
                continue
            changed += 1
            a, b = won(bm, t), won(m, t)
            resc += int(b and not a)
            brok += int(a and not b)
        print(f"  {s:6.2f} {hits:5d}/{n} = {hits / n:6.1%} "
              f"{hits / n - base_of(mk, tt):+7.2%}  "
              f"{changed:8d} {resc:5d} {brok:5d} {resc - brok:+5d}"
              + ("   <- current" if s == 0.0 else ""))

    print(f"\n  baseline {b_hits}/{n} = {b_hits / n:.1%}")


if __name__ == "__main__":
    main()
