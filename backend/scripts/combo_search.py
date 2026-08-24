"""
Do any of the rejected features work TOGETHER that did not work alone?

Every module and shift in this engine was measured one at a time against
everything else at its default. That is a real gap. Two features can each be
worth nothing alone and something together — one supplies a condition the other
needs, or one's errors cancel the other's — and a one-at-a-time ablation is
blind to it by construction.

THE TRAP, STATED BEFORE THE RESULT
==================================
Seven binary toggles make 128 combinations. Scoring all of them against a few
thousand fixtures and keeping the best WILL produce something 1-2% above the
default, whether or not any interaction exists — the best of 128 noisy draws is
high by construction. This repository already knows that: its own calibration
searches 336 dial settings and refuses to write a winner unless it survives a
chronological holdout, for exactly this reason.

So the search half of this script proves nothing on its own. The holdout is the
experiment. Everything before it is a way of generating candidates.

WHAT COUNTS AS AN INTERACTION
=============================
Beating the default is not enough, because a combination that just switches on
the one feature that already works alone is not a chain. The question is whether
the whole is worth more than the parts, so the ledger reported is

    synergy = combo - default  -  sum over toggles of (that toggle alone - default)

A combination whose synergy is near zero is additive: its members do not need
each other and could have been decided separately. Positive synergy is the thing
being hunted, and it is the only result that would justify anything here.

WHAT IS AND IS NOT INCLUDED
===========================
gate_b, eps and bilateral are excluded. Ablation found they changed zero
predictions — eps and bilateral move only the corridor ceiling, never the
selected market — so they cannot interact with anything. Including them would
quadruple the runtime to measure eight identical copies of every result.

under_guard is excluded as a toggle and left on: it is the engine's only route
to Under markets, so disabling it does not test a feature, it removes half the
ladder.

Features are resolved once and reused across all 128 combinations, since feature
resolution does not depend on which toggles are set — the same reason
calibration resolves once and searches after.

RESULT: NO INTERACTION IS POSSIBLE, BECAUSE FIVE OF SEVEN DO NOTHING
====================================================================
Every one of the 128 combinations scores exactly the same on the holdout:
1326/1630 = 81.35%, identical to the default, with synergy zero throughout. The
train spread across 126 scored combinations runs -4 to +0 with a standard
deviation of 1.6 matches.

That is not a weak interaction. It is five inert toggles:

    toggle              markets changed / 998
    burst_sentinel            0
    det                       0
    ulr                       0
    deg                       0
    mfr                       0
    use_possession           14
    use_season_stage         17

The cause is structural rather than statistical. When probability selection took
over, the market began coming from market_select.choose(mu, league_mu, ...).
burst_sentinel, det, ulr, deg and mfr all adjust the OLD flowchart's lean and
corridor scores, and the flowchart no longer picks the market — so they still
compute, still appear in the corridor, and nothing they produce reaches the tip.
The 128-combination search was really a search over four: possession crossed
with season stage.

This invalidates the ablation figures those modules carry in their own
docstring and in the README (ulr +0.24%, det -0.45%, burst_sentinel -1.91%).
Those were measured before probability selection existed. Every one is now
exactly 0.00%.

The finding is not that the modules are worthless. They encode real football
logic and are simply unplugged: they whisper into a corridor nothing reads. The
question worth asking next is what happens if they move `mu` instead of the
lean, which would reconnect five features at once rather than adding an eighth.
"""
from __future__ import annotations

import sys
from copy import deepcopy
from itertools import product
from math import sqrt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300

# A spread of competitions rather than all 52: high and low scoring, strong and
# weak edge, four continents. Enough to see an interaction without paying for
# 128 passes over the whole book.
LEAGUES = ["ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED", "TUR-SL",
           "FRA-L2", "ENG-CH", "JPN-J1", "BRA-SA", "POR-PL", "MLS",
           "ARG-PD", "GRE-SL", "SUI-SL", "MEX-LMX"]

# The module flags worth toggling, plus the two mu shifts.
MODULE_TOGGLES = ["burst_sentinel", "det", "ulr", "deg", "mfr"]
SHIFT_TOGGLES = ["use_possession", "use_season_stage"]
ALL_TOGGLES = MODULE_TOGGLES + SHIFT_TOGGLES

HOLDOUT = 0.35
TOP_N = 12


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def settings_for(cfg, combo: dict):
    """A (config, flags) pair with this combination applied."""
    c = deepcopy(cfg)
    base = dict(c.module_overrides or {})
    for k in MODULE_TOGGLES:
        base[k] = combo[k]
    for k in SHIFT_TOGGLES:
        setattr(c, k, combo[k])
    return c, ModuleFlags(**base)


def score(pairs_by_league, combo: dict) -> tuple[int, int]:
    hits = n = 0
    for code, pairs in pairs_by_league.items():
        c, flags = settings_for(config.get(code), combo)
        for req, total in pairs:
            m = predict_fixture(req, c, module_flags=flags).translated_play.market
            hits += int(won(m, total))
            n += 1
    return hits, n


def main() -> None:
    train, hold = {}, {}
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 100:
            continue
        pairs = [(r, int(hg) + int(ag)) for r, (hg, ag) in pairs]
        pairs.sort(key=lambda p: p[0].match_date)
        cut = int(len(pairs) * (1 - HOLDOUT))
        train[code], hold[code] = pairs[:cut], pairs[cut:]
        print(f"  {code}: {len(pairs)}", flush=True)

    if not train:
        print("no data")
        return
    ntr = sum(len(v) for v in train.values())
    nho = sum(len(v) for v in hold.values())
    print(f"\n{len(train)} leagues, {ntr} train / {nho} holdout, "
          f"{2 ** len(ALL_TOGGLES)} combinations\n")

    # The engine as it ships today.
    default = {k: ModuleFlags().model_dump().get(k, False) for k in MODULE_TOGGLES}
    default.update({k: getattr(config.get(LEAGUES[0]), k) for k in SHIFT_TOGGLES})
    dh, _ = score(train, default)
    print(f"  default on train: {dh}/{ntr} = {dh / ntr:.2%}   {default}\n")

    # Each toggle flipped alone, so synergy can be computed against the parts.
    solo = {}
    for k in ALL_TOGGLES:
        c = dict(default)
        c[k] = not c[k]
        h, _ = score(train, c)
        solo[k] = h - dh
        print(f"  flip {k:18s} alone: {h - dh:+4d} matches on train")

    # Written as it goes. A 128-pass search takes long enough that losing it to
    # a restart costs the whole run, which has already happened once.
    prog = Path(__file__).resolve().parents[1] / ".cache" / f"combo_{LIMIT}.csv"
    prog.parent.mkdir(parents=True, exist_ok=True)
    done = {}
    if prog.exists():
        for ln in prog.read_text().splitlines():
            bits = ln.split(",")
            if len(bits) == len(ALL_TOGGLES) + 1:
                done[bits[0]] = int(bits[-1])
        print(f"  resuming: {len(done)} combinations already scored")

    print("\n  searching all combinations...", flush=True)
    results = []
    with prog.open("a") as fh:
        for i, values in enumerate(product([False, True],
                                           repeat=len(ALL_TOGGLES))):
            combo = dict(zip(ALL_TOGGLES, values))
            key = "".join("1" if v else "0" for v in values)
            if key in done:
                h = done[key]
            else:
                h, _ = score(train, combo)
                fh.write(f"{key},{','.join(str(v) for v in values)},{h}\n")
                fh.flush()
            parts = sum(solo[k] for k in ALL_TOGGLES if combo[k] != default[k])
            results.append((h - dh, (h - dh) - parts, combo))
            if (i + 1) % 16 == 0:
                print(f"    {i + 1}/128", flush=True)
    results.sort(key=lambda r: -r[0])

    print(f"\n  TOP {TOP_N} ON TRAIN (gain vs default, and synergy over the parts)")
    print(f"  {'gain':>5} {'synergy':>8}   toggles differing from default")
    print("  " + "-" * 74)
    for gain, syn, combo in results[:TOP_N]:
        diff = " ".join(f"{k}={'on' if combo[k] else 'off'}"
                        for k in ALL_TOGGLES if combo[k] != default[k]) or "(default)"
        print(f"  {gain:+5d} {syn:+8d}   {diff}")

    print(f"\n  HOLDOUT — the only number that counts")
    dhh, _ = score(hold, default)
    print(f"  {'default':>44}  {dhh}/{nho} = {dhh / nho:.2%}")
    se = sqrt(max(dhh / nho * (1 - dhh / nho), 1e-9) / nho) * nho
    print(f"  {'(one standard error is about':>44}  {se:.0f} matches)\n")
    for gain, syn, combo in results[:TOP_N]:
        h, _ = score(hold, combo)
        diff = " ".join(f"{k[:12]}={'on' if combo[k] else 'off'}"
                        for k in ALL_TOGGLES if combo[k] != default[k]) or "(default)"
        print(f"  {diff:44.44s}  {h}/{nho} = {h / nho:.2%}  "
              f"vs default {h - dhh:+4d}")

    print("\n  BEST SYNERGY ON TRAIN (whole worth more than the parts)")
    for gain, syn, combo in sorted(results, key=lambda r: -r[1])[:6]:
        h, _ = score(hold, combo)
        diff = " ".join(f"{k[:12]}={'on' if combo[k] else 'off'}"
                        for k in ALL_TOGGLES if combo[k] != default[k]) or "(default)"
        print(f"  {diff:44.44s}  train {gain:+4d} syn {syn:+4d}  "
              f"holdout {h - dhh:+4d}")


if __name__ == "__main__":
    main()
