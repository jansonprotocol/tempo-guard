"""
Do red cards explain Athena's losses — and specifically the Over losses?

The hypothesis came from Al Qadsiah v Al-Ittihad: an O1.5 at 89% with +8.9%
edge, lean and market agreeing, that finished 0-1 with Al-Ittihad down to ten
men. Everything about the tip was right and the match stalled. If that is a
pattern rather than one bad night, red cards are a mechanism the goal model has
no view of at all.

Two questions, in order, because the second only matters if the first holds.

    1. Do reds change the score line?  Goals in matches with a red against
       matches without, per league. Folk wisdom runs both ways — a sending-off
       either kills a game or blows it open — so this is worth measuring rather
       than assuming.

    2. Do reds fall disproportionately on Athena's LOSSES, and on the Over
       losses in particular?  Compare the red rate among losing tips with the
       red rate among winning tips, split by side. A red rate that is the same
       in both is a red herring: reds would be happening, just not causing the
       losses.

WHAT THIS CANNOT BECOME WITHOUT MORE WORK
=========================================
A red card is known only after kick-off, so it can never be a feature. The only
usable version is a PROPENSITY — how likely this fixture is to produce one,
from team discipline, league rate and referee, all as-of. That is a second
question and this script does not answer it. Worth remembering that referee
tendency was already measured here and came back at +0.086 split-half, barely
above nothing, so the propensity half of the idea starts from a weak prior even
if the mechanism half holds.

Deliberately small: six leagues, 300 recent matches each. Card data exists in 22
leagues and none of the ESPN-sourced ones, since that reader takes shots,
corners, fouls and possession but not cards.

RESULT: THE MECHANISM IS REAL AND RUNS THE OTHER WAY
====================================================
1,795 fixtures, 18.6% carrying at least one red.

A sending-off OPENS a match rather than stalling it. Goals with a red against
without: +0.15 pooled (+/-0.19), positive in five of six leagues, with only the
Premier League negative.

Which inverts the whole hypothesis. Athena's Over tips do BETTER when a red
appears and its Unders do worse:

    Over tips     79.1% (350) without a red    85.7% (77) with one
    Under tips    84.5% (1111) without         78.2% (257) with one

And reds are UNDER-represented among the Over losses, not over-represented:

    OVER wins      19.2% carried a red
    OVER losses    13.1%
    UNDER wins     17.6%
    UNDER losses   24.6%

So Al Qadsiah was the exception rather than the pattern: an Over that lost
despite a red, which this sample says should have helped it. The damage a red
does is to UNDER tips, which is where the volume is — 257 of the 334 red-card
fixtures here were Unders.

CAN IT BE PREDICTED? WEAKLY, AND NOT ENOUGH
===========================================
A red is known only after kick-off, so the usable form is propensity. Split-half
persistence across ten leagues:

    team discipline   mean r +0.171, positive in 6/10 (Italy, France, Portugal
                      and Scotland all negative)
    referee reds      mean r +0.209, positive in 5/5 but present in only five
                      leagues, since referee names are absent elsewhere

Both are in the same band as the team tags declined earlier at +0.206, and
weaker than that in most competitions. The effect being real does not make it
reachable: a 6-point strike swing multiplied by a propensity estimate that
barely correlates with itself is not a feature, it is noise with a story
attached.

Recorded rather than built. If red-card propensity is ever revisited, the
direction to test is a red-likely fixture arguing FOR the Over, which is the
opposite of what prompted the question.
"""
from __future__ import annotations

import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np
import pandas as pd

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, store
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300
LEAGUES = ["ENG-PL", "ESP-LL", "ESP-L2", "ITA-SA", "FRA-L2", "POR-PL"]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def main() -> None:
    rows = []
    for code in LEAGUES:
        df = store.load_results(code)
        if df.empty or "hr" not in df.columns:
            print(f"  {code}: no card data")
            continue
        # Key on (date, home) so a fixture's cards can be looked up by replay.
        cards = {}
        for r in df.itertuples(index=False):
            if pd.notna(r.hr) and pd.notna(r.ar):
                cards[(r.date.date(), str(r.home))] = int(r.hr) + int(r.ar)

        pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            red = cards.get((req.match_date, req.home_team))
            if red is None:
                continue
            m = predict_fixture(req, cfg, module_flags=flags) \
                .translated_play.market
            rows.append({
                "code": code, "total": int(hg) + int(ag), "market": m,
                "red": red > 0, "side": "over" if m.startswith("O") else "under",
                "won": won(m, int(hg) + int(ag)),
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return
    d = pd.DataFrame(rows)
    n = len(d)
    print(f"\n{n} fixtures, {d['red'].mean():.1%} had at least one red\n")

    # ── 1. Do reds change the score line? ─────────────────────────────
    print("  GOALS: red-card matches vs the rest")
    print(f"  {'league':9s} {'no red':>16} {'with red':>16} {'delta':>8}")
    print("  " + "-" * 54)
    for code, g in d.groupby("code"):
        a = g[~g["red"]]["total"].mean()
        b = g[g["red"]]["total"].mean()
        print(f"  {code:9s} {a:10.2f} ({len(g[~g['red']]):4d}) "
              f"{b:10.2f} ({len(g[g['red']]):3d}) {b - a:+8.2f}")
    a, b = d[~d["red"]]["total"].mean(), d[d["red"]]["total"].mean()
    se = np.sqrt(d[~d['red']]['total'].var() / (~d['red']).sum()
                 + d[d['red']]['total'].var() / d['red'].sum())
    print(f"  {'POOLED':9s} {a:10.2f}       {b:10.2f}      {b - a:+8.2f}  "
          f"+/-{1.96 * se:.2f}")

    # ── 2. Do reds land on the losses, and on the Over losses? ───────
    print(f"\n  RED RATE among Athena's calls")
    print(f"  {'group':22s} {'n':>6} {'red rate':>10}")
    print("  " + "-" * 42)
    for label, sub in (("all wins", d[d["won"]]), ("all losses", d[~d["won"]]),
                       ("OVER wins", d[d["won"] & (d["side"] == "over")]),
                       ("OVER losses", d[~d["won"] & (d["side"] == "over")]),
                       ("UNDER wins", d[d["won"] & (d["side"] == "under")]),
                       ("UNDER losses", d[~d["won"] & (d["side"] == "under")])):
        if len(sub):
            print(f"  {label:22s} {len(sub):6d} {sub['red'].mean():9.1%}")

    # ── The claim, stated directly ────────────────────────────────────
    print(f"\n  STRIKE with and without a red")
    print(f"  {'group':22s} {'no red':>18} {'with red':>18}")
    print("  " + "-" * 60)
    for label, sub in (("all tips", d), ("Over tips", d[d["side"] == "over"]),
                       ("Under tips", d[d["side"] == "under"])):
        nr, wr = sub[~sub["red"]], sub[sub["red"]]
        if len(nr) and len(wr):
            print(f"  {label:22s} {nr['won'].mean():12.1%} ({len(nr):4d}) "
                  f"{wr['won'].mean():12.1%} ({len(wr):3d})")


if __name__ == "__main__":
    main()
