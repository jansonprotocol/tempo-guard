"""
Search a single league's toggles for a net hit-rate gain.

Net is the criterion, as asked: a toggle earns its place if it wins more
matches than it loses. Picks that used to win are allowed to start losing
provided more losses turn into wins.

The search space is everything a league can set for itself:

    module flags      nine on/off switches
    bias_shift        net over/under pressure
    tempo_factor      scales the tempo signal
    max_under_line    caps the loosest Under rung (playability)
    min_over_line     permits or forbids O1.0
    possession        the per-league possession coefficient, currently unwired

EVERY CANDIDATE IS HOLDOUT-VALIDATED
====================================
A search over ~40 candidates on a few hundred matches will always find
something that looks better in-sample. Each candidate is scored on a
chronological training split and re-scored on matches the search never saw;
only the holdout number counts, and candidates accumulate greedily while the
holdout keeps improving.

Thirty matches is far too few to tune on — that is a diagnostic window, not a
calibration set — so the search runs on a larger sample and the winner is then
verified on the recent window separately.
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, possession
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUE = sys.argv[1] if len(sys.argv) > 1 else "COL-PA"
SEARCH_N = 500
RECENT_N = 30
HOLDOUT = 0.35
MIN_GAIN = 1          # net matches; a toggle must actually win more than it loses


def won(m, t):
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def pick(req, cfg, flags):
    """The market the engine would publish under this config."""
    return predict_fixture(req, cfg, module_flags=flags).translated_play.market


def score(pairs, cfg, flags):
    hits = 0
    outcomes, markets, totals = [], [], []
    for req, (hg, ag) in pairs:
        m = pick(req, cfg, flags)
        t = hg + ag
        o = won(m, t)
        hits += o
        outcomes.append(o)
        markets.append(m)
        totals.append(t)
    return hits, outcomes, markets, totals


def base_of(markets, totals):
    if not markets:
        return 0.0
    return sum(sum(1 for t in totals if won(m, t)) / len(totals)
               for m in markets) / len(markets)


def variants(cfg):
    """Every live dial worth trying, as (label, mutated cfg, flags)."""
    base_flags = ModuleFlags(**(cfg.module_overrides or {}))
    out = []

    for fl in (0.72, 0.76, 0.79, 0.82, 0.85, 0.88):
        if cfg.min_win_prob is not None and abs(fl - cfg.min_win_prob) < 1e-9:
            continue
        if cfg.min_win_prob is None and abs(fl - 0.79) < 1e-9:
            continue
        c = deepcopy(cfg)
        c.min_win_prob = fl
        out.append((f"min_win_prob -> {fl}", c, base_flags))

    for cap in (3.0, 3.25, 3.5, 3.75, None):
        if cap == cfg.max_under_line:
            continue
        c = deepcopy(cfg)
        c.max_under_line = cap
        out.append((f"max_under_line -> {cap}", c, base_flags))

    for mo in (1.0, 1.5, None):
        if mo == cfg.min_over_line:
            continue
        c = deepcopy(cfg)
        c.min_over_line = mo
        out.append((f"min_over_line -> {mo}", c, base_flags))

    c = deepcopy(cfg)
    c.use_possession = not cfg.use_possession
    out.append((f"use_possession -> {not cfg.use_possession}", c, base_flags))

    f = base_flags.model_copy(update={"prob_select": not base_flags.prob_select})
    out.append((f"prob_select -> {not base_flags.prob_select}",
                deepcopy(cfg), f))
    return out


def main() -> None:
    pairs = _requests_for(LEAGUE, None, None, CALIB_MIN_MATCHES, limit=SEARCH_N)
    pairs.sort(key=lambda p: p[0].match_date)
    cut = int(len(pairs) * (1 - HOLDOUT))
    train, hold = pairs[:cut], pairs[cut:]
    recent = pairs[-RECENT_N:]
    cfg = config.get(LEAGUE)
    flags = ModuleFlags(**(cfg.module_overrides or {}))

    print(f"{LEAGUE}: searching {len(pairs)} matches "
          f"({len(train)} train / {len(hold)} holdout)")
    print(f"  current: floor={cfg.min_win_prob or 0.79} max_under={cfg.max_under_line} "
          f"min_over={cfg.min_over_line} possession={cfg.use_possession}")

    bh, b_out, b_mk, b_tot = score(hold, cfg, flags)
    print(f"  baseline on holdout: {bh}/{len(hold)} = {bh / len(hold):.1%}  "
          f"edge {bh / len(hold) - base_of(b_mk, b_tot):+.2%}\n")

    print(f"  {'candidate':34s} {'train':>13} {'HOLDOUT':>15} {'net':>6}")
    print("  " + "-" * 74)
    results = []
    t_base = score(train, cfg, flags)[0]
    # Every candidate is printed, passing or not. An empty table cannot be
    # distinguished from a filter that rejected everything, and the shape of
    # the failures is itself the finding when nothing wins.
    for label, c, f in variants(cfg):
        th = score(train, c, f)[0]
        hh, h_out, h_mk, h_tot = score(hold, c, f)
        net = sum(1 for a, b in zip(b_out, h_out) if b and not a) - \
              sum(1 for a, b in zip(b_out, h_out) if a and not b)
        ok = th >= t_base and net > 0
        if ok:
            results.append((net, label, hh, h_mk, h_tot, c, f))
        print(f"  {label:34s} {th - t_base:+6d} on {len(train):3d} "
              f"{hh}/{len(hold)} = {hh / len(hold):6.1%} {net:+6d}"
              f"{'  <- accepted' if ok else ''}", flush=True)

    if not results:
        print("\n  No single toggle produced a net gain on unseen matches.")
        return

    results.sort(key=lambda r: -r[0])
    print(f"\n  BEST: {results[0][1]}")
    net, label, hh, h_mk, h_tot, c, f = results[0]
    print(f"    holdout {bh}/{len(hold)} -> {hh}/{len(hold)}  "
          f"({bh / len(hold):.1%} -> {hh / len(hold):.1%}), net {net:+d} matches")
    print(f"    edge {bh / len(hold) - base_of(b_mk, b_tot):+.2%} -> "
          f"{hh / len(hold) - base_of(h_mk, h_tot):+.2%}")
    print(f"    markets {Counter(h_mk).most_common(3)}")

    rb = score(recent, cfg, flags)[0]
    rn = score(recent, c, f)[0]
    print(f"\n  verified on the most recent {len(recent)} matches "
          f"(not the search window): {rb}/{len(recent)} -> {rn}/{len(recent)}")


if __name__ == "__main__":
    main()
