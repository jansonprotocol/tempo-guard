"""
Is the season-stage gain real, or is 0.150 a number fitted to this data?

The A/B returned rescued 47, broken 30, net +17 on 249 changed markets. That is
the first positive ledger in a long run of nulls, which is exactly when a result
deserves the most suspicion rather than the least. Two things have to hold.

FIRST: IS +17 MORE THAN COIN-FLIP NOISE?
========================================
If the lift moved markets at random, 249 flips would split about 124/125 and a
net of +17 would arrive by chance often enough to be unremarkable. The binomial
spread on 77 decisive flips is about 4.4, so +17 sits near two standard
deviations — suggestive, not settled. Stated explicitly because "first positive
result" is not a significance test.

SECOND: WAS THE CONSTANT FITTED TO THE FIXTURES IT IS SCORED ON?
================================================================
SHIFT=0.150 was measured across 2010-onwards history, and the A/B replays recent
fixtures from that same history. So the constant is partly in-sample, and the
honest checks are:

    chronological split   does the ledger hold on both halves, or only where
                          the constant was estimated?
    shift sensitivity     does the gain survive at 0.05, 0.10, 0.20, 0.30? A
                          signal that only pays at exactly the fitted value is
                          a curve fit. A real one degrades gracefully.

Rows are cached with mu and the league caps, so every candidate shift is
recomputed through market_select directly instead of re-replaying. The OFF arm
is reproduced from the cache and checked against the engine's own answer, so a
divergence between the two shows up as a mismatch rather than as a silent
difference in what is being compared.
"""
from __future__ import annotations

import sys
from collections import Counter
from math import sqrt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, season_stage
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300
FORCE_FRESH = "fresh" in set(sys.argv[2:])
EUROPEAN = {"UCL", "UEL", "UECL", "UECL-Q"}
CACHE = Path(__file__).resolve().parents[1] / ".cache" / f"stage_{LIMIT}.csv"

SHIFTS = [0.05, 0.10, 0.15, 0.20, 0.30]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def build() -> pd.DataFrame:
    if CACHE.exists() and not FORCE_FRESH:
        df = pd.read_csv(CACHE)
        print(f"reusing {CACHE.name} ({len(df)} fixtures)\n")
        return df

    rows, mismatch = [], 0
    for code in sorted(config.load_all().keys()):
        if code in EUROPEAN:
            continue
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            engine = predict_fixture(req, cfg, module_flags=flags) \
                .translated_play.market
            got = market_select.choose(
                req.mu_total, req.league_mu,
                max_under=cfg.max_under_line, min_over=cfg.min_over_line,
                min_win_prob=cfg.min_win_prob)
            if got is None or got[0] != engine:
                mismatch += 1
                continue
            p = season_stage.progress(code, req.home_team, req.away_team,
                                      req.match_date)
            rows.append({
                "code": code, "date": req.match_date,
                "total": int(hg) + int(ag), "mu": req.mu_total,
                "lmu": req.league_mu, "progress": p if p is not None else -1.0,
                "max_under": cfg.max_under_line if cfg.max_under_line else -1.0,
                "min_over": cfg.min_over_line if cfg.min_over_line else -1.0,
                "floor": cfg.min_win_prob if cfg.min_win_prob else -1.0,
                "off": engine,
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    df = pd.DataFrame(rows)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE, index=False)
    print(f"\ncached {len(df)} fixtures "
          f"({mismatch} dropped where the direct call disagreed with the engine)\n")
    return df


def market_with(r, shift: float) -> str:
    """The market this fixture gets once the stage lift is applied."""
    mu = r.mu + (shift if r.progress >= season_stage.FINAL_STRETCH else 0.0)
    got = market_select.choose(
        mu, r.lmu,
        max_under=None if r.max_under < 0 else r.max_under,
        min_over=None if r.min_over < 0 else r.min_over,
        min_win_prob=None if r.floor < 0 else r.floor)
    return got[0] if got else r.off


def ledger(df, shift: float) -> tuple[int, int, int]:
    resc = brok = changed = 0
    for r in df.itertuples(index=False):
        on = market_with(r, shift)
        if on == r.off:
            continue
        changed += 1
        a, b = won(r.off, r.total), won(on, r.total)
        resc += int(b and not a)
        brok += int(a and not b)
    return resc, brok, changed


def show(label, df, shift):
    resc, brok, changed = ledger(df, shift)
    dec = resc + brok
    sd = sqrt(dec * 0.25) if dec else 0.0
    sig = (resc - dec / 2) / sd if sd else 0.0
    print(f"  {label:22s} changed {changed:4d}  rescued {resc:3d}  "
          f"broken {brok:3d}   NET {resc - brok:+4d}   {sig:+.1f} sigma")


def main() -> None:
    df = build()
    if df.empty:
        print("no data")
        return
    df = df.sort_values("date").reset_index(drop=True)
    touched = df[df["progress"] >= season_stage.FINAL_STRETCH]
    print(f"{len(df)} fixtures, {len(touched)} in the closing stretch "
          f"({len(touched) / len(df):.1%})\n")

    print("  SHIFT SENSITIVITY — a real signal should not need one exact value")
    for s in SHIFTS:
        show(f"shift {s:.2f}", df, s)

    half = len(df) // 2
    print(f"\n  CHRONOLOGICAL SPLIT at shift {season_stage.SHIFT:.2f}")
    print(f"    older half: {df.iloc[0]['date']} to {df.iloc[half - 1]['date']}")
    print(f"    newer half: {df.iloc[half]['date']} to {df.iloc[-1]['date']}")
    show("older half", df.iloc[:half], season_stage.SHIFT)
    show("newer half", df.iloc[half:], season_stage.SHIFT)

    print(f"\n  BY DIRECTION at shift {season_stage.SHIFT:.2f}")
    ups = downs = 0
    for r in df.itertuples(index=False):
        on = market_with(r, season_stage.SHIFT)
        if on == r.off:
            continue
        if on.startswith("O") and not r.off.startswith("O"):
            ups += 1
        elif not on.startswith("O") and r.off.startswith("O"):
            downs += 1
    print(f"    under -> over {ups}   over -> under {downs}   "
          f"(rest move along the same side)")


if __name__ == "__main__":
    main()
