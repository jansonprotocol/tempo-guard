"""
Is the cup read honest? The domestic-form fallback, replayed as-of.

`_asof_features_intl` prices a cup fixture from each club's DOMESTIC form —
the only usable signal, since a cup campaign is a handful of matches. That
path has served UCL/UEL/UECL tips without ever being scored on its own: cup
fixtures ride along inside pooled calibration runs, where a few hundred rows
vanish among seven thousand league tips.

It needs scoring separately because it is built differently, and each
difference is a way to be wrong that no league fixture tests:

    - the two sides' rates come from two DIFFERENT leagues, so relative
      strength between them is never observed, only their own scoring
    - league_mu is a hardcoded baseline (INTL_GOAL_AVERAGES), not measured
    - domestic scoring transfers imperfectly: a dominant club in a weak
      league arrives with a rate no European opponent will concede

And the question behind it: the QUALIFIER files (UCL-Q, UEL-Q, UECL-Q) are not
in INTL_LEAGUE_CODES at all, so their fixtures abstain outright. Extending the
fallback to them is one line — worth it only if the read it extends is sound,
and only with a measured baseline, so both are measured here:

    replay   every stored main-phase and qualifier fixture, strictly as-of,
             through the SAME path a live tip takes (Q codes are temporarily
             added to the intl set for the replay — measuring the extension
             is the point)
    base     goals/game in each competition's own stored history, to check
             the hardcoded baselines and derive ones for the Q files

Usage:  python scripts/cup_replay.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, features, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.team_shrink_sweep import wilson

MAIN = ("UCL", "UEL", "UECL")
QUAL = ("UCL-Q", "UEL-Q", "UECL-Q")


def baseline(code: str) -> tuple[float, int]:
    df = store.load_results(code)
    if df is None or df.empty:
        return 0.0, 0
    g = df["hg"].fillna(0) + df["ag"].fillna(0)
    return float(g.mean()), len(df)


def replay(code: str) -> list[tuple[float, bool, str]]:
    """(stated p, hit, market) for every fixture the path will price."""
    df = store.load_results(code)
    if df is None or df.empty:
        return []
    cfg = config.get(code)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    out = []
    for _, r in df.sort_values("date").iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(code, str(r["home"]), str(r["away"]), d)
            if req is None:
                continue
            mk = predict_fixture(req, cfg, module_flags=flags).translated_play.market
        except Exception:
            continue
        if not mk:
            continue
        res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
        if res is None:
            continue
        out.append((market_select.p_win(mk, req.mu_total),
                    hit_weight(res) >= 1.0, mk))
    return out


def show(label: str, rows: list) -> None:
    if len(rows) < 30:
        print(f"{label:8} {len(rows):5} tips — too few to read")
        return
    k = sum(1 for r in rows if r[1])
    hit, says = k / len(rows), sum(r[0] for r in rows) / len(rows)
    w = wilson(k, len(rows))
    print(f"{label:8} {len(rows):5} tips   says {says*100:5.1f}%   "
          f"hit {hit*100:5.1f}%   gap {(hit-says)*100:+5.1f}   "
          f"[{w[0]*100:.0f}-{w[1]*100:.0f}]")


def main() -> None:
    # The live path is disabled; this instrument measures it anyway.
    features.CUP_TIPS_ENABLED = True

    print("stored goals/game against the hardcoded baseline")
    for code in MAIN + QUAL:
        mu, n = baseline(code)
        hard = features.INTL_GOAL_AVERAGES.get(code)
        print(f"  {code:7} {mu:5.2f} over {n:5} matches"
              + (f"   (hardcoded {hard:.2f})" if hard else "   (no baseline)"))

    print("\nMAIN PHASE — the path as it runs live today")
    pooled = []
    for code in MAIN:
        rows = replay(code)
        show(code, rows)
        pooled += rows
    show("ALL", pooled)

    print("\nQUALIFIERS — the extension under test")
    added = [c for c in QUAL if c not in features.INTL_LEAGUE_CODES]
    features.INTL_LEAGUE_CODES.update(added)
    try:
        pooled_q = []
        for code in QUAL:
            rows = replay(code)
            show(code, rows)
            pooled_q += rows
        show("ALL-Q", pooled_q)

        by = {}
        for p, hitv, mk in pooled + pooled_q:
            by.setdefault(mk, []).append((p, hitv, mk))
        print("\nby market, both phases pooled")
        for mk in sorted(by, key=lambda m: -len(by[m])):
            if len(by[mk]) >= 40:
                show(mk, by[mk])
    finally:
        features.INTL_LEAGUE_CODES.difference_update(added)


if __name__ == "__main__":
    main()
