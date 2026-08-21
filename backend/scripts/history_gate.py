"""
How little history is too little? Where the refusal threshold belongs.

The engine already declines a fixture it cannot read: `asof_features` returns
nothing when either side has fewer than MIN_MATCHES=5 rows before the cutoff,
and that is why Al Diriyah — one match into its Saudi Pro League life — got no
tip rather than a guess. So this is not a new mechanism. It is a question about
where an existing dial should sit.

TWO DIFFERENT QUANTITIES
========================
`_find_team_rows` caps at ROLLING_MATCHES=10, so the number the current gate
tests saturates at ten and cannot express "this side has played forty matches".
Both readings are measured here because they answer different questions:

    window   rows in the rolling window, 0-10. How complete the recent form
             picture is. This is what the existing gate tests.
    total    every prior match, uncapped. How well the club is known at all —
             a promoted side in October has a full window and thin history.

The abstention probe found a +6.65% separation at total >= 12, which the current
gate cannot represent no matter how it is set.

WHAT WOULD JUSTIFY MOVING IT
============================
Raising a refusal threshold always raises the hit rate of what remains, because
it removes fixtures. That alone means nothing. Two things have to hold:

  1. The dropped fixtures must lose MORE than the kept ones, by more than the
     error bars, so the gate is finding unreadable matches rather than
     shedding volume at random.
  2. It must survive a chronological split. A threshold tuned on the whole
     sample and read back on the same sample is the failure this codebase has
     already committed twice.

Replayed at min_matches=1 so the thin end is visible; production would never
have issued most of these. Rows are cached.
"""
from __future__ import annotations

import sys
from collections import Counter
from math import sqrt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.calibrate import _requests_for
from app.data import config, features, store
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300
FORCE_FRESH = "fresh" in set(sys.argv[2:])
EUROPEAN = {"UCL", "UEL", "UECL", "UECL-Q"}
CACHE = Path(__file__).resolve().parents[1] / ".cache" / f"history_{LIMIT}.csv"

# Deliberately below the production floor so the fixtures the gate currently
# refuses are still measured rather than assumed bad.
REPLAY_MIN = 1

WINDOW_STEPS = [5, 6, 7, 8, 9, 10]
TOTAL_STEPS = [5, 8, 12, 16, 20, 30]


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(mk, tt) -> float:
    if not len(mk) or not len(tt):
        return 0.0
    n = len(tt)
    return sum(c * sum(1 for t in tt if won(m, t)) / n
               for m, c in Counter(mk).items()) / len(mk)


def depths(df, team, cutoff) -> tuple[int, int]:
    """(window rows capped at 10, every prior match) using the engine's own
    name resolution, so the count matches what the gate would actually see."""
    matched = features._resolve_in_frame(df, team)
    if matched is None:
        return 0, 0
    idx = features._frame_index(df)
    rows = df.loc[idx["by_team"].get(features._norm(matched), df.index[:0])]
    before = rows[rows["date"] < cutoff]
    return min(len(before), features.ROLLING_MATCHES), len(before)


def build() -> pd.DataFrame:
    if CACHE.exists() and not FORCE_FRESH:
        out = pd.read_csv(CACHE)
        print(f"reusing {CACHE.name} ({len(out)} fixtures)\n")
        return out

    rows = []
    for code in sorted(config.load_all().keys()):
        if code in EUROPEAN:
            continue
        try:
            pairs = _requests_for(code, None, None, REPLAY_MIN, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        df = store.load_results(code)
        for req, (hg, ag) in pairs:
            cut = features._cutoff(req.match_date)
            hw, ht = depths(df, req.home_team, cut)
            aw, at = depths(df, req.away_team, cut)
            rows.append({
                "code": code, "date": req.match_date,
                "market": predict_fixture(req, cfg, module_flags=flags)
                          .translated_play.market,
                "total": int(hg) + int(ag),
                "window": min(hw, aw), "hist": min(ht, at),
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    out = pd.DataFrame(rows)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(CACHE, index=False)
    print(f"\ncached {len(out)} fixtures\n")
    return out


def line(label, sub, pool):
    if not len(sub):
        print(f"  {label:20s} none")
        return
    mk, tt = list(sub["market"]), list(sub["total"])
    h = sum(1 for m, t in zip(mk, tt) if won(m, t))
    s = h / len(mk)
    se = sqrt(max(s * (1 - s), 1e-9) / len(mk))
    print(f"  {label:20s} {len(sub):6d} ({len(sub) / pool:5.1%})  "
          f"{s:6.1%} +/-{se:.1%}   edge {s - base_of(mk, tt):+6.2%}")


def sweep(df, col, steps, name):
    print(f"\n  THRESHOLD SWEEP on {name} — kept vs dropped")
    print(f"  {'rule':20s} {'kept':>26}  {'dropped':>22}   {'GAP':>7}")
    print("  " + "-" * 82)
    for k in steps:
        keep, drop = df[df[col] >= k], df[df[col] < k]
        if not len(keep) or not len(drop):
            continue
        ks = sum(1 for m, t in zip(keep["market"], keep["total"])
                 if won(m, t)) / len(keep)
        ds = sum(1 for m, t in zip(drop["market"], drop["total"])
                 if won(m, t)) / len(drop)
        dse = sqrt(max(ds * (1 - ds), 1e-9) / len(drop))
        print(f"  {name} >= {k:<11d} {len(keep):6d} ({len(keep) / len(df):5.1%}) "
              f"= {ks:6.1%}   {len(drop):5d} = {ds:6.1%} +/-{dse:.1%}   "
              f"{ks - ds:+7.2%}")


def main() -> None:
    df = build()
    n = len(df)
    print(f"{n} fixtures, replayed at min_matches={REPLAY_MIN}\n")

    print("  STRIKE BY HISTORY DEPTH (uncapped)")
    for lo, hi, lab in ((1, 4, "1-4"), (5, 7, "5-7"), (8, 11, "8-11"),
                        (12, 15, "12-15"), (16, 24, "16-24"),
                        (25, 49, "25-49"), (50, 10 ** 6, "50+")):
        line(lab, df[(df["hist"] >= lo) & (df["hist"] <= hi)], n)

    print("\n  STRIKE BY WINDOW FULLNESS (what the current gate tests)")
    for k in range(0, 11):
        line(f"window = {k}", df[df["window"] == k], n)

    sweep(df, "hist", TOTAL_STEPS, "hist")
    sweep(df, "window", WINDOW_STEPS, "window")

    # Chronological check on whichever thresholds look worth having.
    df = df.sort_values("date")
    half = len(df) // 2
    print("\n  CHRONOLOGICAL SPLIT (older | newer), kept strike")
    print(f"  {'rule':20s} {'older':>16}  {'newer':>16}")
    print("  " + "-" * 56)
    for col, k in (("hist", 12), ("hist", 20), ("window", 8), ("window", 10)):
        out = []
        for part in (df.iloc[:half], df.iloc[half:]):
            keep = part[part[col] >= k]
            if not len(keep):
                out.append("      none")
                continue
            s = sum(1 for m, t in zip(keep["market"], keep["total"])
                    if won(m, t)) / len(keep)
            out.append(f"{len(keep):6d} = {s:5.1%}")
        print(f"  {col} >= {k:<12d} {out[0]:>16}  {out[1]:>16}")


if __name__ == "__main__":
    main()
