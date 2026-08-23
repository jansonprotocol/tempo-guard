"""
Is the engine worse at the start of a season, and does it know?

Measured before recalibration, `ENG-NL` and `FRA-L2` ran -8.3 and -7.8 over
their most recent 120 matches against -1.5 and -0.1 over the 120 before. That
window straddles the summer break, where the rolling form window reaches across
it and describes squads that no longer exist in that shape.

Two things need separating, and the first was never done:

    IS IT STILL THERE?   Shrinkage pulls every fixture toward its league mean,
                         which is exactly the right response to form you cannot
                         trust. It may already have absorbed the effect.

    WHAT DRIVES IT?      "Early season" is not a measurable property of a
                         fixture. Two things that are: how long the team has
                         been idle, and how far back the ten-match form window
                         has to reach to fill itself.

`break_days` is the larger of the two sides' gaps since their previous match —
the larger, because one rusty team is enough to make a fixture unusual.
`window_days` is the age of the OLDEST match in the form window, which is what
actually decides whether the read describes the current squad.

Calibration is bucketed by both. If the gap widens with either, the engine is
issuing tips it should be damping or withholding, and it currently has no
notion of either quantity.

Usage:  python scripts/restart_effect.py [--n 400] [--leagues ...]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.data import config, store
from app.data.features import ROLLING_MATCHES, _find_team_rows, _norm
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market

BREAK_BUCKETS = ((0, 10), (10, 20), (20, 40), (40, 9999))
WINDOW_BUCKETS = ((0, 80), (80, 120), (120, 200), (200, 99999))


def rows_for(league: str, n: int) -> list[dict]:
    df = store.load_results(league)
    if df is None or len(df) < 200:
        return []
    cfg = config.get(league)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    out = []
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"]
        day = d.date() if hasattr(d, "date") else d
        try:
            req = build_request(league, str(r["home"]), str(r["away"]), day)
        except Exception:
            continue
        if req is None:
            continue
        try:
            mk = predict_fixture(req, cfg,
                                 module_flags=flags).translated_play.market
        except Exception:
            continue
        if not mk:
            continue
        res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
        if res is None:
            continue

        cutoff = pd.Timestamp(d)
        breaks, oldest = [], []
        for team in (str(r["home"]), str(r["away"])):
            prior = _find_team_rows(df, team, cutoff)
            if prior.empty:
                continue
            breaks.append((cutoff - prior["date"].max()).days)
            oldest.append((cutoff - prior["date"].min()).days)
        if not breaks:
            continue
        out.append(dict(
            break_days=max(breaks),
            window_days=max(oldest),
            says=market_select.p_win(mk, req.mu_total),
            hit=res is True or res == "half_win",
            market=mk,
        ))
    return out


def bucket(rows: list[dict], key: str, edges) -> None:
    print(f"\n  by {key}")
    print(f"    {'bucket':>12}{'n':>6}{'says':>8}{'hit':>8}{'gap':>8}   top market")
    for lo, hi in edges:
        sel = [r for r in rows if lo <= r[key] < hi]
        if len(sel) < 30:
            continue
        says = sum(r["says"] for r in sel) / len(sel)
        hit = sum(r["hit"] for r in sel) / len(sel)
        counts: dict[str, int] = {}
        for r in sel:
            counts[r["market"]] = counts.get(r["market"], 0) + 1
        top = max(counts.items(), key=lambda kv: kv[1])
        label = f"{lo}-{hi}" if hi < 9000 else f"{lo}+"
        print(f"    {label:>12}{len(sel):6}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+8.1f}   {top[0]} {top[1]*100//len(sel)}%")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 400
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else
             ["ENG-NL", "FRA-L2", "ENG-CH", "ESP-L2", "NED-ED", "BEL-PL",
              "POR-PL", "TUR-SL", "GER-B2", "SCO-PL"])

    rows: list[dict] = []
    for lg in codes:
        got = rows_for(lg, n)
        rows += got
        print(f"{lg:9} {len(got):5} fixtures", flush=True)

    print(f"\n{len(rows)} fixtures total")
    bucket(rows, "break_days", BREAK_BUCKETS)
    bucket(rows, "window_days", WINDOW_BUCKETS)


if __name__ == "__main__":
    main()
