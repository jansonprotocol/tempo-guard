"""
Does the cross-division fallback earn its tips? Replayed on real promotions.

The exchange rate it applies (scored x0.754 up, x1.345 down, conceded the
reverse) was measured on 789 club-seasons — but measuring the RATE and
trusting the TIPS built on it are different claims. The rate is an average
over full seasons; a tip is issued in a promoted club's first weeks, priced
against a league that has never seen it. So this replays exactly those weeks.

For every stored fixture, the fallback population is identified the same way
the live guard finds it: a side whose own league holds fewer than MIN_MATCHES
rows as-of the match, where an adjacent stored division can supply a window.
Those fixtures are replayed and their tips scored, with the same-league,
same-period fixtures alongside as the control — if the fallback tips hit like
the control tips, the exchange rate carries; if they run hot or cold, the
number here is the honest price of the extra volume.

Only fixtures from 2015 on: earlier seasons exist mostly in the deep-history
leagues and would swamp the comparison with eras the engine does not tip.

Usage:  python scripts/promoted_replay.py [--since 2015]
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

LADDER_CODES = sorted({c for ladder in features.DIVISION_LADDERS for c in ladder})


def fallback_side(league_code: str, df, team: str, cutoff) -> bool:
    """Would the live guard route this side through the fallback?"""
    name = features._aliased(league_code, df, team)
    if len(features._find_team_rows(df, name, cutoff)) >= features.MIN_MATCHES:
        return False
    return features._cross_division_rows(
        league_code, team, cutoff, features.MIN_MATCHES, "home") is not None


def replay(code: str, since: int) -> tuple[list, list]:
    df = store.load_results(code)
    if df is None or len(df) < 200:
        return [], []
    cfg = config.get(code)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    fall, ctrl = [], []
    for _, r in df[df["date"].dt.year >= since].sort_values("date").iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        cutoff = features._cutoff(d)
        h, a = str(r["home"]), str(r["away"])
        try:
            is_fb = (fallback_side(code, df, h, cutoff)
                     or fallback_side(code, df, a, cutoff))
        except Exception:
            continue
        # The control only needs to be the same size ballpark; sample it by
        # month-start so the replay stays tractable across ten seasons.
        if not is_fb and d.day > 7:
            continue
        try:
            req = build_request(code, h, a, d)
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
        row = (market_select.p_win(mk, req.mu_total), hit_weight(res) >= 1.0)
        (fall if is_fb else ctrl).append(row)
    return fall, ctrl


def show(label: str, rows: list) -> None:
    if len(rows) < 30:
        print(f"{label:22} {len(rows):5} tips — too few to read")
        return
    k = sum(1 for r in rows if r[1])
    hit, says = k / len(rows), sum(r[0] for r in rows) / len(rows)
    w = wilson(k, len(rows))
    print(f"{label:22} {len(rows):5} tips   says {says*100:5.1f}%   "
          f"hit {hit*100:5.1f}%   gap {(hit-says)*100:+5.1f}   "
          f"[{w[0]*100:.0f}-{w[1]*100:.0f}]")


def main() -> None:
    args = sys.argv[1:]
    since = int(args[args.index("--since") + 1]) if "--since" in args else 2015

    fall, ctrl = [], []
    for code in LADDER_CODES:
        try:
            f, c = replay(code, since)
            fall += f
            ctrl += c
            print(f"  {code:8} fallback {len(f):4}  control {len(c):4}",
                  file=sys.stderr)
        except Exception as exc:
            print(f"  {code:8} FAILED {exc}", file=sys.stderr)

    print(f"\nsince {since}, {len(LADDER_CODES)} ladder leagues")
    show("FALLBACK fixtures", fall)
    show("control (same leagues)", ctrl)


if __name__ == "__main__":
    main()
