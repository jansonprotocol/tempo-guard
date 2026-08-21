"""
How much is left to win? An oracle bound on every team-quality feature at once.

Possession, shot blends, goal variance, team tags, referee tendency, rest days —
all rejected on holdout, and all of them measure the same underlying thing: how
good these two sides are. Player data, xG feeds and injury lists would measure it
too, just more expensively. Before buying any of that it is worth knowing what
the whole category is worth, and that has an answer that does not require the
data.

`mu_total` is the home side's recent scoring rate plus the away side's. So hand
the selector a mu built from each team's FULL-SEASON rate — matches after this
one included — and it is being told the teams' true quality with no estimation
error at all. No feed, no model and no amount of squad information can beat
knowing the answer. Whatever that arm scores above the honest one is the entire
remaining headroom for team-quality work.

Three arms:

    honest    what the engine does now, reading only the past.
    oracle    perfect knowledge of both sides' season-long scoring rates. The
              ceiling of possession, xG, players, tags, everything in that family.
    perfect   mu set to the match's actual total. Not a model at all — it bounds
              the LADDER rather than the features, and separates "the markets are
              too coarse" from "the teams are unknowable".

Read it this way. If oracle barely beats honest, then team-quality features are
finished as a direction and effort belongs elsewhere — which markets are chosen,
which leagues are played at all. If perfect barely beats oracle, the ladder is the
binding constraint. If perfect is far above both, the information exists and the
engine simply cannot see it from goals.
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
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = ["ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED", "TUR-SL",
           "JPN-J1", "BRA-SA", "POR-PL", "MLS"]
LIMIT = 300
MIN_SEASON_GAMES = 8


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(markets, totals) -> float:
    """
    Average win rate of the chosen markets across the sample.

    Weighted over DISTINCT markets rather than iterated per pick. The naive
    form re-scans every total once per fixture, which is O(n^2): at 11,000
    fixtures that is 2.7 minutes for a single call and it silently turned a
    summary into an hour of arithmetic. The ladder has twelve rungs, so
    counting them collapses it to O(12n) for an identical number.
    """
    if not markets or not totals:
        return 0.0
    n = len(totals)
    return sum(c * sum(1 for t in totals if won(m, t)) / n
               for m, c in Counter(markets).items()) / len(markets)


def season_rates(df: pd.DataFrame) -> dict[tuple, float]:
    """
    (season, team) -> goals scored per game across the WHOLE season.

    Deliberately looks forward. That is the point of the arm: it is the number
    the honest features are trying to estimate, handed over for free.
    """
    out: dict[tuple, float] = {}
    for season, sd in df.groupby("season"):
        teams = pd.concat([sd["home"], sd["away"]]).unique()
        for t in teams:
            hm = sd[sd["home"] == t]
            aw = sd[sd["away"] == t]
            n = len(hm) + len(aw)
            if n < MIN_SEASON_GAMES:
                continue
            scored = hm["hg"].fillna(0).sum() + aw["ag"].fillna(0).sum()
            out[(season, t)] = float(scored) / n
    return out


def report(label, markets, totals):
    hits = sum(1 for m, t in zip(markets, totals) if won(m, t))
    strike = hits / len(markets)
    base = base_of(markets, totals)
    mix = " ".join(f"{m}:{c * 100 // len(markets)}%"
                   for m, c in Counter(markets).most_common(3))
    print(f"    {label:9s} {hits:4d}/{len(markets)} = {strike:6.1%}  "
          f"base {base:6.1%}  edge {strike - base:+6.2%}   {mix}")
    return strike


def main() -> None:
    tot = {"honest": [], "oracle": [], "perfect": [], "totals": []}

    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 100:
            continue
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        df = store.load_results(code)
        df = df[df["hg"].notna() & df["ag"].notna()]
        rates = season_rates(df)

        # Which season each fixture belongs to, so the oracle rate is the one
        # for the campaign actually being played.
        by_date = df.set_index("date")["season"].to_dict()

        hm, om, pm, ts = [], [], [], []
        for req, (hg, ag) in pairs:
            total = int(hg) + int(ag)
            pred = predict_fixture(req, cfg, module_flags=flags)
            season = by_date.get(pd.Timestamp(req.match_date))
            k_h, k_a = (season, req.home_team), (season, req.away_team)
            if season is None or k_h not in rates or k_a not in rates:
                continue

            o_mu = rates[k_h] + rates[k_a]
            o = market_select.choose(
                o_mu, req.league_mu,
                max_under=cfg.max_under_line, min_over=cfg.min_over_line,
                min_win_prob=cfg.min_win_prob)
            p = market_select.choose(
                float(total), req.league_mu,
                max_under=cfg.max_under_line, min_over=cfg.min_over_line,
                min_win_prob=cfg.min_win_prob)
            if o is None or p is None:
                continue

            hm.append(pred.translated_play.market)
            om.append(o[0])
            pm.append(p[0])
            ts.append(total)

        if len(ts) < 80:
            print(f"{code}: too few ({len(ts)})", flush=True)
            continue

        print(f"\n{code}  {len(ts)} matches", flush=True)
        report("honest", hm, ts)
        report("oracle", om, ts)
        report("perfect", pm, ts)

        tot["honest"] += hm
        tot["oracle"] += om
        tot["perfect"] += pm
        tot["totals"] += ts

    if not tot["totals"]:
        print("no data")
        return

    print(f"\n{'=' * 72}\nALL LEAGUES  {len(tot['totals'])} matches")
    h = report("honest", tot["honest"], tot["totals"])
    o = report("oracle", tot["oracle"], tot["totals"])
    p = report("perfect", tot["perfect"], tot["totals"])
    print()
    print(f"  headroom from ANY better model of the teams: {o - h:+.2%}")
    print(f"  headroom left in the ladder beyond that:     {p - o:+.2%}")


if __name__ == "__main__":
    main()
