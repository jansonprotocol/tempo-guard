"""
What should a "no bet" be measured on?

Abstention is only worth having if the matches it drops lose MORE OFTEN than
the ones it keeps. That is the whole test, and it is not the same as the kept
strike going up: a gate that keeps 20% of fixtures will show a higher strike by
sampling noise alone. So every candidate here is scored on the gap

    gap = strike(kept) - strike(dropped)

A gate with a big positive gap is finding fixtures the engine genuinely cannot
read. A gap near zero means it is discarding matches at random and buying its
headline strike with volume. A NEGATIVE gap means the gate is backwards — it is
throwing away the winners.

CANDIDATES
==========
    edge        predicted edge of the chosen market. The obvious choice, and the
                one this codebase already has evidence against: the sharp-lane
                comparison found a rule maximising predicted edge lost on
                REALISED edge to a z-score gate that never computes edge. The
                Poisson tails are overconfident and the largest predicted edges
                are disproportionately where the model is wrong. Included
                precisely so that claim is tested rather than asserted.

    p_win       win probability of the chosen market. Already the floor's
                currency, but the floor never abstains — it falls back to the
                safest buyable rung instead. This asks whether refusing outright
                beats retreating.

    margin      how far the chosen market leads the runner-up. Decisiveness
                rather than confidence: the model may be sure a market wins and
                still be indifferent between three of them.

    stability   whether the pick survives nudging mu by +/-0.15 goals. If a
                tenth of a goal flips the market, the choice was arbitrary and
                the goal estimate is nowhere near that precise.

    unusual     |mu - league_mu|. How far the fixture sits from its league's
                norm. This is the family the z-score sharp gate belongs to, and
                the only selection rule in the engine with a track record.

    history     matches of prior data on the two sides. An honest "I do not
                know" — newly promoted teams and early-season fixtures are read
                off almost nothing.

Nothing is written. This decides what a no-bet should key on before one exists.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import numpy as np

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LEAGUES = ["ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "NED-ED", "TUR-SL",
           "JPN-J1", "BRA-SA", "POR-PL", "MLS", "FRA-L2", "ENG-CH"]
LIMIT = 300

# Fractions of the sample to drop. Reported per gate so the trade between
# volume and strike is visible rather than hidden behind one threshold.
DROP_FRACTIONS = [0.10, 0.25, 0.40]

NUDGE = 0.15


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def gates_for(req, cfg, market, counts) -> dict:
    """Every candidate signal for one fixture, computed the same way for all."""
    mu, lmu = req.mu_total, req.league_mu
    kw = dict(max_under=cfg.max_under_line, min_over=cfg.min_over_line,
              min_win_prob=cfg.min_win_prob)

    scored = market_select.score_markets(mu, lmu)
    ranked = sorted(scored, key=lambda r: -r[1])
    edge = next((e for m, e, _h, _t in scored if m == market), 0.0)
    pw = next((h for m, _e, h, _t in scored if m == market), 0.0)
    margin = (ranked[0][1] - ranked[1][1]) if len(ranked) > 1 else 0.0

    up = market_select.choose(mu + NUDGE, lmu, **kw)
    dn = market_select.choose(mu - NUDGE, lmu, **kw)
    stable = float(bool(up and dn and up[0] == market and dn[0] == market))

    return {
        "edge": edge,
        "p_win": pw,
        "margin": margin,
        "stability": stable,
        "unusual": abs(mu - lmu) if lmu else 0.0,
        "history": float(min(counts.get(req.home_team, 0),
                             counts.get(req.away_team, 0))),
    }


def main() -> None:
    rows = []
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
        df = df[df["hg"].notna() & df["ag"].notna()].sort_values("date")

        for req, (hg, ag) in pairs:
            pred = predict_fixture(req, cfg, module_flags=flags)
            market = pred.translated_play.market
            past = df[df["date"] < np.datetime64(req.match_date)]
            counts = {
                req.home_team: int(((past["home"] == req.home_team) |
                                    (past["away"] == req.home_team)).sum()),
                req.away_team: int(((past["home"] == req.away_team) |
                                    (past["away"] == req.away_team)).sum()),
            }
            g = gates_for(req, cfg, market, counts)
            g["won"] = won(market, int(hg) + int(ag))
            g["code"] = code
            rows.append(g)
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return

    base = sum(1 for r in rows if r["won"]) / len(rows)
    print(f"\n{len(rows)} fixtures, overall strike {base:.1%}\n")
    print(f"  {'gate':10s} {'drop':>5}  {'kept':>16}  {'dropped':>15}  {'GAP':>7}")
    print("  " + "-" * 62)

    for gate in ("edge", "p_win", "margin", "stability", "unusual", "history"):
        vals = np.array([r[gate] for r in rows], dtype=float)
        for frac in DROP_FRACTIONS:
            # Drop the LOWEST values of the gate — the reading of every
            # candidate here is "more is more confident".
            cut = float(np.quantile(vals, frac))
            keep = [r for r, v in zip(rows, vals) if v > cut]
            drop = [r for r, v in zip(rows, vals) if v <= cut]
            if not keep or not drop:
                print(f"  {gate:10s} {frac:5.0%}  no split at this threshold")
                continue
            ks = sum(1 for r in keep if r["won"]) / len(keep)
            ds = sum(1 for r in drop if r["won"]) / len(drop)
            print(f"  {gate:10s} {frac:5.0%}  {len(keep):4d} = {ks:6.1%}  "
                  f"{len(drop):4d} = {ds:6.1%}  {ks - ds:+7.2%}")
        print()


if __name__ == "__main__":
    main()
