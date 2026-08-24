"""
Re-derive `buy from` under the recalibrated probabilities.

Every `buy≥` figure in the log was computed as `break_even x 1.05`, where
break-even comes from the engine's own probability for the rung. Both inputs
have since moved:

    MU_SHRINK      1.00 -> 0.35     probabilities are less extreme
    MIN_WIN_PROB   0.79 -> 0.75     a different set of rungs gets picked
    TEAM_SHRINK    none -> 0.62     team rungs were never shrunk at all

So every published threshold predates the work and is wrong by an unknown
amount. Worse, the 5% margin was itself chosen while probabilities were **10.8
points optimistic on the bets actually placed** — a cushion sized against a
number that was already inflated is not a cushion.

This does two things:

    DRIFT     re-prices every fixture in config/bets.tsv with the CURRENT
              engine and compares the new break-even against the published one.
              Says how stale the log's thresholds are.

    MARGIN    redoes the margin sweep over settled bets using the recalibrated
              break-even. The earlier sweep on old probabilities was flat noise;
              if calibration was the reason, a real threshold should now appear.

Fixtures are re-priced as of their own match date, so this is not a hindsight
re-run — it is what the engine would say today about the same fixture.

Usage:  python scripts/rederive_buyfrom.py
"""
from __future__ import annotations

import math
import re
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing
from app.predict import build_request
from scripts.ledger import read_fixtures

ROOT = Path(__file__).resolve().parents[2]
BETS = ROOT / "config" / "bets.tsv"

# README shows league display names; the engine needs codes.
LEAGUE_CODE = {
    "Premier League": "ENG-PL", "Championship": "ENG-CH", "Serie A": "ITA-SA",
    "Serie B": "ITA-SB", "LaLiga": "ESP-LL", "LaLiga 2": "ESP-L2",
    "Ligue 1": "FRA-L1", "Ligue 2": "FRA-L2", "Eredivisie": "NED-ED",
    "Brasileirão": "BRA-SA", "Brasileirão Série B": "BRA-SB",
    "J1 League": "JPN-J1", "Saudi Pro League": "SAU-PL",
    "Peru Liga 1": "PER-L1", "Colombia Primera A": "COL-PA",
    "Chile Primera División": "CHI-PD", "Süper Lig": "TUR-SL",
    "Belgian Pro League": "BEL-PL", "Swiss Super League": "SUI-SL",
    "Ekstraklasa": "POL-EK", "Liga Portugal": "POR-PL",
    "Allsvenskan": "SWE-AL", "Chinese Super League": "CHN-SL",
    "MLS": "MLS", "Danish Superliga": "DEN-SL",
}
DATE = re.compile(r"(\d{4}-\d{2}-\d{2})")


def readme_rows() -> dict[str, tuple[str, date]]:
    """fixture -> (league code, match date), from the README tables."""
    out = {}
    for ln in (ROOT / "README.md").read_text().splitlines():
        if not ln.startswith("|") or ln.count("|") != 7 or "---" in ln:
            continue
        c = [x.strip() for x in ln.split("|")]
        code = LEAGUE_CODE.get(c[2])
        m = DATE.search(c[6])
        if code and m:
            y, mo, d = map(int, m.group(1).split("-"))
            out[c[3]] = (code, date(y, mo, d))
    return out


def main() -> None:
    meta = readme_rows()
    fx = read_fixtures()

    drift, settled = [], []
    for ln in BETS.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        p = ln.split("\t")
        name, rung, odds, side = p[0], p[1], float(p[2]), p[3]
        if name not in meta or name not in fx:
            continue
        code, day = meta[name]
        home, _, away = name.partition(" v ")
        try:
            req = build_request(code, home, away, day)
        except Exception:
            continue
        if req is None:
            continue

        if side == "-":
            try:
                be_new = pricing.break_even(rung, req.mu_total)
            except ValueError:
                continue
        else:
            ptt = req.p_home_tt05 if side == "H" else req.p_away_tt05
            if not ptt or not (0 < ptt < 1):
                continue
            gf = -math.log(1 - ptt)
            pr = {"O0.5": 1 - math.exp(-gf),
                  "O1.5": 1 - math.exp(-gf) * (1 + gf),
                  "U1.5": math.exp(-gf) * (1 + gf)}.get(rung)
            if not pr or pr <= 0:
                continue
            be_new = 1 / pr

        f = fx[name]
        goals = None
        if f["hg"] is not None:
            goals = (f["hg"] + f["ag"] if side == "-"
                     else (f["hg"] if side == "H" else f["ag"]))
        drift.append((name, rung, odds, be_new))
        if goals is not None:
            s = pricing.settle_fraction(rung, goals)
            settled.append(dict(ret=max(s, 0.0) * odds + (1 - abs(s)),
                                edge=odds / be_new - 1))

    print(f"{len(drift)} bets re-priced, {len(settled)} of them settled\n")

    below = sum(1 for _n, _r, o, b in drift if o < b)
    print(f"bets now sitting BELOW break-even: {below} of {len(drift)} "
          f"({below/len(drift)*100:.0f}%)")
    mean_be = sum(b for _n, _r, _o, b in drift) / len(drift)
    print(f"mean recalibrated break-even {mean_be:.3f}\n")

    def roi(rs):
        return (sum(r["ret"] for r in rs) / len(rs) - 1) * 100 if rs else 0.0

    print("margin sweep on RECALIBRATED break-even")
    print(f"{'require >=':>12}{'bets':>6}{'ROI':>9}")
    for m in (0.0, 0.02, 0.05, 0.08, 0.10, 0.15, 0.20):
        sel = [r for r in settled if r["edge"] >= m]
        if sel:
            print(f"{m*100:11.0f}%{len(sel):6}{roi(sel):+8.1f}%")


if __name__ == "__main__":
    main()
