"""
When Tip 1 and the team lane point the same way, is that two signals or one?

The reasoning is natural and worth taking seriously: Hammarby v GAIS had Tip 1
`O1.5` at 83.2% and Tip 2 `Hammarby O1.5` at 64.1%. Both say "1.5 goals". They
look like independent readings that agree, and agreement usually justifies
leaning in.

Two reasons to check rather than assume.

**The markets are nested, not parallel.** If Hammarby scores twice then the
match total is 2+ by arithmetic. `Hammarby O1.5` is a strict SUBSET of match
`O1.5` — every way the team bet wins is a way the match bet wins, and not the
reverse. So the team probability CANNOT exceed the match probability; 64.1%
below 83.2% is forced, not informative. Agreement between a set and its own
subset is the "set containment mistaken for evidence" trap already on record
in this project.

**They come from one number.** Both probabilities are read off the same fitted
`mu`, through the same model. The team lane is not a second opinion about the
fixture; it is the same opinion sliced differently. If that is right, Tip 1
being strong tells you nothing the team lane's own probability has not already
counted — and the published `buy from` already contains it.

But "if that is right" is the part worth measuring. If nested team lanes
systematically over-deliver when Tip 1 is strong, the model is leaving something
on the table and leaning in IS justified.

So: replay fixtures, keep only cases where the team lane is nested inside Tip 1
(both overs), bucket by how strong Tip 1 was, and compare what the team lane
claimed against what it delivered.

    says     mean probability the engine attached to the TEAM lane
    hit      what the team lane actually did
    gap      hit - says. If this rises with Tip 1's strength, the user is right
             and Tip 1 agreement carries information the team price ignores.
             If it is flat, the agreement is already priced.

Usage:  python scripts/nested_lanes.py [--n 120] [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select, team_total
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture

LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "GER-B2", "ESP-LL", "ESP-L2",
           "ITA-SA", "ITA-SB", "FRA-L1", "FRA-L2", "NED-ED", "POR-PL",
           "BEL-PL", "TUR-SL", "SCO-PL", "DEN-SL", "POL-EK", "JPN-J1",
           "BRA-SA", "MEX-LMX", "SWE-AL", "NOR-EL"]

# Buckets on Tip 1's own probability — "how strongly does the match lane agree".
BANDS = [(0.0, 0.78), (0.78, 0.82), (0.82, 0.86), (0.86, 1.01)]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if not n:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - m) / d, (c + m) / d)


def collect(lg: str, n: int) -> list[tuple[float, float, bool]]:
    """(Tip 1 probability, team-lane probability, team lane won)."""
    df = store.load_results(lg)
    if df is None or len(df) < 200:
        return []
    cfg = config.get(lg)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    out = []
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(lg, str(r["home"]), str(r["away"]), d)
            if req is None:
                continue
            t1 = predict_fixture(
                req, cfg, module_flags=flags).translated_play.market
            if not t1 or not t1.startswith("O"):
                continue
            tt = team_total.candidates(lg, d, req.p_home_tt05, req.p_away_tt05)
        except Exception:
            continue
        if not tt:
            continue
        market, p_team, _edge = tt[0]
        rung = market.split()[-1]              # "TA O1.5" -> "O1.5"
        if not rung.startswith("O"):
            continue
        # Containment needs the TEAM line to be at least the match line. One
        # side scoring 2+ forces a total of 2+, so team `O1.5` sits inside match
        # `O1.5`. But team `O0.5` against match `O1.5` is NOT nested — the home
        # side can score once in a 1-0 that fails the match lane — and those
        # pairs showed a team probability ABOVE the match probability, which is
        # what exposed this filter being wrong the first time.
        if float(rung[1:]) < float(t1[1:]):
            continue
        p1 = market_select.p_win(t1, req.mu_total)
        won = team_total.won(market, int(r["hg"]), int(r["ag"]))
        out.append((p1, p_team, bool(won)))
    return out


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 120
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)

    rows = []
    for lg in codes:
        try:
            rows += collect(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    if not rows:
        print("no nested pairs found")
        return

    print(f"{len(rows)} fixtures where an OVER team lane sits inside an OVER "
          f"Tip 1\n")
    # Containment is a fact about the markets, not a finding — assert it so a
    # future change that breaks the nesting shows up here rather than silently.
    bad = [r for r in rows if r[1] > r[0] + 1e-9]
    print(f"team probability above match probability: {len(bad)} "
          f"(must be 0 — the team lane is a subset)\n")

    print(f"{'Tip 1 says':14}{'n':>7}{'team says':>12}{'team hit':>11}"
          f"{'gap':>8}{'95% CI':>13}")
    for lo, hi in BANDS:
        b = [r for r in rows if lo <= r[0] < hi]
        if len(b) < 30:
            continue
        k = sum(1 for r in b if r[2])
        hit = k / len(b)
        says = sum(r[1] for r in b) / len(b)
        w = wilson(k, len(b))
        print(f"{lo*100:5.0f}-{hi*100:3.0f}%{'':<3}{len(b):7}{says*100:11.1f}%"
              f"{hit*100:10.1f}%{(hit-says)*100:+8.1f}"
              f"   [{w[0]*100:.0f}-{w[1]*100:.0f}]")

    k = sum(1 for r in rows if r[2])
    hit = k / len(rows)
    says = sum(r[1] for r in rows) / len(rows)
    print(f"\n{'ALL':14}{len(rows):7}{says*100:11.1f}%{hit*100:10.1f}%"
          f"{(hit-says)*100:+8.1f}")


if __name__ == "__main__":
    main()
