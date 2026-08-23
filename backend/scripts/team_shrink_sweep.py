"""
Sweep TEAM_SHRINK against the defect the nested-lane test found.

`scripts/nested_lanes.py` measured team lanes over-delivering by +2.7 points on
1,632 fixtures (z = 2.26): the engine claims 60.2% and returns 62.9%. That is
the signature of shrinking a side's scoring rate too far toward the league mean,
which is exactly what `TEAM_SHRINK = 0.62` does. If so, relaxing it should close
the gap — and if the gap does not close as the constant moves, the diagnosis is
wrong and the constant should be left alone.

**RESULT: the diagnosis was wrong and 0.62 stands.** Across ~6,900 fixtures in
two independent windows the current value lands at a gap of -0.1 and +0.8, and
every relaxation degrades it monotonically, to -5.3 at k = 1.00. The +2.7 lives
in the SUBSET nested_lanes selected — over-rung team lanes sitting inside
over-rung match lanes — and not in the lane as a whole, which this scores in
full. A real number on a real slice that generalised to nothing: the project's
own recorded trap, walked into while checking something else.

Swept downward afterwards to check 0.62 was a crossover and not just the edge
of the range tested. It is:

    k       lanes   hit R   hit H   mean gap
    0.38      561   83.3%   83.7%     +5.30
    0.46      712   81.4%   81.0%     +3.90
    0.54      880   78.9%   78.4%     +2.50
    0.62     1079   75.3%   76.0%     +0.30   <- current
    0.70     1279   73.1%   74.7%     -0.70

The zero crossing sits between 0.62 and 0.70, and at a standard error of about
1.3 points those two are not distinguishable on calibration. They ARE
distinguishable on the two things that decide it. **Raw hit rate falls
monotonically as k rises** — 0.62 delivers 75.3/76.0 against 0.70's 73.1/74.7 —
and this project optimises for hit rate over edge. Volume runs the other way,
0.70 offering about 18% more lanes. Hit rate wins, so 0.62 stays.

Relaxing all the way to 1.00 raises team-lane offers from 1,086 to 1,954, but
those extra lanes arrive 5 points overconfident: volume bought by
miscalibration rather than earned.

**One replay pass serves every candidate.** The shrink is invertible: the
published probability is `1 - exp(-gf')` where `gf' = league_mu/2 + k*(gf -
league_mu/2)`, so the raw per-side rate comes back as
`gf = (gf' - league_mu/2)/k + league_mu/2`. Recovering it once means each k is
arithmetic rather than a re-replay, which turns two hours of pricing into
minutes. The cost is the 3-decimal rounding on the stored probability, worth
about 0.005 goals on a rate near 1.75 — far below anything this measures.

**Two windows, because a constant fitted on recent data will flatter itself
there.** RECENT is the last N matches per league; HELD BACK drops those and
scores the N before them. A value that only wins on one of the two is a fit, not
a finding.

**The match lane is watched at the same time.** `p_tt05` is not confined to the
team lane — `pipeline._o25_addon_allowed` gates the `O2.5` add-on on both sides
clearing ADDON_MIN_TT05 = 0.60. Moving the shrink moves those probabilities and
can therefore move Tip 1. The sweep counts how many fixtures change side of that
gate, so a team-lane gain that quietly costs match-lane tips cannot pass
unnoticed.

Usage:  python scripts/team_shrink_sweep.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, features, store
from app.engine import team_total
from app.engine.pipeline import ADDON_MIN_TT05
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture

LEAGUES = ["ENG-PL", "ENG-CH", "GER-BL", "GER-B2", "ESP-LL", "ESP-L2",
           "ITA-SA", "ITA-SB", "FRA-L1", "FRA-L2", "NED-ED", "POR-PL",
           "BEL-PL", "TUR-SL", "SCO-PL", "DEN-SL", "POL-EK", "JPN-J1",
           "BRA-SA", "MEX-LMX", "SWE-AL", "NOR-EL", "CHI-PD", "ARG-PD"]

# Spans both directions: the first run only went upward, which would have
# left 0.62 sitting at the boundary of the tested range rather than shown
# to be a crossover.
CANDIDATES = [0.38, 0.46, 0.54, 0.62, 0.70, 0.78, 0.86, 0.94, 1.00]


def wilson(k: int, n: int, z: float = 1.96) -> tuple[float, float]:
    if not n:
        return (0.0, 0.0)
    p = k / n
    d = 1 + z * z / n
    c = p + z * z / (2 * n)
    m = z * math.sqrt(p * (1 - p) / n + z * z / (4 * n * n))
    return ((c - m) / d, (c + m) / d)


def raw_gf(p_tt05: float, league_mu: float, k: float) -> float:
    """Undo the shrink: recover the per-side rate the features actually built."""
    if not league_mu or league_mu <= 0 or k <= 0:
        return -math.log(max(1e-9, 1.0 - p_tt05))
    shrunk = -math.log(max(1e-9, 1.0 - p_tt05))
    return (shrunk - league_mu / 2) / k + league_mu / 2


def shrunk_p(gf: float, league_mu: float, k: float) -> float:
    return 1.0 - math.exp(-max(0.05, league_mu / 2 + k * (gf - league_mu / 2)))


def collect(lg: str, n: int, back: int) -> list:
    df = store.load_results(lg)
    if df is None or len(df) < 260:
        return []
    ordered = df.sort_values("date")
    if back:
        ordered = ordered.iloc[:-back]
    cfg = config.get(lg)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    k0 = features.TEAM_SHRINK

    out = []
    for _, r in ordered.tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(lg, str(r["home"]), str(r["away"]), d)
            if req is None or req.p_home_tt05 is None or req.p_away_tt05 is None:
                continue
            predict_fixture(req, cfg, module_flags=flags)
        except Exception:
            continue
        lmu = req.league_mu
        out.append((lg, d, lmu,
                    raw_gf(req.p_home_tt05, lmu, k0),
                    raw_gf(req.p_away_tt05, lmu, k0),
                    int(r["hg"]), int(r["ag"])))
    return out


def score(rows: list, k: float) -> tuple:
    """(n, mean stated p, hit rate, fixtures with a side across the O2.5 gate)."""
    n = hits = gate = 0
    p_sum = 0.0
    for lg, d, lmu, gfh, gfa, hg, ag in rows:
        ph = shrunk_p(gfh, lmu, k)
        pa = shrunk_p(gfa, lmu, k)
        gate += (ph >= ADDON_MIN_TT05) + (pa >= ADDON_MIN_TT05)
        try:
            cands = team_total.candidates(lg, d, ph, pa)
        except Exception:
            continue
        if not cands:
            continue
        market, p, _e = cands[0]
        n += 1
        p_sum += p
        hits += team_total.won(market, hg, ag)
    return n, (p_sum / n if n else 0.0), (hits / n if n else 0.0), hits, gate


def report(label: str, rows: list) -> None:
    print(f"\n=== {label} — {len(rows)} fixtures ===")
    print(f"{'k':>6}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'95% CI':>13}"
          f"{'O2.5 gate':>11}")
    for k in CANDIDATES:
        n, says, hit, hits, gate = score(rows, k)
        if not n:
            continue
        lo, hi = wilson(hits, n)
        mark = "  <- current" if abs(k - features.TEAM_SHRINK) < 1e-9 else ""
        print(f"{k:6.2f}{n:7}{says*100:7.1f}%{hit*100:7.1f}%{(hit-says)*100:+8.1f}"
              f"   [{lo*100:.0f}-{hi*100:.0f}]{gate:11}{mark}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)

    for label, back in (("RECENT", 0), ("HELD BACK", n)):
        rows = []
        for lg in codes:
            try:
                rows += collect(lg, n, back)
            except Exception as exc:
                print(f"{lg:9} FAILED {exc}", file=sys.stderr)
        if rows:
            report(label, rows)

    print("\ngap near zero is the target: the lane should deliver what it says.")
    print("A k that wins on RECENT but not HELD BACK is a fit, not a finding.")


if __name__ == "__main__":
    main()
