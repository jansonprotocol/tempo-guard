"""
Is the domestic board overconfident, and if so, where?

The per-league hitrate table is read as a list of broken leagues. It is
mostly not. At the 200 fixtures each row is measured on, the standard
error on a hit rate is about 2.8 points, so a league showing −4 is one
standard error from honest. Re-measured at ~780 apiece, sixteen leagues
that ranged from −11.6 to −1.8 collapsed into a band from −2.8 to +1.7
around a pooled −1.7. ROU-L1, the worst row on the board, came back −2.8.

What survives is not per-league at all: a small, uniform overconfidence
across the whole domestic side. This measures it where it matters.

Three questions, in order:

    1. Is the gap FLAT across stated probability, or sloped? A flat gap
       is a level error and takes a flat debit — the shape that worked
       for cups. A sloped gap is a spread error and a debit would fix
       the middle while breaking both ends.
    2. Does it survive in the PLAYED population? Retrosim scores every
       fixture that produced a tip; the board only publishes those over
       the probability floor and the edge bar. If the overconfidence
       lives in the low tail that never gets played, debiting the whole
       board would be a correction to nothing.
    3. Does it hold in BOTH time windows? The project's standing bar.

Usage:  python scripts/says_calibration.py [--n 800] [--cache rows.pkl]
                                           [--debit 0.017]
"""
from __future__ import annotations

import math
import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, store
from app.engine import market_select
from app.predict import build_request
from app.util.asian_lines import evaluate_market, hit_weight

# Every domestic code the board tips, cups excluded — the cup lane has
# its own debit and its own probation.
LEAGUES = ("ENG-PL", "ENG-CH", "ENG-L2", "ENG-NL", "ESP-LL", "ESP-L2",
           "GER-BL", "ITA-SA", "ITA-SB", "FRA-L1", "FRA-L2", "NED-ED",
           "NED-D2", "POR-PL", "BEL-PL", "TUR-SL", "SCO-PL", "SCO-CH",
           "DEN-SL", "NOR-EL", "SWE-AL", "FIN-VL", "POL-EK", "CZE-FL",
           "ROU-L1", "GRE-SL", "SUI-SL", "UKR-PL", "CRO-1L", "IRL-PD",
           "BRA-SA", "BRA-SB", "ARG-PD", "ARG-CLP", "COL-PA", "PER-L1",
           "CHI-PD", "MEX-LMX", "MLS", "SAU-PL", "JPN-J1", "CHN-SL",
           "MAR-BP", "ALG-L1", "NGA-PL")


def rows_for(code: str, n: int) -> list[dict]:
    """Every fixture in the window that produced a tip, priced as-of."""
    df = store.load_results(code)
    if df is None or len(df) < 150:
        return []
    df = df.dropna(subset=["hg", "ag"]).sort_values("date")
    cfg = config.get(code)
    floor = cfg.min_win_prob or market_select.MIN_WIN_PROB
    out = []
    for r in df.tail(n).itertuples():
        try:
            req = build_request(code, str(r.home), str(r.away), r.date.date())
        except Exception:
            continue
        if req is None or not req.mu_total or not req.league_mu:
            continue
        best = None
        for m, e, p, _q in market_select.score_markets(req.mu_total,
                                                       req.league_mu):
            if not market_select.playable(m, cfg.max_under_line,
                                          cfg.min_over_line):
                continue
            if p < floor:
                continue
            if best is None or p > best[1]:
                best = (m, p, e)
        if best is None:
            continue
        res = evaluate_market(best[0], int(r.hg), int(r.ag))
        if res is None:
            continue
        out.append(dict(d=r.date, code=code, says=best[1], edge=best[2],
                        hit=hit_weight(res) >= 1.0))
    return out


def band(rows, label):
    if len(rows) < 60:
        print(f"  {label:22} too few: {len(rows)}")
        return None
    says = sum(r["says"] for r in rows) / len(rows) * 100
    hit = sum(1 for r in rows if r["hit"]) / len(rows) * 100
    se = math.sqrt(hit / 100 * (1 - hit / 100) / len(rows)) * 100
    gap = hit - says
    flag = "  <--" if abs(gap) > 2 * se else ""
    print(f"  {label:22} n {len(rows):5}  says {says:5.1f}  hit {hit:5.1f}  "
          f"gap {gap:+5.1f} ± {se:.1f}{flag}")
    return gap


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 800
    cache = Path(args[args.index("--cache") + 1]) if "--cache" in args else None

    if cache and cache.exists():
        rows = pickle.loads(cache.read_bytes())
    else:
        rows = []
        for code in LEAGUES:
            got = rows_for(code, n)
            print(f"{code}: {len(got)}", file=sys.stderr, flush=True)
            rows += got
        if cache:
            cache.write_bytes(pickle.dumps(rows))
    rows.sort(key=lambda r: r["d"])
    print(f"\n{len(rows)} domestic tips across "
          f"{len({r['code'] for r in rows})} leagues\n")

    print("POOLED")
    band(rows, "everything")

    # 1. Flat or sloped? Bands of stated probability.
    print("\nBY STATED PROBABILITY — flat means a level error, and a level "
          "error takes a flat debit")
    edges = [(0.0, .78), (.78, .82), (.82, .86), (.86, .90), (.90, 1.01)]
    for lo, hi in edges:
        band([r for r in rows if lo <= r["says"] < hi],
             f"says {lo*100:.0f}-{hi*100:.0f}%")

    # 2. Does it survive where the board actually plays?
    print("\nBY EDGE — the board publishes above +1%, so that is the "
          "population the debit would act on")
    for lo, hi, lab in ((-99, 1.0, "under +1% (withheld)"),
                        (1.0, 3.0, "+1 to +3% (played)"),
                        (3.0, 99, "+3% and up (played)")):
        band([r for r in rows if lo <= r["edge"] * 100 < hi], lab)

    # 3. Both windows.
    print("\nBY WINDOW — the standing bar")
    mid = rows[len(rows) // 2]["d"]
    older = [r for r in rows if r["d"] < mid]
    newer = [r for r in rows if r["d"] >= mid]
    go = band(older, "older half")
    gn = band(newer, "newer half")
    played = [r for r in rows if r["edge"] * 100 >= 1.0]
    band([r for r in played if r["d"] < mid], "older half · played")
    band([r for r in played if r["d"] >= mid], "newer half · played")

    if go is not None and gn is not None:
        print()
        if go < 0 and gn < 0:
            print("Both windows overconfident — same sign, the bar is met.")
        else:
            print("The windows DISAGREE in sign. No debit ships on this.")


if __name__ == "__main__":
    main()
