"""
Baseline hitrates for all three tips — the hero bar's data.

The header bar under the hero image quotes what each tip lane lands at
"in general", so a visitor can hold the session tiles against something
steadier than one weekend. General means: each league's most recent 300
fixtures replayed strictly as-of through the SAME code path a live card
uses (two_tips.tips — tip 1's ladder pick, tip 2's runner-up/team lane,
tip 3's result lane), graded against the real scores, push counted as a
hit exactly as the board counts it. Per-league rates first, then the
plain average across leagues — matching the retrosim table's framing,
where every league speaks with equal weight rather than the busiest
calendar drowning the rest.

Writes config/baselines.tsv (derived, never typed); the app reads it at
render time and shows nothing if the file is missing — a missing
baseline is better than a stale one.

Usage:  python scripts/baselines.py [--n 300] [--leagues MLS,JPN-J1]
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.engine import result_market, team_total
from app.util.asian_lines import evaluate_market
from scripts.two_tips import tips

OUT = Path(__file__).resolve().parents[2] / "config" / "baselines.tsv"
DEFAULT_N = 300
MIN_ROWS = 200          # same thin-league floor as retrosim


def _hit(res) -> bool:
    # The board's convention everywhere: a push plays the rung a notch
    # softer and wins there, so it counts as a hit; only a full or half
    # loss is a miss.
    return res in (True, "half_win", "push")


def replay(league: str, n: int) -> dict | None:
    df = store.load_results(league)
    if df is None or len(df) < MIN_ROWS:
        return None
    recent = df.sort_values("date").tail(n)
    t1 = [0, 0, 0.0]
    t2 = [0, 0, 0.0]
    t3 = [0, 0, 0.0]
    for _, r in recent.iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        hg, ag = int(r["hg"]), int(r["ag"])
        try:
            out = tips(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if out is None:
            continue
        res = evaluate_market(out["t1"][0], hg, ag)
        if res is not None:
            t1[1] += 1
            t1[0] += _hit(res)
            t1[2] += out["t1"][1]
        if out["t2"] is not None:
            mk = out["t2"][0]
            try:
                res = (team_total.won(mk, hg, ag) if mk.startswith("T")
                       else evaluate_market(mk, hg, ag))
            except (ValueError, TypeError):
                res = None
            if res is not None:
                t2[1] += 1
                t2[0] += _hit(res if isinstance(res, str) else bool(res))
                t2[2] += out["t2"][1]
        if out["t3"] is not None:
            won = result_market.won(out["t3"][0], hg, ag)
            t3[1] += 1
            t3[0] += won is not False       # None = DNB push = hit
            t3[2] += out["t3"][1]
    if not t1[1]:
        return None
    return dict(league=league, t1=t1, t2=t2, t3=t3)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else DEFAULT_N
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))

    rows = []
    for lg in codes:
        try:
            out = replay(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
            continue
        if out:
            rows.append(out)
            f = out
            print(f"{lg:9} t1 {f['t1'][0]:4}/{f['t1'][1]:<4}"
                  f" t2 {f['t2'][0]:4}/{f['t2'][1]:<4}"
                  f" t3 {f['t3'][0]:4}/{f['t3'][1]:<4}", flush=True)

    lines = [
        "# Baseline hitrates per league: each tip replayed as-of over the",
        f"# league's most recent {n} fixtures on the current build, push",
        "# counted as a hit. Written by scripts/baselines.py — derived,",
        "# never typed. Read by the app's hero baseline bar, which averages",
        "# the per-league rates with equal weight.",
        "# Trailing columns are the summed CLAIM behind those graded lanes,",
        "# so the app can print hitrate against what the engine promised.",
        "# league\tt1_hits\tt1_n\tt2_hits\tt2_n\tt3_hits\tt3_n"
        "\tt1_says\tt2_says\tt3_says",
    ]
    for f in rows:
        lines.append(f"{f['league']}\t{f['t1'][0]}\t{f['t1'][1]}"
                     f"\t{f['t2'][0]}\t{f['t2'][1]}"
                     f"\t{f['t3'][0]}\t{f['t3'][1]}"
                     f"\t{f['t1'][2]:.4f}\t{f['t2'][2]:.4f}"
                     f"\t{f['t3'][2]:.4f}")
    OUT.write_text("\n".join(lines) + "\n")

    for name, key in (("tip 1", "t1"), ("tip 2", "t2"), ("tip 3", "t3")):
        rates = [f[key][0] / f[key][1] for f in rows if f[key][1] >= 30]
        if rates:
            print(f"{name}: {sum(rates) / len(rates) * 100:.1f}% "
                  f"(avg over {len(rates)} leagues)")
    print(f"written: {OUT}")


if __name__ == "__main__":
    main()
