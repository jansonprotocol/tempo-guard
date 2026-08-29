"""
Can the engine price the RESULT market it has always refused to touch?

The bettor's Tip 3 proposal (29 Aug): 1X when the away win is the least
likely outcome, X2 mirrored, 12 when the draw is, and DNB when one side
is significantly stronger inside its double chance. The engine already
holds a goal expectation per side — published as p_home_tt05/p_away_tt05
and cut into team totals — and two independent Poissons turn that pair
into P(home), P(draw), P(away), hence every lane above.

Two known reasons to distrust it, stated before measuring: the per-side
layer is the engine's weakest (the streak debit exists because of it),
and independent Poissons under-price the draw — the exact outcome 1X/X2
insure against and 12 bets against. So this instrument replays the
result probabilities as-of and grades every lane's claim against what
happened, per half-window, before any Tip 3 is built.

    python scripts/result_lanes.py --n 300 --leagues A,B --dump out.pkl
"""
from __future__ import annotations

import math
import pickle
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.predict import build_request

MAX_G = 10


def _pois(mu: float):
    return [math.exp(-mu) * mu ** k / math.factorial(k)
            for k in range(MAX_G + 1)]


def result_probs(gf_h: float, gf_a: float):
    """(p_home, p_draw, p_away) from two independent Poissons."""
    ph, pa = _pois(gf_h), _pois(gf_a)
    home = draw = away = 0.0
    for i, x in enumerate(ph):
        for j, y in enumerate(pa):
            p = x * y
            if i > j:
                home += p
            elif i == j:
                draw += p
            else:
                away += p
    return home, draw, away


def replay(league: str, n: int, days: int = 730) -> list[dict]:
    df = store.load_results(league)
    if df is None or len(df) < 200:
        return []
    ordered = df.sort_values("date")
    cut = ordered["date"].max() - timedelta(days=days)
    recent = ordered[ordered["date"] >= cut].tail(n)

    out = []
    for _, r in recent.iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if req is None or not req.p_home_tt05 or not req.p_away_tt05:
            continue
        if not (0 < req.p_home_tt05 < 1 and 0 < req.p_away_tt05 < 1):
            continue
        gf_h = -math.log(1 - req.p_home_tt05)
        gf_a = -math.log(1 - req.p_away_tt05)
        p_h, p_d, p_a = result_probs(gf_h, gf_a)
        try:
            hg, ag = int(r["hg"]), int(r["ag"])
        except (ValueError, TypeError):
            continue
        res = "H" if hg > ag else "D" if hg == ag else "A"
        out.append(dict(code=league, d=d, p_h=p_h, p_d=p_d, p_a=p_a,
                        res=res))
    return out


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 300
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    dump = Path(args[args.index("--dump") + 1]) if "--dump" in args else None

    rows: list[dict] = []
    for lg in codes:
        try:
            got = replay(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
            continue
        if got:
            d_says = sum(r["p_d"] for r in got) / len(got)
            d_hit = sum(r["res"] == "D" for r in got) / len(got)
            print(f"{lg:9}{len(got):5}  draw says {d_says*100:5.1f}%  "
                  f"happens {d_hit*100:5.1f}%  gap {(d_hit-d_says)*100:+5.1f}",
                  flush=True)
            rows += got

    if dump is not None:
        dump.write_bytes(pickle.dumps(rows))
        print(f"dumped {len(rows)} result rows -> {dump}")


if __name__ == "__main__":
    main()
