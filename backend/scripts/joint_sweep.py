"""
MU_SHRINK and MIN_WIN_PROB together, because neither can be set alone.

`calibrate_mu.py` says mu is still over-spread: slope 0.711 where 1.0 is right,
so the remaining correction is MU_SHRINK 0.35 -> ~0.25. That single edit is what
`features.py` warns against. The first recalibration shipped 0.60, found the
market mix collapsing onto U4.25, and only recovered when the probability floor
came down from 0.79 to 0.75 — because shrinking mu pulls every probability
toward the league mean, so fewer rungs clear the floor and the selector falls
through to the safest buyable rung.

Two constants, one effect, and this project has already been burned setting one
without the other.

Reported per pair, with the number that motivates the change kept separate:

    gap        hit rate minus claimed probability, pooled
    TAIL gap   the same for tips carrying over +3.5% stated edge. This is the
               defect being fixed — pooled gap is near zero today precisely
               because the under-confident low band cancels the tail.
    hit        raw strike rate, which this project optimises over edge
    EDGE       hit minus the base rate of the markets chosen
    top        share of tips landing on the single most-emitted market — the
               collapse indicator that caught the 0.60 attempt

One replay per k, scored at every floor: the shrink lives in the features and
has to be recomputed, the floor does not.

Usage:  python scripts/joint_sweep.py [--n 120] [--leagues A,B]
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, features, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

KS = [0.25, 0.35]
FLOORS = [0.75]

# ALL leagues, not a hand-picked subset. The first run used twelve big European
# leagues and reported the tail at +2.1 where scripts/edge_bands.py, over all
# 62, reports -2.5 on the same constants. Same engine, opposite sign: on that
# subset the engine is under-confident and there is no defect to fix, so the
# sweep was answering a question nobody asked. A tuning run has to be measured
# on the population the defect was found in.
LEAGUES: list[str] = []


def base_rates(code: str) -> dict[str, float]:
    df = store.load_results(code)
    if df is None or df.empty:
        return {}
    out = {}
    for m in ("O1.0", "O1.5", "O1.75", "O2.25", "O2.5", "O2.75",
              "U2.5", "U2.75", "U3.0", "U3.25", "U3.5", "U3.75", "U4.25"):
        w = [hit_weight(evaluate_market(m, int(h), int(a)))
             for h, a in zip(df["hg"], df["ag"])]
        w = [x for x in w if x >= 0]
        out[m] = sum(1 for x in w if x >= 1.0) / len(w) if w else 0.0
    return out


def run(k: float, n: int, codes: list[str]) -> dict:
    features.MU_SHRINK = k
    features._INDEX_CACHE.clear()
    rows: dict[float, list] = {f: [] for f in FLOORS}
    # Counted, not swallowed. The first version wrapped the whole call in a
    # bare `except Exception: continue` and passed the floor as a keyword
    # predict_fixture does not take — so every iteration raised TypeError,
    # every one was silently skipped, and the sweep printed an empty table
    # rather than an error. A failure that produces no rows must say so.
    skipped = 0
    for lg in codes:
        df = store.load_results(lg)
        if df is None or len(df) < 200:
            continue
        cfg = config.get(lg)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        rates = base_rates(lg)
        for _, r in df.sort_values("date").tail(n).iterrows():
            try:
                req = build_request(lg, str(r["home"]), str(r["away"]),
                                    r["date"].date())
                if req is None:
                    continue
            except Exception:
                continue
            total = int(r["hg"]) + int(r["ag"])
            for f in FLOORS:
                # The floor is a LeagueConfig field, not a predict_fixture
                # argument, so it is varied on a copy of the config.
                c = deepcopy(cfg)
                c.min_win_prob = f
                try:
                    mk = predict_fixture(
                        req, c, module_flags=flags).translated_play.market
                except Exception:
                    skipped += 1
                    continue
                if not mk:
                    continue
                res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
                if res is None:
                    continue
                p = market_select.p_win(mk, req.mu_total)
                rows[f].append((mk, p, res is True or res == "half_win",
                                rates.get(mk, 0.0), total))
    got = sum(len(v) for v in rows.values())
    if skipped > got:
        print(f"  WARNING k={k}: {skipped} pricing failures against {got} "
              f"successes — the sweep is measuring almost nothing",
              file=sys.stderr)
    return rows


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 120
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else (LEAGUES or sorted(store.available_leagues())))
    k0 = features.MU_SHRINK

    print(f"{'k':>6}{'floor':>7}{'n':>7}{'says':>8}{'hit':>8}{'gap':>7}"
          f"{'TAIL':>7}{'base':>8}{'EDGE':>7}{'top':>6}  mix")
    for k in KS:
        rows = run(k, n, codes)
        for f in FLOORS:
            b = rows[f]
            if len(b) < 100:
                continue
            hit = sum(1 for r in b if r[2]) / len(b)
            says = sum(r[1] for r in b) / len(b)
            base = sum(r[3] for r in b) / len(b)
            tail = [r for r in b if (r[1] - r[3]) > 0.035]
            tg = (sum(1 for r in tail if r[2]) / len(tail)
                  - sum(r[1] for r in tail) / len(tail)) if len(tail) > 40 else None
            mix = Counter(r[0] for r in b)
            top = mix.most_common(1)[0]
            mark = "  <- current" if (abs(k - k0) < 1e-9
                                      and abs(f - market_select.MIN_WIN_PROB) < 1e-9) else ""
            print(f"{k:6.2f}{f:7.2f}{len(b):7}{says*100:7.1f}%{hit*100:7.1f}%"
                  f"{(hit-says)*100:+7.1f}"
                  f"{(tg*100 if tg is not None else 0):+7.1f}"
                  f"{base*100:7.1f}%{(hit-base)*100:+7.1f}"
                  f"{top[1]/len(b)*100:5.0f}%  {top[0]}{mark}", flush=True)
    features.MU_SHRINK = k0


if __name__ == "__main__":
    main()
