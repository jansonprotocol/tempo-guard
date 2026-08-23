"""
The market is chosen for one mu and priced at another. Does that cost the tail?

`pipeline` adjusts mu before selection — possession (inert, measured at 0.000)
and season stage (+0.150 goals on 54% of fixtures) — then `market_select.choose`
picks against the ADJUSTED value. But the probability published beside the tip,
and every calibration figure measured in this project, comes from
`market_select.p_win(market, req.mu_total)` — the UNADJUSTED one.

So a fixture in a season's closing stretch is selected as if it will produce
0.15 more goals than it is then priced for. On an Under that inflates the
published probability, because a lower mu makes an Under look safer; the tip is
issued for a busier match and quoted for a quieter one.

That is the right shape for the defect still outstanding: the high-edge band
runs 2.5 points overconfident while the pooled gap sits near zero, and finishing
the mu shrink moved it 0.2 points. A mismatch of this kind would not respond to
the shrink at all, because both arms shrink together and the GAP between them
survives.

Scored both ways on identical fixtures and identical outcomes:

    PRICED AS PUBLISHED   p from req.mu_total — what the log says today
    PRICED AS SELECTED    p from the mu the selector actually used

If the tail gap closes under the second, the defect is the mismatch, and the fix
is to publish the probability for the mu that chose the market.

Usage:  python scripts/mu_mismatch.py [--n 120] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import config, season_stage, store
from app.data import possession as poss
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

BANDS = [(-9, 1.0), (1.0, 2.0), (2.0, 3.5), (3.5, 99)]
NAMES = ["under +1%", "+1 to +2%", "+2 to +3.5%", "over +3.5%"]
MARKETS = ("O1.0", "O1.5", "O1.75", "O2.25", "O2.5", "O2.75",
           "U2.5", "U2.75", "U3.0", "U3.25", "U3.5", "U3.75", "U4.25")


def base_rates(code: str) -> dict[str, float]:
    df = store.load_results(code)
    if df is None or df.empty:
        return {}
    out = {}
    for m in MARKETS:
        w = [hit_weight(evaluate_market(m, int(h), int(a)))
             for h, a in zip(df["hg"], df["ag"])]
        w = [x for x in w if x >= 0]
        out[m] = sum(1 for x in w if x >= 1.0) / len(w) if w else 0.0
    return out


def collect(lg: str, n: int) -> list:
    df = store.load_results(lg)
    if df is None or len(df) < 200:
        return []
    cfg = config.get(lg)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    rates = base_rates(lg)
    out = []
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            req = build_request(lg, str(r["home"]), str(r["away"]), d)
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
        # Rebuild the mu the selector saw. Possession measures 0.000 on every
        # fixture checked but is included so the reconstruction cannot silently
        # drift if it is ever switched on.
        mu_sel = req.mu_total
        try:
            sh = poss.shift(lg, str(r["home"]), str(r["away"]), d)
            if sh:
                mu_sel += sh
        except Exception:
            pass
        try:
            li = season_stage.shift(lg, str(r["home"]), str(r["away"]), d)
            if li:
                mu_sel += li
        except Exception:
            pass
        out.append((mk,
                    market_select.p_win(mk, req.mu_total),
                    market_select.p_win(mk, mu_sel),
                    res is True or res == "half_win",
                    rates.get(mk, 0.0)))
    return out


def report(label: str, rows: list, idx: int) -> None:
    print(f"\n{label}")
    print(f"{'stated edge':14}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}")
    for (lo, hi), name in zip(BANDS, NAMES):
        b = [r for r in rows if lo <= (r[idx] - r[4]) * 100 < hi]
        if len(b) < 40:
            continue
        hit = sum(1 for r in b if r[3]) / len(b)
        says = sum(r[idx] for r in b) / len(b)
        print(f"{name:14}{len(b):7}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+8.1f}")
    hit = sum(1 for r in rows if r[3]) / len(rows)
    says = sum(r[idx] for r in rows) / len(rows)
    print(f"{'ALL':14}{len(rows):7}{says*100:7.1f}%{hit*100:7.1f}%"
          f"{(hit-says)*100:+8.1f}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 120
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    rows = []
    for lg in codes:
        try:
            rows += collect(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    if not rows:
        return
    moved = sum(1 for r in rows if abs(r[1] - r[2]) > 1e-9)
    print(f"{len(rows)} tips, {moved} ({moved/len(rows)*100:.0f}%) selected on "
          f"an adjusted mu")
    report("PRICED AS PUBLISHED — p from req.mu_total", rows, 1)
    report("PRICED AS SELECTED — p from the mu that chose the market", rows, 2)


if __name__ == "__main__":
    main()
