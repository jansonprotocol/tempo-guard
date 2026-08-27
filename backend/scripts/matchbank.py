"""
The match bank: every fixture Athena has actually run, packed for the app.

The web form lets a visitor type in a matchup and get Athena's card. A
static site cannot run the engine, so the form answers from this bank:
the recent-200 replay of every league and cup — the same instrument that
produces the league badges — precomputed with the exact live path
(current floors, cup debit included) and stored as JSON. A matchup
outside the bank gets an honest error, never an invented card.

Writes config/matchbank_retro.json; scripts/webapp.py merges it with the
live board into web/matchbank.json at render time. Re-run this after big
engine changes so the bank reflects the current build.

Usage:  python scripts/matchbank.py
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import club_elo, config, store
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.two_tips import _buy

OUT = Path(__file__).resolve().parents[2] / "config" / "matchbank_retro.json"
SKIP = {"COPA-L", "EC", "WC"}
# Up to two seasons per competition, same window the retrosim table runs.
# The bank doubles as the hero number's sample: at 200 rows per comp the
# most recent 300 playable graded lanes spanned barely two weeks; at the
# full test-run window the search module answers far more matchups and
# the hero window can widen without reaching into stale engine eras.
N = 800
DAYS = 730


def league_entries(code: str) -> tuple[list[str], list[dict]]:
    df = store.load_results(code)
    if df is None or len(df) < 100:
        return [], []
    cfg = config.get(code)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    rows = df.dropna(subset=["hg", "ag"]).sort_values("date")
    import pandas as pd
    cut = rows["date"].max() - pd.Timedelta(days=DAYS)
    rows = rows[rows["date"] >= cut].tail(N)
    teams = sorted(set(map(str, rows["home"])) | set(map(str, rows["away"])))
    out = []
    for _, r in rows.iterrows():
        d = r["date"].date()
        try:
            req = build_request(code, str(r["home"]), str(r["away"]), d)
            if req is None or not req.mu_total:
                continue
            mk = predict_fixture(req, cfg,
                                 module_flags=flags).translated_play.market
        except Exception:
            continue
        if not mk:
            continue
        res = evaluate_market(mk, int(r["hg"]), int(r["ag"]))
        if res is None:
            continue
        p = market_select.stated(code, mk,
                                 market_select.p_win(mk, req.mu_total))
        edge = p - market_select.p_win(mk, req.league_mu)
        w = hit_weight(res)
        mark = "✅" if w >= 1.0 else "✅½" if w > 0 else \
            "◦" if res == "push" else "❌"
        out.append(dict(
            d=str(d), h=str(r["home"]), a=str(r["away"]),
            tip=f"{mk} {p*100:.1f}% {edge*100:+.1f}% · "
                f"{_buy(mk, req.mu_total, p, edge, code)}".replace(
                    "buy>=", "buy≥"),
            score=f"{int(r['hg'])}-{int(r['ag'])}", mark=mark))
    return teams, out


def main() -> None:
    bank = {}
    for code in sorted(store.available_leagues()):
        if code in SKIP:
            continue
        try:
            teams, entries = league_entries(code)
        except Exception as exc:
            print(f"{code}: FAILED {exc}", file=sys.stderr)
            continue
        if not entries:
            continue
        bank[code] = dict(name=config.get(code).name or code,
                          teams=teams, matches=entries)
        print(f"{code}: {len(entries)} matches, {len(teams)} teams")
    OUT.write_text(json.dumps(bank, ensure_ascii=False))
    print(f"bank written: {len(bank)} competitions, "
          f"{sum(len(b['matches']) for b in bank.values())} matches, "
          f"{OUT.stat().st_size // 1024}KB")


if __name__ == "__main__":
    main()
