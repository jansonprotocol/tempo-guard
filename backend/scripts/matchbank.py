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

Usage:  python scripts/matchbank.py              rebuild every card (52 min)
        python scripts/matchbank.py --since      reprice only what is new

WHY --since EXISTS (the bettor, 15 Sep: "should the updater also check
in on these hitrates to keep it reflecting the correct number?"). It
should, and it was not. The two-day refresh ingested the board's results
into the STORE and then re-measured everything off the BANK — but
nothing in the loop rebuilt the bank, so the bank's newest card stayed
at 30 August while the board ran on to mid-September. Every bank-derived
number was quietly frozen: the league badges, the baselines, the
live-safety bands the decline rule reads, all six drift checks, and the
per-market landing line on the card. livebands.py re-measured
conscientiously every two days and computed the same answer each time.

A full rebuild cannot go in the loop — 32,217 cards at 96ms of engine
replay each is 52 minutes against a 30-minute job timeout. So --since
keeps the cards it already has and prices only what the window gained:
about 1,300 fixtures a fortnight, a little over two minutes.

Two things are deliberately NOT cached. Cards dated inside REPRICE_DAYS
of the bank's newest are always re-priced, because ingest_board backfills
results into the store and a card's as-of history may have gained rows
since it was priced. And guard() always runs over the WHOLE bank: it is
3.3 seconds, its confluence walk is as-of across every card at once, and
re-running it is what lets a label rule change reach cards already
banked. So --since caches the expensive, as-of-stable part and recomputes
everything cheap.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import club_elo, config, store
from app.engine import result_market, team_total
from app.util.asian_lines import evaluate_market, hit_weight
from scripts.futurematch import cell1, cell2, cell3
from scripts.two_tips import tips

OUT = Path(__file__).resolve().parents[2] / "config" / "matchbank_retro.json"
SKIP = {"COPA-L", "EC", "WC"}
# Up to two seasons per competition, same window the retrosim table runs.
# The bank doubles as the hero number's sample: at 200 rows per comp the
# most recent 300 playable graded lanes spanned barely two weeks; at the
# full test-run window the search module answers far more matchups and
# the hero window can widen without reaching into stale engine eras.
N = 800
DAYS = 730

REPRICE_DAYS = 30    # cards this near the bank's edge are always re-priced
# What guard() writes onto a card. A reused card sheds them before the
# guard runs again, so a rule change cannot leave a stale label behind.
DERIVED = ("pk", "g", "cs", "st", "bp", "need", "v")


def _cache(reprice_from: str | None) -> dict:
    """(code, date, home, away) -> a card already in the bank, reusable.

    Empty when the bank is missing or a full rebuild was asked for, so
    the caller's loop is the same either way.
    """
    if reprice_from is None or not OUT.exists():
        return {}
    bank = json.loads(OUT.read_text())
    out = {}
    for code, comp in bank.items():
        for m in comp.get("matches", []):
            if m.get("d", "") >= reprice_from:
                continue                       # too near the edge to trust
            out[(code, m["d"], m["h"], m["a"])] = {
                k: v for k, v in m.items() if k not in DERIVED}
    return out


def league_entries(code: str, cache: dict | None = None) -> tuple[list[str], list[dict]]:
    df = store.load_results(code)
    if df is None or len(df) < 100:
        return [], []
    rows = df.dropna(subset=["hg", "ag"]).sort_values("date")
    import pandas as pd
    cut = rows["date"].max() - pd.Timedelta(days=DAYS)
    rows = rows[rows["date"] >= cut].tail(N)
    teams = sorted(set(map(str, rows["home"])) | set(map(str, rows["away"])))
    out = []
    for _, r in rows.iterrows():
        d = r["date"].date()
        hg, ag = int(r["hg"]), int(r["ag"])
        if cache:
            got = cache.get((code, str(d), str(r["home"]), str(r["away"])))
            if got is not None:
                out.append(dict(got))
                continue
        # One call through the live tip path, so the bank stores exactly
        # the three lanes a card would have shown — and each is graded,
        # which is what makes the bank cross-examinable (the bettor's
        # ask, 31 Aug: tip 2 used to be present only on board rows and
        # tip 3 not at all).
        try:
            rr = tips(code, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if rr is None:
            continue
        mk = rr["t1"][0]
        res = evaluate_market(mk, hg, ag)
        if res is None:
            continue
        w = hit_weight(res)
        mark = "✅" if w >= 1.0 else "✅½" if w > 0 else \
            "◦" if res == "push" else "❌"

        entry = dict(
            d=str(d), h=str(r["home"]), a=str(r["away"]),
            tip=cell1(rr, code),
            score=f"{hg}-{ag}", mark=mark)

        if rr["t2"]:
            m2 = rr["t2"][0]
            try:
                r2 = (team_total.won(m2, hg, ag) if m2.startswith("T")
                      else evaluate_market(m2, hg, ag))
            except (ValueError, TypeError):
                r2 = None
            if r2 is not None:
                w2 = hit_weight(r2) if not isinstance(r2, bool) else \
                    (1.0 if r2 else 0.0)
                entry["t2"] = cell2(rr, code, str(r["home"]), str(r["away"]))
                entry["m2"] = "✅" if w2 >= 1.0 else "✅½" if w2 > 0 else \
                    "◦" if r2 == "push" else "❌"

        if rr["t3"]:
            won = result_market.won(rr["t3"][0], hg, ag)
            entry["t3"] = cell3(rr)
            entry["m3"] = "◦" if won is None else ("✅" if won else "❌")

        out.append(entry)
    return teams, out


LANE_RE = __import__("re").compile(
    r"^(?:[✅❌◦]\s*)?(?:\*\*)?(?:[A-Za-z][^*]*? )?([OU]\d+(?:\.\d+)?|1X|X2|12|DNB[12])"
    r"(?:\*\*)?\s+(\d+(?:\.\d+)?)%\s*(?:\*\*)?([+\-−]\d+(?:\.\d+)?)%")


def guard(bank: dict) -> None:
    """Give every past card what a live card carries: the guard's label,
    its confluence score, the STRONG flag, the starred lane, and — where
    a closing price exists — the verdict.

    The score is the as-of walker over the whole bank, every card scored
    only from cards dated before it, exactly as the live guard's table
    was calibrated. The label is the live rule (guard_slices.tier_of and
    label). The verdict is the live rule at football-data's closing price
    (retro_odds): PLAY when the best of the panel clears the label's
    break-even by the registered margin and the tier is not red. A card
    with no price is marked unpriced rather than guessed.
    """
    import datetime as dt
    from scripts import confluence as CF, guard_slices as GS, retro_odds
    from scripts.odds_api import bought
    from scripts.webapp import DNB_GATE, STRONG_SCORE, band_bar

    rows = []
    for code, comp in bank.items():
        for m in comp["matches"]:
            t1 = LANE_RE.match(m["tip"])
            if not t1:
                continue
            m["_mk1"], p1, e1 = t1.group(1), float(t1.group(2)), float(t1.group(3).replace("−", "-"))
            pk, mk, says = 1, t1.group(1), p1
            if m.get("t3"):
                t3 = LANE_RE.match(m["t3"])
                if t3 and t3.group(1).startswith("DNB") and float(t3.group(2)) - p1 > DNB_GATE:
                    pk, mk, says = 3, t3.group(1), float(t3.group(2))
            hit = (m["mark"] if pk == 1 else m.get("m3", "")) != "❌"
            rows.append(dict(d=dt.date.fromisoformat(m["d"]), code=code, h=m["h"], a=m["a"],
                             hit_pick=hit, says_pick=says / 100, mk=mk,
                             _m=m, _p1=p1, _e1=e1, _pk=pk))
    scored = {id(r["_m"]): r for r in CF.walk_best(rows, 0)}
    for r in rows:
        m = r["_m"]
        s = scored.get(id(m))
        cs = s["cscore"] if s else None
        dnb = r["_pk"] == 3
        side = r["mk"][:1] if r["mk"][:1] in ("O", "U") else ""
        lab = GS.label(r["code"], GS.tier_of(r["_p1"] if not dnb else r["says_pick"] * 100,
                                             r["_e1"], side, dnb), cs, dnb,
                       r["says_pick"] * 100, r["mk"])
        m["pk"], m["g"] = r["_pk"], lab
        if cs is not None:
            m["cs"] = round(cs, 1)
        m["st"] = int(cs is not None and cs >= STRONG_SCORE and CF.region(r["code"]) == "Europe")
        lane = r["mk"] if dnb else bought(r["mk"])
        row = retro_odds.find(r["code"], m["d"], m["h"], m["a"])
        bp = retro_odds.price(row, lane) if row else None
        if bp:
            need = band_bar(r["says_pick"] * 100)     # the claim band's bar (12 Sep)
            m["bp"], m["need"] = round(bp, 2), round(need, 2)
            m["v"] = "no play" if lab.endswith("red") or bp < need else \
                ("strong" if m["st"] else "normal")
        m.pop("_mk1", None)


def _reprice_from() -> str | None:
    """The date at and after which cards must be priced afresh: a month
    back from the bank's newest card. None when there is no bank to
    build on, which makes --since behave as a full rebuild."""
    import datetime as dt
    if not OUT.exists():
        return None
    bank = json.loads(OUT.read_text())
    days = [m["d"] for c in bank.values() for m in c.get("matches", []) if m.get("d")]
    if not days:
        return None
    return str(dt.date.fromisoformat(max(days)) - dt.timedelta(days=REPRICE_DAYS))


def main() -> None:
    since = "--since" in sys.argv
    reprice_from = _reprice_from() if since else None
    cache = _cache(reprice_from)
    if since:
        print(f"--since: {len(cache):,} cards reusable, everything from "
              f"{reprice_from} priced afresh"
              if cache else "--since: no bank to build on, pricing every card")
    bank = {}
    kept = made = 0
    for code in sorted(store.available_leagues()):
        if code in SKIP:
            continue
        try:
            teams, entries = league_entries(code, cache)
        except Exception as exc:
            print(f"{code}: FAILED {exc}", file=sys.stderr)
            continue
        if not entries:
            continue
        k = sum(1 for e in entries
                if (code, e["d"], e["h"], e["a"]) in cache)
        kept += k
        made += len(entries) - k
        bank[code] = dict(name=config.get(code).name or code,
                          teams=teams, matches=entries)
        print(f"{code}: {len(entries)} matches, {len(teams)} teams"
              + (f" ({len(entries) - k} priced)" if since else ""))
    if since:
        print(f"reused {kept:,} cards, priced {made:,}")
    guard(bank)
    priced = sum(1 for b in bank.values() for m in b["matches"] if "v" in m)
    print(f"guard: labels on every card, {priced} with a closing-price verdict")
    OUT.write_text(json.dumps(bank, ensure_ascii=False))
    print(f"bank written: {len(bank)} competitions, "
          f"{sum(len(b['matches']) for b in bank.values())} matches, "
          f"{OUT.stat().st_size // 1024}KB")


if __name__ == "__main__":
    main()
