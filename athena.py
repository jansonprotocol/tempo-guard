#!/usr/bin/env python3
"""
ATHENA: Tempo Guard — command line interface.

A local, offline over/under tips engine. Data lives in this repository as
parquet snapshots; tuning lives in config/leagues.json; nothing needs a server
or a database.

    Data — two providers, deliberately
      athena data sync                 git-pull the openfootball sources
      athena data load [--history]     parse them into data/*.parquet
      athena data live                 top up the current season online
      athena data status               what is stored right now

      openfootball (git) carries deep history and the fixture schedule, and
      works entirely offline once committed. football-data.co.uk is fetched on
      request and is current within hours, with measured shots and — from
      2026-27 — expected goals. `data live` merges the second over the first.
      No bookmaker data is ingested from either.

    Calibration
      athena calibrate ENG-PL          replay a league, search better dials
      athena calibrate ENG-PL --apply  ...and write the winner to config/
      athena calibrate --all           every league with stored results

    Single runs
      athena retrosim ENG-PL "Arsenal" "Chelsea" 2026-01-10
      athena futurematch ENG-PL "Arsenal" "Chelsea" 2026-08-22
      athena tips --league ENG-PL --days 7

Run any subcommand with -h for its options.
"""
from __future__ import annotations

import argparse
import sys
from datetime import date, datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "backend"))

from app.data import config, features, loader, sources, store  # noqa: E402
from app.predict import predict_match  # noqa: E402
from app.util.asian_lines import evaluate_market, hit_weight, market_description  # noqa: E402


# ── Presentation helpers ──────────────────────────────────────────────────────

def _rule(char: str = "─", width: int = 74) -> str:
    return char * width


def _parse_date(s: str) -> date:
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except ValueError:
        raise SystemExit(f"Bad date {s!r} — expected YYYY-MM-DD")


def _print_prediction(pred, header: str, actual: tuple[int, int] | None = None) -> None:
    print()
    print(_rule("═"))
    print(f"  {header}")
    print(_rule("═"))
    from app.engine.rationale import confidence_band

    tp = pred.translated_play
    print(f"  TIP           {tp.market}   ({market_description(tp.market)})")
    print(f"  Lean          {pred.corridor.lean}   "
          f"corridor {pred.corridor.low}–{pred.corridor.high} goals")
    # Two distinct things, kept apart deliberately:
    #   signal       how strong the engine's read is (confidence_score)
    #   line safety  how much cushion the chosen line has — translate_play marks
    #                a stretch line like O2.5 "LOW" and a cushioned U3.5 "HIGH"
    print(f"  Signal        {pred.confidence_score:.2f} ({confidence_band(pred.confidence_score)})")
    print(f"  Line safety   {tp.confidence.lower()}")
    if pred.weather_tag:
        print(f"  Weather       {pred.weather_tag}")

    print()
    print("  Why:")
    for line in pred.rationale:
        print(f"    • {line}")

    if actual is not None:
        hg, ag = actual
        graded = evaluate_market(tp.market, hg, ag)
        won = hit_weight(graded) >= 1.0
        print()
        print(_rule())
        print(f"  ACTUAL        {hg}-{ag}  ({hg + ag} goals)")
        print(f"  RESULT        {'✓ HIT' if won else '✗ MISS'}")
    print(_rule("═"))
    print()


def _print_modules(pred) -> None:
    if pred.applied_modules:
        print("  Modules fired: " + ", ".join(pred.applied_modules))
    print()
    print("  Signal trace:")
    for line in pred.explanations:
        print(f"    - {line}")
    print()


# ── data ──────────────────────────────────────────────────────────────────────

def cmd_data(args) -> int:
    if args.action == "sync":
        print("Syncing openfootball sources…")
        results = loader.sync_repos()
        failed = [r for r, s in results.items() if s.startswith("error")]
        if failed:
            print(f"\n  {len(failed)} repo(s) failed. If this machine has no "
                  f"internet access, sync elsewhere and commit data/ instead.")
            return 1
        print(f"\n  {len(results)} repos ready in {loader.CACHE_DIR}")
        return 0

    if args.action == "live":
        print("Refreshing current season from football-data.co.uk…")
        got = loader.refresh_live(
            [args.league] if args.league else None, season=args.season
        )
        print(f"\n  {len(got)} leagues refreshed, "
              f"{sum(got.values())} results now stored for the current season")
        return 0

    if args.action == "load":
        codes = [args.league] if args.league else None
        seasons = [args.season] if args.season else None
        print("Parsing sources into data/…")
        loaded = loader.load_all(codes, seasons, history=args.history)
        total = sum(len(v) for v in loaded.values())
        print(f"\n  Loaded {total} league-seasons into {store.DATA_DIR}")
        return 0

    # status
    st = store.stats()
    if not st:
        print("No data stored yet. Run: athena data sync && athena data load")
        return 1
    print()
    print(f"  {'LEAGUE':8s} {'SEASONS':22s} {'RESULTS':>8s} {'FIXTURES':>9s}  RANGE")
    print(_rule())
    for code, s in st.items():
        print(f"  {code:8s} {','.join(s['seasons']):22s} "
              f"{s['results']:8d} {s['fixtures']:9d}  {s['date_range']}")
    print(_rule())
    print(f"  {len(st)} leagues, "
          f"{sum(s['results'] for s in st.values())} results, "
          f"{sum(s['fixtures'] for s in st.values())} fixtures")
    print()
    return 0


# ── calibrate ─────────────────────────────────────────────────────────────────

def _calibrate_one(code: str, args) -> bool:
    from app.calibrate import calibrate

    before = _parse_date(args.before) if args.before else None
    try:
        rep = calibrate(
            code, before=before, season=args.season,
            apply=args.apply, progress=(lambda m: print(f"  {m}")) if args.verbose else None,
        )
    except ValueError as e:
        print(f"  {code}: {e}")
        return False

    print()
    print(f"  {code}  ({sources.get(code).name if code in sources.LEAGUES else code})")
    print(f"    current       {rep.baseline_hit_rate:6.1%}  over {rep.baseline_sample} matches")
    print(f"    tuned on      {rep.train_best:6.1%}  (training split, was {rep.train_baseline:.1%})")
    # The holdout is the number that matters: matches the dial search never saw.
    print(f"    HOLDOUT       {rep.holdout_tuned:6.1%}  vs {rep.holdout_baseline:.1%} baseline "
          f"→ {rep.improvement:+.1%}  (n={rep.holdout_sample})")
    print(f"    dials         bias_shift={rep.bias_shift:+.2f}  tempo_factor={rep.tempo_factor:.2f}")
    if args.detail:
        print("    by market:")
        for m, (h, s) in rep.verified.by_market().items():
            print(f"      {m:7s} {h:4d}/{s:<4d} {h/s:6.1%}")
    if rep.applied:
        print(f"    APPLIED       written to {config.LEAGUES_FILE.name}")
    elif args.apply:
        print("    not applied   gain did not survive on unseen matches")
    return True


def cmd_calibrate(args) -> int:
    if args.all:
        codes = [c for c in sources.codes() if store.load_results(c).shape[0] > 0]
    elif args.league:
        codes = [args.league]
    else:
        print("Specify a league code or --all")
        return 1

    print(f"Calibrating {len(codes)} league(s)…")
    ok = 0
    for code in codes:
        if _calibrate_one(code, args):
            ok += 1
    print()
    print(f"Done — {ok}/{len(codes)} calibrated.")
    return 0


# ── ablate ────────────────────────────────────────────────────────────────────

def cmd_ablate(args) -> int:
    from app.ablate import ablate_many

    codes = ([args.league] if args.league
             else [c for c in sources.codes() if len(store.load_results(c)) > 100])
    print(f"Ablating {len(codes)} league(s) — disabling one module at a time…")
    per, totals = ablate_many(codes, progress=(lambda m: print(f"  {m}")) if args.verbose else None)

    if not per:
        print("Nothing to ablate — no stored results.")
        return 1

    print()
    print("  MODULE CONTRIBUTION  (positive = disabling it makes results worse)")
    print(_rule())
    print(f"  {'module':16s} {'contribution':>13s} {'preds changed':>14s} {'verdict':>9s}")
    print(_rule())
    for m, t in sorted(totals.items(), key=lambda kv: kv[1]["mean_contribution"]):
        c = t["mean_contribution"]
        verdict = ("HURTS" if c < -0.005 else
                   "helps" if c > 0.005 else
                   "inert" if t["changed"] == 0 else "neutral")
        print(f"  {m:16s} {c:+12.2%} {t['changed']:14d} {verdict:>9s}")
    print(_rule())

    if args.detail:
        for code, (rate, sample, effects) in per.items():
            print(f"\n  {code}  baseline {rate:.1%} (n={sample})")
            for e in effects:
                print(f"    {e.module:16s} without={e.without:6.1%} "
                      f"{e.contribution:+7.2%} changed={e.changed:4d}  {e.verdict}")
    print()
    print("  Defaults already reflect these measurements (engine/types.py).")
    print("  Override per league via 'module_overrides' in config/leagues.json.")
    print()
    return 0


# ── retrosim / futurematch ────────────────────────────────────────────────────

def cmd_retrosim(args) -> int:
    mdate = _parse_date(args.date)
    if mdate >= date.today():
        print(f"{mdate} is not in the past — use `futurematch` for upcoming fixtures.")
        return 1

    valid, reason = features.validate_match_existed(
        args.league, args.home, args.away, mdate
    )
    if not valid:
        print(f"Cannot retro-simulate: {reason}")
        return 1

    pred = predict_match(args.league, args.home, args.away, mdate)
    if pred is None:
        print(f"Not enough history before {mdate} to predict this fixture.")
        return 1

    actual = features.actual_result(args.league, args.home, args.away, mdate)
    _print_prediction(pred, f"RETROSIM — {pred.fixture}  ({args.league}, {mdate})", actual)
    if args.verbose:
        _print_modules(pred)
    return 0


def cmd_futurematch(args) -> int:
    mdate = _parse_date(args.date)
    pred = predict_match(args.league, args.home, args.away, mdate)
    if pred is None:
        print(f"Not enough history to predict {args.home} vs {args.away}. "
              f"Check the team names and that {args.league} data is loaded.")
        return 1

    _print_prediction(pred, f"FUTUREMATCH — {pred.fixture}  ({args.league}, {mdate})")
    if args.verbose:
        _print_modules(pred)
    return 0


# ── tips ──────────────────────────────────────────────────────────────────────

def cmd_tips(args) -> int:
    today = date.today()
    until = today + timedelta(days=args.days)
    codes = [args.league] if args.league else sources.codes()

    rows = []
    for code in codes:
        fixtures = store.load_fixtures(code)
        if fixtures.empty:
            continue
        upcoming = fixtures[
            (fixtures["date"].dt.date >= today) & (fixtures["date"].dt.date <= until)
        ]
        for _, fx in upcoming.iterrows():
            mdate = fx["date"].date()
            pred = predict_match(code, str(fx["home"]), str(fx["away"]), mdate)
            if pred is None:
                continue
            if pred.confidence_score < args.min_confidence:
                continue
            rows.append((mdate, code, pred))

    if not rows:
        print(f"No tips for the next {args.days} days "
              f"(nothing scheduled, or too little history yet).")
        return 0

    rows.sort(key=lambda r: (r[0], -r[2].confidence_score))
    print()
    print(f"  TIPS — next {args.days} days ({len(rows)} fixtures)")
    print(_rule())
    print(f"  {'DATE':11s} {'LEAGUE':8s} {'FIXTURE':44s} {'TIP':7s} {'CONF':>5s}")
    print(_rule())
    for mdate, code, pred in rows:
        print(f"  {mdate.isoformat():11s} {code:8s} {pred.fixture[:44]:44s} "
              f"{pred.translated_play.market:7s} {pred.confidence_score:5.2f}")
    print(_rule())

    if args.explain:
        for mdate, code, pred in rows:
            _print_prediction(pred, f"{pred.fixture}  ({code}, {mdate})")
    else:
        print("  Use --explain for the reasoning behind each tip.")
        print()
    return 0


# ── entry point ───────────────────────────────────────────────────────────────

def main(argv=None) -> int:
    p = argparse.ArgumentParser(
        prog="athena",
        description="ATHENA: Tempo Guard — local over/under tips engine.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__.split("Run any")[0].split("    Data")[1],
    )
    sub = p.add_subparsers(dest="command", required=True)

    # data
    d = sub.add_parser("data", help="fetch, load and inspect match data")
    d.add_argument("action", choices=["sync", "load", "live", "status"],
                   help="sync: git-pull sources | load: parse to parquet | "
                        "live: top up current season online | status: what's stored")
    d.add_argument("--league", help="limit to one league code")
    d.add_argument("--season", help="limit to one season, e.g. 2025-26")
    d.add_argument("--history", action="store_true",
                   help="load every season upstream publishes, not just recent")
    d.set_defaults(func=cmd_data)

    # calibrate
    c = sub.add_parser("calibrate", help="replay a league and tune its dials")
    c.add_argument("league", nargs="?", help="league code, e.g. ENG-PL")
    c.add_argument("--all", action="store_true", help="calibrate every stored league")
    c.add_argument("--apply", action="store_true", help="write improvements to config")
    c.add_argument("--before", help="only replay matches before YYYY-MM-DD")
    c.add_argument("--season", help="limit to one season")
    c.add_argument("--detail", action="store_true", help="per-market breakdown")
    c.add_argument("-v", "--verbose", action="store_true")
    c.set_defaults(func=cmd_calibrate)

    # ablate
    a = sub.add_parser("ablate", help="measure what each engine module is worth")
    a.add_argument("league", nargs="?", help="league code; default: all with enough data")
    a.add_argument("--detail", action="store_true", help="per-league breakdown")
    a.add_argument("-v", "--verbose", action="store_true")
    a.set_defaults(func=cmd_ablate)

    # retrosim
    r = sub.add_parser("retrosim", help="re-simulate a past match and grade it")
    r.add_argument("league")
    r.add_argument("home")
    r.add_argument("away")
    r.add_argument("date", help="YYYY-MM-DD")
    r.add_argument("-v", "--verbose", action="store_true", help="show signal trace")
    r.set_defaults(func=cmd_retrosim)

    # futurematch
    f = sub.add_parser("futurematch", help="predict an upcoming match")
    f.add_argument("league")
    f.add_argument("home")
    f.add_argument("away")
    f.add_argument("date", help="YYYY-MM-DD")
    f.add_argument("-v", "--verbose", action="store_true", help="show signal trace")
    f.set_defaults(func=cmd_futurematch)

    # tips
    t = sub.add_parser("tips", help="tips for upcoming fixtures")
    t.add_argument("--league", help="limit to one league")
    t.add_argument("--days", type=int, default=7, help="lookahead window (default 7)")
    t.add_argument("--min-confidence", type=float, default=0.0,
                   dest="min_confidence", help="hide tips below this confidence")
    t.add_argument("--explain", action="store_true", help="show reasoning per tip")
    t.set_defaults(func=cmd_tips)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
