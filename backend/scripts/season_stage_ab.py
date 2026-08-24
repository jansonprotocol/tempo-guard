"""
Does the season-stage lift change any bets, and are the changes wins?

The effect is real and has survived measurement twice: +0.363 goals in final
rounds retrospectively, +0.150 [+0.112, +0.188] using only what is knowable on
match day. Being real is not the same as being useful. A shift only earns its
place if it moves fixtures across a market boundary and the fixtures it moves
land more often than they did before.

The prior is a null. Possession ran to +/-0.35 goals and netted zero, and this
is smaller and fires on roughly 6% of fixtures — those in a campaign's closing
stretch. So the honest expectation is a handful of changed markets and a ledger
near zero, and that outcome should be reported as plainly as a win.

Judged on the net flip ledger rather than the headline strike, because a strike
rate computed over the whole book moves on rounding when 40 markets change out
of 14,000:

    rescued   a fixture the current engine loses and the lift wins
    broken    a fixture the current engine wins and the lift loses

Reported both across the whole book and restricted to fixtures actually in the
closing stretch, since that is where the toggle can do anything at all — a net
of zero over 14,000 fixtures and a net of zero over the 900 it touches mean
quite different things.
"""
from __future__ import annotations

import sys
from collections import Counter
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config, season_stage
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300
EUROPEAN = {"UCL", "UEL", "UECL", "UECL-Q"}


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(mk, tt) -> float:
    if not len(mk) or not len(tt):
        return 0.0
    n = len(tt)
    return sum(c * sum(1 for t in tt if won(m, t)) / n
               for m, c in Counter(mk).items()) / len(mk)


def report(label, rows, key):
    if not rows:
        print(f"  {label:22s} none")
        return
    mk = [r[key] for r in rows]
    tt = [r["total"] for r in rows]
    h = sum(1 for m, t in zip(mk, tt) if won(m, t))
    s = h / len(rows)
    mix = " ".join(f"{m}:{c * 100 // len(rows)}%"
                   for m, c in Counter(mk).most_common(3))
    print(f"  {label:22s} {h:5d}/{len(rows):5d} = {s:6.1%}  "
          f"edge {s - base_of(mk, tt):+6.2%}   {mix}")


def main() -> None:
    rows = []
    for code in sorted(config.load_all().keys()):
        if code in EUROPEAN:
            continue
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception:
            continue
        if len(pairs) < 100:
            continue
        off = config.get(code)
        on = deepcopy(off)
        on.use_season_stage = True
        flags = ModuleFlags(**(off.module_overrides or {}))

        for req, (hg, ag) in pairs:
            lift = season_stage.shift(code, req.home_team, req.away_team,
                                      req.match_date)
            rows.append({
                "code": code, "total": int(hg) + int(ag),
                "lift": lift or 0.0,
                "off": predict_fixture(req, off, module_flags=flags)
                       .translated_play.market,
                "on": predict_fixture(req, on, module_flags=flags)
                      .translated_play.market,
                "home": req.home_team, "away": req.away_team,
                "date": req.match_date,
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    if not rows:
        print("no data")
        return

    touched = [r for r in rows if r["lift"] > 0]
    print(f"\n{len(rows)} fixtures, lift applies to {len(touched)} "
          f"({len(touched) / len(rows):.1%})\n")

    print("  WHOLE BOOK")
    report("stage OFF", rows, "off")
    report("stage ON", rows, "on")

    print("\n  CLOSING STRETCH ONLY (where the toggle can act)")
    report("stage OFF", touched, "off")
    report("stage ON", touched, "on")

    changed = [r for r in rows if r["off"] != r["on"]]
    resc = [r for r in changed if won(r["on"], r["total"])
            and not won(r["off"], r["total"])]
    brok = [r for r in changed if won(r["off"], r["total"])
            and not won(r["on"], r["total"])]
    print(f"\n  market changed on {len(changed)} fixtures")
    print(f"  rescued {len(resc)}   broken {len(brok)}   "
          f"NET {len(resc) - len(brok):+d}")

    for title, grp in (("RESCUED", resc), ("BROKEN", brok)):
        if not grp:
            continue
        print(f"\n  {title}")
        for r in grp[:10]:
            print(f"    {r['date']} {r['code']:8s} "
                  f"{r['home'][:18]:18s} v {r['away'][:18]:18s} "
                  f"{r['off']:>5s} -> {r['on']:<5s} total {r['total']}")

    by = Counter(r["code"] for r in changed)
    if by:
        print(f"\n  changes by league: "
              f"{'  '.join(f'{c}:{n}' for c, n in by.most_common(8))}")


if __name__ == "__main__":
    main()
