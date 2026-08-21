"""
When the tempo read and the market selector disagree, which one is right?

Kashiwa Reysol v V-Varen Nagasaki is the case that prompted this. Athena's raw
corridor lean was OVER; the probability selector overrode it to U4.25 because
the fixture sat above J1's scoring average and no line beat a typical fixture
there. The match finished 6 goals. The raw read was right and the override was
wrong, and the negative edge printed on the tip was the engine saying it had
nothing — it fell back to the safest rung rather than choosing one.

That is one match and proves nothing. But the override is not rare and it fires
across the whole book, so it is worth knowing whether it earns its place.

THE COMPARISON
==============
On fixtures where the two disagree, two arms are graded on the same result:

    taken   the market the engine actually published.
    lean    the safest PLAYABLE rung on the side the tempo read wanted,
            respecting the league's caps. Safest rather than highest-edge,
            because the question is whether the SIDE was right, and taking a
            greedy rung would confound that with how far up the ladder to go.

Agreement fixtures are reported too, as a control. If the engine does better
when the two agree than when they fight, that is worth knowing regardless of
which arm wins the fight.

A caution the earlier work earned: the override exists because the flowchart
never consults the goal model, which is the disconnect probability selection was
built to fix, and that fix was worth +213 wins over 29,762 matches. So the prior
here is that the override is usually right. One vivid loss is not evidence
against it — the ledger is.

Rows are cached, so re-slicing costs seconds.

RESULT: THE OVERRIDE STAYS
==========================
13,792 fixtures, override fires on 1,510 of them (10.9%):

    taken (what Athena published)   85.7% +/-0.9%   edge -1.99%
    lean  (follow the tempo read)   78.1% +/-1.1%   edge +3.20%

Following the tempo read would have lost 115 bets net. Kashiwa was the tail,
not the rule.

The narrow hypothesis — that the override is wrong specifically when it is a
FALLBACK, i.e. when the pick carries negative edge — is not merely unsupported.
It is backwards:

    edge < 0:   taken 88.0%   lean 75.8%     (12.2 points apart)
    edge >= 0:  taken 83.3%   lean 80.5%     ( 2.8 points apart)

The override is at its most valuable exactly where it looked least defensible.
Negative-edge fallbacks are the highest-strike bucket in the entire book at
88.0%, because a negative edge means the selector found nothing worth backing
and retreated to a rung that lands whether or not the model is right. That is
the third independent time this codebase has found predicted edge pointing the
wrong way for hit rate: the sharp-lane comparison, the abstention probe, and now
this.

And in the exact direction of the Kashiwa loss:

    lean over -> taken (U4.25 100%)   85.5%
    lean over -> lean  (O1.5  100%)   77.0%

STRIKE AND EDGE DISAGREE HERE
=============================
The lean arm wins on edge (+3.20% against -1.99%) while losing badly on strike.
Both are true and they are not in conflict: following the tempo read buys
genuine value at a worse hit rate, because Over rungs simply land less often
than Under ones. The brief is hit rate, so the override is kept. Anyone
optimising for value rather than strike should read this table the other way
round.
"""
from __future__ import annotations

import sys
from collections import Counter
from math import sqrt
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.calibrate import CALIB_MIN_MATCHES, _requests_for
from app.data import config
from app.engine import market_select
from app.engine.types import ModuleFlags
from app.predict import predict_fixture
from app.util.asian_lines import evaluate_market, hit_weight

LIMIT = int(sys.argv[1]) if len(sys.argv) > 1 else 300
FORCE_FRESH = "fresh" in set(sys.argv[2:])
EUROPEAN = {"UCL", "UEL", "UECL", "UECL-Q"}
CACHE = Path(__file__).resolve().parents[1] / ".cache" / f"override_{LIMIT}.csv"


def won(m, t) -> bool:
    return hit_weight(evaluate_market(m, t, 0)) >= 1.0


def base_of(mk, tt) -> float:
    if not len(mk) or not len(tt):
        return 0.0
    n = len(tt)
    return sum(c * sum(1 for t in tt if won(m, t)) / n
               for m, c in Counter(mk).items()) / len(mk)


def safest_on_side(side: str, mu: float, cfg) -> str | None:
    """
    The rung this league allows on `side` that is most likely to land.

    Deliberately the safest rather than the most valuable: the question under
    test is whether the SIDE was right, so the ladder position is held as
    constant as the caps allow.
    """
    want_over = side == "over"
    best, best_p = None, -1.0
    for m in market_select.LADDER:
        if m.startswith("O") != want_over:
            continue
        if not market_select.playable(m, cfg.max_under_line, cfg.min_over_line):
            continue
        p = market_select.p_win(m, mu)
        if p > best_p:
            best, best_p = m, p
    return best


def build() -> pd.DataFrame:
    if CACHE.exists() and not FORCE_FRESH:
        df = pd.read_csv(CACHE)
        print(f"reusing {CACHE.name} ({len(df)} fixtures)\n")
        return df

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
        cfg = config.get(code)
        flags = ModuleFlags(**(cfg.module_overrides or {}))
        for req, (hg, ag) in pairs:
            pred = predict_fixture(req, cfg, module_flags=flags)
            market = pred.translated_play.market
            lean = pred.corridor.lean
            side = "over" if market.upper().startswith("O") else "under"
            alt = safest_on_side(lean, req.mu_total, cfg) \
                if lean in ("over", "under") else None
            rows.append({
                "code": code, "date": req.match_date, "total": int(hg) + int(ag),
                "market": market, "side": side, "lean": lean,
                "alt": alt, "edge": pred.pick_edge,
            })
        print(f"  {code}: {len(pairs)}", flush=True)

    df = pd.DataFrame(rows)
    CACHE.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHE, index=False)
    print(f"\ncached {len(df)} fixtures\n")
    return df


def report(label, mk, tt):
    if not len(mk):
        print(f"  {label:34s} none")
        return
    h = sum(1 for m, t in zip(mk, tt) if won(m, t))
    s = h / len(mk)
    se = sqrt(max(s * (1 - s), 1e-9) / len(mk))
    mix = " ".join(f"{m}:{c * 100 // len(mk)}%"
                   for m, c in Counter(mk).most_common(3))
    print(f"  {label:34s} {h:5d}/{len(mk):5d} = {s:6.1%} +/-{se:.1%}  "
          f"edge {s - base_of(mk, tt):+6.2%}   {mix}")


def main() -> None:
    df = build()
    df = df[df["lean"].isin(["over", "under"])]
    n = len(df)

    dis = df[(df["side"] != df["lean"]) & df["alt"].notna()]
    agr = df[df["side"] == df["lean"]]
    print(f"{n} fixtures — override fires on {len(dis)} ({len(dis) / n:.1%})\n")

    print("  DISAGREEMENT — tempo read says one side, selector took the other")
    report("taken (what Athena published)",
           list(dis["market"]), list(dis["total"]))
    report("lean (follow the tempo read)",
           list(dis["alt"]), list(dis["total"]))

    print("\n  CONTROL")
    report("agreement fixtures", list(agr["market"]), list(agr["total"]))
    report("whole book", list(df["market"]), list(df["total"]))

    # The Kashiwa tip carried a NEGATIVE edge, which is the engine reporting it
    # found nothing worth backing. If the override is only wrong there, the fix
    # is a narrow one rather than reversing the rule.
    print("\n  DISAGREEMENT, split by whether the pick had positive edge")
    for label, sub in (("edge < 0", dis[dis["edge"] < 0]),
                       ("edge >= 0", dis[dis["edge"] >= 0])):
        report(f"{label}: taken", list(sub["market"]), list(sub["total"]))
        report(f"{label}: lean", list(sub["alt"]), list(sub["total"]))

    print("\n  which way the override pushes")
    for lean, sub in dis.groupby("lean"):
        report(f"lean {lean} -> taken", list(sub["market"]), list(sub["total"]))
        report(f"lean {lean} -> lean", list(sub["alt"]), list(sub["total"]))


if __name__ == "__main__":
    main()
