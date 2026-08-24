"""
Do the lanes the venue fix ADDED deliver, or are they filler?

The fix raised team-lane volume 870 to 1,153. Those 479 extra lanes are by
construction marginal: they appear because a home side's probability rose just
far enough to clear a rung floor it previously sat under. Marginal offers are
exactly where a population goes soft, so pooled calibration on the new set can
look fine while the additions quietly under-deliver — the average is carried by
the 674 lanes that were already there.

So they are scored separately. Each fixture is priced twice, once with the
correction and once with `_home_share` forced to 0.5, and its lane is labelled:

    GAINED   offered by the corrected engine only
    KEPT     offered by both
    LOST     offered by the old engine only — scored on the OLD lane, to see
             whether removing them cost anything

If GAINED tracks KEPT, the extra third of volume is real. If it under-delivers,
those lanes need their own floor rather than a welcome.

Usage:  python scripts/gained_lanes.py [--n 120] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import features, store
from app.engine import team_total
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES, wilson


def collect(lg: str, n: int) -> list:
    df = store.load_results(lg)
    if df is None or len(df) < 260:
        return []
    out = []
    orig = features._home_share
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        got = {}
        for label, fn in (("new", orig), ("old", lambda a, b, c: 0.5)):
            features._home_share = fn
            features._INDEX_CACHE.clear()
            try:
                req = build_request(lg, str(r["home"]), str(r["away"]), d)
                if req is None or req.p_home_tt05 is None:
                    got[label] = None
                    continue
                c = team_total.candidates(lg, d, req.p_home_tt05, req.p_away_tt05)
                got[label] = c[0] if c else None
            except Exception:
                got[label] = None
        features._home_share = orig
        hg, ag = int(r["hg"]), int(r["ag"])
        if got["new"] and not got["old"]:
            m, p, _e = got["new"]
            out.append(("GAINED", p, team_total.won(m, hg, ag)))
        elif got["old"] and not got["new"]:
            m, p, _e = got["old"]
            out.append(("LOST", p, team_total.won(m, hg, ag)))
        elif got["new"] and got["old"]:
            m, p, _e = got["new"]
            out.append(("KEPT", p, team_total.won(m, hg, ag)))
    return out


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 120
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)
    rows = []
    for lg in codes:
        try:
            rows += collect(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
    if not rows:
        return
    print(f"{len(rows)} team lanes across both engines\n")
    print(f"{'bucket':10}{'n':>7}{'says':>8}{'hit':>8}{'gap':>8}{'95% CI':>13}")
    for label in ("KEPT", "GAINED", "LOST"):
        b = [r for r in rows if r[0] == label]
        if len(b) < 30:
            continue
        k = sum(1 for r in b if r[2])
        hit, says = k / len(b), sum(r[1] for r in b) / len(b)
        w = wilson(k, len(b))
        print(f"{label:10}{len(b):7}{says*100:7.1f}%{hit*100:7.1f}%"
              f"{(hit-says)*100:+8.1f}   [{w[0]*100:.0f}-{w[1]*100:.0f}]")
    live = [r for r in rows if r[0] in ("KEPT", "GAINED")]
    k = sum(1 for r in live if r[2])
    says = sum(r[1] for r in live) / len(live)
    print(f"\n{'NEW ENGINE':10}{len(live):7}{says*100:7.1f}%{k/len(live)*100:7.1f}%"
          f"{(k/len(live)-says)*100:+8.1f}")
    print("\nGAINED tracking KEPT means the extra volume is real.")


if __name__ == "__main__":
    main()
