"""
How many team lanes was the venue split hiding?

Porto v Arouca showed the fix doing something worse than mispricing. Porto's
scoring probability was 0.779 under the old engine and 0.807 under the
corrected one; the `O0.5` rung carries a floor of 0.80, so the fixture
published NO second tip at all — not a wrong price, an absent lane.

Home sides sitting just under a floor were being pushed below it by a bias of
about 2.8 points, and away sides just above one were being held up by 3.8. Both
directions matter: the first withheld real lanes, the second offered lanes that
should not have qualified.

Counted here on identical fixtures, by pricing each one twice — once with the
correction and once with `_home_share` forced to 0.5, which zeroes BOTH fixes at
once (the shrink target collapses to `league_mu / 2` and the de-bias edge
becomes 0).

Usage:  python scripts/venue_fix_volume.py [--n 150] [--leagues A,B]
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import features, store
from app.engine import team_total
from app.predict import build_request
from scripts.team_shrink_sweep import LEAGUES


def lanes(lg: str, n: int) -> tuple[int, int, int, int]:
    """(gained, lost, kept, total fixtures) comparing corrected against old."""
    df = store.load_results(lg)
    if df is None or len(df) < 260:
        return (0, 0, 0, 0)
    gained = lost = kept = total = 0
    orig = features._home_share
    for _, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        got = {}
        for label, share_fn in (("new", orig),
                                ("old", lambda df_, c, cut: 0.5)):
            features._home_share = share_fn
            features._INDEX_CACHE.clear()
            try:
                req = build_request(lg, str(r["home"]), str(r["away"]), d)
                if req is None or req.p_home_tt05 is None:
                    got[label] = None
                    continue
                c = team_total.candidates(lg, d, req.p_home_tt05, req.p_away_tt05)
                got[label] = c[0][0] if c else None
            except Exception:
                got[label] = None
        features._home_share = orig
        if got.get("new") is None and got.get("old") is None:
            continue
        total += 1
        if got["new"] and not got["old"]:
            gained += 1
        elif got["old"] and not got["new"]:
            lost += 1
        else:
            kept += 1
    return (gained, lost, kept, total)


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 150
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else LEAGUES)
    g = l = k = t = 0
    for lg in codes:
        try:
            a, b, c, d = lanes(lg, n)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
            continue
        g, l, k, t = g + a, l + b, k + c, t + d
    if not t:
        return
    print(f"{t} fixtures that produce a team lane under either engine\n")
    print(f"  lanes GAINED by the fix   {g:5}  ({g/t*100:.1f}%)")
    print(f"  lanes LOST to the fix     {l:5}  ({l/t*100:.1f}%)")
    print(f"  unchanged                 {k:5}  ({k/t*100:.1f}%)")
    # Old and new totals from the three disjoint buckets, rather than from the
    # union: a lane in BOTH engines counts once, so the union is not a valid
    # denominator for either arm.
    old_total, new_total = k + l, k + g
    print(f"\n  lanes offered, old engine  {old_total:5}")
    print(f"  lanes offered, new engine  {new_total:5}")
    print(f"  net change in team-lane volume: "
          f"{(new_total - old_total) / max(1, old_total) * 100:+.1f}%")


if __name__ == "__main__":
    main()
