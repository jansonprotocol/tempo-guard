"""
Does the team lane know WHICH side the goals come from?

Two cards on 29 Aug carried the same read — goals expected from the away
side — and reality split them: Yunnan Yukun (credited "strong attack
away", +22% claimed edge) went down 6-0 without a shot landing, while
Forest needed a penalty. The bettor's hypothesis: side attribution fails
when the credited side's record is THIN (few league matches behind the
rate) or freshly BROKEN (recent form diverging hard from the long window
— the results-only shadow of a coaching change or transfer churn, per
Yukun and Zhejiang both changing coach in January).

This instrument replays the team lane as-of over each league's recent
fixtures — the same top-candidate rule two_tips uses for Tip 2 — and
tags every offer with the credited side's depth and divergence, so the
claim can be graded by slice, in two half-windows, before any constant
moves. Rows are pickled for deeper cuts.

    python scripts/team_attrib.py --n 400 --leagues A,B,... \
        --dump out.pkl
"""
from __future__ import annotations

import math
import pickle
import sys
from datetime import timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.engine import team_total
from app.predict import build_request

RECENT = 8          # the form stub a regime break shows up in first


def _team_stats(df, team: str, cutoff):
    """(matches before cutoff, recent gf/match, long gf/match) for one club.

    Divergence is measured on goals SCORED because every offered rung is a
    read on that side's attack — U1.5 included, which is the weak-attack
    read of the same number.
    """
    past = df[(df["date"] < cutoff)
              & ((df["home"] == team) | (df["away"] == team))]
    n = len(past)
    if n == 0:
        return 0, None, None
    gf = [(r["hg"] if r["home"] == team else r["ag"]) for _, r in
          past.sort_values("date").iterrows()]
    gf = [0 if g != g else int(g) for g in gf]          # NaN-safe
    recent = gf[-RECENT:]
    long = gf[:-RECENT] or recent
    return n, sum(recent) / len(recent), sum(long) / len(long)


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
        h, a = str(r["home"]), str(r["away"])
        try:
            req = build_request(league, h, a, d)
        except Exception:
            continue
        if req is None:
            continue
        cands = team_total.candidates(league, d,
                                      req.p_home_tt05, req.p_away_tt05)
        if not cands:
            continue
        mk, p, edge = cands[0]              # what Tip 2 would offer
        team = h if mk.startswith("TA") else a
        cutoff = r["date"]
        depth, rec, lng = _team_stats(df, team, cutoff)
        try:
            hit = team_total.won(mk, int(r["hg"]), int(r["ag"]))
        except (ValueError, TypeError):
            continue
        out.append(dict(code=league, d=d, mk=mk, team=team, says=p,
                        edge=edge, hit=hit, depth=depth,
                        recent_gf=rec, long_gf=lng))
    return out


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 400
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
            hit = sum(r["hit"] for r in got) / len(got)
            says = sum(r["says"] for r in got) / len(got)
            print(f"{lg:9}{len(got):5}  says {says*100:5.1f}%  "
                  f"hit {hit*100:5.1f}%  gap {(hit-says)*100:+5.1f}",
                  flush=True)
            rows += got

    if dump is not None:
        dump.write_bytes(pickle.dumps(rows))
        print(f"dumped {len(rows)} team-lane rows -> {dump}")


if __name__ == "__main__":
    main()
