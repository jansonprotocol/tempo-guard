"""
Final picking: is the card's starred lane better than always taking tip 1?

The app now marks exactly ONE preferred lane per card (the ★), chosen by
the bettor's protocol. This instrument replays that chooser per league
over the most recent 300 fixtures — the bank now includes the board's
own completed matches via ingest_board — and grades it against the
baseline of always following tip 1. Same as-of discipline as every
other replay.

The chooser, exactly as the card applies it (1 Sep):
  1. a DNB claiming more than DNB_GATE points above tip 1  -> tip 3
  2. otherwise                                             -> tip 1
  3. tip 1 absent and a result lane printed                -> tip 3

Tip 2 is never chosen. The rule this replaced dropped to tip 3 whenever
tip 1 was not playable, and measured 82.08% against always-tip-1's
83.49% across 57 leagues — worse in 36 of them — because tip 3's
baseline sits about five points under tip 1's, so the switch traded
down by construction. The gate keeps only the switches that pay: on the
426 bank cards where a DNB out-claims tip 1 by more than two points, the
DNB grades 94.13% against tip 1's 82.16%, and that holds in both time
windows and with every DNB push stripped out. Double chance is excluded
deliberately — at the same gate a DC switch loses 3.53 points.

Usage:  python scripts/final_pick.py [--n 300] [--leagues A,B]
                                     [--dump path.pkl]
"""
from __future__ import annotations

import pickle
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from app.engine import market_select, result_market, team_total
from app.util.asian_lines import evaluate_market
from scripts.two_tips import tips

DEFAULT_N = 300
MIN_ROWS = 200
BASELINES = Path(__file__).resolve().parents[2] / "config" / "baselines.tsv"


def weak_set() -> set:
    out = set()
    for ln in BASELINES.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if int(p[2]) >= 30 and int(p[1]) / int(p[2]) < 0.80:
            out.add(p[0])
    return out


def _hit(res) -> bool:
    return res in (True, "half_win", "push")


def grade(lane_kind, mk, hg, ag):
    if lane_kind == 3:
        won = result_market.won(mk, hg, ag)
        return True if won is None else won      # DNB push = hit
    try:
        res = (team_total.won(mk, hg, ag) if mk.startswith("T")
               else evaluate_market(mk, hg, ag))
    except (ValueError, TypeError):
        return None
    if res is None:
        return None
    return _hit(res if isinstance(res, str) else bool(res))


def replay(league: str, n: int, weak: set, capped: set) -> list[dict]:
    df = store.load_results(league)
    if df is None or len(df) < MIN_ROWS:
        return []
    recent = df.sort_values("date").tail(n)
    rows = []
    for _, r in recent.iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        hg, ag = int(r["hg"]), int(r["ag"])
        try:
            out = tips(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if out is None:
            continue
        t1mk, p1, e1 = out["t1"]
        t2, t3 = out["t2"], out["t3"]
        t1_play = e1 > 0.01
        t2_play = t2 is not None and t2[2] > 0.01
        # The SHIPPED chooser, called rather than re-implemented: the
        # console line and the table this run writes must never be able
        # to disagree, which they did once when the pick was inlined.
        row = dict(p1=p1, has_t3=t3 is not None,
                   p3=t3[1] if t3 else None,
                   t3_dnb=bool(t3) and t3[0].startswith("DNB"))
        pick = chosen(row)
        mk = t3[0] if pick == 3 and t3 else t1mk
        g_t1 = grade(1, t1mk, hg, ag)
        if g_t1 is None:
            continue
        # Every lane's grade is stored, not just the one the chooser
        # took, so a new chooser variant can be scored from the dump
        # instead of costing another full replay (the bettor's
        # tip-2-never variant, 30 Aug, was answered this way).
        rows.append(dict(
            code=league, d=d, pick=pick, mk=mk,
            hit_pick=grade(pick, mk, hg, ag), hit_t1=g_t1,
            says_pick=(t3[1] if pick == 3 else
                       t2[1] if pick == 2 else p1),
            t1_play=t1_play, t2_play=t2_play,
            hit_t2=grade(2, t2[0], hg, ag) if t2 else None,
            hit_t3=grade(3, t3[0], hg, ag) if t3 else None,
            # What chosen() reads: tip 1's claim, and whether tip 3 is a
            # DNB and what it claims. Stored so a dump can be re-scored
            # under a different gate without another full replay.
            p1=p1, p3=row["p3"], t3_dnb=row["t3_dnb"],
            has_t3=t3 is not None,
            # Tip 2's market, so the "what does tip 2's KIND say about the
            # other lanes" question can be scored offline (the bettor's
            # team-over / team-under observation, 31 Aug).
            mk2=t2[0] if t2 else None,
            # Both clubs, tip 1's OWN market and edge (mk above is the
            # PICKED lane, which is a result lane on 22% of cards, so tip
            # 1's side was not recoverable from it) and tip 2's claim, so
            # a dump can be re-sliced the way the board's search bar
            # slices — by league, by team, by side — without another
            # replay (the confluence guard, 1 Sep).
            h=str(r["home"]), a=str(r["away"]),
            mk1=t1mk, e1=e1, p2=t2[1] if t2 else None))
    return rows


OUT = Path(__file__).resolve().parents[2] / "config" / "final_pick.tsv"


DNB_GATE = 2.0          # keep in step with webapp.DNB_GATE


def chosen(r: dict) -> int:
    """The shipped chooser: tip 1, unless a DNB out-claims it by more
    than DNB_GATE points. Tip 2 is never picked — it graded 12.7 points
    below tip 1 on the same fixtures (30 Aug)."""
    if r.get("p1") is None:
        return 3 if r["has_t3"] else 1
    if (r.get("t3_dnb") and r.get("p3") is not None
            and (r["p3"] - r["p1"]) * 100 > DNB_GATE):
        return 3
    return 1


def write_table(rows: list[dict]) -> None:
    """Per-league final-pick record, for the app's baseline bar."""
    per: dict[str, list[int]] = {}
    for r in rows:
        lane = chosen(r)
        hit = r["hit_t3"] if lane == 3 else r["hit_t1"]
        if hit is None:
            hit = r["hit_t1"]
        d = per.setdefault(r["code"], [0, 0, 0.0])
        d[0] += bool(hit)
        d[1] += 1
        d[2] += r.get("says_pick") or 0.0
    lines = [
        "# Final-pick record per league: the card's starred lane replayed",
        "# as-of over the league's most recent fixtures. Written by",
        "# scripts/final_pick.py --write — derived, never typed. Read by",
        "# the app's baseline bar, which averages the leagues equally.",
        "# The trailing column is the summed CLAIM behind those picks, so",
        "# the app can print the record against what was promised.",
        "# league\thits\tn\tsays",
    ]
    for code, (h, n, s) in sorted(per.items()):
        lines.append(f"{code}\t{h}\t{n}\t{s:.4f}")
    OUT.write_text("\n".join(lines) + "\n")
    rates = [h / n for h, n, _s in per.values() if n >= 30]
    print(f"final pick: {sum(rates)/len(rates)*100:.1f}% over {len(rates)} "
          f"leagues -> {OUT}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else DEFAULT_N
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    dump = Path(args[args.index("--dump") + 1]) if "--dump" in args else None

    weak = weak_set()
    capped = set(market_select.CONSENSUS_CAP_LEAGUES)
    allrows = []
    per = []
    for lg in codes:
        try:
            rows = replay(lg, n, weak, capped)
        except Exception as exc:
            print(f"{lg:9} FAILED {exc}", file=sys.stderr)
            continue
        if not rows:
            continue
        allrows += rows
        graded = [r for r in rows if r["hit_pick"] is not None]
        hp = sum(r["hit_pick"] for r in graded)
        h1 = sum(r["hit_t1"] for r in graded)
        per.append((lg, hp / len(graded), h1 / len(graded), len(graded)))
        print(f"{lg:9} pick {hp/len(graded)*100:5.1f}%  "
              f"tip1 {h1/len(graded)*100:5.1f}%  "
              f"({len(graded)} graded)", flush=True)

    if dump:
        dump.write_bytes(pickle.dumps(allrows))
    if "--write" in args:
        write_table(allrows)
    if per:
        ap = sum(p for _c, p, _t, _n in per) / len(per)
        a1 = sum(t for _c, _p, t, _n in per) / len(per)
        print(f"\nleague-average: final pick {ap*100:.1f}% "
              f"vs always-tip-1 {a1*100:.1f}%  over {len(per)} leagues")


if __name__ == "__main__":
    main()
