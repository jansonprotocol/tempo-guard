"""
Final picking: is the card's starred lane better than always taking tip 1?

The app now marks exactly ONE preferred lane per card (the ★), chosen by
the bettor's protocol. This instrument replays that chooser per league
over the most recent 300 fixtures — the bank now includes the board's
own completed matches via ingest_board — and grades it against the
baseline of always following tip 1. Same as-of discipline as every
other replay.

The chooser, exactly as the card applies it:
  1. weak-tier (tip 1 baseline < 80%) or consensus-capped league, and a
     result lane printed             -> tip 3
  2. tip 1 playable (edge > +1%)    -> tip 1
  3. tip 2 playable                 -> tip 2
  4. a result lane printed          -> tip 3
  5. otherwise                      -> tip 1

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
        # The SHIPPED chooser (see chosen() below): a playable tip 1,
        # else a printed result lane, else tip 1. This used to run the
        # older variant that could pick tip 2, so the console line
        # disagreed with the table the same run wrote — the file was
        # right and the print was stale. One chooser now, both places.
        if t1_play or t3 is None:
            pick, mk = 1, t1mk
        else:
            pick, mk = 3, t3[0]
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
            has_t3=t3 is not None))
    return rows


OUT = Path(__file__).resolve().parents[2] / "config" / "final_pick.tsv"


def chosen(r: dict) -> int:
    """The shipped chooser: a playable tip 1, else a printed result
    lane, else tip 1. Tip 2 is never picked — it graded 12.7 points
    below tip 1 on the same fixtures (30 Aug)."""
    if r["t1_play"]:
        return 1
    return 3 if r["has_t3"] else 1


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
