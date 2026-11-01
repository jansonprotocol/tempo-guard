"""
Is a tracked flag worth anything?

Two flags are kept purely so this question can be asked later:
config/tier_band_log.tsv (does the gap band back the tier? 2 Oct) and
config/league_cell_log.tsv (does the league back the profile? 31 Oct).
Both are frozen at kickoff, so each row is a genuine prediction made
before the match — which is the only kind worth scoring.

    python scripts/flag_worth.py band      does the band back the tier?
    python scripts/flag_worth.py league    does the league back the profile?
    python scripts/flag_worth.py band --all   include backfilled rows

THE TEST THAT MATTERS IS STRATIFIED. A flag's words are not spread
evenly across tiers — a veto lands far more often on a weak card than
on a STRONG one — so the raw hit rate behind a word mostly reports
WHICH CARDS GET THAT WORD, not whether the word knows anything. Each
word is therefore compared with what its own tier mix predicts,
leave-one-out. A word that adds nothing comes out at z ≈ 0 however
alarming its raw rate looks.

BACKFILLED ROWS ARE EXCLUDED BY DEFAULT. The band log's earliest rows
were reconstructed "as-of" rather than read at the time, and the
reading behind them is thin — a median of six of his own bets against
fifteen on the live rows. They are not a prediction anybody could have
acted on, and including them flips the answer: as-of vetoes land 91.9%
and live ones 69.2%. --all puts them back, for comparison.
"""
from __future__ import annotations

import collections
import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, profile_mine as pm  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
HIT_MARKS = ("✅", "◦")        # a hit, and a push, which counts as one

LOGS = {
    "band": dict(path=ROOT / "config" / "tier_band_log.tsv",
                 title="Does the band back the tier?",
                 stratum=2, call=10, read_at=11, ncols=12,
                 order=("confirms", "split", "close", "vetoes", "thin")),
    "league": dict(path=ROOT / "config" / "league_cell_log.tsv",
                   title="Does the league back the profile?",
                   stratum=3, call=9, read_at=10, ncols=11,
                   order=("ok", "close", "veto", "thin")),
}


def results() -> dict:
    """(date, fixture) -> did it land, for every settled card."""
    out = {}
    for f in board.load():
        if not (f.settled and "—" in f.status):
            continue
        sc = f.status.split("—")[-1].strip().split(" ")[0]
        if "-" in sc:
            out[(f.kickoff.split(" ")[0], f.teams)] = f.status[:1] in HIT_MARKS
    return out


def rows(spec: dict, include_backfill: bool = False) -> list[dict]:
    """Settled rows of one flag log, with the result joined on."""
    if not spec["path"].exists():
        return []
    res = results()
    out = []
    for ln in spec["path"].read_text(encoding="utf-8").splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        q = ln.split("\t")
        if len(q) < spec["ncols"]:
            continue
        hit = res.get((q[0], q[1]))
        if hit is None:
            continue
        live = q[spec["read_at"]] != "as-of"
        if not live and not include_backfill:
            continue
        out.append(dict(date=q[0], fixture=q[1], stratum=q[spec["stratum"]],
                        call=q[spec["call"]], live=live, hit=hit))
    return out


def score(rs: list[dict], word: str) -> dict | None:
    """One word, against what its own stratum mix predicts."""
    strata = collections.defaultdict(list)
    for r in rs:
        strata[r["stratum"]].append(r)
    O = E = V = 0.0
    n = 0
    for _s, group in strata.items():
        mine = [r for r in group if r["call"] == word]
        others = [r for r in group if r["call"] != word]
        if not mine or len(others) < 10:
            continue
        q = sum(r["hit"] for r in others) / len(others)
        O += sum(r["hit"] for r in mine)
        E += len(mine) * q
        V += len(mine) * q * (1 - q)
        n += len(mine)
    if not n:
        return None
    if V <= 0:
        # Every card in the comparison group went the same way, so the
        # binomial variance is zero and no z exists. Report it anyway,
        # with z None — main() prints it as unscorable rather than
        # dropping the word, because a word that vanishes from the
        # report reads as a word that was fine.
        lo, hi = pm.wilson(int(O), n)
        return dict(word=word, n=n, hits=int(O), rate=100 * O / n,
                    predicted=100 * E / n, z=None, lo=lo, hi=hi)
    lo, hi = pm.wilson(int(O), n)
    return dict(word=word, n=n, hits=int(O), rate=100 * O / n,
                predicted=100 * E / n, z=(O - E) / math.sqrt(V),
                lo=lo, hi=hi)


def main() -> None:
    args = sys.argv[1:]
    which = next((a for a in args if a in LOGS), "band")
    spec = LOGS[which]
    rs = rows(spec, include_backfill="--all" in args)
    if not rs:
        print(f"no settled rows in {spec['path'].name} yet")
        return

    n, h = len(rs), sum(r["hit"] for r in rs)
    print(f"{spec['title']}")
    print(f"  {spec['path'].name} · {n} settled rows"
          + ("  (backfilled rows INCLUDED)" if "--all" in args
             else "  (live rows only; --all adds the backfilled ones)"))
    print(f"  all of them: {h}/{n} {100 * h / n:.1f}%\n")

    print("  RAW — what each word's cards actually did")
    print(f"    {'word':10} {'cards':>10} {'rate':>7}")
    for word in spec["order"]:
        k = [r for r in rs if r["call"] == word]
        if not k:
            continue
        kh = sum(r["hit"] for r in k)
        print(f"    {word:10} {kh:>4}/{len(k):<5} {100 * kh / len(k):6.1f}%")

    print("\n  STRATIFIED — against what the card's own tier predicts")
    print("  (this is the one that answers the question)")
    print(f"    {'word':10} {'cards':>10} {'rate':>7} {'predicted':>10} "
          f"{'z':>6}   {'95% CI':>15}")
    verdicts = []
    for word in spec["order"]:
        s = score(rs, word)
        if not s:
            continue
        verdicts.append(s)
        ztxt = f"{s['z']:+6.2f}" if s["z"] is not None else "     -"
        print(f"    {s['word']:10} {s['hits']:>4}/{s['n']:<5} "
              f"{s['rate']:6.1f}% {s['predicted']:9.1f}% {ztxt}   "
              f"{s['lo']:5.1f}-{s['hi']:5.1f}%"
              + ("   (no z: the comparison group is unanimous)"
                 if s["z"] is None else ""))

    missing = [w for w in spec["order"]
               if w not in {v["word"] for v in verdicts}
               and any(r["call"] == w for r in rs)]
    if missing:
        print("\n  NOT SCORABLE (too few cards outside the word in every "
              "tier it appears in): " + ", ".join(missing))
    tested = len(verdicts)
    print(f"\n  {tested} words tested, so |z| of about "
          f"{2.5 if tested > 3 else 2.0:.1f} is the honest bar, not 2.0.")
    real = [v for v in verdicts
            if v["z"] is not None and abs(v["z"]) >= 2.5]
    print("\n  CARRIES INFORMATION:")
    if not real:
        print("    none. Every word performs as its tier already predicted —")
        print("    the flag is describing which cards it lands on, not their "
              "outcome.")
    for v in real:
        way = "WORSE" if v["z"] < 0 else "BETTER"
        print(f"    {v['word']:10} {v['rate']:.1f}% against "
              f"{v['predicted']:.1f}% predicted — {way} than its tier, "
              f"z {v['z']:+.2f} on {v['n']} cards")
    dead = [v for v in verdicts
            if v["z"] is not None and abs(v["z"]) < 1.0]
    if dead:
        print("\n  CARRIES NOTHING (z under 1.0 — the word adds no "
              "information at all):")
        for v in dead:
            print(f"    {v['word']:10} {v['rate']:.1f}% against "
                  f"{v['predicted']:.1f}% predicted, z {v['z']:+.2f}")


if __name__ == "__main__":
    main()
