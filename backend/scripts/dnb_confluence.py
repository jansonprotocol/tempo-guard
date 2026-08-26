"""
Rule 5 on trial: does the DNB confluence read actually point at survivors?

The rule, as articulated (probationary, README Rule 5): when Tip 1 is an
over corridor AND Tip 2 is a team lane pointing at side X — directly (X's
own over rung) or by elimination (the OTHER side's under rung, so the
promised goals must come from X) — and the probability is strong, then X
on Draw-No-Bet is a play, confirmed last by a short book price (leg 4,
which needs odds and therefore cannot be backtested here).

This grades legs 1-3 over the store. Every fixture is replayed through the
LIVE pair — `two_tips.tips`, the exact code that fills the board — so a
"confluence fixture" here is precisely one the board would have shown that
way. Side X is then graded on the 90-minute result: win, draw (DNB push),
or loss. The numbers that matter:

    avoid-defeat %   how often X did not lose (the 28-lane archive read
                     measured 78.6% for team-over tags alone)
    DNB break-even   the odds that make the group profitable:
                     odds = (p_win + p_loss) / p_win, pushes returned
    control groups   the same team-lane read WITHOUT the over Tip 1 —
                     if the control matches, the confluence adds nothing
                     and the rule is just "strong attacks rarely lose"

Two-window split (older/newer half by date) applies as everywhere: a rule
that only works in one half is a story, not a rule.

VERDICT (25 Aug 2026, 6,354 pointed pairs, 300 most recent per league):

    all confluence        3131   avoid-defeat 78.1   break-even 1.40
    control (no over T1)  3223   avoid-defeat 77.8   break-even 1.43
    O0.5-pointed          1258   avoid-defeat 73.4   break-even 1.55
    strong rung, X home   1231   avoid-defeat 83.9   break-even 1.26
      older / newer half         86.5 / 81.3         1.21 / 1.31

The archive's 78.6% replicated at scale, so the core observation is real —
but the over Tip 1 adds nothing (control matches), the safe O0.5 tag is
the trap, and the bettable shape is the strong rung (O1.5 direct or U1.5
elimination) on the HOME side, which holds direction in both windows.
Leg 4 becomes a number: the offered DNB price must clear the group's
break-even (~1.30 strong-home, ~1.50 away) — at the 1.22-1.27 the book
typically posts, the group is fair value, not an edge. README Rule 5
carries the refined rule.

Usage:  python scripts/dnb_confluence.py [--n 300]
                                         [--dump rows.pkl | --load rows.pkl]
"""
from __future__ import annotations

import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from scripts.edge_bands import wilson
from scripts.two_tips import tips

SKIP = {"UCL", "UCL-Q", "UEL", "UEL-Q", "UECL", "UECL-Q", "COPA-L",
        "EC", "WC"}


def side_of(t1_market: str, t2_market: str) -> tuple[str, str] | None:
    """The side the pair points at, or None when it points at nothing.

    Returns (X, how) with X in {"H", "A"} and how in {"direct", "elim"}.
    Only called when Tip 1 is an over corridor — leg 1 is the caller's job.
    """
    parts = t2_market.split()
    if len(parts) != 2 or parts[0] not in ("TA", "TB"):
        return None
    side, rung = parts
    if rung.startswith("O"):
        return ("H" if side == "TA" else "A", "direct")
    if rung.startswith("U"):
        return ("A" if side == "TA" else "H", "elim")
    return None


def collect(n: int) -> list[dict]:
    rows = []
    for lg in sorted(store.available_leagues()):
        if lg in SKIP:
            continue
        df = store.load_results(lg)
        if df is None or len(df) < 200:
            continue
        for _, r in df.sort_values("date").tail(n).iterrows():
            if r[["hg", "ag"]].isna().any():
                continue
            d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
            try:
                pair = tips(lg, str(r["home"]), str(r["away"]), d)
            except Exception:
                continue
            if pair is None or pair["t2"] is None:
                continue
            t1, p1, _e1 = pair["t1"]
            m2, p2, _e2, _why = pair["t2"]
            pointed = side_of(t1, m2)
            if pointed is None:
                continue
            x, how = pointed
            hg, ag = int(r["hg"]), int(r["ag"])
            gf, ga = (hg, ag) if x == "H" else (ag, hg)
            rows.append(dict(
                lg=lg, date=r["date"], t1=t1, p1=p1, m2=m2, p2=p2,
                x=x, how=how, over1=t1.startswith("O"),
                res="W" if gf > ga else "D" if gf == ga else "L"))
    return rows


def show(label: str, rs: list[dict]) -> None:
    if len(rs) < 20:
        print(f"  {label:34} too few: {len(rs)}")
        return
    n = len(rs)
    w = sum(1 for r in rs if r["res"] == "W")
    d = sum(1 for r in rs if r["res"] == "D")
    lo = n - w - d
    ad = (w + d) / n
    ci = wilson(w + d, n)
    be = (w + lo) / w if w else float("inf")
    print(f"  {label:34} {n:5}   W {w/n*100:4.1f}  D {d/n*100:4.1f}  "
          f"L {lo/n*100:4.1f}   avoid-defeat {ad*100:5.1f} "
          f"[{ci[0]*100:.0f}-{ci[1]*100:.0f}]   DNB break-even {be:.2f}")


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else 300
    dump = args[args.index("--dump") + 1] if "--dump" in args else None
    load = args[args.index("--load") + 1] if "--load" in args else None

    if load:
        import pickle
        rows = pickle.loads(Path(load).read_bytes())
    else:
        rows = collect(n)
    if dump:
        import pickle
        Path(dump).write_bytes(pickle.dumps(rows))
    conf = [r for r in rows if r["over1"]]
    ctrl = [r for r in rows if not r["over1"]]
    print(f"{len(rows)} pointed pairs replayed "
          f"({len(conf)} with an over Tip 1, {len(ctrl)} without)\n")

    print("THE RULE (leg 1 + leg 2: over corridor + pointed team lane)")
    show("all confluence", conf)
    show("  direct (X's own over rung)", [r for r in conf
                                          if r["how"] == "direct"])
    show("  elimination (other side under)", [r for r in conf
                                              if r["how"] == "elim"])
    mid = sorted(r["date"] for r in conf)[len(conf) // 2]
    show("  older half", [r for r in conf if r["date"] < mid])
    show("  newer half", [r for r in conf if r["date"] >= mid])

    print("\nLEG 3 GRADIENT (tip-2 probability, confluence only)")
    for lab, lohi in (("p2 >= 0.85", (0.85, 2)), ("0.75-0.85", (0.75, 0.85)),
                      ("0.65-0.75", (0.65, 0.75)), ("p2 < 0.65", (0, 0.65))):
        show(lab, [r for r in conf if lohi[0] <= r["p2"] < lohi[1]])

    print("\nBY TIP-2 RUNG (confluence only)")
    by = defaultdict(list)
    for r in conf:
        by[r["m2"].split()[1]].append(r)
    for rung in sorted(by):
        show(rung, by[rung])

    print("\nCONTROL (pointed team lane, Tip 1 NOT an over corridor)")
    show("all control", ctrl)

    print("\nHOME/AWAY SPLIT OF X (confluence)")
    show("X is home", [r for r in conf if r["x"] == "H"])
    show("X is away", [r for r in conf if r["x"] == "A"])

    # The cross-cut the flat tables hint at: the strong-claim rungs (O1.5
    # direct, U1.5 elimination — NOT the safe O0.5 tag) on the home side.
    # This is the shape of every DNB actually taken under the rule, so it
    # gets the full two-window treatment.
    strong = [r for r in conf if r["m2"].split()[1] in ("O1.5", "U1.5")]
    print("\nTHE BETTABLE CANDIDATE (strong rung only: O1.5 direct or "
          "U1.5 elimination)")
    show("strong rung, any venue", strong)
    sh = [r for r in strong if r["x"] == "H"]
    show("strong rung, X home", sh)
    if len(sh) >= 40:
        m = sorted(r["date"] for r in sh)[len(sh) // 2]
        show("  older half", [r for r in sh if r["date"] < m])
        show("  newer half", [r for r in sh if r["date"] >= m])
    show("strong rung, X away", [r for r in strong if r["x"] == "A"])


if __name__ == "__main__":
    main()
