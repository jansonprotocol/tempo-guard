"""
Score the bet ledger against the prices the engine would have paid.

Reads `config/bets.tsv` (what was bought) and joins it to README.md (what was
tipped, and how the match finished). Nothing is re-predicted: the probability
that priced each rung is the one already published for that fixture, inverted
back to a goal expectation so any rung on the ladder can be priced from it —
including rungs neither tip named, which is most of them.

Two conventions run side by side and they disagree, which is the point:

    HIT      the log's convention — a push or half-win counts as a win
    RETURN   the money — a push returns the stake, a half-win returns half at
             odds and half at evens

A bet can be a HIT and still lose money. `Sanfrecce O2.25` finished 1-1: the
bookmaker marked it won and paid EUR 0.60 on a EUR 1.20 stake, which is a half
LOSS. The ledger reports both so the gap is visible rather than argued about.

Cash-outs are scored as if the position were held to full time. That is the
right test of the TIP; it is not what happened to the money, and the
bookmaker export does not carry the amount taken, so the two cannot be
reconciled here. Cashed-out rows are flagged in the output.

Usage:  python scripts/ledger.py [--verbose]
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing
from scripts.backfill_buyfrom import mu_for

ROOT = Path(__file__).resolve().parents[2]
README = ROOT / "README.md"
BETS = ROOT / "config" / "bets.tsv"

TIP = re.compile(r"\b([OU]\d+(?:\.\d+)?)\*{0,2}\s+(\d+(?:\.\d+)?)%")
SCORE = re.compile(r"(\d+)\s*-\s*(\d+)")


def read_fixtures() -> dict[str, dict]:
    """fixture name -> {tip rung, tip probability, home goals, away goals}.

    Reads config/fixtures.tsv, the typed source the whole board renders from.
    This used to parse the README's pipe tables, which made the ledger break
    every time the page's layout moved — the data now never moves.
    """
    out = {}
    for ln in (ROOT / "config" / "fixtures.tsv").read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        _ko, _code, _league, name, tip1, tip2, status = ln.split("\t")
        m = TIP.search(tip1)
        if not m:
            continue
        # Only a SETTLED row carries a result. The status column shows live
        # scores too ("LIVE: 2-1 (90')"), and an earlier version matched those
        # as final — settling bets off matches still being played, which is the
        # same mistake that mis-graded Antwerp off an in-play 1-1.
        s = SCORE.search(status) if status[:1] in ("✅", "❌") else None
        # A live row's running score, kept apart from the final one: it may
        # only ever settle a bet that cannot move again (see bet_state).
        lv = None
        if s is None and status:
            from scripts import liveline
            lv = liveline.score_of(status)
        m2 = TIP.search(tip2)
        out[name] = {
            "rung": m.group(1),
            "p": float(m.group(2)) / 100,
            "rung2": m2.group(1) if m2 else None,
            "p2": float(m2.group(2)) / 100 if m2 else None,
            "hg": int(s.group(1)) if s else None,
            "ag": int(s.group(2)) if s else None,
            "lhg": lv[0] if lv else None,
            "lag": lv[1] if lv else None,
        }
    return out


def bet_state(rung: str, side: str, fx: dict) -> float | None:
    """Settlement fraction for one bet against its fixture row, or None
    while the bet is still open.

    Settles from the FINAL score once the fixture is graded — and EARLY,
    from the live score, in exactly one case: an OVER lane (match or team
    total) already at its MAXIMUM settlement. Goals only accumulate, so a
    fully-won over can never be taken back — the bettor's rule, 28 Aug.
    Everything that can still move waits for the whistle: unders, DNB,
    double chance, and a quarter-line over sitting on a half-win that one
    more goal would upgrade to a full one."""
    hg, ag, live = fx["hg"], fx["ag"], False
    if hg is None:
        if rung.startswith("O") and fx.get("lhg") is not None:
            hg, ag, live = fx["lhg"], fx["lag"], True
        else:
            return None
    if rung == "DNB":
        gf, ga = (hg, ag) if side == "H" else (ag, hg)
        return 1.0 if gf > ga else 0.0 if gf == ga else -1.0
    if rung in ("1X", "X2"):
        return -1.0 if ((hg > ag) if rung == "X2" else (ag > hg)) else 1.0
    goals = hg + ag if side == "-" else (hg if side == "H" else ag)
    s = pricing.settle_fraction(rung, goals)
    if live and s < 1.0:
        return None
    return s


def main() -> None:
    fixtures = read_fixtures()
    rows, missing = [], []
    for ln in BETS.read_text().splitlines():
        if not ln.strip() or ln.startswith("#"):
            continue
        parts = ln.split("\t")
        name, rung, odds, side, cash = parts[:5]
        # Optional 6th column: the rung's probability, for a team lane whose
        # number never reached the fixture tables — the ones derived after
        # kickoff live in the shadow record instead.
        p_over = float(parts[5]) / 100 if len(parts) > 5 and parts[5] else None
        fx = fixtures.get(name)
        if fx is None:
            missing.append(name)
            continue
        rows.append((name, rung, float(odds), side, cash == "1", fx, p_over))

    if missing:
        print("NOT FOUND in README (fix the name in bets.tsv):")
        for n in missing:
            print("   ", n)
        print()

    staked = returned = 0.0
    n_settled = n_hit = 0
    buckets = {"ok": [0, 0.0], "thin": [0, 0.0], "under": [0, 0.0]}
    dead, out = [], []

    for name, rung, odds, side, cash, fx, p_over in rows:
        mu = mu_for(fx["rung"], fx["p"])
        if mu is None:
            print(f"could not invert {name} ({fx['rung']} {fx['p']:.3f})")
            continue

        label = rung if side == "-" else f"{rung}({side})"
        if rung == "DNB":
            # Draw No Bet: settles on the match result — win, push on a draw,
            # loss. The engine neither tips nor prices 1X2, so break-even here
            # is only the bettor's own record; no buy-from judgement applies.
            gf = None if fx["hg"] is None else (
                fx["hg"] if side == "H" else fx["ag"])
            ga = None if fx["hg"] is None else (
                fx["ag"] if side == "H" else fx["hg"])
            out.append((name, rung, odds, None, None, gf, cash, label))
            if gf is not None:
                s = 1.0 if gf > ga else 0.0 if gf == ga else -1.0
                staked += 1
                returned += max(s, 0.0) * odds + (1 - abs(s))
                n_settled += 1
                n_hit += s >= 0
            continue

        if side == "-":
            # An IN-PLAY bet must be priced against the probability that was
            # true when it was struck, not the pre-match one. Backing `O0.5` at
            # half time in a goalless match is a 79% shot; the pre-match mu says
            # 95%, and scoring it that way marks a losing bet as a good buy.
            # A p_override on a match total means exactly that: price this off
            # the supplied probability, because the fixture had moved.
            be = 1 / p_over if p_over else pricing.break_even(rung, mu)
            goals = None if fx["hg"] is None else fx["hg"] + fx["ag"]
        else:
            # A team rung is priced off that SIDE's expectation, which the
            # match mu does not carry. The probability was published with the
            # tip, so it is read off the Tip 2 cell — and every team rung
            # offered is a .5 line, where 1/p IS the break-even.
            p_side = p_over or (fx["p2"] if fx["rung2"] == rung else None)
            be = 1 / p_side if p_side else None
            goals = None if fx["hg"] is None else (
                fx["hg"] if side == "H" else fx["ag"])

        if be is None:
            out.append((name, rung, odds, None, None, goals, cash, label))
            continue

        bf = be * (1 + pricing.DEFAULT_MARGIN)
        tag = "ok" if odds >= bf else ("under" if odds < be else "thin")
        if odds < be:
            dead.append((name, rung, odds, be))

        res = None
        if goals is not None:
            s = pricing.settle_fraction(rung, goals)
            res = max(s, 0.0) * odds + (1 - abs(s))
            staked += 1
            returned += res
            n_settled += 1
            n_hit += s >= 0                      # push counts as a hit
            buckets[tag][0] += 1
            buckets[tag][1] += res
        out.append((name, rung, odds, be, tag, goals, cash, label))

    print(f"{'fixture':38}{'rung':7}{'odds':>6}{'break-even':>11}"
          f"{'verdict':>9}{'goals':>7}{'return':>8}")
    for name, rung, odds, be, tag, goals, cash, label in out:
        b = f"{be:.3f}" if be else "team"
        g = str(goals) if goals is not None else "open"
        if be is None or goals is None:
            r = ""
        else:
            s = pricing.settle_fraction(rung, goals)
            r = f"{max(s, 0.0) * odds + (1 - abs(s)):.2f}"
        flag = " CASHED OUT" if cash else ""
        print(f"{name[:37]:38}{label:9}{odds:6.2f}{b:>11}{tag or '':>9}"
              f"{g:>7}{r:>8}{flag}")

    if not n_settled:
        # An empty or all-open book is the normal state right after a reset,
        # not an error. Reporting zero settled bets beats dividing by it.
        print(f"\nno settled bets yet ({len(rows)} in the ledger)")
        return
    print(f"\n{n_settled} match-rung bets settled at 1 unit")
    print(f"  hit rate (push counts)  {n_hit}/{n_settled} = "
          f"{n_hit / n_settled * 100:.1f}%")
    print(f"  returned {returned:.2f} on {staked:.0f} staked   "
          f"P/L {returned - staked:+.2f}   ROI {(returned / staked - 1) * 100:+.1f}%")

    print("\nby price paid:")
    for k, (n, ret) in buckets.items():
        if n:
            print(f"  {k:6} {n:3} bets   ROI {(ret / n - 1) * 100:+6.1f}%")

    print(f"\nbought below break-even: {len(dead)} of "
          f"{len([o for o in out if o[3]])}")
    for name, rung, odds, be in sorted(dead, key=lambda d: d[2] / d[3]):
        print(f"    {name[:34]:35}{rung:7} paid {odds:.2f}  needed {be:.3f}"
              f"  {(odds / be - 1) * 100:+6.1f}%")


if __name__ == "__main__":
    main()
