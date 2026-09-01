"""Grade the forward log: what the card said, against what happened.

Everything in docs/confluence-guard.md is a REPLAY. A replay can only ever
say what a rule would have done on history that was already in the store
when the rule was written, and this project has watched effects halve when
the bank doubled. The forward log is the answer to that: each labelled
card is stamped at render with its label, its score, the price it needed
and the price the market was showing, and nothing about it can move
afterwards.

This grades the stamped rows once results land, and reports:

  * per label, the hit rate against the number that label PREDICTED
  * the decline rule at its registered bar, against taking everything
  * the gated DNB lane on its own, since that is the only lane with a
    measured positive return

    python scripts/forward_settle.py
    python scripts/forward_settle.py --bar 0.08     a different margin

Nothing here tunes anything. The predictions in webapp.SAYS and the
DECLINE_MARGIN are registered; this only reports how they are doing.
"""
from __future__ import annotations

import math
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing, result_market, team_total
from app.util.asian_lines import evaluate_market
from scripts.webapp import DECLINE_MARGIN, FORWARD, SAYS

ROOT = Path(__file__).resolve().parents[2]
BANK = ROOT / "web" / "matchbank.json"
ORDER = ("super green", "green", "orange", "red", "super red")


def _results() -> dict:
    """Final scores from the rendered bank, keyed by (date, fixture)."""
    import json
    try:
        comps = json.loads(BANK.read_text())["comps"]
    except Exception:
        return {}
    out = {}
    for _code, comp in comps.items():
        for m in comp.get("matches", []):
            sc = (m.get("score") or "").strip()
            if "-" in sc and m.get("mark") in ("✅", "✅½", "◦", "❌"):
                try:
                    h, a = sc.split("-")
                    out[(m["d"], f"{m['h']} v {m['a']}")] = (int(h), int(a))
                except ValueError:
                    pass
    return out


def _settle(lane: str, hg: int, ag: int):
    """(stake fraction won, counted as a hit?) — or None if ungradeable.

    Two different questions, and they must not be answered by one number.
    `evaluate_market` reports a push as "half_win" so the HIT-RATE column
    can count it, which is the project's convention everywhere; paying a
    push as half a win would flatter the money column. `settle_fraction`
    is the money, and it is the same call the retro ROI tables used, so
    forward and retro stay comparable by construction.
    """
    if lane in ("1X", "X2", "12", "DNB1", "DNB2"):
        won = result_market.won(lane, hg, ag)
        if won is None:                     # DNB draw: stake back, a hit
            return 0.0, True
        return (1.0 if won else -1.0), bool(won)
    try:
        if lane.startswith("T"):
            r = team_total.won(lane, hg, ag)
            if r is None:
                return None
            return (1.0 if r else -1.0), bool(r)
        money = pricing.settle_fraction(lane, hg + ag)
        r = evaluate_market(lane, hg, ag)
    except (ValueError, TypeError):
        return None
    if money is None or r is None:
        return None
    return money, (r in (True, "half_win", "push"))


def rows() -> list[dict]:
    if not FORWARD.exists():
        return []
    res = _results()
    out = []
    for ln in FORWARD.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 13:
            continue
        got = res.get((p[1], p[3]))
        if not got:
            continue
        got2 = _settle(p[5], *got)
        if got2 is None:
            continue
        s, hit = got2
        try:
            need, best = float(p[9]), float(p[11] or p[10])
        except ValueError:
            continue
        out.append(dict(d=p[1], code=p[2], fixture=p[3], tip=p[4], lane=p[5],
                        claim=float(p[6]), label=p[7], need=need, best=best,
                        s=s, hit=hit, played=best >= need,
                        pl=(s * (best - 1) if s > 0 else s)))
    return out


def rep(sel: list, lbl: str, says: float | None = None) -> None:
    if not sel:
        print(f"  {lbl:22}      —  nothing settled yet")
        return
    n = len(sel)
    hit = sum(1 for r in sel if r["hit"]) / n
    pl = sum(r["pl"] for r in sel) / n
    se = (100 * math.sqrt(sum((r["pl"] - pl) ** 2 for r in sel) / n / n)
          if n > 1 else 0.0)
    said = f"  said {says*100:5.1f}%  {(hit-says)*100:+5.1f}" if says else ""
    print(f"  {lbl:22} {n:4}  hit {hit*100:5.1f}%{said}"
          f"   ROI {100*pl:+6.2f}% ±{se:4.2f}")


def main() -> None:
    args = sys.argv[1:]
    bar = (float(args[args.index("--bar") + 1]) if "--bar" in args
           else DECLINE_MARGIN)
    R = rows()
    if not R:
        print("nothing settled yet — the log fills as cards are rendered "
              "and empties into results as matches finish")
        return
    print(f"{len(R)} stamped cards have a result\n")
    print("  BY LABEL, against what each one predicted")
    for lb in ORDER:
        rep([r for r in R if r["label"] == lb], lb, SAYS[lb])
    print("\n  THE DECLINE RULE  (bar: break-even "
          f"+{bar*100:.0f}%, registered)")
    play = [r for r in R if r["best"] >= (1 / SAYS[r["label"]]) * (1 + bar)
            and not r["label"].endswith("red")]
    rep(R, "take everything")
    rep(play, "play what clears")
    rep([r for r in R if r not in play], "what was declined")
    print("\n  THE GATED DNB LANE  (the only measured positive)")
    dnb = [r for r in R if r["lane"].startswith("DNB")]
    rep(dnb, "all gated DNBs")
    rep([r for r in dnb if r in play], "DNBs that cleared")
    print("\n  retro said: gated DNB +3.7% at real quotes, ladder about "
          "break-even.\n  This table is the only thing that can confirm "
          "or refuse that.")


if __name__ == "__main__":
    main()
