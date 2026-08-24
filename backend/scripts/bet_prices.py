"""
Did the prices paid justify the bets placed?

The hit-rate column answers a different question. A bet can be right and still
be a bad buy: `Genoa U1.5` won at 1.18 on a 75.1% read that needed 1.33 to
break even, and repeated a hundred times that lane loses money while looking
like a success in the log.

Every row here is a bet whose price was actually recorded — the afternoon block
was logged without prices and cannot be scored. `P` is the engine's published
probability for the rung bought. Break-even comes from `pricing`, inverting P
back to a goal expectation for match rungs and taking `1 / P` directly for team
rungs, which are `.5` lines and cannot push.

`won` is settled under the money convention, NOT the log's full-win convention:
a push returns the stake and a half-win returns half at odds. That is why the
returns column and the hit-rate column disagree, and the disagreement is the
point.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import pricing
from scripts.backfill_buyfrom import buy_from, mu_for

# fixture, rung, P%, odds, total goals (None = unsettled), side goals for team
# rungs (None = match rung)
BETS = [
    # label,                 rung,     P,    odds, total, side_goals
    ("Sanfrecce v Kawasaki",  "O2.25", 60.2, 1.35,  2,    None),
    ("Al-Fateh v Al-Ettifaq", "O2.5",  61.3, 1.50,  1,    None),
    ("Fenerbahçe (team)",     "O1.5",  64.7, 1.32,  None, 3),
    ("Antwerp v Genk",        "O1.5",  82.5, 1.24,  8,    None),
    ("Basel (team)",          "O1.5",  78.8, 1.83,  None, 2),
    ("Al-Shabab (team)",      "O0.5",  80.8, 1.23,  None, 0),
    ("Saint-Étienne",         "U4.5",  86.6, 1.15,  4,    None),
    ("Śląsk v Widzew",        "U3.5",  84.3, 1.32,  6,    None),
    ("UTC v Comerciantes",    "U3.5",  80.7, 1.19,  4,    None),
    ("Fluminense v Remo",     "O1.5",  72.7, 1.33,  2,    None),
    ("Espanyol v Real Madrid","O1.5",  79.5, 1.17,  2,    None),
    ("Nice v Lorient",        "U3.25", 80.7, 1.33,  0,    None),
    ("Toulouse v Lyon",       "U4.5",  87.8, 1.12,  2,    None),
    ("Troyes (team)",         "U1.5",  79.1, 1.48,  None, 0),
    ("Genoa (team)",          "U1.5",  75.1, 1.18,  None, 0),
    ("Parma (team)",          "U1.5",  81.6, 1.33,  None, 0),
    ("Benevento v Modena",    "U3.5",  82.2, 1.29,  3,    None),
    ("Empoli v Cremonese",    "O1.5",  81.3, 1.34,  1,    None),
    ("Heerenveen v Zwolle",   "U4.5",  84.5, 1.29,  2,    None),
    ("Juan Pablo II v ADT",   "U3.25", 83.1, 1.42,  None, None),
    ("Águilas v Millonarios", "O1.5",  84.2, 1.34,  None, None),
    ("Ceará v Londrina",      "U3.0",  84.9, 1.37,  None, None),
    ("Tolima v Bucaramanga",  "O1.5",  75.6, 1.40,  None, None),
    ("Internacional v Mineiro","O1.5", 75.0, 1.34,  None, None),
    ("Huachipato v Limache",  "U4.5",  85.9, 1.16,  None, None),
    ("Santa Fe v América",    "O1.5",  80.8, 1.49,  None, None),
    ("Cruzeiro v Flamengo",   "O1.5",  88.0, 1.32,  None, None),
    ("U. Católica v Ñublense","O1.5",  83.3, 1.25,  None, None),
    ("Ind. Medellín v Cúcuta","U3.5",  80.1, 1.36,  None, None),
]

# The Águilas and Tolima O1.5 bets were taken off a Tip 2 quoted as O1.75; the
# O1.5 they actually bought wins on the same totals, so it carries Tip 2's
# probability, not Tip 1's. Corrected here rather than in the table above so
# the source stays a transcript of what was published.
_OVERRIDE_P = {"Águilas v Millonarios": 77.5}


def breakeven(rung: str, p_pct: float) -> float:
    bf = buy_from(rung, p_pct)
    return bf / (1 + pricing.DEFAULT_MARGIN)


def settle(rung: str, total: int) -> float:
    return pricing.settle_fraction(rung, total)


def main() -> None:
    rows, staked, returned, settled_n, wins = [], 0.0, 0.0, 0, 0
    for label, rung, p, odds, total, side in BETS:
        p = _OVERRIDE_P.get(label, p)
        be = breakeven(rung, p)
        bf = be * (1 + pricing.DEFAULT_MARGIN)
        ev = (odds * (p / 100) - 1) if rung.endswith(".5") and "." in rung else None
        goals = side if side is not None else total
        res = None
        if goals is not None:
            s = settle(rung, goals)
            res = max(s, 0.0) * odds + (1 - abs(s))
            staked += 1
            returned += res
            settled_n += 1
            wins += s > 0
        rows.append((label, rung, p, odds, be, bf, res))

    print(f"{'bet':26}{'rung':6}{'P':>7}{'odds':>7}{'break-even':>11}"
          f"{'buy from':>10}{'verdict':>10}{'returned':>10}")
    under = under_settled = under_ret = 0
    over = over_settled = over_ret = 0
    for label, rung, p, odds, be, bf, res in rows:
        ok = odds >= bf
        verdict = "ok" if ok else ("UNDER-BE" if odds < be else "thin")
        r = f"{res:.2f}" if res is not None else "open"
        print(f"{label[:25]:26}{rung:6}{p:6.1f}%{odds:7.2f}{be:11.3f}{bf:10.2f}"
              f"{verdict:>10}{r:>10}")
        if ok:
            over += 1
            if res is not None:
                over_settled += 1
                over_ret += res
        else:
            under += 1
            if res is not None:
                under_settled += 1
                under_ret += res

    print(f"\n{settled_n} settled at 1 unit each, {wins} won on the money "
          f"convention")
    print(f"staked {staked:.0f}  returned {returned:.2f}  "
          f"P/L {returned - staked:+.2f}  ROI {(returned/staked - 1)*100:+.1f}%")

    # What the engine thought the settled book was worth BEFORE any ball was
    # kicked. If this is also negative, the loss is the buying, not the luck.
    ev_sum = n_ev = 0.0
    for label, rung, p, odds, total, side in BETS:
        if (side if side is not None else total) is None:
            continue
        p = _OVERRIDE_P.get(label, p)
        be = breakeven(rung, p)
        if round(float(rung[1:]) % 1, 2) != 0.5:
            mu = mu_for(rung, p / 100)
            ev = pricing.expected_value(rung, mu, odds) if mu else odds / be - 1
        else:
            ev = odds * (p / 100) - 1
        ev_sum += ev
        n_ev += 1
    print(f"model EV of that same book, priced before kickoff: "
          f"{ev_sum / n_ev * 100:+.1f}% per bet")
    print(f"\nbought AT or ABOVE the buy-from price: {over} bets, "
          f"{over_settled} settled, ROI {(over_ret/over_settled - 1)*100:+.1f}%"
          if over_settled else "")
    print(f"bought BELOW it:                       {under} bets, "
          f"{under_settled} settled, ROI {(under_ret/under_settled - 1)*100:+.1f}%"
          if under_settled else "")

    dead = [r for r in rows if r[3] < r[4]]
    print(f"\nbought below BREAK-EVEN — negative by the engine's own number "
          f"however the match goes: {len(dead)} of {len(rows)} "
          f"({len(dead)/len(rows)*100:.0f}%)")
    for label, rung, p, odds, be, _bf, _res in dead:
        print(f"    {label[:25]:26}{rung:6} paid {odds:.2f}  needed {be:.3f}"
              f"   {(odds/be - 1)*100:+5.1f}%")


if __name__ == "__main__":
    main()
