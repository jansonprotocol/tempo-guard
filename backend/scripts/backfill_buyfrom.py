"""
Backfill a "buy from" price onto every tip already published in the README.

The obvious way — re-run every fixture through the engine — does not work for
the history: names drift, some leagues were configured after the tip was
issued, and a re-run today would answer with today's form rather than the form
the tip was actually built on. It would quietly republish different tips.

So this inverts instead. The published probability IS the engine's own P for
that rung on that day, and `p_win(market, mu)` is strictly monotonic in mu, so
the mu that produced a printed P can be recovered by bisection and the rung
priced from it. Nothing is re-predicted; the numbers in the table are taken as
given and only translated into odds.

Rounding: probabilities are printed to one decimal (some early rows to zero),
so a recovered mu carries about +/-0.005 of slack and the price about +/-0.5%.
That is far below the margin being applied and does not change a decision.

Team rungs (`U1.5`, `O1.5`, `O0.5` on one side) are all `.5` lines and cannot
push, so `1 / P` is already the break-even and no inversion is needed.

Usage:  python scripts/backfill_buyfrom.py            # preview
        python scripts/backfill_buyfrom.py --write    # rewrite README.md
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.engine import market_select, pricing

README = Path(__file__).resolve().parents[2] / "README.md"

# A tip cell holds a rung and the probability that follows it. Team lanes name
# the side first ("**Basel O1.5** 78.8%"), so the rung is matched on its own
# and the side is irrelevant to the price.
TIP = re.compile(r"\b([OU]\d+(?:\.\d+)?)\*{0,2}\s+(\d+(?:\.\d+)?)%")


def mu_for(market: str, p: float, lo: float = 0.05, hi: float = 6.0) -> float | None:
    """
    Recover the goal expectation that gives this rung this probability.

    The upper bracket is 6.0, not something safely enormous, because
    `p_win` is only monotonic while its Poisson tail stays inside the 12-goal
    truncation. Past about mu 7 an Over line starts LOSING probability as mu
    rises — `p_win("O1.0", 9.0)` is 0.876 against 0.900 at mu 2.3 — and
    bisection on a non-monotonic function silently returns nonsense. No
    fixture in this engine carries mu anywhere near 6.
    """
    f = lambda mu: market_select.p_win(market, mu)
    p_lo, p_hi = f(lo), f(hi)
    if not (min(p_lo, p_hi) - 1e-6 <= p <= max(p_lo, p_hi) + 1e-6):
        return None
    rising = p_hi > p_lo
    for _ in range(200):
        mid = (lo + hi) / 2
        if (f(mid) < p) == rising:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2


def buy_from(market: str, p_pct: float) -> float | None:
    p = p_pct / 100
    if not 0 < p < 1:
        return None
    line = float(market[1:])
    if round(line % 1, 2) == 0.5:      # cannot push — 1/P is exact
        return (1 / p) * (1 + pricing.DEFAULT_MARGIN)
    mu = mu_for(market, p)
    if mu is None:
        return None
    try:
        return pricing.buy_from(market, mu)
    except ValueError:
        return None


def annotate(cell: str) -> str:
    """Append `buy>=X` to a tip cell, once."""
    if "buy≥" in cell:
        return cell
    m = TIP.search(cell)
    if not m:
        return cell
    price = buy_from(m.group(1), float(m.group(2)))
    if price is None:
        return cell
    return f"{cell.rstrip()} · buy≥{price:.2f} "


def main() -> None:
    write = "--write" in sys.argv
    out, changed = [], 0
    for ln in README.read_text().splitlines():
        # Only the two fixture tables: 6 columns, tips in 4 and 5.
        if ln.startswith("|") and ln.count("|") == 7 and "---" not in ln:
            cells = ln.split("|")
            if TIP.search(cells[4]) or TIP.search(cells[5]):
                before = ln
                cells[4] = annotate(cells[4])
                cells[5] = annotate(cells[5])
                ln = "|".join(cells)
                changed += before != ln
        out.append(ln)

    print(f"{changed} rows annotated")
    if write:
        README.write_text("\n".join(out) + "\n")
        print(f"wrote {README}")
    else:
        for ln in out:
            if "buy≥" in ln:
                print(ln)


if __name__ == "__main__":
    main()
