"""
The published two-tip output: Tip 1 exactly as the engine picks it, Tip 2 as the
runner-up it passed over, with the reason it lost stated rather than implied.

Tip 1 is deliberately untouched. It is the behaviour the live log measures, and
any rule layered on top drifts away from the only number with evidence behind
it. An earlier version overrode it and cost seven points of hit rate.

Tip 2 is the market that ALMOST surfaced: highest probability among the playable
rungs the engine did not take, carrying a real edge. Its floor is RELATIVE to
Tip 1 rather than absolute — a flat 65% once hid Sanfrecce's O2.75 at 60.2%
carrying more edge than the tip itself. What matters is how far the alternative
sits below its own Tip 1, not its bare number.

Usage:  python scripts/two_tips.py LEAGUE "Home" "Away" YYYY-MM-DD  [...]
        python scripts/two_tips.py --file fixtures.tsv
"""
from __future__ import annotations

import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import club_elo, config
from app.engine import market_select, pricing, team_total
from app.engine.types import ModuleFlags
from app.predict import build_request, predict_fixture

FLOOR = market_select.MIN_WIN_PROB
MIN_EDGE = 0.010
MIN_TIP2_ABS = 0.55
MAX_TIP2_GAP = 0.25

# Within a tier every rung has the same win probability but not the same
# downside, so ties resolve to the softer settlement: U3.75 half-loses at 4
# where U3.0 loses outright, O2.25 half-loses at 2 where O2.75 loses outright.
PREFER = ["U3.75", "U3.5", "U3.25", "U3.0", "O1.75", "O1.5", "O2.25", "O2.5", "O2.75"]


def tips(lg: str, h: str, a: str, d: date):
    req = build_request(lg, h, a, d)
    if req is None:
        return None
    cfg = config.get(lg)
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    t1 = predict_fixture(req, cfg, module_flags=flags).translated_play.market

    sc = [(m, e, p) for m, e, p, _q in
          market_select.score_markets(req.mu_total, req.league_mu)
          if market_select.playable(m, cfg.max_under_line, cfg.min_over_line)]
    # Every measured overconfidence lands on the PUBLISHED numbers — cup
    # overs read 3.5 points hot, domestic rungs above says 90% read 1.2 —
    # through one gate (market_select.stated). The debit hits stated
    # probability AND edge by the same amount, so a hot tip must clear the
    # playable bar on its honest number, while Tip 1's SELECTION stays
    # with the engine, untouched as ever.
    sc = [(m, e - (p - market_select.stated(lg, m, p, base_p=p - e)),
           market_select.stated(lg, m, p, base_p=p - e)) for m, e, p in sc]
    by = {m: (e, p) for m, e, p in sc}
    if t1 not in by:
        return None
    e1, p1 = by[t1]
    floor = cfg.min_win_prob or FLOOR

    # Same probability as Tip 1 means the SAME BET on a different line — O1.75
    # wins on exactly the totals O1.5 does. Offering it is offering one wager
    # twice, so equal-probability rungs are excluded.
    cands = [(m, e, p) for m, e, p in sc
             if abs(p - p1) > 1e-9 and e >= MIN_EDGE
             and p >= max(MIN_TIP2_ABS, p1 - MAX_TIP2_GAP)]
    t2 = None
    if cands:
        top = max(p for _m, _e, p in cands)
        tier = [c for c in cands if abs(c[2] - top) < 1e-9]
        tier.sort(key=lambda c: PREFER.index(c[0]) if c[0] in PREFER else 99)
        m2, e2, p2 = tier[0]
        why = (f"floor {(p2 - floor) * 100:+.1f}" if p2 < floor
               else "lower edge" if e2 <= e1 else "runner-up")
        t2 = (m2, p2, e2, why)

    # A team total is a different market rather than another rung, so it is
    # compared on EDGE — how far each beats a typical fixture of its own kind —
    # and only replaces the ladder runner-up when it beats it on that measure.
    # The streak debit lands here, on the PUBLISHED number and edge, before
    # that comparison — both sides of it are then published figures, the same
    # footing the ladder lanes already stand on through stated().
    tt = team_total.candidates(lg, d, req.p_home_tt05, req.p_away_tt05)
    if tt:
        m3, p3, e3 = tt[0]
        side = h if m3.startswith("TA") else a
        deb = team_total.streak_debit(lg, side, d, m3)
        p3, e3 = p3 - deb, e3 - deb
        # The debited edge must still clear the same bar candidates()
        # applied to the raw one — a streak-inflated offer that only
        # existed because of its streak stops printing here.
        if e3 >= team_total.MIN_EDGE and (t2 is None or e3 > t2[2]):
            t2 = (m3, p3, e3, "team total")
    return dict(lg=lg, mu=req.mu_total, lmu=req.league_mu,
                t1=(t1, p1, e1), t2=t2)


def _undebited(lg: str, market: str, p: float) -> float:
    """The raw engine probability behind a published one — the gate run
    backwards. Exact everywhere except the plateau at the domestic knee,
    where the conservative (larger) end is returned, which prices the
    break-even on the honest side."""
    if lg in club_elo.CUPS:
        if market.split()[-1].startswith("O"):
            return p + club_elo.OVER_SAYS_DEBIT
        return p
    if p >= market_select.HIGH_SAYS_FROM:
        return p + market_select.HIGH_SAYS_DEBIT
    return p


def buy_parts(market: str, mu: float, p: float, edge: float | None = None,
              lg: str | None = None) -> tuple[float, float] | None:
    """(buy-from price, margin over the tip's own break-even) — or None.

    The price is computed on the BLENDED probability (pricing.blend_p):
    the tip's published number pulled toward its league's playable record,
    0.4/0.6 below it and 0.8/0.2 above it. The margin is what the printed
    price still holds over the tip's OWN break-even — it shrinks when the
    blend reaches down to make a lower-probability lane buyable, and can
    go negative there, which is the bettor's declared trade, printed
    rather than hidden."""
    play = market_select.league_play_hit(lg) if lg else None
    pb = pricing.blend_p(p, play) if p > 0 else p
    hc = (1 + pricing.CURSE_HAIRCUT
          if edge is not None and edge > pricing.CURSE_EDGE else 1.0)
    mg = ((1 + pricing.DEFAULT_MARGIN) * hc * (p / pb) - 1) if p > 0 else 0.0
    try:
        be = pricing.buy_from(market, mu, stated_edge=edge)
        if lg is not None and p > 0:
            raw = _undebited(lg, market, p)
            if raw > p:
                be *= raw / p
        if p > 0:
            be *= p / pb
        return be, mg
    except (ValueError, IndexError):
        pass
    if p <= 0:
        return None
    return (1 / pb) * (1 + pricing.DEFAULT_MARGIN) * hc, mg


def buy_value(market: str, mu: float, p: float, edge: float | None = None,
              lg: str | None = None) -> float | None:
    """The buy-from threshold as a number — everything _buy prints except
    the formatting, so instruments can average what a card would say."""
    parts = buy_parts(market, mu, p, edge, lg)
    return parts[0] if parts else None


def _buy(market: str, mu: float, p: float, edge: float | None = None,
         lg: str | None = None) -> str:
    """
    The price to check the book against: break-even, margin, curse haircut,
    all on the blended probability — plus the honest label of what margin
    survives over the tip's own break-even after the blend.

    Match rungs are priced from the goal distribution, because a quarter or
    whole line can push and `1 / p` would misprice it. Every team rung on offer
    (`U1.5`, `O1.5`, `O0.5`) is a `.5` line, which cannot push — there `1 / p`
    IS the break-even, so the same margin is applied to it directly rather
    than leaving the column blank.

    `edge` carries the tip's stated edge through to `pricing.buy_from`, which
    adds ~3% for anything above +3.5%. That was a rule applied by hand on every
    bet; the printed number now already includes it.
    """
    parts = buy_parts(market, mu, p, edge, lg)
    if parts is None:
        return "buy>=  — "
    be, mg = parts
    return (f"buy>={be:.2f} ({mg * 100:+.1f}% margin)"
            .replace("(-", "(−"))


def main() -> None:
    args = sys.argv[1:]
    rows = []
    if args[:1] == ["--file"]:
        for ln in Path(args[1]).read_text().splitlines():
            if ln.strip() and not ln.startswith("#"):
                rows.append(ln.split("\t"))
    else:
        rows = [args[i:i + 4] for i in range(0, len(args), 4)]

    for lg, h, a, d in rows:
        try:
            r = tips(lg, h, a, date.fromisoformat(d))
        except Exception as exc:
            print(f"{lg:8s} {h} v {a}   ERROR {exc}")
            continue
        if r is None:
            print(f"{lg:8s} {h} v {a}   NO TIP — insufficient history")
            continue
        m1, p1, e1 = r["t1"]
        line = (f"{lg:8s} {h[:22]:22.22s} v {a[:20]:20.20s} "
                f"mu {r['mu']:4.2f}/{r['lmu']:4.2f}  "
                f"TIP1 {m1:6s} {p1:5.1%} {e1:+6.2%} {_buy(m1, r['mu'], p1, e1, r.get('lg'))}")
        if r["t2"]:
            m2, p2, e2, why = r["t2"]
            line += (f"   TIP2 {m2:6s} {p2:5.1%} {e2:+6.2%} "
                     f"{_buy(m2, r['mu'], p2, e2, r.get('lg'))} ({why})")
        else:
            line += "   TIP2 — none"
        print(line)


if __name__ == "__main__":
    main()
