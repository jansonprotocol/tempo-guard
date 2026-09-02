"""Write the as-of slice table the card's guard label is computed from.

The confluence score needs four counters per card: the league's own
record on the starred lane, each club's, the side's, and each club
crossed with the side. Replaying that at render time would cost a full
pass per board, so it is derived ONCE here and read as a table.

Strictly derived, never typed. The numbers come from replaying the
league's stored fixtures as-of, exactly as scripts/confluence.py does,
and the file is rewritten whenever the bank moves.

The table is cumulative-to-today rather than as-of-per-card, which is the
correct thing for a FORWARD card: today's fixture should see everything
that has already happened. The as-of discipline in confluence.py exists
to grade the rule honestly on history; here there is no history to
protect, only a card that has not kicked off.

    python scripts/guard_slices.py --write          all leagues
    python scripts/guard_slices.py --write --out part.tsv
    python scripts/guard_slices.py --leagues E0,I1

Read by scripts/webapp.py to print a label per card. The label rules and
their frozen thresholds live in docs/confluence-guard.md; the constants
below must stay in step with that document, which is the registered spec.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import store
from scripts.confluence import (CLUB_WINDOW, KPRIOR, MIN_LEAGUE, MIN_SLICE,
                                Counter, Mem, _side, region)
from scripts.final_pick import chosen, grade
from scripts.two_tips import tips

OUT = Path(__file__).resolve().parents[2] / "config" / "guard_slices.tsv"

# MUST match the bank the frozen thresholds were calibrated on. The score
# is a sum of shrunk deviations, and shrinkage depends on how many cards
# each slice holds: a league or side row built from 300 fixtures is pulled
# harder toward the baseline than one built from 1,500, so the same card
# scores SMALLER on a shallower table. Built at 300 against thresholds
# fitted at 1,500, live scores spanned -5.0 to +3.8 against a super-green
# bar of +6.34 — no card could ever have earned a super label. Club rows
# are unaffected either way, since Mem caps them at CLUB_WINDOW.
CALIBRATION_N = 1500

# The frozen thresholds, set on the European population and registered in
# docs/confluence-guard.md. Changing either is a new experiment, not a
# tuning: the live period is being graded against the hit rates these
# produced.
SUPER_GREEN = 6.34
SUPER_RED = -12.99


def replay(league: str, n: int) -> dict:
    """One forward pass, keeping the counters rather than the scores."""
    df = store.load_results(league)
    if df is None or len(df) < 200:
        return {}
    lg, side = Counter(), Counter()
    team, ts = Mem(CLUB_WINDOW), Mem(CLUB_WINDOW)
    for _i, r in df.sort_values("date").tail(n).iterrows():
        d = r["date"].date() if hasattr(r["date"], "date") else r["date"]
        try:
            out = tips(league, str(r["home"]), str(r["away"]), d)
        except Exception:
            continue
        if out is None:
            continue
        t1mk, p1, _e1 = out["t1"]
        t3 = out["t3"]
        pick = chosen(dict(p1=p1, has_t3=t3 is not None,
                           p3=t3[1] if t3 else None,
                           t3_dnb=bool(t3) and t3[0].startswith("DNB")))
        mk = t3[0] if pick == 3 and t3 else t1mk
        hit = grade(pick, mk, int(r["hg"]), int(r["ag"]))
        if hit is None:
            continue
        h, a = str(r["home"]), str(r["away"])
        lg.add((league,), hit)
        team.add((league, h), hit, d)
        team.add((league, a), hit, d)
        s = _side(mk) if mk not in ("1X", "X2", "12", "DNB1", "DNB2") else mk
        if s:
            side.add((league, s), hit)
            ts.add((league, h, s), hit, d)
            ts.add((league, a, s), hit, d)
    return {"lg": lg, "team": team, "side": side, "ts": ts}


def rows_for(code: str, C: dict) -> list[str]:
    out = []
    h, n = C["lg"].d[(code,)]
    if n < MIN_LEAGUE:
        return out
    out.append(f"{code}\tLG\t\t\t{h}\t{n}")
    for (lc, t), q in C["team"].d.items():
        if len(q) >= MIN_SLICE:
            out.append(f"{lc}\tTEAM\t{t}\t\t{sum(x[1] for x in q)}\t{len(q)}")
    for (lc, s), (hh, nn) in C["side"].d.items():
        if nn >= MIN_SLICE:
            out.append(f"{lc}\tSIDE\t\t{s}\t{hh}\t{nn}")
    for (lc, t, s), q in C["ts"].d.items():
        if len(q) >= MIN_SLICE:
            out.append(f"{lc}\tTS\t{t}\t{s}\t{sum(x[1] for x in q)}\t{len(q)}")
    return out


def score(code: str, home: str, away: str, mk: str, says: float,
          tab: dict) -> float | None:
    """The card's confluence score, from the written table.

    Mirrors confluence.walk_best exactly: the claim and the club and side
    reads against the league baseline, and the crossing against the
    club's own rate so the same cards are not counted twice.
    """
    base = tab.get((code, "LG", "", ""))
    if not base or base[1] < MIN_LEAGUE:
        return None
    b = base[0] / base[1]
    tot = (says - b) * 100
    s = _side(mk) if mk not in ("1X", "X2", "12", "DNB1", "DNB2") else mk
    club = {}
    for t in (home, away):
        cell = tab.get((code, "TEAM", t, ""))
        if cell and cell[1] >= MIN_SLICE:
            sh = (cell[0] + b * KPRIOR) / (cell[1] + KPRIOR)
            club[t] = sh
            tot += (sh - b) * 100
    if s:
        cell = tab.get((code, "SIDE", "", s))
        if cell and cell[1] >= MIN_SLICE:
            sh = (cell[0] + b * KPRIOR) / (cell[1] + KPRIOR)
            tot += (sh - b) * 100
        for t in (home, away):
            cell = tab.get((code, "TS", t, s))
            if cell and cell[1] >= MIN_SLICE:
                sh = (cell[0] + b * KPRIOR) / (cell[1] + KPRIOR)
                tot += (sh - club.get(t, b)) * 100
    return tot


def tier_of(p: float | None, e: float | None, side: str, dnb: bool) -> str:
    """The card's tier from the card alone, in percentage points.

    ONE definition, read by the live card (webapp._label_of) and by the
    bank (matchbank), so a past card and a live card can never be
    tiered by two copies of the rule: a gated DNB or a high, modest-edge
    tip 1 is green; a low claim, or a middling claim on an OVER, is red;
    the rest orange.
    """
    if dnb or (p is not None and p >= 84 and (e is None or e < 1.0)):
        return "green"
    if p is None:
        return "orange"
    if p < 76 or (p < 80 and side == "O"):
        return "red"
    return "orange"


def label(code: str, tier: str, sc: float | None, is_dnb: bool) -> str:
    """The five labels. Outside Europe the score is silent — it measured
    -0.06 there — so no card gets a super-label off it."""
    if region(code) != "Europe":
        return tier
    if tier == "green":
        return "super green" if (is_dnb or (sc is not None and sc >= SUPER_GREEN)) \
            else "green"
    if tier == "red":
        return "super red" if (sc is not None and sc <= SUPER_RED) else "red"
    return tier


def read_table(path: Path = OUT) -> dict:
    """Load the written table into the shape score() wants."""
    tab = {}
    if not path.exists():
        return tab
    for ln in path.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) != 6:
            continue
        tab[(p[0], p[1], p[2], p[3])] = (int(p[4]), int(p[5]))
    return tab


def main() -> None:
    args = sys.argv[1:]
    n = int(args[args.index("--n") + 1]) if "--n" in args else CALIBRATION_N
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    lines = [
        "# As-of slice records behind the card's guard label. Written by",
        "# scripts/guard_slices.py --write — derived, never typed. Read by",
        "# scripts/webapp.py, which scores each card against these and",
        "# prints one of five labels. The rules and their FROZEN",
        "# thresholds are registered in docs/confluence-guard.md; this",
        "# file is only the evidence they are applied to.",
        "# Club rows keep the last "
        f"{CLUB_WINDOW} cards; league and side rows keep everything.",
        "# league\tkind\tteam\tside\thits\tn",
    ]
    for c in codes:
        try:
            C = replay(c, n)
        except Exception as exc:
            print(f"{c:9} FAILED {exc}", file=sys.stderr)
            continue
        if C:
            lines += rows_for(c, C)
    out = Path(args[args.index("--out") + 1]) if "--out" in args else OUT
    if "--write" in args:
        out.write_text("\n".join(lines) + "\n")
        print(f"{len(lines)-8} slice rows -> {out}")
    else:
        print(f"{len(lines)-8} slice rows (dry run; pass --write)")


if __name__ == "__main__":
    main()
