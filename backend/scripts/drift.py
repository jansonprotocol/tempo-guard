"""Watch the rules that cannot move, and say when one has gone stale.

    python scripts/drift.py            measure, write config/drift.tsv
    python scripts/drift.py --dry      measure, print, write nothing

WHY THIS EXISTS (the bettor, 14 Sep). The two-day refresh re-measures
everything that is ALLOWED to move: the live-safety bands, the flip
rows, the strike combos. Everything else — the tier's cuts, the colour
ladder, the label rates, the band bars, the strong-score bar — is
hardcoded, so it can only be wrong until a person looks. Two of them
were, and both were found by reading rather than by measuring:

    the tier's Over clause   declined the BETTER of two groups for
                             weeks (overs claiming 76-80 land 79.5%
                             on the bank, unders 77.7%)
    STRONG at 0.71           landed 75.4% for -1.9% against normal's
                             76.2% and +1.5%

This reports on them every two days. It CHANGES NOTHING — that is the
point. A rule the record is judged against must not move on its own, or
the record has no fixed standard and an improvement cannot be told from
a ruler that shifted. So each check prints what it measures, what the
code says, and a flag when the two have parted; moving a constant stays
a decision someone makes, with the number in front of them.

Five checks:

    label      each label's measured rate against its registered SAYS
    side       overs against unders inside each claim band — this is
               the check that would have caught the Over clause
    ladder     each colour band against the band below it
    bar        each claim band's ROI at its bar, and where the best bar
               sits now
    strong     the star's cards against the rest, at the current bar
    cell       each league's overs and unders against what they claimed,
               at a threshold set wide because the gap does not persist

Reads the bank only (config/matchbank_retro.json, red cards kept in so a
declined group can still be seen). Writes config/drift.tsv, committed by
the bank-refresh workflow so the history of a drift is on the record.
"""
from __future__ import annotations

import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "drift.tsv"

SAYS_AT = 2.0        # points between a label's rate and its SAYS before it flags
SIDE_AT = 2.0        # points between overs and unders in a claim band
BAR_AT = 0.02        # how far the best bar may sit from the set one
MIN_N = 200          # a check needs this many cards to speak

# The league x side cell. Its threshold is DELIBERATELY far out, and the
# reason is measured (15 Sep): a cell's gap against its claim does not
# carry from one period to the next. Split the bank in half by date and
# correlate each cell's gap across the two halves and r = +0.20, on a
# spread of about 3 points either side — Norwegian overs ran -8.0 then
# +0.8, Swiss unders -1.6 then -10.4. A flag at 3 or 4 points would fire
# on a dozen cells a fortnight and every one of them would be noise. So
# the check reports the wide ones and only FLAGS a cell that is further
# out than the swing can explain.
CELL_AT = 8.0        # points of gap before a market cell flags
CELL_SHOW = 4.0      # points of gap before it is worth printing at all
CELL_MIN_N = 150     # kept cards in the cell before it may speak


def _side(cell: str | None) -> str:
    m = re.search(r"(?:^|[^A-Za-z])([OU])\d", (cell or "").replace("*", ""))
    return m.group(1) if m else ""


def _cards() -> list[dict]:
    """Every graded bank card, with what the rules read off it."""
    from scripts import bankrates as br, guard_slices as GS
    out = []
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            cell = m.get("t3") if m.get("pk") == 3 else m.get("tip")
            got = br._hit(m.get("m3") if m.get("pk") == 3 else m.get("mark"))
            c, e = br._claim(cell), br._edge(cell)
            if got is None or c is None:
                continue
            dnb = m.get("pk") == 3 and "DNB" in (m.get("t3") or "")
            tier = GS.tier_of(c, e, _side(cell), dnb)
            out.append(dict(code=code, claim=c, edge=e, side=_side(cell),
                            hit=bool(got), score=m.get("cs"), dnb=dnb,
                            label=GS.label(code, tier, m.get("cs"), dnb, c),
                            v=m.get("v"), bp=m.get("bp"), st=m.get("st")))
    return out


def _rate(g: list[dict]) -> tuple[int, float | None]:
    return len(g), (sum(x["hit"] for x in g) / len(g) * 100 if g else None)


def _roi(g: list[dict]) -> float | None:
    """Return per unit staked. A push counts as a win here, the same
    convention the sweep that SET the bars used, so the two numbers stay
    comparable; it flatters every bar equally."""
    if not g:
        return None
    return sum((x["bp"] - 1) if x["hit"] else -1 for x in g) / len(g) * 100


def check_labels(cards) -> list[dict]:
    """Each label's measured rate against the SAYS the hover prints."""
    from scripts import webapp
    rows = []
    for lab, says in sorted(webapp.SAYS.items(), key=lambda x: -x[1]):
        n, hit = _rate([c for c in cards if c["label"] == lab])
        if not n:
            continue
        d = None if hit is None else hit - says * 100
        rows.append(dict(check="label", subject=lab, n=n, measured=hit,
                         code_says=says * 100, delta=d,
                         flag=int(n >= MIN_N and d is not None and abs(d) > SAYS_AT)))
    return rows


def check_sides(cards) -> list[dict]:
    """Overs against unders inside each claim band. The tier is meant to
    be side-blind below its green rule; where a side lands better and the
    rules treat it worse, a clause is pointing the wrong way."""
    rows = []
    for lo, hi in ((70, 76), (76, 80), (80, 85), (85, 90), (90, 101)):
        g = [c for c in cards if lo <= c["claim"] < hi]
        no, ho = _rate([c for c in g if c["side"] == "O"])
        nu, hu = _rate([c for c in g if c["side"] == "U"])
        if not no or not nu:
            continue
        d = ho - hu
        rows.append(dict(check="side", subject=f"claim {lo}-{hi}: over vs under",
                         n=no + nu, measured=ho, code_says=hu, delta=d,
                         flag=int(min(no, nu) >= MIN_N and abs(d) > SIDE_AT)))
    return rows


def check_ladder(cards) -> list[dict]:
    """Each colour against the band below it: the ladder is only a ladder
    while every rung lands above the next one down."""
    from scripts import webapp
    order = ["green+", "green", "orange", "pink"]
    rows = []
    for a, b in zip(order, order[1:]):
        na, ha = _rate([c for c in cards if c["label"] == a])
        nb, hb = _rate([c for c in cards if c["label"] == b])
        if not na or not nb:
            continue
        rows.append(dict(check="ladder", subject=f"{a} over {b}", n=na + nb,
                         measured=ha, code_says=hb, delta=ha - hb,
                         flag=int(min(na, nb) >= MIN_N and ha <= hb)))
    return rows


def check_bars(cards) -> list[dict]:
    """Each claim band's ROI at the bar the code sets, and the best bar in
    a cent-by-cent sweep. A bar far from the best point is not wrong — the
    sweep is noisy and the bar is deliberately conservative — but it is
    worth seeing when it has drifted."""
    from scripts import webapp
    rows = []
    priced = [c for c in cards if c["bp"]]
    for band, bar in webapp.BAND_BAR.items():
        lo = 85.0 if band == "85+" else 80.0 if band == "80–85" else 75.0
        hi = 101.0 if band == "85+" else 85.0 if band == "80–85" else 80.0
        g = [c for c in priced if lo <= c["claim"] < hi]
        at = [c for c in g if c["bp"] >= bar]
        best, best_roi = bar, _roi(at)
        for cent in range(105, 146):
            t = cent / 100
            sub = [c for c in g if c["bp"] >= t]
            r = _roi(sub)
            if len(sub) >= 80 and r is not None and (best_roi is None or r > best_roi):
                best, best_roi = t, r
        rows.append(dict(check="bar", subject=f"{band} at {bar:.2f}", n=len(at),
                         measured=_roi(at), code_says=best_roi,
                         delta=best - bar,
                         flag=int(len(g) >= MIN_N and abs(best - bar) > BAR_AT)))
    return rows


def check_strong(cards) -> list[dict]:
    """The star against the rest, on the plays the bar fires on."""
    from scripts import webapp
    plays = [c for c in cards if c["v"] in ("normal", "strong") and c["bp"]]
    s = [c for c in plays if c["score"] is not None and c["score"] >= webapp.STRONG_SCORE]
    o = [c for c in plays if c not in s]
    ns, hs = _rate(s)
    no, ho = _rate(o)
    if not ns or not no:
        return []
    return [dict(check="strong", subject=f"star at {webapp.STRONG_SCORE:+.2f} vs the rest",
                 n=ns, measured=hs, code_says=ho, delta=hs - ho,
                 flag=int(ns >= 50 and hs <= ho)),
            dict(check="strong", subject="star ROI vs the rest", n=ns,
                 measured=_roi(s), code_says=_roi(o),
                 delta=(_roi(s) or 0) - (_roi(o) or 0),
                 flag=int(ns >= 50 and (_roi(s) or 0) <= (_roi(o) or 0)))]


def check_cells(cards) -> list[dict]:
    """Each league's market against what its cards claimed.

    Reads scripts/cellrates, NOT the `cards` list, on purpose: that is the
    same table the board prints on the card, so a number the bettor reads
    while buying and a number this report flags can never disagree.

    What this check is FOR is narrow. It is not a decline rule and it must
    not become one — see CELL_AT above for why, and scripts/cellrates for
    the whole measurement. It is here to catch a cell that has gone
    somewhere the ordinary swing cannot reach: a league that changed its
    scoring, a feed that started grading a rung wrong, a market the engine
    has quietly stopped modelling. Everything narrower than that is noise
    wearing a number.
    """
    from scripts import cellrates
    rows = []
    for key, r in cellrates.table().items():
        if len(key) != 2 or r["n"] < CELL_MIN_N:
            continue                      # the whole market, not a band
        code, side = key
        gap = r["hit"] - r["says"]
        if abs(gap) < CELL_SHOW:
            continue
        word = "overs" if side == "O" else "unders"
        rows.append(dict(check="cell", subject=f"{code} {word}", n=r["n"],
                         measured=r["hit"], code_says=r["says"], delta=gap,
                         flag=int(abs(gap) > CELL_AT)))
    return sorted(rows, key=lambda r: r["delta"])


def measure() -> list[dict]:
    cards = _cards()
    return (check_labels(cards) + check_sides(cards) + check_ladder(cards)
            + check_bars(cards) + check_strong(cards) + check_cells(cards))


def write(rows: list[dict], today) -> None:
    f = lambda v: "" if v is None else f"{v:.2f}"
    lines = [
        "# What the frozen rules measure today, against what the code says.",
        "# Written by scripts/drift.py in the two-day refresh. REPORT ONLY:",
        "# nothing here changes a rule — a rule the record is judged against",
        "# must not move on its own, or an improvement cannot be told from a",
        "# ruler that shifted. A flag is an invitation to look, not a verdict.",
        f"# thresholds: label {SAYS_AT:.0f} points, side {SIDE_AT:.0f} points, "
        f"bar {BAR_AT:.2f}, floor {MIN_N} cards",
        f"# written\t{today}",
        "# check\tsubject\tn\tmeasured\tcode_says\tdelta\tflag",
    ]
    for r in rows:
        lines.append("\t".join([r["check"], r["subject"], str(r["n"]),
                                f(r["measured"]), f(r["code_says"]),
                                f(r["delta"]), str(r["flag"])]))
    OUT.write_text("\n".join(lines) + "\n")


def main() -> None:
    import datetime as dt
    rows = measure()
    print("drift — the frozen rules against the bank:")
    last = None
    for r in rows:
        if r["check"] != last:
            print(f"  [{r['check']}]")
            last = r["check"]
        m = "  —  " if r["measured"] is None else f"{r['measured']:6.2f}"
        c = "  —  " if r["code_says"] is None else f"{r['code_says']:6.2f}"
        d = "  —  " if r["delta"] is None else f"{r['delta']:+6.2f}"
        print(f"    {r['subject']:38} n={r['n']:6}  {m}  vs {c}  {d}"
              f"{'   <== FLAG' if r['flag'] else ''}")
    n = sum(r["flag"] for r in rows)
    print(f"  {n} flag{'s' if n != 1 else ''}")
    if "--dry" not in sys.argv:
        write(rows, dt.date.today().isoformat())
        print(f"written: {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
