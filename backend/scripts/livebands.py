"""Measure the live-safety bands on the board's own record and label them.

    python scripts/livebands.py            measure, write config/live_bands.tsv
    python scripts/livebands.py --dry      measure, print, write nothing
    python scripts/livebands.py --days 21  the window (default 21)

THE RULE (the bettor, 12 Sep). A card the board did not stake — an Athena
lane or a watch card — carries a live-safety tag by its lane and its
printed-edge band. The bands were first set by hand off the session's
tally; from here they are MEASURED every two days on the board's settled
cards, red cards out, and labelled by hit rate:

    at or above 79%   safe
    77% to 79%        cautious
    under 77%         unsafe

A band with fewer than MIN_N graded cards in the window keeps the SEED
label (webapp.LIVE_TAG, the bettor's hand table) — a label read off a
dozen cards would flip every fortnight on noise, which is the opposite of
a signal. The window is rolling so the labels follow the regime the
bettor is actually playing in, not the bank: the bank says the deep-
negative Athena band is its best slice (+4.6 on 3,539 cards) and the
first week of September ran it at 68.8%, and it is the second number
the tag exists for.

Nothing here touches the engine, the guard's slices or the labels. It
decides which unstaked cards are OFFERED for a live buy and where they
file (Declined takes the unsafe ones) — and since 12 Sep a Declined card
is out of the record too (the bettor: "everything that now is a declined
card must be removed from what now actual hitrates are"), so the tag
reaches every hit rate on the board and in the bank through the two
predicates webapp.counts and bankrates.counts.

The measurement itself keeps only the RED cards out (webapp.not_red): a
band measured on a record that already dropped its own unsafe cards
could never come back, and a label that cannot move is not a label.

Writes config/live_bands.tsv, read by webapp.live_tag — a typed table
like the others, so the page and the README render from the same rule.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "live_bands.tsv"

SAFE_AT = 79.0          # hit rate at or above which a band is "safe"
CAUTIOUS_AT = 77.0      # below this it is "unsafe"; between, "cautious" (the bettor, 12 Sep: 77)
MIN_N = 15              # fewer graded cards than this: keep the seed label (the bettor, 12 Sep: 15)
DAYS = 21               # rolling window, in days, of settled kickoffs
BANDS = ("+1 up", "−1..+1", "−4..−1", "−4 down")
LANES = ("athena", "watch", "priced")

# THE SEED (the bettor's hand bands, 12 Sep). Whether a card the board did
# not stake — an Athena lane, or a watch card — is one to buy into in
# play, read off its printed EDGE and its lane. Set from the session's
# negative-edge tally: Athena cards at −4 or worse were 22 of 32 in the
# first week and the only slice that broke; watch cards above +1 were the
# weakest watch slice at 76.7% on thirty. The priced lane — the board's
# own PLAYs (the bettor, 12 Sep: "this one doesn't have a live tag") — is
# seeded from the bank's priced plays by edge band, 76.9% above +1, 73.0%
# around zero, 66.0% and 73.3% in the negative bands, all under claim.
# INFORMATION ONLY on that lane: a priced play is the board's stake and
# files under Playable or Running whatever its tag says, and it stays in
# the record.
SEED = {
    "athena": {"+1 up": "safe", "−1..+1": "safe", "−4..−1": "safe",
               "−4 down": "unsafe"},
    "watch":  {"+1 up": "unsafe", "−1..+1": "safe", "−4..−1": "cautious",
               "−4 down": "cautious"},
    "priced": {"+1 up": "cautious", "−1..+1": "unsafe", "−4..−1": "unsafe",
               "−4 down": "unsafe"},
}

# THE FLIP (the bettor, 12 Sep, off the filter "priced play, live
# unsafe": tip 1 landed 4 of 7 while tip 2 and tip 3 landed 2 of 2 — "if
# that keeps up, when priced play and live unsafe, flip to tip 2 or 3";
# then, seeing tip 3 ahead on the safe and cautious filters too: "worth
# flipping more combos?"). A priced play that also prints a tip 2 or a
# tip 3 is asked, per TAG BAND and per LANE, whether that lane beats tip
# 1 on the same cards. Measured on the board window, paired (only cards
# where both lanes graded); a combo is flipped when the lane beats tip 1
# by FLIP_AT points on at least MIN_N cards. Under MIN_N the bank seeds
# it, paired the same way over its priced plays:
#
#     band      lane    n     lane   tip 1   diff
#     unsafe    tip 3   92    81.5   69.6   +12.0   flipped
#     unsafe    tip 2   92    76.1   72.8    +3.3   hold
#     cautious  tip 3  493    77.9   76.3    +1.6   hold
#     cautious  tip 2  836    61.2   76.3   −15.1   hold
#     safe      tip 3  137    77.4   74.5    +2.9   hold
#     safe      tip 2  126    73.0   73.0    +0.0   hold
#
# So only one combo starts flipped; the session (tip 3 ahead of tip 1 on
# every band, on 14 to 46 paired cards) can flip the others itself at 15
# cards. Display only: the pill reads "live unsafe · flipped → tip 3",
# the card stays a priced play and stays in the record.
FLIP_AT = 5.0
FLIP_SEED = {("unsafe", "tip3"): "flipped", ("unsafe", "tip2"): "hold",
             ("cautious", "tip3"): "hold", ("cautious", "tip2"): "hold",
             ("safe", "tip3"): "hold", ("safe", "tip2"): "hold"}
# The bank's paired numbers behind each seed, for the pill's hover:
# (lane hit, tip 1 hit, n).
FLIP_BANK = {("unsafe", "tip3"): (81.5, 69.6, 92), ("unsafe", "tip2"): (76.1, 72.8, 92),
             ("cautious", "tip3"): (77.9, 76.3, 493), ("cautious", "tip2"): (61.2, 76.3, 836),
             ("safe", "tip3"): (77.4, 74.5, 137), ("safe", "tip2"): (73.0, 73.0, 126)}

_BANDS: dict | None = None


def edge_band(edge: float) -> str:
    return ("+1 up" if edge >= 1 else "−1..+1" if edge > -1
            else "−4..−1" if edge > -4 else "−4 down")


def bands() -> dict[tuple[str, str], dict]:
    """The MEASURED band table (config/live_bands.tsv), falling back to
    the seed where the file is missing or a band is absent. Read once."""
    global _BANDS
    if _BANDS is None:
        got = read() or {}
        _BANDS = {}
        for lane, seeds in SEED.items():
            for band, seed in seeds.items():
                _BANDS[(lane, band)] = got.get((lane, band)) or dict(
                    n=0, hit=None, said=None, label=seed, source="seed")
        for (band, tipn), seed in FLIP_SEED.items():
            key = ("flip", f"{band} {tipn}")
            _BANDS[key] = got.get(key) or dict(
                n=0, hit=None, said=None, label=seed, source="seed")
    return _BANDS


def flip(lane: str | None, tag: str | None, has_tip2: bool, has_tip3: bool) -> str | None:
    """Which lane a priced play flips to — "tip 3" or "tip 2" — or None:
    not a priced play, no tag, no such lane on the card, or the flip for
    that band and lane is not on. Tip 3 first, where both print."""
    if lane != "priced" or tag not in ("safe", "cautious", "unsafe"):
        return None
    for tipn, has in (("tip3", has_tip3), ("tip2", has_tip2)):
        if has and bands()[("flip", f"{tag} {tipn}")]["label"] == "flipped":
            return tipn[:3] + " " + tipn[3]
    return None


def flip_label(tip_hit: float | None, tip1_hit: float | None, n: int, seed: str) -> tuple[str, str]:
    """(label, source): measured when n is enough, else the seed."""
    if tip_hit is None or tip1_hit is None or n < MIN_N:
        return seed, "seed"
    return ("flipped" if tip_hit - tip1_hit >= FLIP_AT else "hold"), "measured"


def flip_study(days: int = DAYS, today: dt.date | None = None) -> list[dict]:
    """Six rows, lane "flip", band "<tag> tip2" / "<tag> tip3": n paired
    cards in the window (priced plays with that tag where tip 1 and that
    lane both graded), hit = that lane's hit rate, said = TIP 1's hit rate
    on the same cards (the column is reused; the header says so), label,
    source."""
    from scripts import board, webapp
    today = today or dt.date.today()
    since = (today - dt.timedelta(days=days)).isoformat()
    pair = {k: [0, 0, 0] for k in FLIP_SEED}          # n, lane hits, tip 1 hits
    for f in board.load():
        if not f.settled or f.kickoff[:10] < since:
            continue
        if webapp.record_lane(f) != "priced":
            continue
        tag = webapp.record_tag(f)
        m1 = f.status[:1]
        if tag not in ("safe", "cautious", "unsafe") or m1 not in ("✅", "❌", "◦"):
            continue
        for tipn, cell in (("tip2", f.tip2), ("tip3", f.tip3)):
            mk = cell.lstrip().replace("*", "")[:1]
            if mk not in ("✅", "❌", "◦"):
                continue
            t = pair[(tag, tipn)]
            t[0] += 1
            t[1] += mk != "❌"
            t[2] += m1 != "❌"
    rows = []
    for (band, tipn), seed in FLIP_SEED.items():
        n, h, h1 = pair[(band, tipn)]
        hit = (h / n * 100) if n else None
        hit1 = (h1 / n * 100) if n else None
        lab, src = flip_label(hit, hit1, n, seed)
        rows.append(dict(lane="flip", band=f"{band} {tipn}", n=n, hit=hit, said=hit1,
                         label=lab, source=src))
    return rows


def tag(lane: str | None, edge: float | None) -> str | None:
    """'safe', 'cautious' or 'unsafe' by lane and printed edge; None for
    no lane (a red card, an abstention) or no printed edge."""
    if lane not in SEED or edge is None:
        return None
    return bands()[(lane, edge_band(edge))]["label"]


def label(hit_pct: float | None, n: int, seed: str) -> tuple[str, str]:
    """(label, source): measured when n is enough, else the seed."""
    if hit_pct is None or n < MIN_N:
        return seed, "seed"
    if hit_pct >= SAFE_AT:
        return "safe", "measured"
    if hit_pct >= CAUTIOUS_AT:
        return "cautious", "measured"
    return "unsafe", "measured"


def measure(days: int = DAYS, today: dt.date | None = None) -> list[dict]:
    """One row per lane and band: n, hit, said, label, source."""
    from scripts import board, webapp
    today = today or dt.date.today()
    since = (today - dt.timedelta(days=days)).isoformat()
    tally: dict[tuple[str, str], list] = {(l, b): [0, 0, 0.0] for l in LANES for b in BANDS}
    for f in board.load():
        if not f.settled or f.kickoff[:10] < since or not f.tip1 or f.tip1.startswith("—"):
            continue
        if not webapp.not_red(f):        # red out; unsafe IN, or a band could never recover
            continue
        lane = webapp.record_lane(f)
        if lane is None:
            continue
        cell = f.tip3 if webapp._star_any(f) == 3 else f.tip1
        e, c = webapp._edge(cell), webapp._claim(cell)
        if e is None or c is None:
            continue
        t = tally[(lane, webapp.edge_band(e))]
        t[0] += 1
        t[1] += f.status.startswith("✅")
        t[2] += c
    rows = []
    for lane in LANES:
        for band in BANDS:
            n, h, c = tally[(lane, band)]
            hit = (h / n * 100) if n else None
            lab, src = label(hit, n, SEED[lane][band])
            rows.append(dict(lane=lane, band=band, n=n, hit=hit,
                             said=(c / n) if n else None, label=lab, source=src))
    return rows


def write(rows: list[dict], days: int, today: dt.date) -> None:
    lines = [
        "# The live-safety bands, MEASURED on the board's settled cards, red",
        "# cards out, over a rolling window; written by scripts/livebands.py",
        "# every two days (the bank-refresh workflow) and read by",
        "# scripts/webapp.py for the live tag on every unstaked card. A band",
        f"# under {MIN_N} cards keeps the seed label (the bettor's hand table,",
        f"# 12 Sep). Thresholds: safe >= {SAFE_AT:.0f}, cautious >= {CAUTIOUS_AT:.0f}, else unsafe.",
        f"# window\t{days} days to {today.isoformat()}",
        "# The 'flip' rows: priced plays with that tag that also print that lane,",
        "# paired — n cards where both graded, hit = THAT lane, said = tip 1 on",
        f"# the same cards; flipped when the lane beats tip 1 by {FLIP_AT:.0f} points.",
        "# lane\tband\tn\thit\tsaid\tlabel\tsource",
    ]
    for r in rows:
        lines.append("\t".join([
            r["lane"], r["band"], str(r["n"]),
            "" if r["hit"] is None else f"{r['hit']:.1f}",
            "" if r["said"] is None else f"{r['said']:.1f}",
            r["label"], r["source"]]))
    OUT.write_text("\n".join(lines) + "\n")


def read() -> dict[tuple[str, str], dict] | None:
    """The written table, or None when it does not exist yet."""
    if not OUT.exists():
        return None
    out = {}
    for ln in OUT.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 7:
            continue
        out[(p[0], p[1])] = dict(n=int(p[2]), hit=(float(p[3]) if p[3] else None),
                                 said=(float(p[4]) if p[4] else None),
                                 label=p[5], source=p[6])
    return out


def main() -> None:
    dry = "--dry" in sys.argv
    days = DAYS
    if "--days" in sys.argv:
        days = int(sys.argv[sys.argv.index("--days") + 1])
    today = dt.date.today()
    rows = measure(days, today)
    print(f"live bands, last {days} days to {today}, red cards out:")
    for r in rows:
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        print(f"  {r['lane']:6} {r['band']:8} n={r['n']:3}  hit {hit}  -> {r['label']:8} ({r['source']})")
    flips = flip_study(days, today)
    print("the flip on priced plays, per tag band, paired with tip 1:")
    for r in flips:
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        t1 = "   —  " if r["said"] is None else f"{r['said']:5.1f}%"
        print(f"  {r['band']:14} n={r['n']:3}  lane {hit}  tip 1 {t1}  -> {r['label']:8} ({r['source']})")
    rows += flips
    if not dry:
        write(rows, days, today)
        print(f"written: {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
