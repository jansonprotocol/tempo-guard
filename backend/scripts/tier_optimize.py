"""
The tier optimizer: re-measures the floors and the near-tier "from" bands.

THE BETTOR, 4 Oct, on the near groups that held up on their own (all
seven rates 0-3% under the bar but under 1.17: 93.3% on 15; 5%+ under the
bar but under 1.20: 84.5% and 83.4% on 168 and 163): "make those the real
strong from and strong watch from cards, with an optimizer inside the
workflow, that whenever those drop under 81%, or if another becomes
better ... Medium has not enough to prove now, so leave those as is, but
whenever a better analysis happens let it change them. Make sure
everything stays in sync."

Run every two days by bank-refresh, before the render, so the board, the
search words, the band-check table and Learn all read the same file.

THE POPULATION is the one every band table uses: this session's settled,
counted cards (not red, not declined), each at its FIRST-SIGHT best price
against its stamped play bar (config/forward_log.tsv).

THE FLOORS. Each tier's floor is its break-even, 1 / hit, over the cards
its rules select BEFORE any floor is applied — so the floor never decides
its own sample:
    STRONG        all seven rates at 80%+, no more than 5% under the bar
    MEDIUM        fewer than seven, at or over the bar
    STRONG WATCH  5% or more under the bar
A floor moves only on FLOOR_MIN_N settled cards or more (MEDIUM, at 40,
is held) and only when the new break-even is FLOOR_STEP or more away
from the old one, so it does not twitch a cent with every result.

THE MEDIUM BANDS (4 Oct). Each gap band of the MEDIUM tier gets a state:
good (80%+ and a gain on 5+: a mark on the pill), skip (a loss under 70%:
advised SKIP, still counted), hard (the same on 30+: declined from that
day on) or none. Measured with the hard rule off, so a declined band can
still recover.

THE FROM BANDS. Under the (new) floors, the near groups are the cards
that met a tier's rules but not its price; each gap band of each group
is ON while it lands FROM_HIT or more on FROM_MIN_N or more, and OFF
otherwise. A band that later earns it comes on by itself.

Writes config/tier_params.tsv (what the board reads) and appends every
change to config/tier_params_log.tsv. Moves nothing in the engine.
"""
from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scripts import board, forward_settle as fs, webapp as w  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
PARAMS = ROOT / "config" / "tier_params.tsv"
LOG = ROOT / "config" / "tier_params_log.tsv"

FLOOR_MIN_N = 100
FLOOR_STEP = 0.02
FROM_HIT = 81.0
FROM_MIN_N = 15
# MEDIUM by gap band (the bettor, 4 Oct): a mark where it has paid, SKIP
# advice where it has lost, a hard decline once the loss holds on HARD_N.
MED_GOOD_HIT, MED_GOOD_N = 80.0, 5
MED_BAD_HIT, MED_HARD_N = 70.0, 30


def cards() -> list[dict]:
    fx = board.load()
    byday = {(f.kickoff.split(" ")[0], f.teams): f for f in fx}
    final = {}
    for f in fx:
        if f.settled and "—" in f.status:
            sc = f.status.split("—")[-1].strip().split(" ")[0]
            if "-" in sc:
                try:
                    h, a = sc.split("-")
                    final[(f.kickoff.split(" ")[0], f.teams)] = (int(h), int(a))
                except ValueError:
                    pass
    first: dict = {}
    for ln in w.FORWARD.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 13 or p[1] < w.SESSION_DATE or fs._artefact(p[5]):
            continue
        first.setdefault((p[1], p[3]), p)
    out = []
    for key, p in first.items():
        if key not in final or key not in byday:
            continue
        f = byday[key]
        if p[7].endswith("red") or w.is_declined(f):
            continue
        try:
            need, best = float(p[9]), float(p[11] or p[10])
        except ValueError:
            continue
        got = fs._settle(p[5], *final[key])
        if got is None or need <= 0:
            continue
        gap = (best / need - 1) * 100
        out.append(dict(key=key, best=best, gap=gap, band=w.gap_band(gap),
                        seven=w.all_seven(f, p[7]), hit=bool(got[1]),
                        pl=got[0] * (best - 1) if got[0] > 0 else got[0]))
    return out


def rate(xs: list[dict]) -> tuple[int, float | None]:
    return len(xs), (sum(x["hit"] for x in xs) / len(xs) * 100 if xs else None)


def main() -> None:
    # Measure with the MEDIUM hard rule off: a band it declines must stay
    # in the sample, or it could never show that it has recovered.
    w._MEDBAND_OFF = True
    cs = cards()
    under = w.STRONG_UNDER
    pops = {
        "strong": [c for c in cs if c["seven"] and c["gap"] >= -under],
        "medium": [c for c in cs if not c["seven"] and c["gap"] >= 0],
        "strong watch": [c for c in cs if c["gap"] <= -w.STRONG_WATCH_UNDER],
    }
    old_floor = dict(w.TIER_MIN)
    old_bands = {g: set(b) for g, b in w.FROM_BANDS.items()}
    floors, rows, changes = {}, [], []
    for t, pop in pops.items():
        n, hit = rate(pop)
        be = 100 / hit if hit else None
        cur = old_floor[t]
        if be is None:
            note = "no settled cards"
            new = cur
        elif n < FLOOR_MIN_N:
            note = f"held: {n} settled, needs {FLOOR_MIN_N}"
            new = cur
        elif abs(be - cur) < FLOOR_STEP:
            note = f"held: break-even {be:.3f} within {FLOOR_STEP} of {cur:.2f}"
            new = cur
        else:
            new = round(be, 2)
            note = f"moved from {cur:.2f}"
            changes.append(f"floor {t}: {cur:.2f} -> {new:.2f} ({hit:.1f}% on {n})")
        floors[t] = new
        rows.append(["floor", t, f"{new:.2f}", str(n),
                     f"{hit:.1f}" if hit is not None else "",
                     f"{be:.3f}" if be else "", note])
    near = {
        "strong": [c for c in pops["strong"] if c["best"] < floors["strong"]],
        "strong watch": [c for c in pops["strong watch"]
                         if c["best"] < floors["strong watch"]],
    }
    for g, pop in near.items():
        for _lo, _hi, band in w.GAP_BANDS:
            xs = [c for c in pop if c["band"] == band]
            if not xs:
                continue
            n, hit = rate(xs)
            on = n >= FROM_MIN_N and hit >= FROM_HIT
            was = band in old_bands.get(g, set())
            if on != was:
                changes.append(f"from {g} {band}: {'on' if on else 'off'} "
                               f"({hit:.1f}% on {n})")
            rows.append(["from", g, band, "on" if on else "off", str(n),
                         f"{hit:.1f}", f"near {g}: rules met, under the "
                         f"{floors[g]:.2f} floor"])
    today = dt.date.today().isoformat()
    meds = [c for c in pops["medium"] if c["best"] >= floors["medium"]]
    for _lo, _hi, band in w.GAP_BANDS:
        xs = [c for c in meds if c["band"] == band]
        if not xs:
            continue
        n, hit = rate(xs)
        roi = sum(c["pl"] for c in xs) / n * 100
        if n >= MED_GOOD_N and hit >= MED_GOOD_HIT and roi > 0:
            state = "good"
        elif roi < 0 and hit < MED_BAD_HIT:
            state = "hard" if n >= MED_HARD_N else "skip"
        else:
            state = "none"
        prev = w.MED_BANDS.get(band)
        was = prev[0] if prev else "none"
        since = (prev[4] if prev and was == "hard" and prev[4] else today) \
            if state == "hard" else ""
        if state != was:
            changes.append(f"medium {band}: {was} -> {state} "
                           f"({hit:.1f}% on {n}, {roi:+.1f}%)")
        rows.append(["medband", band, state, str(n), f"{hit:.1f}",
                     f"{roi:+.1f}", since])
    stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%d %H:%M")
    head = ("# Tier floors and near-tier FROM bands, set by scripts/tier_optimize.py\n"
            f"# (two-day bank refresh). Last run {stamp} UTC on {len(cs)} settled\n"
            "# counted cards at first-sight prices. Floors move on "
            f"{FLOOR_MIN_N}+ cards and a {FLOOR_STEP:.2f}+ step;\n"
            f"# a FROM band is on at {FROM_HIT:.0f}%+ on {FROM_MIN_N}+ cards. "
            "Read by webapp at import.\n"
            "# MEDIUM bands: good at 80%+ with a gain on 5+; skip on a loss under 70%;\n"
            f"# hard (declined from the date in the last column) on {MED_HARD_N}+.\n"
            "# kind\ttier\tvalue/band\tn|state\thit|n\tbreak-even|hit\tnote\n"
            "# medband\tband\tstate\tn\thit\troi\thard since\n")
    PARAMS.write_text(head + "".join("\t".join(r) + "\n" for r in rows))
    if changes:
        new_log = not LOG.exists()
        with LOG.open("a") as fh:
            if new_log:
                fh.write("# Every change tier_optimize.py made: when, what, on what.\n")
            for c in changes:
                fh.write(f"{stamp}\t{c}\n")
    for r in rows:
        print("\t".join(r))
    print("changes:", "; ".join(changes) if changes else "none")


if __name__ == "__main__":
    main()
