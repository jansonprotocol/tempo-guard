"""Measure the live-safety bands on the bank, per league, and label them.

    python scripts/livebands.py            measure, write config/live_bands.tsv
    python scripts/livebands.py --dry      measure, print, write nothing

THE RULE (the bettor, 12 Sep). A card the board did not stake — an Athena
lane or a watch card — carries a live-safety tag by its lane and its
printed-edge band; a priced play carries the same tag for information.
Labelled by hit rate:

    at or above 79%   safe
    77% to 79%        cautious
    under 77%         unsafe

WHERE THE BANDS ARE MEASURED (the bettor, 13 Sep: "build it and re-tag
the board"). They were first set by hand, then measured on the board's
last three weeks across all leagues. Tested out of sample that read was
pointing the wrong way: bands fitted on the bank before 1 August, then
judged on the 798 unstaked cards from August on, the session's global
"unsafe" cards landed 88.0% against 84.5% for its "safe" ones, while
per-league bank bands separated them 85.5 to 75.0 — and on the board's
own 305 settled unstaked cards, where the session bands were fitted IN
sample, the per-league bank bands still split 83.3 to 66.7 against 85.5
to 75.9. The deep-negative Athena band that the session called unsafe
on 50 cards lands 85 to 95 in seventeen leagues on the bank (Brazil
88.8 on 276, the Championship 91.0 on 222, LaLiga 2 89.7 on 389).

So from 13 Sep the tag reads, in this order:
    1. the LEAGUE's own bank row for that lane and band, when it has
       MIN_LEAGUE cards or more            (source "league")
    2. the whole bank's row for the lane and band, red out
                                            (source "global")
    3. the seed, the bettor's hand table    (source "seed")
The bank ingests the board's completed cards every two days, so the
session keeps feeding the measurement; it no longer overrides it.

Nothing here touches the engine, the guard's slices or the labels. It
decides which unstaked cards are OFFERED for a live buy and where they
file (Declined takes the unsafe ones), and since 12 Sep a Declined card
is out of the record too, so the tag reaches every hit rate on the board
and in the bank through webapp.counts and bankrates.counts. The
measurement keeps only the RED cards out: a band measured on a record
that already dropped its own unsafe cards could never come back.

The flip study (tip 2 / tip 3 against tip 1 on priced plays) stays on
the board's last three weeks, paired; see flip_study.

Writes config/live_bands.tsv, read by webapp.live_tag and
bankrates.tag — a typed table like the others, so the page, the README
and the bank render from the same rule.
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
MIN_N = 15              # fewer graded cards than this: fall through (the bettor, 12 Sep: 15)
MIN_LEAGUE = 50         # a league's own band needs this many bank cards to speak (13 Sep)
DAYS = 21               # rolling window, in days, for the flip study on the board
GLOBAL = "*"            # the league column of a whole-bank row
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
# From 12 to 21 Sep the tag was INFORMATION ONLY on that lane: a priced
# play was the board's stake and filed under Playable whatever its tag
# said. The 21 Sep profile study (scripts/profile_study.py) measured the
# cost — unsafe priced plays landed 73.1% on 193 against 83.4 for the
# rest of the playable board, −4.0% at closing, both halves under 75 —
# and the bettor moved them: an unsafe priced play files under Declined
# like any other unsafe card (webapp.is_declined, bankrates.counts).
# The MEASUREMENT here is unchanged and still keeps only red out, so a
# priced band that recovers comes back by itself.
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
# by FLIP_AT points on at least MIN_N cards. A PUSH ON THE FLIPPED LANE
# IS NO BET (the bettor, 12 Sep): a draw-no-bet draw returns the stake
# and earns nothing, so the card leaves the pair rather than counting as
# a hit the way the tiles score it — the flip claims the lane would
# have EARNED more than tip 1. Tip 1 keeps the board's convention (a
# printed push wins the softer rung actually played). Under MIN_N the
# bank seeds it, paired and scored the same way over its priced plays:
#
#     band      lane    n     lane   tip 1   diff
#     unsafe    tip 3   77    77.9   63.6   +14.3   flipped
#     unsafe    tip 2   92    76.1   72.8    +3.3   hold
#     cautious  tip 3  408    73.3   75.5    −2.2   hold
#     cautious  tip 2  836    61.2   76.3   −15.1   hold
#     safe      tip 3  121    74.4   76.9    −2.5   hold
#     safe      tip 2  126    73.0   73.0    +0.0   hold
#
# So only one combo starts flipped; the session (tip 3 ahead of tip 1 on
# every band) can flip the others itself at 15 cards. Display only: the
# pill reads "live unsafe · flipped → tip 3", the card stays a priced
# play and stays in the record.
FLIP_AT = 5.0
FLIP_MIN_BOARD = MIN_LEAGUE     # the board overrides the bank's flip only at this many paired cards
FLIP_KEYS = tuple((b, t) for b in ("unsafe", "cautious", "safe") for t in ("tip3", "tip2"))
# The last hand seed, kept only for a checkout with no bank at all.
FLIP_SEED = {k: ("flipped" if k == ("unsafe", "tip3") else "hold") for k in FLIP_KEYS}

# THE STRIKE COMBOS (the bettor, 13 Sep: "start declining these ... make
# a calibration updater in the 2-day auto updater to check if these
# bands still are valid; when a combo sits below 77% decline it"). A
# card's lane and its strikes (webapp.strikes: score under 0, priced
# play / watch card, over lane on a score under 0) make a combo; each
# combo is measured on the board's settled cards, red out, every two
# days, and a combo landing under DECLINE_AT on COMBO_MIN_N cards or more
# is DECLINED — the card files under Declined and leaves the record like
# a red one. Under the floor the seed stands: the four the bettor named
# off the session's numbers (priced + score under 0 + over 75.0 on 16,
# watch + score under 0 69.2 on 13, watch + score under 0 + over 44.4 on
# 9, Athena + score under 0 + over 76.5 on 17). The measurement keeps
# only the red cards out, so a declined combo can come back.
#
# THE BANK LEADS, SINCE 16 SEP, and this is the third time the same
# lesson has been paid for. The bands moved off the board onto the bank
# on 12 Sep because the board-global read came out INVERTED; the flip
# moved on 13 Sep because "unsafe · flipped" stood on 15 and 23 board
# cards against 302 and 417 in the bank. The combos were left measuring
# the board's ~460 settled cards at a floor of FIFTEEN, and all four
# declines turned out to rest on 10 to 22 cards each while the bank said
# keep on every one of them:
#
#   athena  score<0 + over lane      board 73.7% on 19   bank 81.5% on 2357
#   watch   score<0 + watch card     board 60.0% on 15   bank 84.7% on  688
#   watch   score<0 + watch + over   board 40.0% on 10   bank 86.9% on  360
#   priced  score<0 + priced + over  board 72.7% on 22   bank 77.2% on  569
#
# Forty percent on ten matches against 86.9% on three hundred and sixty.
# The bank's two halves agree with each other and with its newest cards,
# so this was never a regime the board was catching early — it was ten
# matches. The bettor's rule for the whole project settles it: the bank
# has to reflect how Athena would have behaved on future matches, or
# every measurement taken from it is describing a board that never
# existed.
#
# So the bank sets the label and the board overrides it only at
# COMBO_MIN_BOARD cards, exactly as the flip works. The seed survives
# only for a checkout with no bank at all.
DECLINE_AT = 77.0
COMBO_MIN_N = MIN_N
COMBO_MIN_BOARD = MIN_LEAGUE    # the board overrides the bank's combo only at this many
COMBO_SEED = {
    ("priced", "score under 0 + priced play + over lane"): "decline",
    ("watch", "score under 0 + watch card"): "decline",
    ("watch", "score under 0 + watch card + over lane"): "decline",
    ("athena", "score under 0 + over lane"): "decline",
}

_BANDS: dict | None = None


def edge_band(edge: float) -> str:
    return ("+1 up" if edge >= 1 else "−1..+1" if edge > -1
            else "−4..−1" if edge > -4 else "−4 down")


def bands() -> dict[tuple, dict]:
    """The MEASURED band table (config/live_bands.tsv): whole-bank rows
    keyed (lane, band), league rows keyed (league, lane, band), flip rows
    keyed ("flip", "<tag> tipN"). A missing whole-bank or flip row falls
    back to its seed. Read once."""
    global _BANDS
    if _BANDS is None:
        got = read() or {}
        _BANDS = {}
        for lane, seeds in SEED.items():
            for band, seed in seeds.items():
                _BANDS[(lane, band)] = got.get((GLOBAL, lane, band)) or dict(
                    n=0, hit=None, said=None, label=seed, source="seed")
        for (code, lane, band), row in got.items():
            if code not in (GLOBAL, "flip"):
                _BANDS[(code, lane, band)] = row
        for (code, lane, band), row in got.items():
            if code == "combo":
                _BANDS[("combo", lane, band)] = row
        for (lane, band), seed in COMBO_SEED.items():
            _BANDS.setdefault(("combo", lane, band), dict(
                n=0, hit=None, said=None, label=seed, source="seed"))
        for (band, tipn), seed in FLIP_SEED.items():
            key = ("flip", f"{band} {tipn}")
            _BANDS[key] = got.get(("flip", "flip", f"{band} {tipn}")) or dict(
                n=0, hit=None, said=None, label=seed, source="seed")
            # the bank's own pairing, written beside it, for the hover
            _BANDS[("flipbank", f"{band} {tipn}")] = got.get(
                ("flipbank", "flip", f"{band} {tipn}")) or dict(
                n=0, hit=None, said=None, label=seed, source="seed")
    return _BANDS


def band_row(lane: str, edge: float, code: str | None = None) -> dict:
    """The row the tag reads for this card: the league's own where it is
    measured, else the whole bank's (or the seed)."""
    b = edge_band(edge)
    if code:
        row = bands().get((code, lane, b))
        if row and row["source"] == "league":
            return row
    return bands()[(lane, b)]


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


def flip_label(tip_hit: float | None, tip1_hit: float | None, n: int, seed: str,
               floor: int = MIN_N, source: str = "measured") -> tuple[str, str]:
    """(label, source): measured when n reaches the floor, else the seed."""
    if tip_hit is None or tip1_hit is None or n < floor:
        return seed, "seed"
    return ("flipped" if tip_hit - tip1_hit >= FLIP_AT else "hold"), source


def _flip_pairs_bank() -> dict[tuple[str, str], list]:
    """(tag, tipN) -> [n, lane hits, tip 1 hits] over the bank's priced
    plays, tagged as the bank reads them now, a push on the lane no bet."""
    from scripts import bankrates as br
    G = ("✅", "❌", "◦")
    pair: dict[tuple[str, str], list] = {k: [0, 0, 0] for k in FLIP_KEYS}
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            if br.lane(m) != "priced":
                continue
            tg, m1 = br.tag(m, code), m.get("mark")
            if tg not in ("safe", "cautious", "unsafe") or m1 not in G:
                continue
            for tipn, key in (("tip2", "m2"), ("tip3", "m3")):
                mk = m.get(key)
                if mk not in ("✅", "❌"):
                    continue
                t = pair[(tg, tipn)]
                t[0] += 1
                t[1] += mk != "❌"
                t[2] += m1 != "❌"
    return pair


def combo_label(hit_pct: float | None, n: int, seed: str,
                floor: int | None = None) -> tuple[str, str]:
    """(label, source): "decline" under DECLINE_AT on `floor` cards or
    more, "keep" at or above it, the seed under the floor.

    `floor` defaults to COMBO_MIN_N, which is what the BANK is held to.
    The board is held to COMBO_MIN_BOARD instead — a much higher bar,
    because the board only gets to overrule the bank when it has enough
    cards to be worth hearing."""
    if hit_pct is None or n < (COMBO_MIN_N if floor is None else floor):
        return seed, "seed"
    return ("decline" if hit_pct < DECLINE_AT else "keep"), "measured"


def combo(lane: str | None, strikes: list[str]) -> str:
    """"decline" or "keep" for a card's lane and strikes."""
    if not lane or not strikes:
        return "keep"
    row = bands().get(("combo", lane, " + ".join(strikes)))
    return row["label"] if row else "keep"


def _combo_bank() -> dict:
    """(lane, combo) -> [n, hits] over the whole bank, red cards out —
    the same not_red rule the board's own study uses, so a declined
    combo can still come back."""
    from scripts import bankrates as _br
    tally: dict[tuple[str, str], list] = {}
    for code, comp in _br.bank().items():
        for m in comp.get("matches", []):
            got = _br._hit(m.get("mark"))
            if got is None or not _br.not_red(m):
                continue
            ln = _br.lane(m)
            if ln is None:
                continue
            key = (ln, " + ".join(_br.strikes(m, code)) or "clean")
            t = tally.setdefault(key, [0, 0, 0.0])
            t[0] += 1
            t[1] += bool(got)
            t[2] += _br._claim(m.get("tip")) or 0.0
    return tally


def combo_study() -> list[dict]:
    """One row per lane and strike combination seen on the board's settled
    cards, red out (webapp.not_red, so a declined combo can come back):
    n, hit, said (the claim), label, source."""
    from scripts import board, webapp
    tally: dict[tuple[str, str], list] = {}
    for f in board.load():
        if not f.settled or f.status[:1] not in ("✅", "❌", "◦"):
            continue
        if not f.tip1 or f.tip1.startswith("—") or not webapp.not_red(f):
            continue
        lane = webapp.record_lane(f)
        if lane is None:
            continue
        key = (lane, " + ".join(webapp.strikes(f)) or "clean")
        c = webapp._claim(f.tip3 if webapp._star_any(f) == 3 else f.tip1) or 0.0
        t = tally.setdefault(key, [0, 0, 0.0])
        t[0] += 1
        t[1] += f.status[:1] != "❌"
        t[2] += c
    # THE BANK SETS THE LABEL, the board overrides only at the floor —
    # see COMBO_MIN_BOARD for the four declines that stood on ten to
    # twenty-two cards each.
    bank = _combo_bank()
    rows = []
    for (lane, band) in sorted(set(tally) | set(bank) | set(COMBO_SEED)):
        seed = COMBO_SEED.get((lane, band), "keep")
        bn, bh, bc = bank.get((lane, band), (0, 0, 0.0))
        bhit = (bh / bn * 100) if bn else None
        blab, bsrc = combo_label(bhit, bn, seed)
        bsrc = "bank" if bsrc == "measured" else bsrc
        rows.append(dict(league="combobank", lane=lane, band=band, n=bn, hit=bhit,
                         said=(bc / bn) if bn else None, label=blab, source=bsrc))
        n, h, c = tally.get((lane, band), (0, 0, 0.0))
        hit = (h / n * 100) if n else None
        lab, src = combo_label(hit, n, blab, COMBO_MIN_BOARD)
        src = "board" if src == "measured" else bsrc
        if src != "board":
            lab = blab
        rows.append(dict(league="combo", lane=lane, band=band, n=n, hit=hit,
                         said=(c / n) if n else None, label=lab, source=src))
    return rows


# ── RELEASE: the cards the tier refuses that the record says it should not ──
#
# The bettor, 16 Sep, after typing "declined, tip 1 <75" into the new
# filter and getting 11 of 12: "if there are cards that are hitting above
# 77% inside a certain profile, we need to get them back in."
#
# The tier calls a card red below a 76% claim. On one rung that refusal is
# measurably wrong. market_select.MIN_WIN_PROB is 0.75, so the selector
# only ever returns a rung that clears 75% — and when nothing does, it
# falls through to the safest buyable line instead. Those fall-through
# cards are a different population from the ones the tier was cut for, and
# on the O1.5 rung they land far above what they claim: 241 bank cards at
# 80.9% against a claim near 73, +5.53% at closing prices.
#
# It is NOT the consensus cap, though the cap makes it worse (in those
# seven leagues a raw 78.0% is published as 72.1% against an actual
# 81.0%). Remove the two leagues that produced the observation and the
# cap effect vanishes; this one gets stronger. And it is not a fitted
# window: every claim cut from 68 to 75 clears both halves of the bank,
# breaking only at 76 where the 75-76 pile-up begins.
#
# SUPER RED IS NOT RELEASED, and that is the guard rail this rule earns
# its keep by. Inside the very same profile a super-red card lands 75.8%
# in the first half and returns -2.38%, against red's 78.3% and +5.53%.
# The confluence score is doing real work at the bottom, so a card the
# score condemns stays condemned.
#
# Measured on the BANK, both halves by date, and re-measured every two
# days like every other rule here. A profile that stops clearing 77 goes
# back to declined by itself, with no one's permission.
RELEASE_AT = 77.0        # both halves must land at or above this
RELEASE_MIN_N = 120      # and the profile needs this many bank cards


def release_study() -> list[dict]:
    """One row per rung for the sub-floor cards the tier calls red.

    Red only — super red is never a candidate. Split in half by date, and
    a profile is released only when BOTH halves clear RELEASE_AT, because
    putting a refused card back on a playable board is a larger claim
    than taking a playable one off.
    """
    import re as _re
    from scripts import bankrates as _br
    from app.engine import market_select as _ms
    rung_re = _re.compile(r"(?:^|[^A-Za-z])([OU]\d+(?:\.\d+)?)")
    floor = _ms.MIN_WIN_PROB * 100
    cards: list[dict] = []
    for code, comp in _br.bank().items():
        # The population is the one the rule is APPLIED to, and since
        # 20 Sep an international card is never released (see released()),
        # so its cards are not evidence for or against anybody's release.
        # Leaving them in pooled a 75.6% set with an 81.7% one and revoked
        # a live release on the strength of CONCACAF and the AFC.
        if code.startswith("INT-"):
            continue
        for m in comp.get("matches", []):
            # "red" OR "released": the candidate population is every card
            # the TIER refuses, whatever the label ended up as. Filtering
            # on "red" alone made the rule EAT ITSELF — the moment the
            # bank was rebuilt the released cards were labelled
            # "released", dropped out of their own measurement, and the
            # next refresh would have written the table without them and
            # revoked the release two days later. Not because they
            # stopped landing: because they had been believed.
            # Super red is still never a candidate.
            if (m.get("g") or "") not in ("red", "released") or not m.get("d"):
                continue
            got = _br._hit(m.get("mark"))
            claim = _br._claim(m.get("tip"))
            if got is None or claim is None or claim >= floor:
                continue
            rm = rung_re.search((m.get("tip") or "").replace("*", ""))
            if not rm:
                continue
            cards.append(dict(rung=rm.group(1), hit=bool(got), d=m["d"],
                              claim=claim))
    cards.sort(key=lambda x: x["d"])
    cut = cards[len(cards) // 2]["d"] if cards else ""
    rows = []
    for rung in sorted({c["rung"] for c in cards}):
        g = [c for c in cards if c["rung"] == rung]
        a = [c for c in g if c["d"] < cut]
        b = [c for c in g if c["d"] >= cut]
        n = len(g)
        hit = sum(c["hit"] for c in g) / n * 100 if n else None
        ha = sum(c["hit"] for c in a) / len(a) * 100 if a else None
        hb = sum(c["hit"] for c in b) / len(b) * 100 if b else None
        ok = (n >= RELEASE_MIN_N and ha is not None and hb is not None
              and ha >= RELEASE_AT and hb >= RELEASE_AT)
        rows.append(dict(league="release", lane=rung,
                         band=f"under {floor:.0f}", n=n, hit=hit,
                         said=(sum(c["claim"] for c in g) / n) if n else None,
                         label="released" if ok else "declined",
                         source=f"halves {ha:.1f}/{hb:.1f}" if ha and hb
                                else "too few"))
    return rows


_RELEASED: frozenset | None = None


def released_rungs() -> frozenset:
    """The rungs currently released, read once.

    CACHED, and it matters: label() is the gate every card goes through,
    and a bank rebuild calls it 32,000 times. Re-reading live_bands.tsv
    per card turned a 3-second relabel into a file-system benchmark.
    """
    global _RELEASED
    if _RELEASED is None:
        rows = read() or {}
        _RELEASED = frozenset(
            lane for (lg, lane, _band), r in rows.items()
            if lg == "release" and r.get("label") == "released")
    return _RELEASED


def released(rung: str | None, claim: float | None,
             code: str | None = None) -> bool:
    """Is this card one the record has put back on the board?

    Read by guard_slices.label, which is the ONE gate the board and the
    bank both label through, so a released card reads the same on either.

    AN INTERNATIONAL CARD IS NEVER RELEASED (20 Sep). The release profile
    is pooled across the whole bank, and the international set walked in
    with 217 sub-floor O1.5 cards — 150 CONCACAF, 67 AFC, the two weakest
    confederations — landing 75.6% against the club population's 81.7%.
    Pooled they read 78.8% with a 75.5% first half, which fails the rule
    and would have revoked a live release for every club card on the
    board. Split, the club profile is untouched: 81.7%, halves 79.2/84.3.
    That is the same Simpson's trap the ladder-rate table had to be
    rescued from, arriving this time inside a live-safety rule, and the
    honest answer to it is that a population measured at 75.6% does not
    get to put refused cards back on a playable board — neither its own,
    nor anybody else's. An international release needs its own profile
    and its own measurement; until there is one, the tier's refusal
    stands.
    """
    from app.engine import market_select as _ms
    if not rung or claim is None or claim >= _ms.MIN_WIN_PROB * 100:
        return False
    if code and code.startswith("INT-"):
        return False
    return rung in released_rungs()


def flip_study(days: int = DAYS, today: dt.date | None = None) -> list[dict]:
    """Six rows, lane "flip", band "<tag> tip2" / "<tag> tip3": n paired
    cards in the window (priced plays with that tag where tip 1 graded and
    that lane WON or LOST — a push is no bet), hit = that lane's hit rate,
    said = TIP 1's hit rate on the same cards (the column is reused; the
    header says so), label, source."""
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
            if mk not in ("✅", "❌"):          # ungraded, or a push: no bet
                continue
            t = pair[(tag, tipn)]
            t[0] += 1
            t[1] += mk != "❌"
            t[2] += m1 != "❌"
    # THE BANK FIRST (13 Sep, the same lesson as the bands): the bank's
    # pairing sets the label; the board overrides it only at
    # FLIP_MIN_BOARD paired cards. The three-week read had "unsafe ·
    # flipped" on 15 and 23 cards against 302 and 417 bank cards saying
    # hold.
    bank_pair = _flip_pairs_bank()
    rows = []
    for (band, tipn) in FLIP_KEYS:
        bn, bh, bh1 = bank_pair[(band, tipn)]
        bhit = (bh / bn * 100) if bn else None
        bhit1 = (bh1 / bn * 100) if bn else None
        blab, bsrc = flip_label(bhit, bhit1, bn, FLIP_SEED[(band, tipn)], MIN_N, "bank")
        rows.append(dict(league="flipbank", lane="flip", band=f"{band} {tipn}", n=bn,
                         hit=bhit, said=bhit1, label=blab, source=bsrc))
        n, h, h1 = pair[(band, tipn)]
        hit = (h / n * 100) if n else None
        hit1 = (h1 / n * 100) if n else None
        lab, src = flip_label(hit, hit1, n, blab, FLIP_MIN_BOARD, "board")
        if src == "seed":                       # under the floor: the bank's word
            lab, src = blab, bsrc
        rows.append(dict(league="flip", lane="flip", band=f"{band} {tipn}", n=n,
                         hit=hit, said=hit1, label=lab, source=src))
    return rows


def tag(lane: str | None, edge: float | None, code: str | None = None) -> str | None:
    """'safe', 'cautious' or 'unsafe' by lane, printed edge and — where
    the league has a measured row — league; None for no lane (a red
    card, an abstention) or no printed edge."""
    if lane not in SEED or edge is None:
        return None
    return band_row(lane, edge, code)["label"]


def label(hit_pct: float | None, n: int, seed: str, floor: int = MIN_N,
          source: str = "measured") -> tuple[str, str]:
    """(label, source): measured when n reaches the floor, else the seed
    with source "seed"."""
    if hit_pct is None or n < floor:
        return seed, "seed"
    if hit_pct >= SAFE_AT:
        return "safe", source
    if hit_pct >= CAUTIOUS_AT:
        return "cautious", source
    return "unsafe", source


def measure(days: int = DAYS, today: dt.date | None = None) -> list[dict]:
    """The band rows: one per lane and band for the whole bank (league
    "*"), then one per league, lane and band where the league has any
    card there — labelled "league" at MIN_LEAGUE cards, else the row is
    written for the record but not read. Red cards out, unsafe cards IN.
    `days` is unused here (the flip study takes it); kept for the CLI."""
    from scripts import bankrates as br
    tally: dict[tuple, list] = {}
    for code, comp in br.bank().items():
        for m in comp.get("matches", []):
            if not br.not_red(m):
                continue
            lane = br.lane(m)
            if lane is None:
                continue
            cell = m.get("t3") if m.get("pk") == 3 else m.get("tip")
            got = br._hit(m.get("m3") if m.get("pk") == 3 else m.get("mark"))
            e, c = br._edge(cell), br._claim(cell)
            if got is None or e is None or c is None:
                continue
            b = edge_band(e)
            for key in ((GLOBAL, lane, b), (code, lane, b)):
                t = tally.setdefault(key, [0, 0, 0.0])
                t[0] += 1
                t[1] += got
                t[2] += c
    rows = []
    for lane in LANES:
        for band in BANDS:
            n, h, c = tally.get((GLOBAL, lane, band), (0, 0, 0.0))
            hit = (h / n * 100) if n else None
            lab, src = label(hit, n, SEED[lane][band], MIN_N, "global")
            rows.append(dict(league=GLOBAL, lane=lane, band=band, n=n, hit=hit,
                             said=(c / n) if n else None, label=lab, source=src))
    for code in sorted({k[0] for k in tally if k[0] != GLOBAL}):   # a SET: the generator repeated a code per key
        for lane in LANES:
            for band in BANDS:
                if (code, lane, band) not in tally:
                    continue
                n, h, c = tally[(code, lane, band)]
                hit = h / n * 100
                glob = next(r for r in rows if r["league"] == GLOBAL
                            and r["lane"] == lane and r["band"] == band)
                lab, src = label(hit, n, glob["label"], MIN_LEAGUE, "league")
                if src == "seed":
                    lab, src = glob["label"], "thin"     # written, not read
                rows.append(dict(league=code, lane=lane, band=band, n=n, hit=hit,
                                 said=c / n, label=lab, source=src))
    return rows


def write(rows: list[dict], days: int, today: dt.date) -> None:
    lines = [
        "# The live-safety bands, MEASURED on the bank, red cards out; written by",
        "# scripts/livebands.py every two days (the bank-refresh workflow) and read",
        "# by scripts/webapp.py and scripts/bankrates.py for the live tag. The tag",
        f"# reads the league's own row where it has {MIN_LEAGUE} cards or more (source",
        "# 'league'), else the whole bank's row (league '*', source 'global'), else",
        "# the seed. A league row under the floor is written as 'thin' and not read.",
        f"# Thresholds: safe >= {SAFE_AT:.0f}, cautious >= {CAUTIOUS_AT:.0f}, else unsafe.",
        f"# written\t{today.isoformat()}",
        "# The 'flip' rows: priced plays with that tag that also print that lane,",
        f"# paired on the board's last {days} days — n cards where tip 1 graded and",
        "# that lane won or lost (a push on the lane is no bet), hit = THAT lane,",
        f"# said = tip 1 on the same cards; flipped when the lane beats tip 1 by {FLIP_AT:.0f}.",
        "# The 'combo' rows: a lane and its strikes (webapp.strikes) on the board's",
        f"# settled cards, red out; 'decline' under {DECLINE_AT:.0f}% on {COMBO_MIN_N}+ cards, else",
        "# 'keep', the seed under the floor. A declined combo files under Declined.",
        "# league\tlane\tband\tn\thit\tsaid\tlabel\tsource",
    ]
    for r in rows:
        lines.append("\t".join([
            r.get("league", GLOBAL), r["lane"], r["band"], str(r["n"]),
            "" if r["hit"] is None else f"{r['hit']:.1f}",
            "" if r["said"] is None else f"{r['said']:.1f}",
            r["label"], r["source"]]))
    OUT.write_text("\n".join(lines) + "\n")


def read() -> dict[tuple, dict] | None:
    """The written table keyed (league, lane, band) — flip rows under
    league "flip" — or None when it does not exist yet."""
    if not OUT.exists():
        return None
    out = {}
    for ln in OUT.read_text().splitlines():
        if ln.startswith("#") or not ln.strip():
            continue
        p = ln.split("\t")
        if len(p) < 8:
            continue
        out[(p[0], p[1], p[2])] = dict(n=int(p[3]), hit=(float(p[4]) if p[4] else None),
                                       said=(float(p[5]) if p[5] else None),
                                       label=p[6], source=p[7])
    return out


def main() -> None:
    dry = "--dry" in sys.argv
    days = DAYS
    if "--days" in sys.argv:
        days = int(sys.argv[sys.argv.index("--days") + 1])
    today = dt.date.today()
    rows = measure(days, today)
    print("live bands on the bank, red cards out — whole bank:")
    for r in rows:
        if r["league"] != GLOBAL:
            continue
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        print(f"  {r['lane']:6} {r['band']:8} n={r['n']:5}  hit {hit}  -> {r['label']:8} ({r['source']})")
    lg = [r for r in rows if r["league"] != GLOBAL]
    spoke = [r for r in lg if r["source"] == "league"]
    differs = [r for r in spoke if r["label"] != next(
        g["label"] for g in rows if g["league"] == GLOBAL and g["lane"] == r["lane"] and g["band"] == r["band"])]
    print(f"  league rows: {len(lg)} written, {len(spoke)} at the {MIN_LEAGUE}-card floor, "
          f"{len(differs)} of those differ from the whole bank")
    flips = flip_study(days, today)
    print("the flip on priced plays, per tag band, paired with tip 1 — the bank, then "
          f"the board's last {days} days (board overrides at {FLIP_MIN_BOARD}):")
    for r in flips:
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        t1 = "   —  " if r["said"] is None else f"{r['said']:5.1f}%"
        who = "bank " if r["league"] == "flipbank" else "board"
        print(f"  {who} {r['band']:14} n={r['n']:4}  lane {hit}  tip 1 {t1}  -> {r['label']:8} ({r['source']})")
    rows += flips
    combos = combo_study()
    print(f"the strike combos — the BANK first, red out, the board overriding "
          f"only at {COMBO_MIN_BOARD}+ cards (decline under {DECLINE_AT:.0f} "
          f"on {COMBO_MIN_N}+):")
    for r in combos:
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        print(f"  {r['lane']:7} {r['band']:44} n={r['n']:3}  hit {hit}  -> {r['label']:8} ({r['source']})")
    rows += combos
    rel = release_study()
    print(f"the RELEASE profiles — cards the tier calls red below the selector's own "
          f"floor, super red never a candidate (both halves must clear "
          f"{RELEASE_AT:.0f} on {RELEASE_MIN_N}+):")
    for r in rel:
        hit = "   —  " if r["hit"] is None else f"{r['hit']:5.1f}%"
        said = "  —  " if r["said"] is None else f"{r['said']:4.1f}%"
        print(f"  {r['lane']:7} {r['band']:10} n={r['n']:4}  hit {hit}  claim {said}"
              f"  -> {r['label']:8} ({r['source']})")
    rows += rel
    if not dry:
        write(rows, days, today)
        print(f"written: {OUT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
