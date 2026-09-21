"""Profile the Declined board for winners and the Playable/Watch boards for losers.

    python scripts/profile_study.py        prints the whole study; writes nothing

THE ASK (the bettor, 21 Sep): "profile some winners and release them, leaving
a real low hitrate behind on declined and possibly increase the playable and
watch hitrate — only make a profile if that sample batch brings a good
hitrate back inside the playable and watch boards. On the flip side, look at
the losers still on playable and watch: is that something we can profile
that can move those fully to declined."

HOW IT MEASURES. Every graded bank card, filed the way the bank files it
(bankrates.counts decides declined; lane and tag as the board reads them),
with every feature the card carries — colour, confluence score, STRONG,
claim, edge, side, team/match total, lane, live tag, strike count and combo,
region — plus three things computed AS-OF over the bank in date order so no
card sees the future: the league's hit rate against the bank's (the bettor's
"−2..+2 band" idea), and the card's own grid — the three league cells and
three bank-wide cells the card prints (colour, side, strikes), from which
the bettor's hand rule is scored ("I don't play any card with one cell under
77 or two at 78"). A declined card's grid is measured against declined cards,
exactly as cardgrid does it. Then two chronological halves, and a profile
only qualifies when BOTH halves clear the bar — the release study's rule.

WHAT IT FOUND, 21 Sep, on 34,152 graded cards split at 2025-08-16:

  RELEASE: nothing. Of 639 declined profiles with 120+ cards, none clears
  80 in both halves; five clear 77, the best 78.9 on 166 and the rest at
  77.6-77.7. The boards they would return to land 83.2 (playable) and 83.4
  (watch). A release at 77.7 lowers the board it joins, which is exactly the
  bettor's condition failing. Declined lands 75.0 and is FALLING (76.0 then
  73.9); the "−2..+2" gap band inside it lands 74.8 on 1,809 — the
  declined average.

  DECLINE: one real profile, and it is a card the board keeps on purpose.
  Inside playable, a PRICED PLAY — a card whose closing price cleared the
  bar, the market paying long — lands 75.4 on 391 (76.6 then 71.1) against
  83.4 for the rest, and the priced plays tagged live UNSAFE land 73.1 on
  193 (74.7 then 65.7). Since 12 Sep a priced play counts whatever its tag
  says; the record says the unsafe ones should not. Watch is clean: 0 of
  448 profiles. The union of every distinct playable decline profile is 520
  cards at 73.5; without them the playable board reads 83.6 from 83.2.

  THE BETTOR'S GRID RULE separates, mildly: on playable his "skip" cards
  land 80.0 on 2,311 against 83.9 for "play", and the gap widens in the
  second half (77.7 against 83.6). By the weakest cell alone it is a
  gradient with a cliff at 80, not 77 — under 77: 79.7, 77-80: 79.8,
  80-85: 85.0, 85+: 88.6. Inside watch the rule does nothing (83.4 / 83.7).
  The cards it skips are weaker winners, not losers.

NOTHING HERE CHANGES A RULE. Whether an unsafe priced play files as
declined is a rules change with a live cost — it moves cards the bettor
sees today — and that is his call, logged in config/hypotheses.tsv.
"""

import sys, re, collections, statistics, itertools, json
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from scripts import bankrates as br, cardgrid, cellrates
from scripts.confluence import region

PLAY_EDGE = br.PLAYABLE_EDGE
MIN_CELL = cardgrid.MIN_N

bank = br.bank()
cards = []
for code, comp in bank.items():
    for m in comp.get("matches", []):
        got = br._hit(m.get("mark"))
        if got is None:
            continue
        tip = m.get("tip") or ""
        claim, edge = br._claim(tip), br._edge(tip)
        if claim is None or edge is None:
            continue
        cell = (m.get("t3") if m.get("pk") == 3 else tip) or ""
        lane_txt = cell.lstrip("✅❌◦ *")
        side = cellrates.side_of(tip) or ("O" if lane_txt.startswith("O") else "U" if lane_txt.startswith("U") else "-")
        team = "team" if "(team)" in tip else "match"
        ln = br.lane(m)
        tg = br.tag(m, code)
        st = br.strikes(m, code)
        declined = not br.counts(m, code)
        g = m.get("g") or ""
        cs = m.get("cs")
        cards.append(dict(
            d=m["d"], code=code, region=region(code), hit=bool(got),
            g=g, cs=cs, strong=int(m.get("st") or 0), pk=m.get("pk"),
            claim=claim, edge=edge, side=side, team=team,
            lane=ln or "none", tag=tg or "none", nstr=len(st), strikes=" + ".join(st) or "clean",
            declined=declined,
            pop=("declined" if declined else
                 "watch" if ln == "watch" else
                 "playable" if edge >= PLAY_EDGE else "athena"),
        ))
cards.sort(key=lambda c: c["d"])
print(f"{len(cards)} graded cards; by population:",
      dict(collections.Counter(c['pop'] for c in cards)))

# ---- AS-OF features: league-vs-all gap, and the three grid cells ----
class AsOf:
    def __init__(self): self.t = collections.defaultdict(lambda: [0, 0])
    def rate(self, k):
        n, h = self.t[k]; return (h / n * 100 if n >= MIN_CELL else None), n
    def add(self, k, hit):
        self.t[k][0] += 1; self.t[k][1] += hit

cnt = AsOf()     # counted (non-declined) pool, like the board's grid for a playable card
dec = AsOf()     # declined pool, like the grid for a declined card
for c in cards:
    pool = dec if c["declined"] else cnt
    keys = [("lg", c["code"]), ("all",),
            ("lg-col", c["code"], c["g"]), ("all-col", c["g"]),
            ("lg-side", c["code"], c["side"]), ("all-side", c["side"]),
            ("lg-str", c["code"], c["nstr"]), ("all-str", c["nstr"])]
    r = {k[0]: pool.rate(k) for k in keys}
    lg, al = r["lg"][0], r["all"][0]
    c["gap"] = (lg - al) if (lg is not None and al is not None) else None
    here = [r[k][0] for k in ("lg-col", "lg-side", "lg-str")]
    both = here + [r[k][0] for k in ("all-col", "all-side", "all-str")]
    known = [x for x in both if x is not None]
    c["grid_min"] = min(known) if known else None
    c["grid_n78"] = sum(1 for x in known if x <= 78.0) if known else None
    c["grid_known"] = len(known)
    # the bettor's rule: any cell under 77, or two cells at/under 78 -> skip
    c["bettor_skip"] = (None if not known else
                        (c["grid_min"] < 77.0 or c["grid_n78"] >= 2))
    for k in keys:
        pool.add(k, c["hit"])

# ---- chronological halves ----
mid = cards[len(cards) // 2]["d"]
for c in cards:
    c["half"] = "A" if c["d"] < mid else "B"
print(f"halves split at {mid}")

def band_cs(v):
    if v is None: return "none"
    return ("<-13" if v < -12.99 else "-13..0" if v < 0 else "0..+4" if v < 4
            else "+4..+6.3" if v < 6.34 else ">=6.34")
def band_claim(v): return "<75" if v < 75 else "75-80" if v < 80 else "80-85" if v < 85 else "85-90" if v < 90 else "90+"
def band_edge(v):  return "<-4" if v < -4 else "-4..-1" if v < -1 else "-1..+1" if v < 1 else "+1..+4" if v < 4 else "+4..+8" if v < 8 else ">=+8"
def band_gap(v):
    if v is None: return "unknown"
    return "<-4" if v < -4 else "-4..-2" if v < -2 else "-2..+2" if v <= 2 else "+2..+4" if v <= 4 else ">+4"

FEATS = {
    "colour":   lambda c: c["g"],
    "score":    lambda c: band_cs(c["cs"]),
    "strong":   lambda c: "strong" if c["strong"] else "-",
    "claim":    lambda c: band_claim(c["claim"]),
    "edge":     lambda c: band_edge(c["edge"]),
    "side":     lambda c: c["side"],
    "total":    lambda c: c["team"],
    "lane":     lambda c: c["lane"],
    "tag":      lambda c: c["tag"],
    "strikes":  lambda c: str(c["nstr"]),
    "combo":    lambda c: c["strikes"],
    "region":   lambda c: c["region"],
    "lg-vs-all":lambda c: band_gap(c["gap"]),
    "grid":     lambda c: ("unknown" if c["bettor_skip"] is None else "skip" if c["bettor_skip"] else "play"),
    "gridmin":  lambda c: ("unknown" if c["grid_min"] is None else
                           "<77" if c["grid_min"] < 77 else "77-80" if c["grid_min"] < 80 else "80-85" if c["grid_min"] < 85 else "85+"),
}

def tally(pop, keyfn):
    t = collections.defaultdict(lambda: {"A": [0, 0], "B": [0, 0]})
    for c in cards:
        if c["pop"] not in pop: continue
        k = keyfn(c)
        t[k][c["half"]][0] += 1; t[k][c["half"]][1] += c["hit"]
    out = []
    for k, v in t.items():
        nA, hA = v["A"]; nB, hB = v["B"]
        out.append((k, nA + nB, (hA / nA * 100) if nA else None, (hB / nB * 100) if nB else None,
                    ((hA + hB) / (nA + nB) * 100)))
    return out

def baseline(pop):
    r = tally(pop, lambda c: "all")[0]
    return r

MIN_N = 120
print("\n=== BASELINES (n, half A, half B, all) ===")
for pop in (("declined",), ("playable",), ("watch",), ("athena",), ("playable", "watch")):
    k, n, a, b, al = baseline(pop)
    print(f"  {'+'.join(pop):16} n={n:6}  A {a:5.1f}  B {b:5.1f}  all {al:5.1f}")

def search(pop, direction, bar, label):
    """direction 'up': both halves >= bar (release candidates);
       'down': both halves <= bar (decline candidates)."""
    found, tested = [], 0
    names = list(FEATS)
    combos = [(n,) for n in names] + list(itertools.combinations(names, 2))
    for fs in combos:
        keyfn = lambda c, fs=fs: " & ".join(f"{f}={FEATS[f](c)}" for f in fs)
        for k, n, a, b, al in tally(pop, keyfn):
            if n < MIN_N or a is None or b is None: continue
            tested += 1
            ok = (a >= bar and b >= bar) if direction == "up" else (a <= bar and b <= bar)
            if ok:
                found.append((al, n, a, b, k))
    found.sort(key=lambda r: (-r[0] if direction == "up" else r[0]))
    print(f"\n=== {label}: {len(found)} of {tested} profiles (n>={MIN_N}) clear {bar} in BOTH halves ===")
    # drop profiles that are strict sub-slices adding nothing: keep top 25 by pooled rate
    for al, n, a, b, k in found[:25]:
        print(f"  {al:5.1f}  n={n:5}  A {a:5.1f}  B {b:5.1f}   {k}")
    return found

rel = search(("declined",), "up", 80.0, "DECLINED -> release candidates (both halves >= 80)")
rel77 = search(("declined",), "up", 77.0, "DECLINED -> at the existing 77 bar")
dn_p = search(("playable",), "down", 75.0, "PLAYABLE -> decline candidates (both halves <= 75)")
dn_w = search(("watch",), "down", 75.0, "WATCH -> decline candidates (both halves <= 75)")
dn_pw = search(("playable", "watch"), "down", 76.0, "PLAYABLE+WATCH -> decline candidates (both halves <= 76)")

# ---- the bettor's own rule, tested directly on the boards he plays ----
print("\n=== THE BETTOR'S GRID RULE on playable+watch (any cell <77, or two cells <=78 -> skip) ===")
for k, n, a, b, al in sorted(tally(("playable", "watch"), FEATS["grid"])):
    print(f"  {k:8} n={n:6}  A {a or 0:5.1f}  B {b or 0:5.1f}  all {al:5.1f}")
print("  by the weakest grid cell:")
for k, n, a, b, al in sorted(tally(("playable", "watch"), FEATS["gridmin"])):
    print(f"  {k:8} n={n:6}  A {a if a else 0:5.1f}  B {b if b else 0:5.1f}  all {al:5.1f}")
print("\n=== league-vs-all gap band, inside DECLINED ===")
for k, n, a, b, al in sorted(tally(("declined",), FEATS["lg-vs-all"])):
    print(f"  {k:8} n={n:6}  A {a if a else 0:5.1f}  B {b if b else 0:5.1f}  all {al:5.1f}")
print("\n=== league-vs-all gap band, inside PLAYABLE+WATCH ===")
for k, n, a, b, al in sorted(tally(("playable", "watch"), FEATS["lg-vs-all"])):
    print(f"  {k:8} n={n:6}  A {a if a else 0:5.1f}  B {b if b else 0:5.1f}  all {al:5.1f}")


# ---- FOLLOW-UPS ----
def show(title, pop, keyfn, order=None):
    print(f"\n=== {title} ===")
    rows = tally(pop, keyfn)
    rows.sort(key=lambda r: (order.index(r[0]) if order and r[0] in order else 99, r[0]))
    for k, n, a, b, al in rows:
        if n >= 60:
            print(f"  {k:22} n={n:6}  A {a or 0:5.1f}  B {b or 0:5.1f}  all {al:5.1f}")

show("PLAYABLE by lane (single feature)", ("playable",), FEATS["lane"])
show("PLAYABLE by tag", ("playable",), FEATS["tag"])
show("PLAYABLE by edge band", ("playable",), FEATS["edge"], ["<-4","-4..-1","-1..+1","+1..+4","+4..+8",">=+8"])
show("PLAYABLE by weakest grid cell", ("playable",), FEATS["gridmin"], ["<77","77-80","80-85","85+","unknown"])
show("PLAYABLE by region", ("playable",), FEATS["region"])
show("PLAYABLE priced plays, by weakest grid cell", ("playable",),
     lambda c: (FEATS["gridmin"](c) if c["lane"]=="priced" else "not priced"), ["<77","77-80","80-85","85+","unknown"])
show("PLAYABLE priced plays, by tag", ("playable",),
     lambda c: (c["tag"] if c["lane"]=="priced" else "not priced"))

# union of the distinct decline profiles that cleared <=75 in both halves, and the residual board
def in_union(c):
    if c["pop"] != "playable": return False
    gm = FEATS["gridmin"](c)
    return ((c["lane"]=="priced" and gm=="77-80") or
            (c["edge"]>=8 and c["region"]=="RoW") or
            (c["tag"]=="unsafe") or
            (c["lane"]=="priced" and 1 <= c["edge"] < 4) or
            (c["lane"]=="priced" and c["cs"] is not None and 0 <= c["cs"] < 4))
print("\n=== UNION of the distinct playable decline profiles, and what is left ===")
for lab, sel in (("moved to declined", in_union), ("playable that remains", lambda c: c["pop"]=="playable" and not in_union(c))):
    t = {"A":[0,0],"B":[0,0]}
    for c in cards:
        if sel(c): t[c["half"]][0]+=1; t[c["half"]][1]+=c["hit"]
    n=t["A"][0]+t["B"][0]; h=t["A"][1]+t["B"][1]
    print(f"  {lab:22} n={n:6}  A {t['A'][1]/t['A'][0]*100:5.1f}  B {t['B'][1]/t['B'][0]*100:5.1f}  all {h/n*100:5.1f}")

# the bettor's grid rule, within each population
for pop in (("playable",),("watch",),("declined",)):
    show(f"BETTOR'S GRID RULE inside {pop[0]}", pop, FEATS["grid"], ["play","skip","unknown"])
