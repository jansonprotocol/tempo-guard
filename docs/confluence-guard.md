# Scoring a card against the searches a human would run on it

**The proposal (1 Sep).** After Athena prints a tip, run it back through
the board's own search bar the way the bettor does by hand: the league's
record on that lane, then the record filtered to each club, to the tip's
side, and to club-and-side. Every slice reading above the league baseline
is a flag for the tip, every slice below it a flag against. Sum the flags
and colour the card — super green through super red — and where the card
comes out badly, pivot to another lane or decline the bet.

The worked example was **Aston Villa v Arsenal, 31 Aug 2026**, which
printed `Tip 1 O1.5 82.0% +2.1% buy≥1.26` and finished **0-1**.

## The one thing that had to change

The board's search bar looks **backwards over completed matches**. A slice
it shows today contains the very fixture you would have been betting on.
Scored that way the rule cannot fail, and the number it produces means
nothing.

`scripts/confluence.py` therefore builds every slice **strictly as-of**,
in one forward pass, scoring all of a date's cards before any of them feed
the counters. A score is only ever computed from cards dated earlier than
the one being scored. Everything below is out-of-sample by construction.

Replayed over the most recent 600 fixtures in each of 57 leagues:
**31,314 cards, 25,494 of them graded on tip 1.**

## The example reproduces

As-of, with only pre-match history:

| slice | as-of | the bettor's hand count |
|---|---|---|
| ENG-PL tip 1 baseline | 83.3% (575) | 83.5% (708) |
| the claim, 82.0, vs baseline | −1.3 | −1.5 |
| + Aston Villa | 86.8% (33/38) | 86.3% (63/73) |
| + Arsenal | 77.1% (27/35) | 72.9% (51/70) |
| + over | 76.4% (191/250) | 79.7% (306/384) |
| + Arsenal + over | 69.2% (18/26) | 67.3% (33/49) |

**Score −2**, the same verdict reached by hand, on a card the shipped
guard marks orange and which lost. The differences are the window — 600
stored fixtures against the board's 708 — and the as-of cut.

## It works, and it is monotone

Scored on **the final pick** — the lane the card actually stars, tip 1 on
most cards and a gated DNB on 394 of them — with a continuous score that
sums the shrunk deviations rather than their signs:

| decile | mean score | n | final pick | older | newer |
|---|---|---|---|---|---|
| 1 | −16.65 | 2,549 | 80.62% | 79.90 | 81.26 |
| 2 | −9.21 | 2,549 | 81.37% | 80.43 | 82.15 |
| 5 | −0.81 | 2,549 | 83.05% | 83.55 | 82.57 |
| 8 | +5.86 | 2,549 | 85.80% | 85.59 | 86.01 |
| 10 | +17.07 | 2,553 | 87.39% | 87.98 | 86.67 |

Ten deciles, monotone bar one wobble, and both time windows agree in
nearly every one. Keeping the top 40% grades **86.06%** against the
83.65% base — older 86.04, newer 86.07, which is as stable as anything
this project has measured.

**So the idea is real.** What follows is why it still should not ship as
proposed.

## The searches are not what is doing the work

Each family of searches, run on its own, scored as the lift from dropping
everything at or below zero:

| searches used | kept | hit | lift |
|---|---|---|---|
| **the claim vs the league baseline, alone** | 6,544 | 87.27% | **+3.81** |
| clubs only | 7,023 | 83.64% | +0.18 |
| side only | 4,160 | 84.69% | +1.22 |
| club × side only | 7,176 | 84.99% | +1.53 |
| everything but the claim | 9,602 | 84.41% | +0.95 |
| **all six, as proposed** | 10,659 | 85.44% | **+1.98** |

The single claim check beats all six together, and adding the club
searches **halves** it. The club slices on their own are worth +0.18 —
noise.

## And the claim check is the printed probability in disguise

Matched selectivity on the final pick:

| keep | confluence score | the claim alone |
|---|---|---|
| top 50% | 85.63% (+1.97) | **86.90% (+3.24)** |
| top 40% | 86.06% (+2.40) | **87.45% (+3.79)** |
| top 25% | 86.63% (+2.98) | **88.61% (+4.96)** |

And where the two disagree, both keeping the top 40%:

| | n | hit | vs base |
|---|---|---|---|
| both keep | 5,413 | 88.58% | +4.93 |
| **score keeps, claim drops** | 4,785 | **83.20%** | −0.46 |
| **claim keeps, score drops** | 4,785 | **86.17%** | +2.51 |
| both drop | 10,511 | 80.18% | −3.47 |

The claim is right when they disagree. Normalising it against the league
baseline does not rescue it either — head-to-head on tip 1, `p1 ≥ 0.84
but rel < +2` grades 85.84% while `rel ≥ +2 but p1 < 0.84` grades 83.78%,
barely above the 83.46% base.

What survives is a **residual**, the score's effect measured inside claim
deciles so the claim is held roughly fixed: **+1.34 points overall, +2.14
in the older window and +0.90 in the newer.** Real, positive in both
halves, and a third of what the headline table suggests.

## It cannot promote a red card

Guard tier × score, tip 1 hit rate:

| tier | ≤−2 | −1 | 0 | +1 | ≥+2 |
|---|---|---|---|---|---|
| green | 84.8 | 85.9 | 86.2 | 89.0 | 88.8 |
| orange | 81.9 | 81.9 | 83.0 | 83.3 | 85.8 |
| **red** | **79.4** | 76.8 | 76.1 | 78.7 | **76.2** |

Inside green and orange the score adds about four points. **Inside red it
inverts.** Letting a now-red card through on a good confluence score would
make that bucket worse, not better. The score can demote; it cannot
rescue.

## No pivot beats standing the pick

On the cards the score condemns, every lane graded on the same fixtures:

| condemned at | n | stand | → tip 1 | → tip 2 | → tip 3 |
|---|---|---|---|---|---|
| score ≤ −1 | 9,689 | **81.61%** | 81.47% | 68.36% | 77.92% |
| score ≤ 0 | 15,331 | **82.22%** | 82.07% | 68.29% | 78.17% |
| score ≤ +1 | 19,706 | **82.85%** | 82.66% | 68.75% | 78.87% |

Restricted to cards where a tip 3 actually exists (6,719 at score ≤ 0):
standing grades 81.62% against tip 3's 78.17%, and that holds in both
windows (80.72/79.42 older, 82.44/77.02 newer).

**So "pivot to tip 2, or tip 3 if available" is measurably wrong.** The
only alternative that beats standing is NONE — and the dropped cards
still grade 82.22%, so declining them is a real cost, paid for only if
the price says so.

## Priced, it does not reach money

5,981 tip-1 rungs settled at real maximum closing prices:

| | n | hit | ROI |
|---|---|---|---|
| all | 5,981 | 82.2% | −1.86% ± 0.52 |
| drop score ≤ 0 | 2,189 | 84.1% | −1.51% ± 0.84 |

Two windows: older −1.25% → **−0.15%**, newer −2.47% → **−2.84%**. The
gain is one window and the other gets worse. Identical in shape to the
risk guard measured the same day: **hit rate separates, the price absorbs
it.**

Tip 2 carries the largest residual (+2.05 inside claim deciles, positive
in both windows) and **cannot be priced at all** — football-data has no
team-total columns. That lane's money question stays open.

## Does a deeper bank help? No

The bettor's own caveat: a card from a year ago had only half a season of
look-back, a recent one has two seasons, so the tank should be filled.
The walk records exactly how much history stood behind each score, and
the answer runs the other way.

Final-pick spread, low score against high, by history at scoring time:

| club history | n | low | high | spread |
|---|---|---|---|---|
| 0–4 cards | 5,098 | 79.13% | 86.03% | **+6.90** |
| 4–10 | 5,098 | 81.21% | 84.23% | +3.02 |
| 10–17 | 5,098 | 83.37% | 85.76% | +2.39 |
| 17–27 | 5,098 | 82.11% | 84.90% | +2.79 |
| 27–119 | 5,102 | 83.26% | 86.55% | +3.29 |

The same shape holds inside each time window separately, so it is not
merely that early cards are also old cards. Beyond about ten cards of club
history the effect is **flat**, between +2.4 and +3.3, and the sharpest
separation is in the thinnest bucket — where the club slices cannot vote
at all and the score reduces to the claim. That is the same finding as
the ablation, arriving from a different direction.

**One hypothesis tested and rejected along the way.** If thin slices score
better because they are RECENT — a four-card club history is the last four
matches, a 119-card one averages across a different squad and manager —
then the slices should be windowed. Swept on the final pick, they should
not be: cumulative +2.17, last-8 +1.85, last-12 +1.64, last-20 +1.78,
last-30 +1.74, last-40 +1.86, last-60 +1.96. Every window is worse than
remembering everything. Staleness is not the mechanism.

## Verdict

The proposal is a genuine signal and almost all of it is a signal the card
already prints. What is left after the claim is held fixed is **+1.34
points of hit rate**, halving between time windows, unable to promote a
red card, with no pivot worth taking and no ROI behind it.

That is worth a **fourth input to the existing guard**, weighted small and
one-directional — it demotes, it never promotes. It is not worth a new
five-colour system, it does not justify rerouting a condemned pick to
tip 2 or tip 3, and it gives no reason to enlarge the bank.

---

# Corrections and the deeper run (1 Sep, later)

Three things above needed fixing, and the bank was doubled to settle the
last of them.

## The ablation table was not selectivity-matched

It set "the claim alone, +3.81" beside "all six, +1.98" as though those
were comparable. They are not: the claim row keeps **6,544** cards
(25.7%) and the six-search row keeps **10,659** (41.8%). A stricter rule
buys hit rate for free, so that pairing does not support the weight put
on it. The bettor caught this.

**The six-search line is a real result on its own terms**: 41.8% of cards
kept at 85.44% against an 83.46% base, older 85.53 and newer 85.34.

## And the score IS additive to the claim

Ranking both the same way and selecting exactly the same NUMBER of cards:

| w on score | keep 40% | keep 25% | keep 15% |
|---|---|---|---|
| 0.0 — claim alone | 87.45 | 88.61 | 88.83 |
| 0.2 | 87.38 | **88.78** | **89.36** |
| 0.5 | **87.55** | 88.44 | 89.12 |
| 1.0 — score alone | 86.05 | 86.63 | 87.00 |

The optimum is w ≈ 0.2–0.5, not 0. And inside the claim's own top 40% —
cards a probability filter already likes — sorting by score gives
85.41 → 86.94 → 88.47 → 88.98 across quartiles, monotone, both windows.
So the earlier verdict that the searches "add nothing the guard doesn't
already have" was too strong. They add a little, as a refinement rather
than a replacement.

## The six searches double-count, and fixing it helps

In a league that leans one way — Serie A, where nearly every pick is an
under — "Pisa" and "Pisa on unders" are the SAME thirty cards. Two of the
six checks are one measurement double-weighted, and the double weight
lands on whichever club is extreme.

Making the crossing report a **residual** — club × side measured against
what the club's own rate already said, rather than against the league —
removes the overlap by construction. On the shallow bank it lifted the
score-alone version (86.63 → 87.12 at keep-25%) and, more usefully, cut
its cross-window gap from 1.38 points to 0.09.

## The deeper bank: the effect is about HALF what it looked

The bettor's own caveat deserved a direct test, so the replay was rerun
at **1,500 fixtures per league** — **68,341 cards, 62,528 graded**,
roughly double the first pass and about four seasons instead of one and a
half.

| | shallow (25,494) | deep (62,528) |
|---|---|---|
| base | 83.65% | 83.70% |
| claim alone, keep 40% | 87.45% | 87.16% |
| best blend, keep 40% | 87.55% (w=0.5) | 87.23% (w=0.5) |
| **gain from the score** | **+0.10** | **+0.07** |
| best blend, keep 25% | 88.78% (+0.17) | 88.17% (+0.27) |
| **quartile spread inside claim top 40%** | **+3.57** | **+2.08** |

Everything survives in sign and nothing survives in size. The quartile
spread — the cleanest evidence the score sorts what the claim cannot —
falls from 3.57 points to 2.08. The de-duplicated version stays monotone
across the four quartiles (86.23 → 86.53 → 87.57 → 88.31) where the
as-proposed version does not (86.47 → 86.13 → 87.36 → 88.68).

**So filling the tank did not strengthen the finding — it shrank it.**
That is the honest answer to the question that prompted the run, and it
is worth more than the run confirming what the smaller sample said.

Depth per card behaves the same way as before, with no upward trend:
club history 0–6 gives a spread of +5.36, 6–15 gives +3.14, 15–27 gives
+2.08, 27–50 gives +3.57, 50–261 gives +2.37.

## The three selections, priced

Settled at real maximum closing prices across football-data's 16 European
leagues, on the FINAL PICK — ladder rungs through a mu fitted to the
book's 2.5 line, DNBs derived exactly from its 1X2. Thresholds are set on
the whole scored population and then applied, so the selection cannot
refit itself to the leagues that happen to carry odds.

**8,121 final picks priced, 212 of them DNBs.**

| selection | n | odds | hit | ROI |
|---|---|---|---|---|
| everything priced | 8,121 | 1.16 | 81.8% | −1.67% ± 0.46 |
| **1. keep top 40% by claim** | 2,566 | 1.13 | **86.2%** | −1.57% ± 0.74 |
| **2. keep top 15% blended** | 245 | 1.09 | 85.7% | **−0.96% ± 2.03** |
| **3. claim top 40%, best score quartile** | 433 | 1.11 | 86.1% | **−2.44% ± 1.72** |

Two windows:

| selection | older | newer |
|---|---|---|
| everything | −0.97% | −2.37% |
| 1. claim top 40% | −1.69% | −1.43% |
| 2. blended top 15% | −1.43% | −0.57% |
| 3. best quartile | −3.28% | −1.83% |

**This is the whole project's finding in four rows.** Selection 1 raises
the hit rate from 81.8% to 86.2% — four and a half points, exactly what
the score and claim were built to do — and moves ROI by **ten basis
points**, from −1.67% to −1.57%. The average price falls from 1.16 to
1.13 at the same time. The market takes the entire gain as a shorter
quote.

Selection 3 is the sharpest version of the lesson: adding the score on
top of the claim holds hit rate at 86.1% and makes ROI **worse**, −1.57%
to −2.44%, in both windows. The extra selectivity picks shorter prices
than it picks winners.

Selection 2 is the only one near break-even at −0.96%, and it keeps 245
of 8,121 priced cards with a standard error of ±2.03. That is three plays
in a hundred at a precision that cannot tell −0.96% from +3%. It is a
lead, not a result.

## Verdict, after the corrections

Replacing the one written above, which was drawn before the
selectivity-matched tests and the deeper bank.

**The idea is sound and it is small.** The score sorts cards the printed
claim cannot tell apart — 2.08 points across quartiles inside the claim's
own top 40%, monotone, on 62,528 cards — and blended at w ≈ 0.2–0.5 it
beats the claim alone at matched selectivity by 0.07 to 0.27 points. The
de-duplicated slice set is the one to use: same signal, far steadier
across time windows.

**It is worth less than it first appeared.** Doubling the bank halved the
effect. Anything built on it should be sized for +2 points of sorting
power, not the +3.6 the first pass suggested.

**And it does not reach money.** Priced on 8,121 final picks, the best
claim-based selection converts 4.4 points of hit rate into 0.1 points of
ROI, because the average quote falls from 1.16 to 1.13 in step. Adding
the score on top makes ROI worse in both windows.

What it should be: a **small, one-directional fourth input to the risk
guard** — it demotes, it never promotes, and it cannot rescue a red card.
What it should not be: a five-colour system, a reroute rule to tip 2 or
tip 3, or a reason to enlarge the bank.

---

# Re-checks: the effect is European, and the red inversion was noise

A fresh read of everything above found four claims resting on cells too
small or too narrow to carry them. Three were re-run on the deep bank
with the refined scorer. Two of them changed.

## The red-tier inversion does not survive

The doc said, in its strongest sentence, that inside the guard's red tier
the score **inverts** — 79.4% at score ≤−2 against 76.2% at ≥+2. That came
from cells of roughly 420 and 1,630 cards, on the shallow bank, with the
sign score, and it was never re-run. Everything else halved when the bank
doubled.

Re-run on 62,528 cards with the de-duplicated continuous score, by score
quartile within each tier:

| tier | n | overall | Q1 (worst) | Q2 | Q3 | Q4 (best) | spread |
|---|---|---|---|---|---|---|---|
| green | 16,296 | 87.81% | 87.24 | 86.60 | 88.39 | 89.00 | +1.77 |
| orange | 36,607 | 83.42% | 81.64 | 83.02 | 83.46 | 85.56 | +3.92 |
| **red** | 9,625 | 77.80% | 76.97 | 76.60 | 78.35 | **79.27** | **+2.29** |

**There is no inversion.** Red sorts in the normal direction, +2.29, and
positive in both windows (+3.03 older, +1.40 newer). The earlier figure
was noise in a thin cell, and the doc stated it as fact.

**The advice does not change, but the reason does.** A red card with the
best possible score grades **79.27%** — still below orange's *worst*
quartile at 81.64%, and far below green's 87.81%. So promotion remains
unjustified, not because a good score means nothing inside red, but
because **red's ceiling sits under orange's floor**. That is a claim about
levels, which the data supports, rather than about direction, which it
does not.

## Era was the wrong cut — the effect is regional

Two era readings in this file disagree in sign: the shallow bank gave
+2.14 older against +0.90 newer, the deep bank +1.48 older against +3.04
newer. An unstable confound usually means the real variable is something
else.

It is not league mix — the region shares barely move between halves
(Europe 72.0% of the older half, 68.9% of the newer). It is region
itself. Quartile spread inside the claim's top 40%, refined scorer:

| slice | n | Q1 | Q2 | Q3 | Q4 | spread |
|---|---|---|---|---|---|---|
| ALL | 62,528 | 86.04 | 86.29 | 87.76 | 88.55 | +2.52 |
| **Europe** | 44,043 | 85.24 | 85.99 | 86.60 | 88.81 | **+3.57** |
| — older | 22,021 | | | | | +1.82 |
| — newer | 22,022 | | | | | +3.68 |
| **Americas** | 12,556 | 87.17 | 87.97 | 88.37 | 87.11 | **−0.06** |
| — older | 6,278 | | | | | −0.41 |
| — newer | 6,278 | | | | | +1.49 |
| RoW | 5,929 | 87.16 | 90.37 | 89.86 | 91.43 | +4.27 |
| — older | 2,964 | | | | | +5.77 |
| — newer | 2,965 | | | | | +1.75 |

**The score is a European-leagues effect.** +3.57 in Europe, positive in
both windows. **Exactly zero in the Americas** — −0.06 overall, −0.41 in
one half. RoW reads +4.27 on 5,929 cards, which is about 300 per
quartile-half, and swings +5.77 to +1.75; that is a sample too small to
carry a claim either way.

This is the cut that should have been made first, and it changes what the
finding is: not "a card can be scored against its own searches" but "a
card in a European league can be."

## Which undercuts selection 2 rather than supporting it

The blended top-15% selection kept only 245 of 8,121 priced cards, and the
obvious reading was that it concentrates in leagues football-data cannot
see — the same territory as the open tip-2 question, and therefore a
lead worth chasing.

Region composition says otherwise:

| region | share of the bank | share of the blended top 15% |
|---|---|---|
| Europe | 70.4% | **52.8%** |
| Americas | 20.1% | 23.4% |
| RoW | 9.5% | **23.8%** |

RoW is **two and a half times over-represented** in that selection, and
RoW is precisely where the score does not work stably. Selection 2's
−0.96% is therefore drawn disproportionately from the population where the
signal is weakest, on 245 cards at ±2.03. It is not a lead. It is what a
noisy sub-population looks like when it lands on the favourable side.

## Club memory: neutral, and the earlier harm explained

The proposal that the club slices should forget — squads and styles change
— was swept properly, windowing **only** the club reads and leaving the
league and side reads cumulative:

| club memory | score alone (k25) | best blend (k25) | quartile spread |
|---|---|---|---|
| cumulative | 86.62 | 88.13 | +2.08 |
| last 20 | 86.69 | 88.10 | +2.37 |
| last 30 | 86.60 | 88.16 | +2.29 |
| last 40 | 86.51 | 88.13 | +2.52 |
| last 60 | 86.57 | 88.12 | +2.12 |
| last 365 days | 86.66 | 88.12 | +2.21 |
| last 550 days | 86.59 | 88.16 | +2.04 |
| last 730 days | 86.65 | 88.10 | +2.07 |

Everything within about 0.15 points. **Club memory is neutral.** Last-40
does edge cumulative in both windows (+1.48 against +1.28 older, +3.04
against +2.47 newer) but by less than the standard error on the cells, so
it ships as a **reasoned default** — it never loses and the mechanism is
real — and not as a measured gain.

**It does explain the earlier result, though.** An earlier sweep found
windowing actively harmful (last-30 at +1.74 against cumulative +2.17).
That sweep windowed *every* slice, including "ENG-PL on overs" — a league
statistic built from hundreds of cards, which has no staleness problem and
every reason to want all of them. With the league reads left alone the
harm disappears. The earlier test was measuring the wrong thing, and the
conclusion drawn from it — "staleness is not the mechanism" — was not
supported by it.

`walk_best` in `scripts/confluence.py` now carries all three refinements,
so none of this lives only in a scratch file.

## Two smaller corrections

**"As stable as anything this project has measured"**, said of the
86.04/86.07 window split, was luck of the shallow cut. The same selection
on the deep bank splits 86.40/87.85.

**The blend's best `w` is read off the table**, which is a free parameter
chosen after seeing the answer. Live expectation should be quoted at the
pre-registered **w = 0.2**, not at whichever row happens to top its column.

---

# The frozen spec: two layers, five labels

Everything above is measurement. This is the design it licenses, written
down before anything is built so the live period has something to be
graded against.

## Architecture: the chooser flips, the guard only demotes

Two layers, each one-directional.

**Layer 1 — the chooser (unchanged).** Star tip 1 unless a DNB out-claims
it by more than `DNB_GATE` = 2.0 points, in which case star tip 3. This is
the only validated flip in the project and it stays where it is.

**Layer 2 — the guard.** Takes the starred lane and labels it. Its action
space is **PLAY or NO PLAY**. It never re-picks a lane.

Putting the flip in layer 1 and keeping it out of layer 2 is not tidiness,
it is what the pivot table forces: on cards the score condemns, standing
grades 82.22% against tip 3's 78.17% *even where a tip 3 exists*, both
windows, and tip 2's 68.29%. **There is no lane to flip to.** A guard that
offered one would be offering a measured loss.

## Why promotion is impossible, and now verified

The five labels only make sense if the score stratifies *within* a tier
and never lifts a card past a tier boundary. Red's ordering was checked
earlier; green's and orange's were not. Both windows, by score quartile:

| window | tier | Q1 (worst) | Q4 (best) |
|---|---|---|---|
| both | green | 86.84 | 89.00 |
| both | orange | 81.75 | 85.30 |
| both | red | 77.51 | 79.52 |
| older | green | 86.93 | 88.80 |
| older | orange | 81.55 | 84.81 |
| older | red | 77.03 | 80.82 |
| newer | green | 86.94 | 89.31 |
| newer | orange | 81.49 | 85.63 |
| newer | red | 77.74 | 78.07 |

**orange's best < green's worst, and red's best < orange's worst, in
every window.** The tightest margin is the older window's red best (80.82)
against orange worst (81.55) — 0.73 points, holding but not comfortably.

So a card cannot score its way up a tier. Promotion is ruled out by the
structure of the data, not by a rule someone imposed.

## The labels, and their frozen thresholds

The score speaks **only in Europe** (+3.57 there, −0.06 in the Americas,
unreadable in RoW), so outside Europe the label is the tier and no card
receives a super-label. Thresholds are set on the European population and
then **frozen** — recomputing percentiles nightly would make the labels
drift and nothing falsifiable.

```
score      walk_best: continuous, de-duplicated, club memory 40,
           MIN_SLICE 15, prior 25, league and side reads cumulative
blend w    0.20   (the pre-registered row, not the table's best)

super green   green tier AND (gated DNB OR score >= +6.34)
green         green tier, otherwise
orange        orange tier
red           red tier, otherwise
super red     red tier AND score <= -12.99
```

Gated DNBs are assigned to super green **by the gate, not by rank**. They
grade **92.27%** on 1,125 cards, and their printed claim understates them
by 4 to 8 points, so any rank built on `says` would file them mid-orange.
That would be the scoring machinery contradicting a validated result.

## What each label is predicting

This table is the guard's own `says`. It is what the live period grades
against.

| label | n | share | hit | older | newer |
|---|---|---|---|---|---|
| **super green** | 3,083 | 4.9% | **89.56%** | 89.14 | 90.10 |
| green | 13,213 | 21.1% | 87.40% | 86.63 | 88.25 |
| orange | 36,607 | 58.5% | 83.42% | 83.28 | 83.55 |
| red | 7,873 | 12.6% | 77.96% | 78.43 | 77.49 |
| **super red** | 1,752 | 2.8% | **77.05%** | 76.08 | 77.93 |

Monotone in both windows across all five.

## One thing the design got wrong, and the fix

**Super red does not earn its own action.** It grades 77.05% against
plain red's 77.96% — nine tenths of a point. Dropping super red alone
lifts the book from 83.70% to 83.89%, forfeiting 2.8% of cards that still
win three times in four. The score simply has little left to say inside
red: the tier already identified the bad population, and its internal
spread (+2.29) is the smallest of the three.

So **NO PLAY attaches to the RED TIER, not to super red.** That is 15.4%
of cards at about 77.8%, and it is the cut that was already measured as
the guard's best move. Super red stays as *emphasis within* red — "the
tier says avoid and the searches agree" — not as a separate decision.

The action table:

| label | action |
|---|---|
| super green | play, largest stake band |
| green | play |
| orange | play |
| red | **no play** |
| super red | **no play**, and do not be tempted |

## Tip 2 is the stronger signal, and it is still unpriceable

The region split was run on tip 2 as well, and it is the largest
confluence effect in the project:

| region | n | spread | older | newer |
|---|---|---|---|---|
| ALL | 46,301 | +5.27 | +3.93 | +6.26 |
| **Europe** | 34,008 | **+6.07** | +5.30 | +7.25 |
| **Americas** | 7,829 | **+3.92** | +6.19 | +3.95 |
| RoW | 4,464 | +0.28 | −9.42 | +10.76 |

**+6.07 in Europe against tip 1's +3.57, and it survives in the Americas
too** — where tip 1's effect is exactly zero. Only RoW is noise, and
violently so.

This is the one place a paid experiment is now clearly justified. Tip 2 is
a team total, football-data carries no team-total columns, and the signal
is strongest in the most liquid region rather than the thinnest — so the
"soft market" framing does not apply and the question is simply whether
the price allows it. A coverage probe of team-total markets on the odds
API is the next spend, ahead of any further ladder work.

Note what tip 2 is *not*: a lane to flip to. Its base rate is 69% against
the final pick's 83.7%, and the pivot table already refused it. A tip-2
confluence signal would be a **separate ticket**, priced on its own, not a
replacement for a condemned pick.

## Registered, and what would falsify it

Frozen: the thresholds above, `w` = 0.20, club window 40, the tier rules
as shipped, Europe-only scoring, DNBs hard-assigned.

Predicted: the five hit rates in the table above, ±1 point, on forward
cards.

Falsified if: the label ordering breaks on live cards, or super green
fails to clear green, or red fails to underperform orange. The retro
record says all three hold in both halves of 62,528 cards; the live month
is what turns that into a shipped claim rather than a replayed one.

**And none of this is an ROI claim.** Every retro cut converts hit rate
into almost nothing after price — the best claim-based selection moved
ROI ten basis points while the average quote fell from 1.16 to 1.13. The
guard ships as *information with a registered prediction*, and NO PLAY
ships as protocol. Whether skipping red pays is a question only real
forward quotes can answer, because even super red wins three in four and
declining it forfeits those wins.

---

# The rule that was never tested: decline on the PRICE

Everything measured so far selects a card and then looks at what it
costs. The bettor's question inverts that: the guard picks a lane, finds
the minimum odds that lane needs, and if the market will not pay it,
**declines**.

That is a different kind of rule. Every losing rule in this file selected
for a higher hit rate and watched the quote shorten by exactly as much.
A price-conditional rule does not select for hit rate at all — it selects
for cards where **the market has not shortened enough**. It is the first
rule here that hunts a book error rather than an engine strength.

Each label already carries a hit rate, so it implies a break-even price
of 1/hit. Bars learned on the OLDER half only, then applied to the newer:

| label | train hit | break-even |
|---|---|---|
| super green | 89.23% | 1.121 |
| green | 86.62% | 1.154 |
| orange | 83.28% | 1.201 |
| red | 78.35% | 1.276 |
| super red | 76.34% | 1.310 |

The split is cleanly out-of-sample by accident of coverage:
football-data carries two seasons, the deep bank about four, so **all
8,121 priced cards fall in the newer half** and none of them touched the
bars that judge them.

| rule | n | odds | hit | ROI |
|---|---|---|---|---|
| play everything | 8,121 | 1.16 | 81.8% | −1.67% ± 0.46 |
| decline under break-even | 2,597 | 1.29 | 77.1% | −0.91% ± 1.06 |
| decline under break-even +2% | 1,967 | 1.31 | 75.5% | −0.84% ± 1.26 |
| decline under break-even +4% | 1,479 | 1.34 | 74.4% | −0.21% ± 1.51 |
| **decline under break-even +6%** | 1,127 | 1.36 | 73.7% | **+0.83% ± 1.77** |
| **decline under break-even +8%** | 845 | 1.39 | 72.7% | **+1.49% ± 2.10** |
| decline under break-even +12% | 463 | 1.44 | 69.8% | +1.59% ± 3.03 |

**A monotone gradient from −1.67% to +1.59%, crossing zero at about +5%.**
The hit rate FALLS the whole way — 81.8% down to 69.8% — which is the
point. This is the first thing in the project to make money by taking
*worse* bets at *better* prices.

No single positive cell is significant on its own (±1.77 on +0.83). The
gradient across seven thresholds is the evidence, not any one row.

## Two controls, both of which could have killed it

**Does the label do any work, or is it just "longer prices pay"?** Matched
selectivity, price alone against the label rule:

| bar | label rule | plain price, same n |
|---|---|---|
| +4% | −0.21% | −0.29% |
| +6% | **+0.83%** | −0.41% |
| +8% | **+1.49%** | +0.18% |

The label adds about 1.2 to 1.3 points at the tighter bars, and nothing
at +4%. Inside the error bars, but consistent in direction.

**Is it an artifact of the fitted mu?** Ladder prices are derived — a mu
fitted to the book's 2.5 line, the rung read off it — and selecting on a
derived price while settling at the *same* derived price biases returns
upward wherever the fit overestimates. If that were the mechanism, the
effect would be strongest in the lanes furthest from 2.5.

| distance from 2.5 | n | all | kept at +6% |
|---|---|---|---|
| 0.6–1.1 | 2,298 | −2.53% | **+1.48%** |
| 1.1–1.6 | 1,990 | −3.42% | (none kept) |
| 1.6+ (heavy extrapolation) | 3,615 | −0.63% | **−4.53%** |

The opposite of the artifact signature: positive in the moderate band,
**negative** where extrapolation is worst. The control passes.

## But most of it is the DNB lane

Splitting the priced set by how its price was built:

| | n | odds | hit | ROI |
|---|---|---|---|---|
| **ladder rungs only** | 7,909 | 1.16 | 81.8% | −1.88% |
| ladder, decline +6% | 1,070 | 1.36 | 73.8% | **+0.03%** |
| ladder, decline +8% | 799 | 1.39 | 73.0% | +0.69% |
| **DNBs only** | 212 | 1.16 | 82.1% | **+6.05% ± 1.98** |
| DNBs, decline +6% | 57 | 1.34 | 71.9% | **+15.92% ± 4.88** |

**The ladder reaches break-even and stops.** −1.88% to about +0.3%, well
inside its error bars. Worth having, not worth celebrating.

**The DNB lane is the finding.** +6.05% before any price bar at all, on
prices that are EXACT rather than fitted — derived arithmetically from the
book's 1X2 with no Poisson step anywhere. And it splits clean:

| | n | hit | ROI |
|---|---|---|---|
| older half | 106 | 84.9% | +6.14% ± 3.07 |
| newer half | 106 | 79.2% | +5.96% ± 2.52 |

Two halves, six-tenths of a point apart. Behind it: the market prices
these at an implied 86.2% of decisive matches and Athena wins **92.0%**
of them — the same underclaim the DNB lane was found to have on 30
August, now showing up as money at real quotes.

It also explains the super-green cell: 65 of its 90 kept cards are DNBs.

## The caveat that decides what happens next

**The DNB price here is synthesised, not quoted.** It is built from the
best home price and the best away price across every book, which assumes
both can be taken at once and carries no DNB-market margin of its own.
A real draw-no-bet quote from a single book will be worse — possibly much
worse, since DNB is a secondary market.

So the honest statement is: **at fair synthetic prices the gated DNB lane
returns +6%, two windows, on exact arithmetic.** Whether it survives a
real quote is unmeasured, and it is now the single most valuable thing
the odds API could answer — `draw_no_bet` is a market it carries, and
212 historical bets is a smaller ask than any backfill considered so far.

That question outranks the team-total probe. Tip 2's signal is larger in
hit-rate terms, but tip 2 has never shown a positive return at any price;
the DNB lane just did.

---

# The go/no-go: is the synthetic DNB price reachable? (1 Sep)

The +6.05% on 212 gated DNBs was settled at a price we BUILT — the best
home quote and the best away quote across every book, combined
arithmetically — which assumes both legs are takeable at once and carries
no draw-no-bet margin of its own. If a real quote is 6% worse the edge is
gone. That single unknown gated everything else.

It needs no historical data and no settlement, only today's board: fetch
`draw_no_bet` and `h2h` for upcoming fixtures in one request, rebuild the
synthetic price from the same event, and compare. **60 credits, 24
fixtures, 48 comparable prices.**

## The gap widens with the price, and we bet the short end

| synthetic band | n | mean gap | median | worst |
|---|---|---|---|---|
| **1.00–1.25** | 5 | **−1.81%** | −2.27% | −2.38% |
| **1.25–1.45** | 10 | **−2.85%** | −2.90% | −4.51% |
| 1.45–1.80 | 7 | −3.50% | −3.61% | −4.00% |
| 1.80–2.60 | 8 | −3.71% | −3.76% | −5.42% |
| 2.60+ | 18 | −6.07% | −6.56% | −16.30% |

**Every one of the 48 real quotes is worse than the synthetic** — no book
gives the arbitrage away. But the shading is not flat: books shade their
draw-no-bet line hardest on longshots and barely at all on short
favourites, which is exactly where the gated DNBs live. **Our 212 bets
averaged 1.160.**

## Which leaves the edge intact, at about +3.7%

| gap applied | P | ROI |
|---|---|---|
| synthetic, as backtested | 1.160 | +6.04% |
| all prices pooled, mean −4.19% | 1.111 | +2.05% |
| **the band we bet (1.00–1.45), mean −2.50%** | **1.131** | **+3.66%** |
| the band we bet, median −2.34% | 1.133 | +3.81% |

Break-even sits at P = 1.086, a gap of **−6.3%**. **Of the 15 short-price
quotes, none were worse than that** — the worst in the 1.00–1.25 band was
−2.38%, leaving nearly four points of headroom.

**The go/no-go passes.** The gated DNB lane is playable at real quotes at
roughly +3.7%, down from +6.04% but clearly above zero.

## What the probe also found, which constrains how it is played

**Liquidity is thin.** Draw-no-bet is quoted by **1 to 4 books** per
fixture against 7 to 13 for the match odds. That is a stake-size
constraint, not a price problem, but it is real.

**Two leagues on our board carry no DNB market at all** — NED-ED and
POR-PL, 6 of the 30 fixtures probed. A gated DNB there cannot be played
as a DNB.

## The residual uncertainty, stated plainly

The backtest's synthetic came from football-data's **maximum CLOSING**
1X2 across its book panel. This probe's synthetic came from the odds
API's EU books at a pre-match moment. If football-data's max-closing runs
higher than the odds API's best, the true haircut against the backtested
price is **larger** than the −2.50% measured here, and this estimate is
optimistic by that difference.

The headroom absorbs a fair amount of it — break-even needs −6.3% and the
in-band worst case was −4.51% — but the number to trust is the forward
log, not this one. n is 48 prices on a single day, and the base result is
212 bets.

---

# Should a declined card fall back to another lane? (1 Sep)

The bettor's rule, from the Swansea card: tip 1 is struck on price, so
the DNB is next in line, and if IT clears its own bar the card plays that
instead.

This is not the pivot already rejected. That one flipped when the SCORE
condemned a pick and lost. This one flips only when the PRICE fails and
the alternative's own price clears — and price-conditional selection is
the one thing here that has ever produced a positive return. Two margins
swept separately, because the star's bar rests on a validated label hit
rate and the fallback's only on its own printed claim.

**8,121 priced cards, 2,476 with a priced fallback as well.**

| | n | odds | hit | ROI |
|---|---|---|---|---|
| star bar +6%, alone | 1,008 | 1.35 | 74.8% | **+1.71%** |
| star +6%, else fall back at +0% | 1,741 | 1.48 | 60.8% | −0.49% |
| star +6%, else fall back at +6% | 1,550 | 1.49 | 61.9% | −0.62% |
| **the fallback bets alone** | 733 | 1.66 | **41.5%** | **−3.53%** |

The fallback destroys the only positive result in the project. And
loosening its bar HELPS — −3.53% at +0% against −6.35% at +12% — which is
the tell.

## Why: the claim is not a rate

The fallback DNBs **claim 72.2% and hit 41.5%**. Sliced by how far the
price sits above what that claim demands:

| price over break-even | n | odds | claims | hit | ROI |
|---|---|---|---|---|---|
| +0–5% | 165 | 1.40 | 73.1% | **52.7%** | +1.03% |
| +5–15% | 256 | 1.52 | 72.4% | 44.1% | +0.09% |
| +15–35% | 207 | 1.72 | 72.0% | 37.2% | −5.01% |
| +35%+ | 105 | 2.26 | 70.7% | **25.7%** | −16.60% |

The claim is flat at about 72% the whole way down while the truth
collapses from 52.7% to 25.7%. Monotone, and in both windows (−3.82%
older, −3.24% newer).

**This is the anti-predictive property in its purest form.** A bar of the
shape "price ≥ 1/rate × margin" selects cards where the market disagrees
with `rate` most. If `rate` is a validated hit rate, that finds market
error. If `rate` is the engine's own claim, it finds ENGINE error — and
the harder the bar, the more of it you buy.

Same arithmetic, opposite sign, and the only difference is whether the
number in the denominator has been measured:

* star bar, from the label's validated rate → **+1.71%**
* fallback bar, from the card's own claim → **−3.53%**

That is why the card marks those lanes `claim-based`. It is not a
cosmetic caveat; it is the whole distinction between finding the market's
mistake and finding your own.

## Two corrections this run produced

**The shipped rule is better than previously reported.** The +0.83%
quoted for the star bar at +6% included red-labelled cards. The card
already refuses those, and excluding them as it does gives **+1.71% on
1,008 bets**. The earlier figure understated what ships.

**The fallback is refused, and the card should keep showing it.** The
per-lane verdicts stay — a reader is entitled to see that the DNB was
4.4% short — but the guard must never promote a fallback to a play. What
the card shows and what it recommends are different things, and this is
one of the places they have to stay different.

## How often it fires, and where the edge really sits

A figure of "one or two qualifying bets a week" was quoted earlier in
conversation and it was wrong by about seven times: it was the gated-DNB
rate, attached by mistake to the whole rule.

Over **468 distinct match days** in the 16 priced leagues, **1,008 of
8,121 cards clear the bar — 12.4%, or 2.15 plays per match day**, call it
fifteen a week. DNBs clearing the bar are 57 over the same 468 days,
which is about **0.85 a week**, and that is the number the original claim
belonged to.

The composition matters more than the rate:

| slice | n | share of plays | odds | hit | ROI |
|---|---|---|---|---|---|
| everything that clears | 1,008 | 100% | 1.35 | 74.8% | +1.71% ± 1.82 |
| **the DNBs** | 57 | **5.7%** | 1.34 | 71.9% | **+15.92% ± 4.88** |
| **the ladder rungs** | 951 | **94.3%** | 1.35 | 75.0% | **+0.86% ± 1.91** |

**5.7% of the bets carry 53% of the profit.** The ladder, which is 94% of
what would actually be placed, returns +0.86% against an error bar of
±1.91 — not distinguishable from zero. And the DNB's +15.92% is at
synthetic prices; the measured in-band haircut of −2.5% takes it to
roughly +13%, on 57 bets.

So the rule has two honest modes, and they are not variations of each
other:

**Narrow** — DNBs only, when they clear. About one bet a week, the
strongest measured edge in the project, and a sample small enough that no
single month can confirm or refuse it.

**Wide** — everything that clears. Fifteen a week and a +1.71% headline,
but mostly ladder bets whose own return is statistically zero. Its real
argument is that it fills the forward log fifteen times faster, and the
forward log is the only thing that will settle any of this.

The two can be run together: **stake narrow, log wide.** The card already
labels every lane and the log already stamps every quoted one, so the
wide record accumulates whether or not it is backed.

---

# Should each league face its own bar? (1 Sep)

A fair question, since the engine is per-league almost everywhere — its
own goal mean, tempo factor, floor, shrink, and for seven leagues a
consensus cap — while the DECISION layer judges all 56 competitions
against one pooled label rate and one 6% margin. Serie A's final pick
grades about a point above the pooled rate, so on the face of it its
orange cards are being asked for more price than they need.

Fixed the way the confluence score fixes its club slices: shrink the
league's own record toward the pooled rate rather than replacing it. Two
variants — **cell** (the league's record for that label) and **offset**
(the league's overall deviation, applied to every label) — with every
rate learned on the TRAIN half and applied to the TEST half.

**Matched to the same 1,008 bets, so this is not the volume effect:**

| rule | n | odds | hit | ROI | older / newer |
|---|---|---|---|---|---|
| **POOLED (shipped)** | 1,008 | 1.35 | 74.8% | **+1.71%** | +0.62 / +2.80 |
| cell, prior 100 | 1,007 | 1.34 | 74.5% | +0.64% | +0.58 / +0.69 |
| cell, prior 400 | 1,008 | 1.35 | 74.2% | +0.65% | +0.79 / +0.52 |
| offset, prior 200 | 1,007 | 1.34 | 74.1% | +0.07% | +0.33 / −0.20 |

And the swap is blunt. At prior 400 the two rules share 88% of their
bets; of the 119 they disagree on, the ones per-league **adds** return
−1.78% and the ones it **drops** return +7.17%.

## Why it fails

The per-league bar LOWERS the requirement where a league historically
hits well — Serie A 1.273 → 1.225 — and raises it where it does not. But
a league that hits well is one **the market also knows hits well**, and
its prices are already short. So the adjustment admits short-priced bets
in efficiently-priced markets and excludes the long-priced ones that
carried the value.

**The bar works because it is uniform and the market is not.** It does
not judge the match; it judges the price against a FIXED reference, and
market disagreement is the entire signal. Moving the reference toward the
league's own record cancels the thing being measured.

## The caveat that stops this being a clean rejection

Pooled's advantage is one window. Older: pooled +0.62 against cell-400's
+0.79, where per-league is BETTER. Newer: +2.80 against +0.52. The whole
gap sits in the recent half, which is the pattern that has killed six
hypotheses in this file, and it does not get an exemption here.

So the honest verdict is **no evidence per-league helps**, not "per-league
is worse". Pooled stays because it is simpler and already shipped, not
because it won.

## Where per-league fairness already lives

Worth stating, because the concern behind the question is right even
though the fix is not. By the time a card reaches the bar, Serie A has
passed through its own goal mean, a `tempo_factor` of 0.40 against the
0.50 default, a floor raised to 0.78, the relative-overreach debit
measured against ITS OWN base rate, and the consensus cap — one of only
seven leagues that gets it. The confluence score then reads Serie A's own
league baseline, its own side split (1,295 of 1,483 cards are unders,
hitting 87.6% against overs' 76.7%), and its own clubs.

A Bundesliga card and a Ligue 1 card are already treated differently
everywhere that judges the MATCH. The price bar is the one place they
should not be, because there the reference has to stay fixed for the
comparison to mean anything.
