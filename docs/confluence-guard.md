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
