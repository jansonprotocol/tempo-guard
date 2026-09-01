# The odds layer — a draft, not a build

**Status: proposal.** Nothing here is shipped, and no odds data is in the
repo yet. This is the design to argue with before any of it is written.

The premise: Athena decides *what is likely*, and has never seen a price.
That stays true. This layer sits after the engine, after the chooser, and
answers one question the engine is deliberately unable to answer — **is
the market paying enough for this to be worth backing?** It can only ever
downgrade a play or move it to another lane. It can never create one, and
nothing it computes may ever flow back into a probability.

Today `playable` means "Athena thinks this lane has value **if** the price
is there". This layer would produce the line that says whether the price
actually is.

---

## 1. What is obtainable

`football-data.co.uk` is reachable from this environment and publishes
free historical closing odds, per season, as CSV. It comes in two shapes,
and the difference matters.

**Main files** (`mmz4281/<season>/<div>.csv`) — 22 of our competitions:

- 1X2 opening and closing, from B365, Pinnacle, William Hill, plus
  market Max and Avg columns
- Over/Under **2.5 only**, opening and closing
- Asian handicap on the *result*, not on goals

**New-format files** (`new/<COUNTRY>.csv`) — 17 more of ours:

- **1X2 closing only.** No totals at all.

Measured against the bank as it stands (29,996 graded rows, 57
competitions):

| | leagues | bank rows |
|---|---|---|
| full odds (1X2 + O/U 2.5) | 22 | 12,816 |
| 1X2 closing only | 17 | 8,045 |
| **covered** | **39** | **20,861 — 70%** |
| no source | 18 | 9,135 |

Uncovered: Saudi, Peru, Chile, Colombia, Paraguay, Brazil Série B, the
Dutch second tier, the African leagues, Ukraine, Croatia, Czechia, and
every UEFA cup. Those cards would simply never receive a verdict, which
is the correct behaviour — silence, not a guess.

Unibet is not a historical source. It would only ever be the live quote
for upcoming fixtures, entered the way prices are entered today.

### A second source — what it can and cannot do

**Cross-validation does not need one.** The main files already carry the
same match priced by Bet365, **Pinnacle**, William Hill, and the market
Max and Avg. Pinnacle is the sharpest book in the file and the natural
reference; disagreement between it and the Avg column is itself a signal,
and a lane where the books disagree wildly is one to distrust. This is
free, already downloaded, and should be the first cross-check built.

**Gap-filling does need one, and there is no free bulk option.** The
`new/` list does not include Saudi, Peru, Chile, Colombia, Brazil Série B
or the Dutch second tier — the leagues we are actually missing. Probed
from this environment:

| source | status | verdict |
|---|---|---|
| the-odds-api.com | 401 without a key | reachable; historical is a paid tier |
| api-sports.io (api-football) | 403 without a key | reachable; covers Saudi, Peru, Chile, Colombia, Brazil B |
| sportmonks | 401 without a key | reachable; paid |
| historicdata.betfair.com | 403 | account required; major leagues only |

So filling the 18 uncovered competitions is a **key decision, not a code
decision**. api-football is the best fit for our particular gaps. Nothing
should be built against it until a key exists, and the layer must work
correctly with those leagues simply absent — silence is a valid verdict.

## 2. The coverage problem, and the way through it

Our lanes are not the lanes the market file carries.

| lane | needs | in the file |
|---|---|---|
| tip 1 ladder (U4.25, O1.5, U3.0…) | totals at arbitrary rungs | **2.5 only** |
| tip 2 team totals | team over/under | **never** |
| tip 2 match rungs | as tip 1 | 2.5 only |
| tip 3 DNB | DNB price | **derivable from 1X2** |
| tip 3 double chance | DC price | **derivable from 1X2** |

Two derivations carry most of the weight.

**Result lanes, exactly.** Strip the overround from (1/H, 1/D, 1/A) to get
vig-free `p_h, p_d, p_a`, then DNB1 = `p_h / (p_h + p_a)`, 1X = `p_h + p_d`,
12 = `p_h + p_a`. These are exact, not approximations, and they are
available on **all 39 covered leagues** including the 1X2-only ones. Tip 3
is the lane the chooser now diverts to, so this is the important half.

**Ladder rungs, by implication.** The 2.5 line fixes the market's own goal
expectation: invert the vig-free P(over 2.5) through the same Poisson the
engine uses to recover a market `mu`, then price *any* rung off it —
U4.25, O1.5, U3.0 — with `market_select.p_win`. That is an approximation
(it assumes the market's goal distribution has the shape ours does), and
it must be validated before it is trusted: compare implied 2.5 back
against the file's own 2.5 as a null test, and where both open and close
exist, check the implied mu is stable between them.

**Team totals have no source and never will.** Tip 2 stays unpriced. Since
the chooser never stars tip 2, this costs the final-pick layer nothing.

### The rung actually bought is the whole line

`U4.25` is an engine rung; the ticket struck against it is `U4.5`, and the
same holds down the ladder (`U3.0` is bought as `U3.5`). The layer prices
**what is bought**, not what is printed — which is convenient, because
whole lines are what the implied-mu derivation prices most cleanly and
they cannot push.

## 3. The thing that must not be got wrong

The obvious rule — *reject anything priced below `buy≥`* — would gut the
book. Measured on the 74 positions in `config/bets.tsv` that sit on a lane
whose `buy≥` was printed:

| | |
|---|---|
| struck **below** `buy≥` | **57 of 74 — 77%** |
| median price against the bar | **−5.6%** |
| p25 / p75 | −8.3% / −1.5% |
| worst | Braga DNB 1.29 against 1.52 (−15%) |

So `buy≥` is not a play bar in practice; it is an aspiration, and three
quarters of a real book lives underneath it. A naive gate keeps 23% of the
volume. At an 83.7% hitrate, volume is most of the edge.

This is the single largest design risk, and it is why the layer must be
**fitted, not assumed**.

## 3a. The volume problem is concentrated, and there is a ladder out of it

The bettor's read — low-tempo leagues pay too little on the under to be
worth backing — is where most of the NO PLAYs would land. It is
measurable without any odds at all, because the bank carries the scores.

Across the **14,649** bank cards whose tip 1 is an under at 4.0 or above
(average 2.65 goals, tip 1 claiming 85.1%):

| line bought | hits | hitrate | break-even |
|---|---|---|---|
| U5.5 | 13,900 | 94.89% | **1.054** |
| **U4.5** (today's play) | 12,724 | **86.86%** | **1.151** |
| **U3.5** (the pivot) | 10,624 | **72.52%** | **1.379** |
| U2.5 | 7,348 | 50.16% | 1.994 |

And on those same cards a result lane prints only **41%** of the time,
grading 78.48% — the DNB subset **81.10%**, break-even **1.233**.

Three things follow.

**U3.5 is a real candidate, not a consolation.** Its break-even of 1.379
sits in a far more competitive stretch of the price curve than U4.5's
1.151. Short prices carry disproportionately heavy margin — the
favourite-longshot bias — so a book quoting 1.14 on a 1.151 lane can be
quoting 1.40 on a 1.379 one and leave real room. Whether that is true is
exactly what the odds data would settle, and it is the first thing to
measure.

**The pivot is a price decision, never a probability one.** Athena claims
85.1% for its own rung and has no opinion about U3.5; the 72.52% above is
an empirical bank rate, not an engine number. Any pivot rule has to carry
that distinction, and the pivoted lane's break-even has to come from the
measured rate with its own confidence interval.

**Tip 3 cannot be the whole answer.** It is only available on 41% of these
cards. Where it exists and is a DNB it is strong — 81.10% at a 1.233 bar,
comfortably inside normal DNB pricing. Where it does not, the choice is
U4.5, a rung down, or no play.

So the pivot order to test is: **U4.5 → DNB (if printed) → U3.5 → no
play**, with each step gated on its own measured break-even rather than on
`buy≥`.

## 4. What the layer would actually compute

Per lane, per card:

1. `p_athena` — the printed claim (already there)
2. `p_market` — vig-free market probability from the derivations above
3. `edge = p_athena − p_market`
4. `price_est` — the offered decimal, market probability re-loaded with the
   measured per-market overround
5. `breakeven = 1 / p_athena`, and for quarter lines the half-win-aware
   version already in `pricing.settle_fraction`
6. `verdict`

Comparing **probability to probability** is more robust than guessing a
decimal, because the overround is the noisy part. The decimal is still
produced, because it is what a human reads.

The verdict is not `price < buy≥`. Candidates, to be fitted:

- **NO PLAY** when the market's probability *exceeds* Athena's by more than
  some margin — the market is saying we are wrong, and it is usually the
  better-informed party
- **NO PLAY** when the estimated price sits below break-even by more than
  a tolerance
- **DIVERT** to the next lane whose verdict passes *and* whose historical
  record supports it — the chooser's existing constraint applies, so never
  tip 2
- **PLAY** otherwise

Every threshold gets swept and validated in two independent time windows,
same bar as every constant in this engine.

## 5. How it gets judged

One question decides whether it ships: **does the picker's 83.7% hitrate
actually make money at market prices, and does the layer make it more?**

Three measurements, in order:

1. **Baseline.** Replay the final pick over the 20,861 covered bank rows at
   estimated market closing prices. Compute realised return per unit with
   push and half-win settlement honest. If this is already positive, the
   layer's job is to raise it; if it is negative, the layer's job is to
   find the subset where it is not.
2. **Volume retained.** For each candidate threshold, the fraction of cards
   still played. A rule that keeps 20% is not a rule, it is a different
   product.
3. **Two-window validation.** Fitted on the older half, scored on the
   newer, and the thresholds must survive.

A guardrail worth stating now: if the honest answer is that the picked
lanes do not beat closing prices, the layer must say so plainly rather
than be tuned until it appears to. That result would be worth more than a
feature.

## 6. Constraints this must honour

- **Odds never reach the engine.** New module, read-only against
  `config/`, no import from `app/engine/` except the pricing curves used to
  invert the market's own number. Nothing writes back to a probability.
- **Derived, never typed.** A cache like `config/lane_cache.json`,
  rebuildable by deleting it.
- **Selection never reads debits** — unchanged, this sits after selection.
- **Silence over a guess.** No source, no verdict.

## 7. Open questions

- Closing or opening odds? Closing is sharper and is what the layer should
  be judged against; opening is closer to what a bettor actually sees.
  Probably: judge on closing, quote on opening.
- Does the implied-mu derivation hold on quarter lines, where our ladder
  mostly lives? Unvalidated, and it could sink the tip 1 half.
- How stale is a closing price as a proxy for what Unibet shows this
  afternoon? Measurable against `bets.tsv`'s 151 real paid prices.
- Do we want a verdict on unplayable lanes too, or only on what is offered?
