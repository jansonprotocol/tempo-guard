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


---

# First cross-check against real odds (1 Sep)

Run over the board's own fixtures, **28–31 Aug**, taking the final picker's
lane and ignoring the bettor's own plays. 266 settled fixtures, 134 in
odds-covered leagues, **119 matched to a real Bet365 closing price**.
Result lanes derive exactly from 1X2; ladder rungs derive through a market
`mu` recovered from the 2.5 line, which remains the unvalidated step.

| gate | plays | avg odds | hit | ROI |
|---|---|---|---|---|
| take all | 119 | 1.17 | 82.4% | **−3.8% ± 4.2** |
| accept 10% under `buy≥` | 60 | 1.24 | 78.3% | −3.1% |
| accept 8% under `buy≥` | 43 | 1.26 | 76.7% | −3.5% |
| strict, at or above `buy≥` | 7 | 1.40 | 71.4% | +1.3% |

At the **market maximum** rather than Bet365: take-all −2.0%, and the
strict gate keeps 14 plays at +6.9%. Shopping the line is worth about
1.8 points and still does not clear.

**The hitrate is real and the price is not enough.** 82.4% is exactly what
the picker advertises. At an average 1.17 it needs 1.214. The overround on
that market averaged **5.60%**, so an edge-free bet returns −5.30%; the
picker returned −3.8%. That difference is the engine's whole edge: about
**1.5 points, swamped by a 5.6-point margin.**

**Athena and the closing line agree almost exactly.**

| | market implied | Athena said | actually landed |
|---|---|---|---|
| under lanes (78) | 81.7% | **81.8%** | 76.9% |
| over lanes (40) | 80.0% | **80.2%** | 92.5% |

Two tenths of a point apart on both sides. On match totals the engine is
not seeing anything the market has missed — which is the honest reason the
ladder cannot pay after margin.

**The over/under split is a hot window, not a finding.** Market `mu`
averaged 2.82 across the window; the fixtures actually produced 3.34 goals.
Overs beat their claim and unders missed theirs by the same cause with
opposite signs. "Skip the unders" would be fitting to four goal-heavy days.

**The pivot is rejected.** Playing U3.5 where the picker says U4.5 returns
**−23.9%** on those 78 lanes (52.6% at an average 1.50); two lines down is
worse. The longer price is longer for a reason — the market prices the
lower rungs efficiently, and the engine has no edge there to spend.

**What this does not yet test: the result lanes.** The DNB gate fired
**once** in this window. That is where the engine claims +12% to +30%, and
where prices derive exactly from 1X2 with no Poisson step. It is the one
live hypothesis left and needs a window wide enough to hold a few hundred
of them.

**Caveats.** n=119 over four days, ±4.2 points — this does not establish a
negative any more than 66 bets established a positive. Ladder prices are
modelled, not observed. Treat it as the first honest look, not a verdict.


---

# Second run: what the 119-row check missed (1 Sep)

Two things. The sample was tiny, and the lane the engine actually claims an
edge on was never tested. Joined the retrosim bank to **two full seasons**
of closing odds across 16 leagues: **8,069 ladder lanes and 4,355 result
lanes**, 2024-08 to 2026-05, Pinnacle closing where present.

## The ladder is settled, and not in our favour

| | n | odds | hit | ROI |
|---|---|---|---|---|
| every tip 1 lane | 8,069 | 1.12 | 82.6% | **−3.49% ± 0.43** |

Eight standard errors below zero. And bucketing by how far Athena
out-claims the market shows **no gradient at all** — −3.4, −3.3, −4.3,
−2.3, −4.5 across the five bands. The claimed edge carries no information
about the return. Match totals are finished as a money lane.

## The result lane is close, and splits hard by kind

| | n | odds | hit | ROI |
|---|---|---|---|---|
| DNB | 2,551 | 1.38 | 58.0% | **−0.71% ± 1.02** |
| double chance | 1,804 | 1.30 | 73.6% | −4.73% ± 1.36 |

DNB is indistinguishable from break-even before any selection. Double
chance loses, consistent with the chooser already refusing it.

## The finding: the claimed edge is ANTI-predictive

| DNB, claim vs market | n | odds | ROI |
|---|---|---|---|
| Athena claims **5+ points less** | 1,113 | 1.18 | **+2.14% ± 1.06** |
| Athena claims 5 less to level | 457 | 1.31 | −2.3% |
| Athena claims 0–5 more | 364 | 1.42 | −1.9% |
| Athena claims **10+ more** | 363 | 1.95 | **−7.46% ± 4.13** |

Split by side: where the market claims more than Athena, **+0.84%** on
1,570 lanes; where Athena claims more, **−3.20%** on 981.

And the top bucket survives the two-window test as cleanly as anything in
this project:

| | n | window | ROI |
|---|---|---|---|
| older half | 556 | 2024-08 → 2025-05 | **+2.10% ± 1.48** |
| newer half | 557 | 2025-05 → 2026-05 | **+2.17% ± 1.53** |

Settlement: 73.0% win, 17.0% push, 10.1% lose.

**Why this is not absurd.** `result_market.py` already records it: *"DNB
underclaims at every band (+1.3 to +8.5)… the lane never overclaims
anywhere measured — its failure mode is modesty."* Where the engine
underclaims hardest it is most wrong in the safe direction, and those are
heavy favourites, where the market's own favourite-longshot bias leaves
something on the table. Two independent errors pointing the same way.

**What it means for the layer.** It is not a veto sitting after the
chooser. On this evidence it is a *different selector*: the picker stars
the highest claim, and the money is on the lanes with the **lowest** claim
relative to the market. That inverts the design in section 4 and has to be
settled before anything is built.

**Caveats.** DNB prices are derived from 1X2 carrying the full 1X2
overround; a real DNB market may price tighter or wider, and that alone
could move +2.14% either side of zero. Seven buckets were searched to find
this one. The rule would play roughly 13% of cards. Nothing here is
established enough to bet differently tomorrow — it is established enough
to be the thing the next run tests properly.


---

# The inverse play, tested (1 Sep)

The bettor's idea: if the printed side is unbackable at the offered price,
maybe the other side is the play — Athena says over, but the goal
expectation is weak enough that the under is where the value sits.

Tested on 7,613 lanes with both sides priceable, two seasons, market
maximum prices.

| | n | avg odds | ROI |
|---|---|---|---|
| back Athena's side | 7,613 | 1.15 | **−1.87% ± 0.47** |
| back the inverse | 7,613 | 6.17 | **−20.53% ± 2.35** |

Rejected, and not narrowly. Every bucket of disagreement is worse on the
inverse than on Athena's own side.

**The fair version fails the same way.** Scanning the whole ladder for the
line with the highest expected value — rather than flipping the printed
one — picks `O4.5` on 3,414 fixtures and `U1.0` on 2,515, with a claimed
median EV of **+14.3%**. Every choice is an extreme of the ladder, because
EV is a *ratio* and long shots inflate it, and the Poisson tail is exactly
where the model is least trustworthy. That is the same trade the inverse
test priced at −20%.

**But the buckets do contain a real gradient**, and it points somewhere:

| Athena's mu vs the market's | n | ROI on Athena's side |
|---|---|---|
| much lower (< −0.5) | 419 | **−0.98% ± 2.43** |
| lower (−0.5 to −0.2) | 973 | −2.39% |
| agree (± 0.2) | 3,807 | −1.59% |
| higher (+0.2 to +0.5) | 1,958 | −2.01% |
| much higher (> +0.5) | 456 | **−3.35% ± 1.41** |

The engine does best where it is more **bearish** than the market and
worst where it is more **bullish**. Its overs are where it is most wrong.

## The structural problem, stated plainly

The book averages **1.15**. At 1.15 against a 5.6% margin, a model has to
be roughly **five points more accurate than the market** merely to break
even — and the measurement says Athena and the market agree to within two
tenths of a point. No amount of lane selection fixes that, because the
arithmetic is in the price, not the pick.

This is why every route through the ladder closes, and why the only
positive result in the whole study sits at **1.18–1.50** — the DNB lanes.
The same relative edge is worth far more in absolute terms at a longer
price.

**The path that remains**, in order of evidence:

1. **DNB where Athena underclaims the market by 5+ points** — +2.14% ±
   1.06 on 1,113 lanes, +2.10% / +2.17% across two independent windows.
2. **Always take the best available price** — worth +3.58 points, and the
   live sweep shows Unibet (NL) sitting at or below consensus on every
   lane of the current board.
3. **Stop treating the ladder as a money lane.** It remains the honest
   headline hitrate; it is not a bet.

Volume will be low — the DNB rule fires on roughly 1.5% of cards. The
bettor has accepted that trade. What must not be oversold is the size:
+2.14% at an average 1.18 is about two cents per euro staked, and it needs
hundreds of bets before it can be distinguished from nothing.


---

# Tip 2 and straight 1X2, tested (1 Sep)

**Tip 2 is closed, permanently.** Queried the feed's full market list for a
fixture: `alternate_totals`, `btts`, `double_chance`, `h2h_3_way`,
`spreads` and `totals` are quoted — **`team_totals` is not**. No source
prices a team over/under in these leagues, so tip 2 can never receive a
verdict. Worth noting what IS there: `alternate_totals` would let the
ladder be priced exactly instead of derived, and `double_chance` is quoted
directly rather than derived from 1X2.

**Straight 1X2 was worth testing** because the prices are long — 2.2 for a
home side against 1.15 on the ladder — and the arithmetic that kills the
ladder is mostly the price. The engine never prints a 1X2 lane but it
computes one: tip 1's claim fixes the goal expectation, tip 3's claim
fixes how it splits between the sides, so (home, draw, away) is
recoverable without an engine replay. 2,876 fixtures, exact closing prices.

| | n | odds | hit | ROI |
|---|---|---|---|---|
| back the home side always | 2,876 | 2.19 | 53.8% | **−5.78% ± 1.89** |
| back the draw always | 2,876 | 4.56 | 24.5% | +0.08% ± 3.47 |
| back the away side always | 2,876 | 6.69 | 21.7% | −4.36% ± 4.24 |

And bucketed by the engine's claimed edge over the market:

| claimed edge | n | odds | ROI |
|---|---|---|---|
| +0 to +2 pts | 831 | 4.11 | −15.93% |
| +2 to +5 pts | 1,113 | 5.01 | +4.47% ± 6.18 |
| +5 to +10 pts | 1,485 | 7.01 | −8.72% |
| **+10 pts and up** | 923 | 7.60 | **−23.47%** |

Rejected. The back-the-favourite line needs no recovery step at all and is
cleanly negative on its own.

## The pattern is now confirmed three times over

| lane family | biggest claimed edges |
|---|---|
| match totals ladder | no gradient at all |
| DNB | −7.46% at +10 pts, **+2.14% where the engine claims LESS** |
| straight 1X2 | −23.47% at +10 pts |

**Wherever Athena disagrees most with the market, Athena is most wrong.**
That is not a fault in one lane; it is a property of this model against
this market, and it holds across three independent families on real money.

The single exception is the one place the engine is *conservative*: DNB
lanes where it underclaims the market by 5+ points. That fits the same
story rather than contradicting it — `result_market.py` already recorded
that the lane's failure mode is modesty, and the market's
favourite-longshot bias pays for it.

## Where this leaves the project

Athena is a well-calibrated football predictor. That is a real
achievement, and it is measurably NOT the same thing as an edge over a
bookmaker — the two probabilities agree to within two tenths of a point,
and the margin sits between them.

Three honest conclusions:

1. **The hitrate is real and should keep being published.** It is not a
   betting instruction.
2. **One narrow money lane survives**, worth about two cents per euro, on
   1.5% of cards.
3. **Line shopping is worth more than the model** — +3.58 points against
   +2.14 — and needs no engine change at all.


---

# Is the probability read the best we can make? (1 Sep)

The decisive test, and it needed no new data: score Athena and the market
as *forecasters* on the same fixtures, on lines **neither of them chose**,
with Brier score and log loss. 7,613 fixtures, two seasons.

| line | n | Brier Athena | Brier market | winner |
|---|---|---|---|---|
| U1.5 | 7,613 | 0.1888 | **0.1839** | market |
| U2.5 | 7,613 | 0.2496 | **0.2431** | market |
| U3.5 | 7,613 | 0.2014 | **0.1961** | market |
| U4.5 | 7,613 | 0.1108 | **0.1084** | market |
| **overall** | | **0.1876** | **0.1829** | market by 2.6% |

**The two forecasts are genuinely independent.** Correlation between the
goal expectations is only **0.562**, mean absolute difference 0.25 goals.
Athena is not copying the market — it sees something different. Both are
close on average (Athena 2.740, market 2.695, actual 2.667).

**But the independent part is noise.** Blending them, sweeping the weight
on Athena from 0 to 1:

| weight on Athena | Brier (all) | older half | newer half |
|---|---|---|---|
| **0.0 — market alone** | **0.18287** | 0.18299 | 0.18276 |
| 0.2 | 0.18313 | 0.18333 | 0.18293 |
| 0.5 | 0.18417 | 0.18455 | 0.18380 |
| 1.0 — Athena alone | 0.18762 | 0.18842 | 0.18683 |

**The optimal weight on Athena is zero**, monotonically, in both windows.
Adding any of it to the market makes the forecast worse. Whatever Athena
knows that the market does not, it does not help predict goals.

**And it is not a timing artefact.** The obvious defence — the engine
prices days ahead while a closing line carries team news it cannot have —
does not survive. Against the **opening** line the gap is still there:

| | Athena | market | gap |
|---|---|---|---|
| vs closing | 0.18762 | 0.18287 | +2.60% |
| vs opening | 0.18709 | 0.18329 | +2.07% |

Best blend weight against the opening line is also **0.00**. There is no
early window in which Athena is ahead and the market catches up.

## What that means for the mission

The mission reads *"most accurate tipper, therefore positive ROI"*. The
measurement separates those two claims:

- **Accurate:** yes, and demonstrably so — 83.7% on the final pick, and a
  Brier of 0.188 is a real forecaster, not a coin flip.
- **More accurate than the market:** no. 2.1–2.6% behind on every line,
  before and after team news, and it adds nothing in combination.
- **Therefore positive ROI:** does not follow, and the price data says it
  does not hold. The market's number sits between Athena's and the truth,
  and the margin sits on top of that.

## The three paths that are still open

None of them is an accuracy path.

1. **Line shopping** — +3.58 points, needs no engine change, and the live
   board shows Unibet at or below consensus on every lane.
2. **The DNB modesty bucket** — +2.14%, two-window validated. It works
   *because* the engine is wrong in a known direction that happens to
   align with the market's favourite-longshot bias.
3. **Thin markets** — the Jong PSV 1.44 against a 1.21 consensus was a
   19% overlay in a league the odds feed barely covers. That is where the
   bettor's own realised profit came from, and it is a market-inefficiency
   play, not a model play.

## If accuracy itself is the goal

The market's advantage is almost certainly information the engine has no
access to: confirmed lineups, injuries, rest days, table motivation,
travel and weather. The engine has Elo, form, venue and its cup debits.
api-football carries lineups and injuries on the same key that now serves
prices — that is a real roadmap for closing a 2% Brier gap, and it is a
completely different project from finding a bet.

**What must not happen** is feeding the market price into the engine. The
blend test already says the best mixture is the market alone, so it would
buy nothing, and it would destroy the one genuinely defensible claim this
project has: that the 83.7% was produced without ever seeing a price.


---

# Before buying a lineups feed: how much could it possibly be worth? (1 Sep)

Team news, injuries and confirmed lineups are most of what arrives between
a bookmaker's OPENING price and its CLOSING price. So the open-to-close
improvement is a measurable **upper bound** on what those features could
buy the engine — and it needs no new data at all.

On the 6,328 fixtures carrying both an opening and a closing line:

| | Brier |
|---|---|
| Athena | 0.18630 |
| market at OPEN | 0.18319 — Athena **1.70%** behind |
| market at CLOSE | 0.18206 — Athena **2.33%** behind |

Everything that arrives between open and close is worth **+0.616%** of
Brier. Athena's gap to the *opening* line is **1.70%**.

**So late news can close at most 36% of the gap — and that is a ceiling,
not an estimate**, because the open-to-close move also contains late money
and steam, not only team sheets. The real team-news share is smaller. The
market's goal expectation only moves 0.087 goals on average across that
whole window.

Put plainly: **a perfect lineups-and-injuries feature would still leave
Athena behind the opening line.** Roughly two thirds of the gap is in the
base model, present before anybody knows who is playing.

## Where the other two thirds probably is

The store already carries more than the engine leans on: shots, **shots on
target**, corners, fouls, cards and referee, across ~9,900 rows per major
league. `features.py` does use `hst`/`ast` — for a per-team shots-on-target
rate and a league conversion rate — but the ratings that set the goal
expectation are built mainly from goals.

That is the known weak spot in this class of model. Goals are a noisy
realisation; shot volume and shots on target regress far better between
matches, which is why every public xG-style rating outperforms a
goals-only Elo. Rebuilding the attack/defence ratings on shots on target
with a fitted conversion, rather than on goals, is the cheapest large
experiment available — the data is already in the store, so it costs a
retrosim run and nothing else.

## Recommended order

1. **Shot-based ratings** — free, data already local, targets the two
   thirds of the gap that lineups cannot reach.
2. **Lineups and injuries** — needs an api-football key (none is set;
   `ODDS_API_KEY` serves prices only, and the odds feed carries no team
   news). Bounded at a third of the gap, and only worth doing after (1),
   because the harness is the same.
3. Whatever remains after those two is measured, not guessed.

The retrosim harness for either is the same shape as `final_pick.py`:
replay as-of, score Brier and log loss against the market's line on lines
neither party chose, and require both time windows to agree.
