# Archive — 20 to 23 August 2026, pre-calibration engine

Closed on 23 Aug 2026 when the engine was recalibrated. Everything in this
folder was produced by the engine as it stood BEFORE that work, and the
headline numbers are **not comparable** to anything recorded after it. Kept as
a record of what was published at the time, and of how the defects were found.

    log.md      the live log, verbatim as it stood in README.md
    bets.tsv    every bet placed, with the price paid

---

## The numbers as published

    Tip 1     101 / 120 settled     84.2%
    Tip 2      67 / 100 settled     67.0%
    Bets       55 /  79 settled     69.6%   ROI -10.1%

Twenty-three leagues, three days, one full Saturday slate.

## The same tips re-run on the calibrated engine

    Tip 1      99 / 118             83.9%
    Tip 2      56 /  80             70.0%

Strike rate barely moves. What changed is underneath it: the market mix came
off `U4.25`, the probabilities became honest, and realised edge over base rate
went **+1.35 to +2.23**. The old 84.2% was substantially bought rather than
earned.

## Why the bets lost money at a 70% strike rate

Three findings, each measured, each of which alone would have been enough:

**1. The probabilities were 10.8 points optimistic on the bets actually
placed.** Across 26 leagues the engine claimed 85.7% and delivered 81.2%; on
this bet book it claimed 80.4% and delivered 69.6%, because the bets chased the
extremes where the error was worst. The cause was mu being over-spread by 2.4x
— `actual_total = 1.640 + 0.424 * mu` — with the level fine and only the spread
wrong.

**2. `buy from` inherited that error, so the margin never existed.** Every
threshold published here was computed as break-even x 1.05 from an inflated
probability. Re-priced on the calibrated engine, thresholds moved UP on **76 of
80 bets**, mean **+6.8%** — larger than the 5% margin they were meant to
protect. "Break-even plus 5%" was in practice break-even minus 2%.

**3. Re-scored honestly, 64% of bets were negative EV**, not the 33% reported
at the time.

## What the log got right

- **Tip 1 beat every deviation from it.** Tip 2, team lanes, middle rungs and
  price-chasing all did worse, on every cut tried.
- **Losses are boundary events.** All 14 misses in the first 95 tips lost by a
  goal; only two were genuine blowouts. The engine was not misreading matches.
- **The team lane is real but must be bought at a team-lane price.** `Basel
  O1.5` at 1.83 and `Troyes U1.5` at 1.48 were the best buys in the book;
  `Genoa U1.5` at 1.18 and `Fenerbahçe O1.5` at 1.32 were the worst. Same lane,
  same engine — the price was the whole difference.

## What it got wrong, and how

Recorded because the failure modes repeated:

- **Small samples read as findings.** The buy-from threshold showed +18.4% at
  60 settled bets and -4.2% at 79. The discipline split showed a 34-point gap
  that survived neither a corrected bucket definition nor more data.
- **Slices chosen because they looked bad, then tested on the same data.** The
  1.20-1.39 odds band and the season-restart effect both dissolved when tested
  properly; the second turned out to be a symptom of the mu defect.
- **Set containment mistaken for evidence.** "One rung safer rescues 8 losses
  and breaks 0 winners" — the second half is arithmetic, since a safer rung's
  winning totals are a superset.
- **Six engine defects found only because live screenshots kept arriving**:
  non-deterministic name resolution (hash-seed dependent, 16 colliding groups
  across 10 leagues), accent folding blind to `ø`/`ł`/`æ`, Denmark issuing tips
  off 15-month-old form, MLS split nine ways, and two abbreviation gaps.

## Minimum odds this era established

Computed from settlement, not from `1 / hit-rate`, because pushes and half-wins
return the stake rather than the price:

    Tip 1    break-even 1.211    at +5% margin  1.27
    Tip 2    break-even 1.393    at +5% margin  1.46

The bet book averaged about 1.29 against a Tip 1 requirement of 1.27, which is
why it came out roughly flat before the calibration error and negative after it.
