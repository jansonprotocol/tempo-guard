# ATHENA: Tempo Guard

A local over/under goals engine for football. It reads match results, computes
point-in-time form features, and produces a total-goals tip on the Asian line
ladder (`O1.75`, `U3.75`, …) together with the reasoning behind it.

Everything runs offline from this repository. No server, no database, no API
keys. Match data lives in `data/` as parquet, tuning lives in `config/` as JSON,
and both travel with the repo.

```
athena data status                                    # what's loaded
athena tips --days 7 --explain                        # tips for the week
athena retrosim ENG-PL "Arsenal" "Chelsea" 2026-01-04 # re-run a past match
athena calibrate ENG-PL --detail                      # tune a league
athena ablate                                         # what each module is worth
```

## Quick start

```bash
pip install -r backend/requirements.txt

python athena.py data sync     # clone the openfootball sources (needs internet)
python athena.py data load     # parse them into data/*.parquet
python athena.py data status   # confirm what landed

python athena.py tips --days 7
```

`data sync` is the only step that touches the network. Once `data/` is
committed, everything else works offline — and because the snapshots are in the
repo, a fresh clone can predict immediately without syncing at all.

## The two flows

### Calibration

```
athena calibrate ENG-PL [--apply] [--detail] [--before YYYY-MM-DD]
athena calibrate --all --apply
```

1. Loads that league's completed matches
2. Re-simulates every one of them through the engine, as of its own date
3. Reports the hit rate
4. Searches the dial grid (`bias_shift` × `tempo_factor`) for a better setting
5. Verifies the winner **on a chronological holdout the search never saw**
6. Writes the result to `config/leagues.json` only if the gain survives

Step 5 is the important one. Searching 336 dial combinations against ~300
matches will always find a combination that looks 2–3% better purely by chance.
Splitting the season — tune on the earlier 70%, verify on the later 30% —
separates a real improvement from a lucky fit. A change is written only when it
clears both `MIN_IMPROVEMENT` and `MIN_HOLDOUT_SAMPLE`.

### Single runs

```
athena retrosim    ENG-PL "Arsenal" "Chelsea" 2026-01-04   # past — also graded
athena futurematch ENG-PL "Arsenal" "Chelsea" 2026-08-29   # upcoming
athena tips --league ENG-PL --days 7 --explain             # everything upcoming
```

`retrosim` refuses dates that are not in the past and verifies the fixture
actually happened before simulating it, then grades its own call against the
real score. Add `-v` to any of them for the raw signal trace.

## How a tip is produced

```
data/*.parquet  →  asof_features  →  evaluate_athena  →  translate_play  →  tip
                   (last 10 games)   (modules + dials)   (Asian line)
```

`asof_features` reads only matches **strictly before** the fixture date, so
retrosim and calibration reproduce what the engine would have said on the
morning of the match. There is no lookahead anywhere in the pipeline.

The engine is a stack of named modules. Which ones are active is decided by
measurement, not intuition — see below.

## Which modules earn their place

```
athena ablate            # every league
athena ablate ENG-PL --detail
```

Ablation disables one module at a time and measures the effect on hit rate over
a full replay. Run across nine leagues and ~2,900 matches, the result was blunt:

| Module | Contribution | Verdict |
|---|---|---|
| `ulr` low tempo → Under | +0.24% | **on** |
| `deg` defensive decline | +0.03% | **on** |
| `mfr` momentum | +0.03% | **on** |
| `under_guard` low goal expectation | 0.00% | **on** — the only Under pathway |
| `gate_b`, `eps`, `bilateral` | 0.00% | **off** — changed zero predictions |
| `det` volatility | −0.45% | **off** — cost accuracy in 4 leagues |
| `burst_sentinel` chaos → force Over | **−1.91%** | **off** — cost accuracy in *all nine* |

Disabling the five non-earners is worth **+2.4%** in-sample and **+1.5%** on a
chronological holdout (7 of 9 leagues improve).

Two further modules, `InlineVeto` and `S-LOCK`, were deleted outright: both were
unreachable. `InlineVeto` was fed a hardcoded `quality_ok = True`, and `S-LOCK`
compared the lean against itself, so neither could ever fire.

The disabled modules' code is kept, not deleted. Their inputs — volatility,
phase stability — are exactly the signals that richer data such as xG would make
meaningful. Any league can switch one back on via `module_overrides` in
`config/leagues.json`, and these toggles are far higher-leverage calibration
dials than the bias and tempo factors, because a toggle changes the selected
market outright.

### A note on the tempo signal

`tempo_index` used to clip at 0.9 and was then scaled past its own ceiling,
pinning about 63% of matches to the maximum. The signal was effectively a
constant, which starved every module gated on low tempo — `gate_b`, `ulr` and
`mfr` could fire on a literal handful of matches per season. Normalising it so a
typical fixture lands mid-range was worth **+2.8%** on its own and revived
`ulr`. `test_tempo_index_is_not_saturated` guards against a regression.

## Accuracy, honestly

Raw hit rate is a misleading target, because bookmakers price close to the true
probability. What matters is beating the **base rate of the line you bet** by
more than the bookmaker's margin.

For scale: always betting U4.5 in Serie B scores 91.6% and still loses money,
because U4.5 pays about 1.09 and needs 91.7% to break even. Meanwhile 58% on
O2.5 is profitable.

### Does the engine have skill?

Measured over full available history, where samples are large enough for the
confidence interval to be tighter than the effect:

| League | n | hit rate | base rate | edge | ±95% CI | real? |
|---|---|---|---|---|---|---|
| ENG-PL | 9,662 | 78.0% | 75.6% | **+2.4%** | 0.8% | yes |
| ESP-LL | 5,179 | 81.0% | 77.2% | **+3.8%** | 1.1% | yes |
| ITA-SA | 4,765 | 80.0% | 77.6% | **+2.4%** | 1.1% | yes |
| GER-BL | 4,761 | 80.9% | 77.3% | **+3.6%** | 1.1% | yes |
| FRA-L1 | 4,102 | 79.9% | 75.4% | **+4.5%** | 1.2% | yes |

Yes. Every league beats its own base rate by more than its confidence interval.
That is genuine signal, not a coin landing well.

### Is that enough to be profitable?

It depends entirely on the margin you are charged, and the answer sits right on
the boundary:

| League | at 2% margin | 3% | 4% | 5% |
|---|---|---|---|---|
| ENG-PL | +0.9% | +0.1% | −0.8% | −1.6% |
| ESP-LL | +2.2% | +1.4% | +0.6% | −0.3% |
| ITA-SA | +0.8% | +0.0% | −0.8% | −1.7% |
| GER-BL | +2.0% | +1.2% | +0.4% | −0.5% |
| FRA-L1 | +3.0% | +2.2% | +1.4% | +0.5% |

Soft books charging 5% beat the engine almost everywhere. Sharp books running
2–3% on major-league totals do not. The margin assumption is doing more work
than any tuning decision in this repository.

**This is modelled, not measured.** Both the base rate and the required margin
are inferred from goal distributions rather than read from real prices. It is a
reason to go and get odds data, not a result to bet on.

### What deep history corrected

Earlier single-season measurements put Portugal, China and Croatia above
break-even and the Premier League far below. Re-measured on full history those
rankings dissolve: ESP-LL moved from +0.1% to −0.2%, and the leagues that looked
profitable had 150–600 matches each, where the confidence interval is ±4–8
points. Selecting the best 5 of 39 leagues on a single season finds noise
reliably. Nothing under a few thousand matches should be trusted per league.

## Layout

```
athena.py              CLI entry point
config/leagues.json    per-league dials — calibration writes here
data/<LEAGUE>/*.parquet match snapshots, committed
backend/app/
  data/                openfootball parser, loader, store, features, config
  engine/              prediction pipeline, types, module flags, rationale
  predict.py           features -> Prediction
  calibrate.py         replay, dial search, holdout verification
  ablate.py            per-module contribution measurement
  util/asian_lines.py  Asian line grading
backend/tests/         pytest suite (no data or network needed)
```

## Data

Two providers, chosen for opposite strengths.

| | openfootball (git) | football-data.co.uk (HTTP) |
|---|---|---|
| Access | clone once, then offline | fetched on request |
| History | deep — 27 seasons of England | 2000 onward for most |
| Freshness | weekly auto-update, often 3–6 weeks behind | within hours of kick-off |
| Fixtures | full schedule months ahead | played matches only |
| Statistics | goals, half-time, scorers | goals, **shots on target**, corners, cards, referee, **xG from 2026-27** |
| Coverage | 47 competitions incl. all UEFA cups | 30 leagues, incl. China/Japan/Brazil/MLS |

```
athena data sync              # git-pull openfootball
athena data load --history    # parse every season it publishes
athena data live              # top up the current season online
athena data status
```

`data live` merges rather than overwrites: live results win, and the git
fixture schedule survives where a match has not been played yet. Neither
provider alone is sufficient for a season in progress — one has the schedule,
the other has the results.

**59 competitions · ~176,000 results · 7.9 MB committed.** Europe, South
America, North America, Asia, Africa, and all six UEFA club competitions.

### No bookmaker data, by design

football-data.co.uk files carry ~132 columns, of which ~108 are bookmaker
prices. The parser reads a strict allowlist of football columns and discards
every odds column at parse time; `assert_no_odds()` fails loudly if one ever
reaches the store.

Odds encode the market's own forecast. Feeding them in would make ATHENA partly
a market-follower, and any apparent edge would be the bookmakers' opinion
echoed back. The engine forms its view from football alone.

The cost of that choice is real and worth stating: without prices,
profitability can only ever be **modelled** from goal distributions, never
measured against what a bet would actually have paid.

Add a competition by adding an entry to `backend/app/data/sources.py`. Season
keys follow each competition's own convention — European winter leagues use
`2025-26`, calendar-year leagues (Brazil, MLS, Japan, the Nordics) use `2025`,
and football-data spells the same thing `2025/2026`, so all three are compared
on the starting year.

### What the data does and does not carry

Goals, half-time scores, dates and goalscorers are exact. Two consequences
worth knowing:

- **Shots on target are estimated**, not measured — openfootball publishes no
  shot counts, so `sot_proj_total` is derived from expected goals. It feeds only
  the strict O2.5 gate; every other feature is goal-derived and exact.
- **Knockout ties are recorded at 90 minutes.** A line reading
  `3-2 a.e.t. (3-0, 1-0)` is stored as 3-0, because over/under settles on
  regulation time. Forfeits (`[awarded]`) and cancellations are flagged and
  excluded from features rather than treated as real results.

### Promoted teams

A team with no history in the league it is about to play in gets no tip — its
past matches live in a different competition's file. On the opening weekend
this means roughly three fixtures per league are skipped. Team matching
deliberately returns "no match" rather than guessing: a missing tip is
recoverable, a tip built on the wrong club's form is not.

## Tests

```bash
cd backend && python -m pytest tests/ -q
```

Pure logic only — no data files, no network.
