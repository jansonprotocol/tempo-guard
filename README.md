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
more than the bookmaker's margin — roughly 5.3% relative, on any line.

For scale: always betting U4.5 in Serie B scores 91.6% and still loses money,
because U4.5 pays about 1.09 and needs 91.7% to break even. Meanwhile 58% on
O2.5 is profitable.

Current state after the prune, over ~3,000 replayed matches:

```
hit rate      80.4%   (was 77.7%)
base rate     78.5%   of the same markets the engine chose
edge          +1.9%   (was +1.2%)
needed        +4.1%   to clear the margin
```

So the engine is measurably better than it was, and one league (ESP-LL, 84.7% vs
83.7% needed) now clears the bar — but it is not yet profitable across the
board. The remaining gap is an information problem, not a tuning problem: the
best feature scores AUC 0.548 against a coin-flip 0.50, and a Dixon-Coles
attack/defence model built on the same goals-only data does no better. Closing
it needs xG, lineups, and odds — not more rules.

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

Sourced from [openfootball](https://github.com/openfootball) — public domain,
plain-text, git-native. Currently loaded:

| Competition | Results 25/26 | Fixtures 26/27 |
|---|---|---|
| Premier League, La Liga, Serie A | 380 each | 380 each |
| Bundesliga, Ligue 1, Eredivisie, Primeira | ~300 each | ~306 each |
| Championship | 557 | 552 |
| 2. Bundesliga, Segunda, Serie B, Ligue 2 | partial | — |
| Champions League | 189 | — |

Add a competition by adding an entry to `backend/app/data/sources.py`.

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
