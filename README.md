# ATHENA — TEMPO GUARD · BETA STAGE 1

## CURRENT CONFIRMED HITRATE: 90.0%

**18 / 20 settled** · over/under markets only · live tips, not backtests

---

## Pending FUTURE match bettips

| Live | League | Fixture | Play | Date | Modelled | Edge |
|---|---|---|---|---|---|---|
| ⏱ LIVE: 1-0 (82') — winning | LaLiga | Real Betis v Real Sociedad | U4.25 (≤3, half win at 4) | 2026-08-21 | 84% | −2.9% |
| — not started | Colombia Primera A | Jaguares v Boyacá Chicó | U3.0 (≤3 goals) | 2026-08-21 | 88% | +7.8% |
| — not started | Colombia Primera A | Alianza Valledupar v Deportivo Pereira | U2.75 (≤2, half loss at 3) | 2026-08-22 | 81% | +21.7% |
| — not started | Chile Primera División | Audax Italiano v Unión La Calera | U3.0 (≤3 goals) | 2026-08-22 | 82% | +12.6% |
| ⏱ LIVE: 0-0 (43') — needs 2, goal ruled out | Peru Liga 1 | Alianza Atlético v Sporting Cristal | O1.5 (2+ goals) | 2026-08-21 | 83% | +10% |
| — not started | Chinese Super League | Three Towns v Jinmen Tiger | U4.25 (≤3, half win at 4) | 2026-08-22 | 79% | +0.1% |

## Completed FUTURE match bettips

| Result | League | Fixture | Play | Date | Modelled | Edge |
|---|---|---|---|---|---|---|
| ✅ HIT — 1-1 | LaLiga | Rayo Vallecano v Alavés | O1.5 | 2026-08-20 | 82% | +7.7% |
| ✅ HIT | Brasileirão Série B | Athletic v CRB | O1.0 | 2026-08-21 | 90% | +1.0% |
| ✅ HIT | Brasileirão Série B | Novorizontino v América-MG | O1.0 | 2026-08-21 | 90% | +1.1% |
| ✅ HIT | J1 League | FC Tokyo v JEF United | U4.25 | 2026-08-21 | 89% | +0.9% |
| ❌ MISS — 6 goals | J1 League | Kashiwa Reysol v V-Varen Nagasaki | U4.25 | 2026-08-21 | 86% | −2.3% |
| ✅ HIT — 0-3 | Saudi Pro League | Al Riyadh v Al Nassr | O1.5 | 2026-08-21 | 91% | +12% |
| ✅ HIT — 0-3 | Allsvenskan | Sirius v Häcken | O2.25 | 2026-08-21 | 80% | +24% |
| ✅ HIT — 1-3, half win | Peru Liga 1 | FC Cajamarca v Atlético Grau | U4.25 | 2026-08-21 | 89% | +1% |
| ✅ HIT — 2-0 | Premier League | Arsenal v Coventry | O1.5 | 2026-08-21 | 82% | +1.9% |
| ✅ HIT — 2-0 | LaLiga 2 | Córdoba v Girona | O1.5 | 2026-08-21 | 80% | +9.3% |
| ✅ HIT — 1-2 | Serie B | Vicenza v Catanzaro | O1.5 | 2026-08-21 | 77% | +5.2% |
| ✅ HIT — 1-0 | Ligue 2 | Sochaux v Guingamp | U3.0 | 2026-08-21 | 84% | +9.1% |
| ✅ HIT — 0-1 | Ligue 2 | Pau v Nancy | U3.0 | 2026-08-21 | 84% | +8.6% |
| ✅ HIT — 1-1 | Ligue 2 | Boulogne v Red Star | U3.0 | 2026-08-21 | 83% | +7.9% |
| ✅ HIT — 1-1 | Ligue 2 | Clermont v Dijon | U4.25 | 2026-08-21 | 90% | +1.1% |
| ✅ HIT — 0-1 | Ligue 2 | Dunkerque v Montpellier | U4.25 | 2026-08-21 | 84% | −5.1% |
| ✅ HIT — 2-0 | Ligue 1 | Marseille v Strasbourg | O1.5 | 2026-08-21 | 84% | +6.6% |
| ❌ MISS — 0-1, one short | Saudi Pro League | Al Qadsiah v Al Ittihad | O1.5 | 2026-08-21 | 89% | +8.9% |
| ✅ HIT — 0-4, half win | Süper Lig | Erzurumspor v Galatasaray | U4.25 | 2026-08-21 | 84% | −0.5% |
| ✅ HIT — 2-0 | Belgian Pro League | Standard Liège v La Louvière | U4.25 | 2026-08-21 | 84% | −1.0% |

---

### Notes on the log

- Sample is 20 bets. Far too small to mean anything yet. Stage 1 is about accumulating live results.
- All tips issued as-of the morning of the match. In-play fixtures run as if unstarted — no live information reaches them.
- Retrosims are never logged here. Those are engine optimisation, not tips.
- Pending → completed once a result is confirmed.

### On the 22 Aug batch

- **Alianza Valledupar** — +21.7% edge, the largest issued, and the first `U2.75` in the log. Signal 0.83, the highest confidence so far, with the sharp lane agreeing on the same rung. Note it is a *half loss* at 3 goals, not a push.
- **Colombia is a measured negative-edge league** (−2.07% over 300 fixtures) and was on the cut list before the cull failed its own test. Two tips here regardless, because league-level past performance does not persist (r = +0.153).
- **Chile is new** — 6,240 matches from 2017, added via ESPN for this fixture. First tip from a league with no track record here at all.
- **Three Towns** at +0.1% is a fallback, not a read: lean says over, selector took U4.25.

### Live scores

Scores in the pending table are a snapshot, not a feed — they are whatever was
last reported and go stale between updates.

### Notes on individual calls

- **Kashiwa (the miss)** — raw tempo read said *over*; selector overrode to U4.25. Finished 6 goals. Raw read was right. Edge was −2.3%: engine had found nothing and fell back to the safest rung. Override measured across 1,510 fixtures since: 85.7% vs 78.1% for following the lean. Override stays.
- **Cajamarca and Erzurumspor** — both settled on the push. 4 goals = half win on U4.25 = win under the full-win convention. Two of the 17 wins are half wins, so a bettor who does not offset the line scores 15 clean wins, 2 pushes and 2 losses instead of 17 and 2. Worth stating: the headline rate depends on the convention.
- **Erzurumspor** — a −0.5% fallback that took the safest rung and needed it: 0-4 landed exactly on the half win, one goal from a loss.
- **Negative-edge tips** (Dunkerque −5.1%, Betis −2.9%, Standard −1.0%, Erzurumspor −0.5%) — fallbacks, not reads. Logged as issued, not recommended.
- **Marseille is the red-card finding, live.** O1.5 tip, 1-0 at 51 minutes, Strasbourg sent off, finished 4-0. Reds open matches rather than stalling them — measured at +0.15 goals across 1,795 fixtures, with Over tips scoring 85.7% when a red appears against 79.1% when none does.
- **Standard Liège** — the other side of it: an Under that survived a red. La Louvière went down to ten and it still finished 2-0, comfortably inside U4.25.
- **Al Qadsiah (the second miss)** — O1.5 at 89% and +8.9% edge, lean and market agreeing, high line safety. Everything that has marked the good calls in this log, and it finished 0-1. Al-Ittihad went down to ten men. Nothing in the tip was wrong; the match simply had one goal in it.
- **Ligue 2 swept 5/5.** Three U3.0 calls at +7.9% to +9.1% edge — the strongest cluster issued — all landed, plus both U4.25s. The league flagged as noisy produced the cleanest night.
- **Dunkerque** — the counterpoint to Kashiwa. Same shape: lean said *over*, selector overrode to U4.25, edge −5.1%. Finished 0-1. Override was right this time. Two live cases, one each way; the 1,510-fixture measurement (85.7% vs 78.1%) is what settles it, not these.
- **Al Nassr** — first tip where lean and market agreed. First sharp lane to land (O2.5). Finished 0-4.
- **Sirius** — won, but least trustworthy of the batch: priced off 17 matches per side instead of 84 due to a naming split.
- **Arsenal, Vicenza** — priced off stale stores (PL ends 2026-05-24, Serie B 2026-05-08). Last season's form only. Both landed; two matches prove nothing.

### Known data defects

- **Era-split team names** — 15 leagues, ~73 names. `KS Cracovia` (2023-25) vs `Cracovia` (2026), `IK Sirius` vs `Sirius`, `AIK Solna` vs `AIK`. Cause: 2026 seasons arriving from a different provider. Effect: thin predictions and false refusals. Cracovia was refused on 4 matches when 72 exist. Detector is fuzzy and overcounts — needs a manual pass.
- **Stale stores** — Premier League and Serie B end in May 2026.
- **No-tips resolved** — Al Faisaly 0-2 Neom, Al Hazem 0-1 Al Diriyah, Cracovia **3-2** Wieczysta. An earlier version of this note said all three finished under three goals; that was wrong. Cracovia was read at 1-2 with fourteen minutes left and finished on five, which would have beaten a U4.25 and lost a U3.0. Two of three would have been safe Unders, not three. Still not counted either way — a refusal is not a bet — and the error is left visible because grading declined fixtures from partial scores is exactly the habit that turns a no-tip rule into a tip.

---

## Most recent key updates

**Season stage — enabled, all leagues.** First feature dial to default on. Inert for the first 92% of a season. Across the closing 9%: 81.3% → 82.7%, 47 rescued / 30 broken. Positive at every shift 0.05–0.30 and in both halves. No single test clears 2σ; older seasons contributed +4 of +17.

**Module layer found dead.** `burst_sentinel`, `det`, `ulr`, `deg`, `mfr` change **zero** markets out of 998. They move the old flowchart's lean scores; probability selection no longer reads them. All 128 on/off combinations score identically: 1326/1630 = 81.35%.

**Reconnecting them fails both ways.** Adding their opinion to `mu` is monotonically harmful (−22 net at scale 2.0, >4σ). Inverting it collapses the market mix to 82% U4.25 — buying certainty, not information. Wire kept at 0.0.

**Ceiling measured.** Perfect knowledge of both teams' season-long scoring rates is worth **+2.3%** (81.3% → 83.6%). Knowing the actual result is worth +18.7%. Team-quality features are effectively exhausted.

**Eight ideas rejected on holdout.** BurstSentinel · probability ceiling · probability sharp lane · goal variance · additive mismatch · multiplicative mu blend · possession · referee tendency · rest days.

**Three of my own recommendations withdrawn after measurement.** Abstention lane (92.8% tail was 83 fixtures; 86.6% at 851, and the slope was entirely O1.0). Edge gate (backwards — dropping low-edge fixtures costs 9.5 points). League cull (league quality does not persist: r = +0.153).

**History gate confirmed already correct.** `MIN_MATCHES = 5` sits on the real cliff (1-4 matches → 72.8%). Raising it moves kept strike by 0.4 points while discarding a fifth of the book.

**Two leagues added** — Saudi Pro League (1,169 matches), Peru Liga 1 (6,860), both via ESPN.

**Recurring pattern worth naming.** Five separate times, something that raised hit rate turned out to be buying near-certain lines rather than predicting: floor sweep, O1.0 tail, abstention lane, league cull, inverted module tilt. Strike up, edge down, market mix collapsing to U4.25 is the signature.

---

## Athena's main mission

**Give the highest hit rate in bet tips. Over/under markets only. Nothing else.**

- Hit rate is the objective. Edge is diagnostic, not the target.
- No live odds, ever. Bookmaker lines would saturate the model and bias it toward following the market instead of pricing football.
- Recent data matters most. Current and last season carry the weight.
- Fewer, sharper plays beat more plays at a lower hit rate.
- A net gain counts even if it breaks some winners: 7 rescued for 2 broken is progress.

## Athena engine — key features

**Probability-based market selection.** For every rung on the Asian ladder, `P(win | mu)` from Poisson, scored against a typical fixture in the same league. Picks the best edge subject to a probability floor (0.79 default, per-league overridable). Confirmed on 29,762 unseen matches: 80.60% strike, +213 net wins, 18/27 leagues ≥80%.

**As-of everything.** Every feature reads only matches strictly before the fixture date. No lookahead anywhere in the pipeline.

**Chronological holdout on every claim.** Tune on the earlier portion, verify on the later. Nothing is written unless the gain survives data the search never saw.

**Full-win grading convention.** Half-wins count as wins — the bettor offsets the line. `U4.25` at 4 goals is a win.

**Per-league playability caps.** `max_under_line` / `min_over_line`. Low-tempo leagues capped at U3.5; `O1.0` restricted to four declared low-tempo competitions. This is a judgement about market prices the engine has no access to.

**Season stage.** Closing-stretch lift of +0.15 goals, placed as-of from matches played versus the league's typical season length.

**Sharp lane with a confidence veto.** Fires at 0.7σ from league norm with the lean agreeing, vetoed below 0.70 win probability. Strike 60.2% → 65.6%, edge +4.86% → +6.79%.

**History gate.** Refuses a fixture when either side has fewer than 5 prior matches. Produces genuine no-tips rather than guesses.

**Team tags in output.** Attack, defence, possession, form, table position — descriptive only, never fed back into selection.

**Three data providers.** openfootball, football-data.co.uk, ESPN. 52 leagues.

**No odds anywhere.** `assert_no_odds` fails loudly if a bookmaker-shaped column ever reaches a frame.

---

## Reference

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

Disabling the five non-earners was worth **+2.4%** in-sample and **+1.5%** on a
chronological holdout (7 of 9 leagues improve).

### Those figures are now obsolete — every module contributes exactly 0.00%

They were measured before probability selection existed, and probability
selection changed what a module can reach. The market now comes from
`market_select.choose(mu, league_mu, …)`, while `burst_sentinel`, `det`, `ulr`,
`deg` and `mfr` all adjust the **old flowchart's lean and corridor scores** — and
the flowchart no longer picks the market. They still compute, they still shape
the corridor shown in the output, and nothing they produce reaches the tip.

Measured directly over 998 fixtures across five leagues:

| toggle | markets changed |
|---|---|
| `burst_sentinel`, `det`, `ulr`, `deg`, `mfr` | **0 each** |
| `use_possession` | 14 |
| `use_season_stage` | 17 |

A search over all 128 on/off combinations of those seven returned the identical
holdout score for every one of them — 1326/1630 = 81.35% — because only the two
that move `mu` can change anything at all. See `scripts/combo_search.py`.

The modules are not worthless, they are unplugged: they encode real football
logic and whisper it into a corridor nothing reads. Reconnecting them would mean
letting them move `mu` rather than the lean.

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
