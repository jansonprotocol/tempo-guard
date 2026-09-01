# From fixture to card: the whole path, and every gate on it

What happens between a match entering Athena and a card appearing on the
board, in order, with the rule each stage applies and the constant that
sets it. Written so the flow can be checked against the code rather than
remembered — every number here is a named constant in the file cited.

---

## Stage 0 — the fixture arrives

`config/fixtures.tsv` is **typed by hand**: league code, both clubs,
kickoff. It is one of only seven typed files in the project; everything
else on this page is derived from it and from the results store.

`app/data/store.py` holds the results history — about 128,500 fixtures
across 63 competitions. `scripts/ingest_board.py` folds the board's own
completed matches back into it, so a fixture graded yesterday is history
today.

**Gate 0.** A club with fewer than `MIN_MATCHES = 5` resolvable prior
fixtures is skipped outright (`features.py:47`). No card is printed.

---

## Stage 1 — the goal expectation

`app/predict.build_request` resolves as-of features — strictly earlier
matches only — and produces `mu_total`, the expected goals in this match,
plus `p_home_tt05` / `p_away_tt05`, the chance each side scores at all.
Those two per-side numbers are what every lane below is cut from.

Two shipped corrections land here:

| correction | value | what it fixes |
|---|---|---|
| `MU_SHRINK` | 0.35, per-league overrides | the raw mu is over-spread; pulling it toward the league mean was worth more than any model change |
| `BIG_MATCH_DEBIT` | 0.15 goals | top-six clashes score under their own expectation |

The engine never sees a price. **No odds enter at any point before
Stage 5**, deliberately — this is the constraint the whole project is
built on.

---

## Stage 2 — the three lanes

`scripts/two_tips.py::tips()` turns one mu into up to three offers.

### Tip 1 — the match totals ladder

`market_select.score_markets(mu)` prices every rung (O1.5, U3.0, U4.25 …)
off the same mu. The engine picks one. **Tip 1's selection is
deliberately untouched by every rule added since** — an earlier version
overrode it and cost seven points of hit rate.

Gates, in order:

1. **`playable()`** — the rung must sit inside the league's own
   `max_under_line` / `min_over_line`.
2. **`stated()`** — the *published* probability is debited before it
   faces any bar. Two debits live here:
   - `HIGH_SAYS_FROM = 0.90`, `HIGH_SAYS_DEBIT = 0.012` — claims above
     90% read about 1.2 points hot.
   - `CONSENSUS_CAP_LEAGUES` — leagues whose claims are capped entirely.
3. **`MIN_WIN_PROB = 0.75`** — the floor, overridable per league.
4. **`MIN_EDGE = 0.010`** — the rung must beat a typical fixture of its
   kind by a point.

The debit hits probability **and** edge by the same amount, so a hot tip
must clear the bar on its honest number.

### Tip 2 — the runner-up, or a team total

The best rung tip 1 passed over, subject to:

- `abs(p − p1) > 0` — an equal-probability rung is the *same bet* on a
  different line, and offering it is offering one wager twice
- `MIN_EDGE = 0.010`
- `p ≥ max(MIN_TIP2_ABS 0.55, p1 − MAX_TIP2_GAP 0.25)` — the floor is
  **relative** to tip 1, not absolute

A **team total** (`team_total.candidates`) replaces it when it wins on
*edge*, since it is a different market rather than another rung. The
streak debit (`STREAK_FROM = 0.35`) is applied first, and the debited
edge must still clear `MIN_EDGE = 0.02` — an offer that only existed
because of a form streak stops printing here.

### Tip 3 — the result lane, on probation

`result_market.choose()` prices 1X / X2 / 12 and upgrades to DNB when one
side is strong enough.

| constant | value |
|---|---|
| `RESULT_TILT` | 1.10 on the home/away odds ratio |
| `DC_FLOOR` | 0.72 |
| `DNB_FROM` | 0.65 |
| `MIN_EDGE` | 0.02 |
| `_MIN_SAMPLE` | 150 fixtures before a league prices this lane at all |

---

## Stage 3 — the chooser picks ONE lane (the ★)

`webapp.py` / `final_pick.py::chosen()`.

```
star = tip 1
  unless tip 3 is a DNB claiming more than DNB_GATE = 2.0 points
  above tip 1                                    -> star = tip 3
  or tip 1 is absent and a result lane printed   -> star = tip 3
```

**Tip 2 is never starred.** It graded 12.7 points below tip 1 on the same
fixtures. **Double chance is never gated in** — at the same gate a DC
switch lost 3.53 points where the DNB gained 9.08.

This is the project's **only validated flip**, and it is the only place
in the flow where one lane replaces another.

---

## Stage 4 — the guard labels the starred lane

`webapp.py::_label_of` + `guard_slices.py`. Two inputs.

**The tier**, from the card alone:

```
green    a gated DNB, OR tip 1 claiming >=84% on an edge under 1.0
red      tip 1 under 76%, OR under 80% on an OVER
orange   everything else
```

**The confluence score**, from `config/guard_slices.tsv` — the league's
own record on that lane, each club's, the side's, and each club crossed
with the side, each shrunk toward the league baseline and summed as
deviations in points. The crossing is measured against the *club's* rate,
not the league's, so the same cards are not counted twice.

**The score is silent outside Europe** (+3.57 there, −0.06 in the
Americas), so non-European cards are labelled from the tier alone.

```
super green   green tier AND (gated DNB OR score >= +6.34)
green         green tier otherwise
orange        orange tier
red           red tier otherwise
super red     red tier AND score <= -12.99
```

Gated DNBs are assigned to super green **by the gate, not by rank** —
they grade 92.27%, while their printed claim understates them, so any
ranking on `says` would file them mid-orange.

| label | predicted hit | n | both windows |
|---|---|---|---|
| super green | 89.56% | 3,083 | 89.14 / 90.10 |
| green | 87.40% | 13,213 | 86.63 / 88.25 |
| orange | 83.42% | 36,607 | 83.28 / 83.55 |
| red | 77.96% | 7,873 | 78.43 / 77.49 |
| super red | 77.05% | 1,752 | 76.08 / 77.93 |

**Promotion is impossible by construction**: in every window orange's
best score quartile lands under green's worst, and red's best under
orange's worst.

---

## Stage 5 — the FIRST time a price is consulted

`scripts/odds_api.py` writes `config/odds_quotes.tsv` — consensus, best,
and which book, across 7–13 EU books per lane. The renderer reads that
file and never calls the API itself.

**The decision:**

```
needs = (1 / label's predicted hit rate) x (1 + DECLINE_MARGIN 0.06)

red or super red        -> NO PLAY, regardless of price
best quote >= needs     -> PLAY
best quote <  needs     -> DECLINE
```

The bar is not looking for good cards. It is looking for cards **the
market has underpriced** — which is why the hit rate falls as the bar
rises (81.8% at +0%, 72.7% at +8%) while the return climbs.

**Every lane gets its own verdict**, but only the starred lane's bar
comes from a validated hit rate. The others are computed from their own
printed claim and marked `claim-based`, because a bar built on an
unvalidated claim selects for **engine** error rather than market error
and returns −3.53% where the validated one returns +1.71%.

**There is no fallback.** If the starred lane declines, the card is a no
play; the other lanes still show their arithmetic, but the guard never
promotes one.

---

## Stage 6 — render, and stamp

`scripts/board.py --write` renders `web/index.html`. Lanes are ordered
**star first**, then whichever of tip 1 / tip 3 the star is not, then tip
2 last.

Every labelled card carrying a live quote is written **once, on first
sight**, to `config/forward_log.tsv` with its label, score, claim,
required price and the quote. First sight rather than latest, because a
re-render catches a moved price and would flatter or damn the rule by
accident. `scripts/forward_settle.py` grades those rows as results land.

---

## What a bet must pass, end to end

1. Both clubs have ≥ 5 prior fixtures
2. The rung is playable inside its league's lines
3. It survives `stated()` — the high-claim debit and any consensus cap
4. Published probability ≥ 75%, published edge ≥ 1.0%
5. It is the lane the chooser starred (tip 1, or a DNB clearing the
   2.0-point gate)
6. Its guard label is **not** red or super red
7. The best quote across books ≥ 1/label-rate × 1.06

Seven gates. **12.4% of priced cards clear the last one** — about 2.15
plays per match day across 16 leagues.

---

## What the flow does NOT do

- **No odds enter before Stage 5.** The engine has never seen a price
  when it chooses a lane, and that is deliberate.
- **No lane is ever swapped on price.** Only the DNB gate at Stage 3
  moves the star, and it does so on claimed probability.
- **Nothing recomputes its own thresholds.** Every constant above is
  frozen and registered; a card scored today uses the same bars as one
  scored last month, which is what makes the forward log gradeable.
