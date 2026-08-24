# Archive — 23 August 2026, the first calibrated slate

One day. Sixty-five settled tips across twenty-two leagues, twenty-seven bets,
and five engine defects found and fixed while the slate ran. Closed on 24 Aug
so the next stretch starts on a clean count.

    log.md      the live log, verbatim as it stood in README.md
    bets.tsv    every bet placed, with the price paid

---

## The numbers

    Tip 1     56 / 65     86.2%
    Tip 2     37 / 50     74.0%
    Bets      22 / 27     81.5%     ROI +6.1%

Comparable to the pre-calibration era only in shape, not in level — that book
ran 84.2% on Tip 1 and **-10.1% ROI**, because the strike rate was bought with
prices that could not pay for it. This one strikes higher AND makes money, which
is the whole point of the recalibration.

## What was actually learned

### Price discipline is first-order. Stake size is second.

The single most useful measurement of the day, from 1,941 replayed tips through
4,000 simulated 200-bet sequences:

    regime                        return/bet   flat 4% median   halved
    bought at buy-from               +5.32%        1.46x          0%
    bought at break-even             +0.65%        1.01x          1%
    bought 2% under break-even       -1.22%        0.87x          4%

Moving the STAKE from 2% to 8% at a good price changes the outcome 1.22x to
1.97x. Moving the PRICE from buy-from to break-even at an unchanged stake
changes it 1.46x to 1.01x — it deletes the system. The margin is the edge;
there is nothing else in there.

**Flat 4% is the right stake** and beats every Kelly variant tested: it matches
quarter Kelly's growth (1.46x vs 1.47x) at two-thirds the drawdown (20% vs 30%)
and never halved the bankroll in 4,000 runs, where quarter Kelly did. Full Kelly
is a disaster — a HIGHER median than half Kelly and a 5th percentile of 0.06x.

Bets are also near-independent: same-day hit-rate variance is **1.10x** the
binomial across 4,703 match-days, so ten bets on one slate behave like about
nine independent ones, not one large position.

### Live betting: the rungs decay at completely different rates

Five bets were placed in play, on the reasoning that Athena's pre-match read
still stands and the price has drifted longer. The read does still stand — but
the pre-match probability cannot price a bet struck at half time, and the rungs
are not one thing. Counted over 57,092 matches that reached the break at 0-0:

    league tempo        n     O0.5    buy≥     O1.5    buy≥     O2.5    buy≥
    under 2.5       14740    70.3%    1.49    36.0%    2.92    13.0%    8.08
    2.5 - 2.8       34479    75.4%    1.39    41.5%    2.53    16.9%    6.21
    2.8 - 3.1        7873    79.0%    1.33    47.3%    2.22    19.7%    5.33

**One more goal still probably arrives; two is a coin flip that loses.** Two
bets on this page make the point:

    Göteborg O0.5   1.22 paid, 1.39 needed    -3.5%    won
    Porto    O1.5   1.63 paid, 2.53 needed   -33.5%    won

Both won. Both were the same idea at a longer price. Nothing in the pre-match
tip distinguished them. And the drift itself is the warning: Porto's `O1.5`
moved 1.30 to 1.63, up 25%, while the fair price moved 1.30 to 2.53, up 95%.
A number that rises while its true value rises faster always looks like value.

### Volume was never a threshold problem

The day opened with "only 5 bets survive, I would prefer to go wider". Three
measured answers, in order of what they were worth:

1. **A third of the team lanes were invisible.** The venue defect (below) was
   withholding them outright, not mispricing them: 870 lanes became 1,153, and
   the added ones perform identically to the ones already there — 76.8% against
   76.4%, a difference of 0.4 points on a standard error of 2.5.
2. **Tip 2 is where the prices live.** 76% of fixtures produce one, and a third
   of them need 1.45+ where Tip 1 essentially never does.
3. **Cutting the margin is not an option**, per the staking table above.

## Five defects found, all upstream of the model

Not one was in the maths. The ladder, the settlement and the pricing were right
throughout; every fault was in what reached them.

**1. The team lane was split by venue — 7.7 points.** `_shrink_side` pulled both
sides toward `league_mu / 2`, on the stated reasoning that half the league mean
IS the per-side mean. It is not: home teams average 1.502 goals and away 1.154
against a shared target of 1.328, and every one of twelve leagues checked missed
by the same ±0.174. Separately `VENUE_BLEND = 0.35` left `gfh` about 0.113 goals
low before shrinkage even ran. Measured on 13,872 selection-free observations:

    original (shared shrink target)     HOME +4.1   AWAY -3.6   split 7.7
    per-side shrink target              HOME +2.5   AWAY -1.8   split 4.3
    + venue de-bias                     HOME +0.8   AWAY +0.3   split 0.5

The correction is symmetric, so `mu_total` is exactly unchanged and the match
lane never moved.

**2. Seven name failures, each a club with full history.** `København` filed as
`FC Copenhagen`, `Başakşehir` as `Buyuksehyr` (629 rows), `PSG` as `Paris SG`
(433), `Sporting Gijón` as `Sp Gijon`, `U. De Chile` as `Universidad de Chile`
(738), `Cienciano` split by era, `Inter de Bogotá` scoring 75.0 on a cutoff of
88. Each returned "insufficient history" with four seasons in the store.

**3. Half-time scores are censored on 0-0 finishes in 23 leagues.** 2,524
matches, not one retained. Drop the nulls and you have deleted exactly the
losing cases, so every one of those leagues reports that a goalless half
produces a second-half goal 100.0% of the time.

**4. The Allsvenskan is missing ~187 matches of the 2025 season.** 240 rows in
each neighbouring year, 53 in 2025. Every Swedish tip runs on the current season
alone.

**5. Promoted clubs abstain with their history one division down.** Le Mans (328
rows in FRA-L2), Elversberg (102 in GER-B2), Racing Santander (336 in ESP-L2).
No alias can reach it. Recurs for ~3 clubs per league every August, and is the
largest single volume fix still outstanding.

## The failure mode this project has, stated plainly

**Every defect above was invisible to the aggregate check that should have
caught it**, because the halves cancel:

    venue split      home +4.1, away -3.6   →  pooled +0.2
    team rungs       O1.5 +5.8, U1.5 -5.3   →  pooled +0.4
    edge tail        low +1.4, tail -2.5    →  pooled -0.4

Three separate defects, one shape. **The engine's pooled calibration figures are
close to worthless as a safety check** — they passed all three.

And the mirror error, committed repeatedly on 23 Aug: **a real number on a real
slice that generalises to nothing.** Six hypotheses died in one session —
nested-lane over-delivery (z = 2.26, dead on the full lane), a slope error
(+17.1 on n=42), side-level over-dispersion (right idea, 0.62 points and the
wrong direction), the translation layer (there isn't one), the mu mismatch (real,
worth 0.2 points), and residual mu spread as the cause of the tail.

What worked, every time:

- **Instruments with no selection in them.** `tt05_calibration` scores every
  priceable fixture — no floors, no ranking. That is what finally found the
  venue split after four aggregate checks passed it.
- **Two windows.** A constant that wins on recent data and not held-back data is
  a fit, not a finding.
- **A/B on identical fixtures.** Forcing the old behaviour back and re-running
  is what proved the shrink fix earned its place when the lane-level test could
  not see it.
- **Frozen membership.** A band defined by the quantity being changed moves
  under you; `tail_paired` fixes the fixtures first.

## The one defect left open

**High-edge tips are ~2.5 points overconfident, and it is not fixable.**

    stated edge        n    says     hit    gap    base    REAL
    under +1%       2704   83.3%   84.7%   +1.4   84.4%   +0.3
    +1 to +2%        807   82.9%   83.1%   +0.2   81.4%   +1.7
    +2 to +3.5%     1193   82.0%   81.9%   -0.1   79.2%   +2.7
    over +3.5%      2872   81.6%   79.1%   -2.5   74.8%   +4.3

Every market is calibrated (O1.5 +0.2, U3.0 -0.5, U4.25 -0.5). Every probability
band is calibrated. Every league base-rate band is calibrated. Only the top-edge
slice is not — and freezing that slice and re-pricing it shows the gap closing
cleanly as `MU_SHRINK` falls (-2.9, -2.4, -1.9, -1.4 at k = 0.35 to 0.20) while
the LIVE band barely moves, because it re-populates with newly-extreme fixtures.

That is the **winner's curse**: ranking by an estimate selects the fixtures whose
estimate came in high. It cannot be shrunk away — closing it would need k ≈ 0.06,
which is "ignore the fixture, price the league". The correct response is to price
it: **high-edge tips need roughly 3% more price, permanently.**

`MU_SHRINK` stays at **0.35**: on 4,724 tips, 0.25 gives 82.8% against 82.3%,
half a point on a standard error of 0.55, with edge unchanged and no mix
collapse. There is no case for moving it.

## Engine constants as they stood

    MU_SHRINK            0.35    MLS 0.15, IRL-PD 0.10
    TEAM_SHRINK          0.62    validated both directions, ~6,900 fixtures
    MIN_WIN_PROB         0.75    coupled to MU_SHRINK; 0.70 costs 4 points of
                                 strike for 1 of edge
    VENUE_BLEND          0.35    plus a symmetric de-bias, see defect 1
    DEFAULT_MARGIN       0.05

    Tip 1   break-even 1.211    at +5% margin  1.27
    Tip 2   break-even 1.393    at +5% margin  1.46

## Scripts written on 23 Aug

Each answers one question and states its own trap in the header.

    staking.py            stake rules against real per-bet returns
    ht_zero.py            what a 0-0 half time is worth, and the censoring guard
    live_ht_table.py      every Over rung priced at half time
    edge_bands.py         does the edge number sort? by band, market, league
    tt05_calibration.py   the per-side feature, free of lane selection
    team_shrink_sweep.py  TEAM_SHRINK both directions, two windows
    team_floor_sweep.py   floors instead of shrink, k held fixed
    gained_lanes.py       do the lanes the venue fix added actually deliver
    venue_fix_volume.py   how many lanes the split was hiding
    joint_sweep.py        MU_SHRINK and MIN_WIN_PROB together
    tail_paired.py        the tail with its membership frozen
    mu_mismatch.py        chosen on one mu, priced at another
    side_dispersion.py    is one side's goal count Poisson
    nested_lanes.py       is Tip 1 agreeing with Tip 2 information
    lane_selection.py     is the rung split caused by the selector
    name_audit.py         what name failures actually cost
    sort_tables.py        keep the log in kickoff order
    headline.py           derive the counts instead of typing them
