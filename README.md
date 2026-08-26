<p align="center">
  <img src="docs/athena-logo.png" alt="" width="120">
</p>

# ATHENA — TEMPO GUARD · BETA STAGE 2

**📱 Live board: [tempo-guard.vercel.app](https://tempo-guard.vercel.app)** — the same derived data as this page, rendered as an app; redeployed from `web/` on every merge.


## CURRENT CONFIRMED HITRATE: 82.8%

    lane                        Tip 1              Tip 2
    all matches             24 / 29   82.8%     15 / 20   75.0%
    played lanes  >+1%      16 / 18   88.9%     15 / 20   75.0%
    placed bets             17 / 23   73.9%    ROI -3.8%

**All matches** is the engine: every fixture priced, bet or not. **Played lanes** is the same count over the lanes with real edge — what was buyable, tracked in its own block below. **Placed bets** is the book. Rendered by `python scripts/board.py` from `config/fixtures.tsv`, never typed · over/under markets only · live tips, not backtests

Reset on **24 Aug 2026**, after the first full slate on the calibrated engine.
That day is archived at
**[archive/2026-08-23-first-calibrated-slate/](archive/2026-08-23-first-calibrated-slate/)**
— sixty-five settled tips, twenty-seven bets, five engine defects found and
fixed while it ran, and the measurements behind every rule below.

    lane        23 Aug, calibrated engine
    Tip 1       56 / 65     86.2%
    Tip 2       37 / 50     74.0%
    Bets        22 / 27     81.5%     ROI +6.1%

The era before that — 20 to 23 Aug, pre-calibration — is at
**[archive/2026-08-pre-calibration/](archive/2026-08-pre-calibration/)**. It ran
84.2% on Tip 1 and **-10.1% ROI**: a strike rate bought with prices that could
not pay for it.

## The four rules the last slate established

**1. Buy at `buy≥` or do not buy.** Measured over 1,941 tips and 4,000 simulated
sequences: at threshold a book returns 1.46x, at break-even 1.01x, two percent
under 0.87x. The margin is the entire edge.

**2. Flat 4% of bankroll.** Matches quarter Kelly's growth at two-thirds the
drawdown, and never halved the bankroll in 4,000 runs. Bigger stakes on a bad
price accelerate the loss rather than rescue it.

**3. ~~High-edge tips need about 3% more price than published.~~ Now applied
automatically — `buy≥` already carries it.** Tips over +3.5% stated edge come in
2.5 points overconfident — the winner's curse of ranking by an estimate, not a
defect that can be fixed. Measured twice on separate populations at −2.5 and
−2.9, against roughly zero in every band below, so `pricing.buy_from` adds 3.2%
to those tips and nothing to the rest. **Do not add it again by hand.**

**4. In play, price the rung and not the tip.** At 0-0 half time a 2.7-goal
league gives `O0.5` 75.4% (needs 1.39) and `O1.5` 41.5% (needs 2.53). The
pre-match probability priced 90 minutes and says nothing about which of those
you are buying.

**5. The DNB confluence rule (backtested 25 Aug — refined, still
probationary).** Draw No Bet on side X only when the strands point the same
way. The 6,354-fixture replay (`dnb_confluence.py`, every pair regenerated
through the live tip code) kept the rule's core and sharpened every leg:

- **The pointed team lane is real** — the archive's 78.6% avoid-defeat
  replicated at scale: 78.1% on 3,131 confluence fixtures.
- **Leg 1 (the Over Tip 1) carries no weight of its own**: the control
  group — pointed team lane WITHOUT an over corridor — avoids defeat at
  the same 77.8%. Keep it as context, not as a strand.
- **The strand that matters is WHICH rung points.** `O1.5` direct (81.1%,
  break-even 1.32) and `U1.5` elimination (82.7%, break-even 1.27 — the
  Novorizontino shape) are the real reads. The safe-looking `O0.5` tag is
  the trap: 73.4%, needs 1.55, which no book offers for these sides.
- **Venue is a full leg.** Strong rung + X at home: 83.9% avoid defeat on
  1,231 fixtures, break-even 1.26, and it holds both windows (older 1.21,
  newer 1.31 — same direction, no flip). X away needs 1.45+ even on the
  strong rungs. Reims, Everton, Al-Shabab were all home sides.
- **Leg 4 gets a number.** "Short price confirms" is now: the offered DNB
  price must CLEAR the group's break-even — **≥1.30 for strong-rung home,
  ≥1.50 away, never on an `O0.5`-only read**. At 1.22–1.27 the strong-home
  group is roughly fair, not an edge; the 6-0 live run sits inside that
  variance. The rule finds survivors; the price decides the bet.

**6. The line-translation ladder.** When the book does not offer the tipped
line, translate DOWN in safety, never down in price. Best to worst: the
SOFTER line one notch above the tip (U4.5 over a U4.25/U3.75 tip — converts
the half-loss outcome into a full win, usually for a couple of cents); the
tip as printed at its buy≥; the HARDER half-line (U3.5 for U3.75) **only at
buy≥ + 0.07–0.10** — its win probability is identical (both win at ≤3), but
at exactly 4 goals it loses the full stake where the quarter line loses
half, and exactly-4 lands 12–14% of the time at these expectations. Below
that premium the book is keeping the insurance money.

**Minimum average odds, from settlement rather than `1 / hit-rate`:**

    Tip 1    break-even 1.211    at +5% margin  1.27
    Tip 2    break-even 1.393    at +5% margin  1.46

## 🟢 Playable lanes — edge above +1%

> [!TIP]
> The block the bankroll follows: every lane carrying an edge above **+1%**, Tip 1 and Tip 2 alike. A tip at zero edge is the base rate wearing a probability — measured over 7,576 tips, lanes under +1% stated edge returned +0.3 points of real edge against +1.7 to +4.3 for everything above. A cell below the threshold says so instead of hiding; the counter counts lanes, not cards.

**Playable — 31 / 38   ·   81.6%**   ·   **Tip 1 — 16 / 18   ·   88.9%**   ·   **Tip 2 — 15 / 20   ·   75.0%**

<table align="left"><tr><th align="left">✅ 0-1 · 24-08 18:30 <b>Bologna v Lazio</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (80.5 +0.0)</td><td>U3.0 77.4% +2.3%<br>buy≥1.47</td><td>✅ U4.25 90.1% +1.4%<br>buy≥1.17 · lower edge</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-2 · 24-08 18:40 <b>Neom v Al-Qadsiah</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>O1.5 83.9% +4.3%<br>buy≥1.29</td><td>✅ <b>Al-Qadsiah O1.5</b> 57.1% +19.0%<br>buy≥1.90 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 (half win at 4) · 24-08 19:00 <b>Brøndby v Silkeborg</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Danish Superliga (84.7 +0.8)</td><td>U4.25 82.2% +1.1%<br>buy≥1.30</td><td>❌ U3.75 65.6% +1.4%<br>buy≥1.47 · floor −9.4</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-3 · 24-08 19:00 <b>Malmö v Djurgården</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (78.7 −4.6)</td><td>O1.5 84.8% +5.9%<br>buy≥1.28</td><td>❌ <b>Malmö O1.5</b> 60.1% +12.8%<br>buy≥1.80 · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 0-0 · 24-08 19:30 <b>Osasuna v Levante</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 75.7% +1.4%<br>buy≥1.39</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-6 · 24-08 20:00 <b>Jong FC Utrecht v Heracles</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (81.5 −1.3)</td><td>O1.5 83.9% +2.3%<br>buy≥1.25</td><td>✅ <b>Heracles O0.5</b> 83.8% +11.1%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 24-08 20:00 <b>Jong PSV v TOP Oss</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (81.5 −1.3)</td><td>U4.25 82.4% +2.6%<br>buy≥1.30</td><td>✅ U3.75 65.9% +3.5%<br>buy≥1.46 · floor −9.1</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 · 24-08 20:45 <b>Reims v Annecy</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (79.5 −3.0)</td><td>O1.5 77.4% +5.5%<br>buy≥1.40</td><td>✅ <b>Reims O0.5</b> 81.4% +7.6%<br>buy≥1.33 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-3 · 24-08 21:00 <b>Fulham v Chelsea</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (83.5 +0.1)</td><td>O1.5 81.3% +1.4%<br>buy≥1.29</td><td>✅ <b>Chelsea O0.5</b> 84.0% +9.2%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 24-08 21:30 <b>Málaga v Deportivo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (81.6 −5.5)</td><td>U3.0 79.1% +1.1%<br>buy≥1.42</td><td>✅ U2.75 58.5% +1.4%<br>buy≥1.61 · floor −16.5</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-4 · 25-08 00:30 <b>Athletic v Novorizontino</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>— under +1%</td><td>✅ <b>Athletic U1.5</b> 75.2% +13.1%<br>buy≥1.44 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-0 (push at 3) · 25-08 00:30 <b>Sport Recife v América-MG</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>U3.0 83.1% +1.4%<br>buy≥1.33</td><td>❌ U2.75 63.9% +1.9%<br>buy≥1.48 · floor −11.1</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 · 25-08 01:30 <b>Everton v U. de Concepción</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga de Primera (81.5 +0.2)</td><td>— under +1%</td><td>✅ <b>Everton O0.5</b> 83.7% +5.8%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 25-08 18:05 <b>Abha v Al-Khaleej</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>U4.25 85.8% +3.9%<br>buy≥1.28</td><td>✅ U3.75 70.7% +5.5%<br>buy≥1.42 · floor −4.3</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 25-08 18:10 <b>Al-Taawoun v Al-Fayha</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>U4.25 85.4% +3.5%<br>buy≥1.25</td><td>✅ U3.75 70.0% +4.8%<br>buy≥1.43 · floor −5.0</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-3 · 25-08 20:00 <b>Al-Ettifaq v Al-Nassr</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>O1.5 84.2% +4.5%<br>buy≥1.29</td><td>✅ <b>Al-Nassr O1.5</b> 56.4% +18.2%<br>buy≥1.92 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-3 · 25-08 20:00 <b>Al-Shabab v Al-Riyadh</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>— under +1%</td><td>✅ <b>Al-Shabab O0.5</b> 84.0% +5.9%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 25-08 21:00 <b>Valencia v Real Betis</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>U4.25 88.0% +1.1%<br>buy≥1.21</td><td>✅ U3.75 74.0% +1.7%<br>buy≥1.32 · floor −1.0</td></tr></table>
<table align="left"><tr><th align="left">❌ 4-1 (90'; goal at 90', tie to ET) · 25-08 21:00 <b>LASK v Celtic</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 87.2% +1.1%<br>buy≥1.22</td><td>❌ U3.75 72.7% +1.6%<br>buy≥1.34 · floor −9.3</td></tr><tr><td colspan="3"><sub>🏆 1st leg Celtic 3-0 LASK Linz · level 4-4 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 (push at 3) · 26-08 00:30 <b>Juventude v CRB</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>U3.0 83.3% +1.5%<br>buy≥1.32</td><td>❌ U2.75 64.1% +2.0%<br>buy≥1.48 · floor −10.9</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 26-08 03:00 <b>Cúcuta v Alianza Valledupar</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>U3.0 83.0% +3.3%<br>buy≥1.33</td><td>✅ U4.25 93.3% +1.8%<br>buy≥1.13 · lower edge</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 43' 1-0 <b>Rapid Vienna v Hearts</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 87.4% +1.8%<br>buy≥1.22 · <i>room for 3 · half from the 3rd</i></td><td>U3.75 73.0% +2.7%<br>buy≥1.33 · floor −9.0 · <i>room for 2</i></td></tr><tr><td colspan="3"><sub>🏆 1st leg Heart of Midlothian 2-2 Rapid Vienna · Rapid Vienna lead 3-2 on aggregate — Hearts need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 26-08 21:00 <b>Real Madrid v Real Sociedad</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 81.2% +6.8%<br>buy≥1.33</td><td><b>Real Madrid O1.5</b> 67.0% +24.7%<br>buy≥1.62 · team</td></tr></table>
<table align="left"><tr><th align="left">🟢 26-08 21:00 <b>Celje v Slovan Bratislava</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 87.6% +1.5%<br>buy≥1.21</td><td>U3.75 73.3% +2.2%<br>buy≥1.33 · floor −8.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Slovan Bratislava 1-1 NK Celje · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 26-08 21:00 <b>Lyon v Fenerbahçe</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>— under +1%</td><td>U3.75 72.2% +1.1%<br>buy≥1.35 · floor −9.8</td></tr><tr><td colspan="3"><sub>🏆 1st leg Fenerbahce 1-1 Lyon · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 26-08 21:00 <b>Viking v Dinamo Zagreb</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 87.4% +1.3%<br>buy≥1.21</td><td>U3.75 73.1% +2.0%<br>buy≥1.33 · floor −8.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg Dinamo Zagreb 2-2 Viking FK · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 26-08 23:00 <b>Boyacá Chicó v Fortaleza</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>U3.0 81.9% +2.2%<br>buy≥1.35</td><td>U4.25 92.8% +1.2%<br>buy≥1.14 · lower edge</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 03:30 <b>Atl. Nacional v Dep. Cali</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>— under +1%</td><td>O1.75 72.8% +5.6%<br>buy≥1.57 · floor −2.2</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 17:00 <b>KuPS v Shamrock Rovers</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 90.0% +4.5%<br>buy≥1.21</td><td>U3.75 77.3% +6.9%<br>buy≥1.31 · floor −4.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Shamrock Rovers 1-1 KuPS Kuopio · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 18:00 <b>Ararat-Armenia v U. Craiova</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 90.9% +2.4%<br>buy≥1.16</td><td>U3.75 78.7% +3.9%<br>buy≥1.29 · floor −3.3</td></tr><tr><td colspan="3"><sub>🏆 1st leg CSU Craiova 1-1 Ararat-Armenia · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 18:00 <b>Jablonec v Rangers</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.9% +3.3%<br>buy≥1.19</td><td>U3.75 75.4% +5.0%<br>buy≥1.34 · floor −6.6</td></tr><tr><td colspan="3"><sub>🏆 1st leg Rangers 1-0 Jablonec · Rangers carry 1-0 on aggregate — Jablonec need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 18:00 <b>M. Tel-Aviv v Lugano</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.3% +3.7%<br>buy≥1.22</td><td>U3.75 76.0% +5.6%<br>buy≥1.33 · floor −6.0</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Lugano 2-1 Maccabi Tel-Aviv · Lugano carry 2-1 on aggregate — M. Tel-Aviv need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 18:00 <b>Qarabağ v Twente</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.8% +3.2%<br>buy≥1.19</td><td>U3.75 75.3% +4.9%<br>buy≥1.34 · floor −6.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Twente 0-1 FK Qarabag · Qarabağ carry 1-0 on aggregate — Twente need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 18:45 <b>Monaco v Górnik</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 79.4% +3.4%<br>buy≥1.32</td><td>O2.25 58.6% +6.8%<br>buy≥1.66 · floor −23.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg Gornik Zabrze 2-3 AS Monaco · Monaco carry 3-2 on aggregate — Górnik need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 18:45 <b>Freiburg v Motherwell</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 82.3% +6.3%<br>buy≥1.32</td><td>O2.25 63.3% +11.5%<br>buy≥1.55 · floor −18.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Motherwell 1-3 SC Freiburg · Freiburg carry 3-1 on aggregate — Motherwell need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Plzeň v Crvena zvezda</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.5% +2.9%<br>buy≥1.15</td><td>U3.75 79.7% +4.8%<br>buy≥1.28 · floor −2.3</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Kauno Žalgiris v Beşiktaş</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 89.8% +1.3%<br>buy≥1.18</td><td>U3.75 76.9% +2.0%<br>buy≥1.28 · floor −5.1</td></tr><tr><td colspan="3"><sub>🏆 1st leg Besiktas 3-0 Kauno Zalgiris · Beşiktaş carry 3-0 on aggregate — Kauno Žalgiris need 3 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Omonia v St. Truiden</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.6% +3.0%<br>buy≥1.15</td><td>U3.75 79.8% +5.0%<br>buy≥1.28 · floor −2.2</td></tr><tr><td colspan="3"><sub>🏆 1st leg Sint-Truidense 1-0 Omonia Nicosia · St. Truiden carry 1-0 on aggregate — Omonia need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Hradec Králové v Panathinaikos</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.4% +2.8%<br>buy≥1.20</td><td>U3.75 74.6% +4.2%<br>buy≥1.35 · floor −7.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg Panathinaikos 2-2 FC Hradec Králové · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Inter Escaldes v Drita</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.0% +3.4%<br>buy≥1.19</td><td>U3.75 75.5% +5.1%<br>buy≥1.34 · floor −6.5</td></tr><tr><td colspan="3"><sub>🏆 1st leg Drita Gjilan 2-2 Inter D'Escaldes · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Pafos v Dinamo City</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 81.9% +5.9%<br>buy≥1.32</td><td>O2.25 62.6% +10.9%<br>buy≥1.56 · floor −19.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg Dinamo City 1-1 Pafos · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Riga v Klaksvík</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 87.1% +1.5%<br>buy≥1.22</td><td>U3.75 72.6% +2.2%<br>buy≥1.34 · floor −9.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg KI Klaksvik 0-0 Riga FC · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 19:00 <b>Brann v PAOK</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.2% +2.6%<br>buy≥1.20</td><td>U3.75 74.4% +4.0%<br>buy≥1.36 · floor −7.6</td></tr><tr><td colspan="3"><sub>🏆 1st leg PAOK 1-1 SK Brann · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:00 <b>Sétif v Ben Aknoun</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue Professionnelle 1 (90.5 +2.9)</td><td>U3.0 84.9% +1.8%<br>buy≥1.29</td><td>U2.75 66.5% +2.6%<br>buy≥1.43 · floor −8.5</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:00 <b>AGF v Benfica</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>— under +1%</td><td>U3.75 75.9% +1.1%<br>buy≥1.29 · floor −6.1</td></tr><tr><td colspan="3"><sub>🏆 1st leg Benfica 3-1 AGF · Benfica carry 3-1 on aggregate — AGF need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:00 <b>CSKA Sofia v OFI</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 90.8% +2.3%<br>buy≥1.16</td><td>U3.75 78.6% +3.7%<br>buy≥1.29 · floor −3.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg OFI Crete 3-0 CSKA Sofia · OFI carry 3-0 on aggregate — CSKA Sofia need 3 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:00 <b>Thun v Lech</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.2% +2.7%<br>buy≥1.16</td><td>U3.75 79.2% +4.4%<br>buy≥1.29 · floor −2.8</td></tr><tr><td colspan="3"><sub>🏆 1st leg Lech Poznan 7-0 FC Thun · Lech carry 7-0 on aggregate — Thun need 7 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:00 <b>Ajax v Sion</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 77.6% +1.6%<br>buy≥1.35</td><td>O2.25 55.7% +3.9%<br>buy≥1.73 · floor −26.3</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Sion 2-4 Ajax Amsterdam · Ajax carry 4-2 on aggregate — Sion need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:00 <b>St. Gallen v Nordsjælland</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.0% +3.4%<br>buy≥1.19</td><td>U3.75 75.5% +5.2%<br>buy≥1.34 · floor −6.5</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Nordsjælland 1-0 St. Gallen · Nordsjælland carry 1-0 on aggregate — St. Gallen need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:30 <b>Celta v Osasuna</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 75.7% +1.5%<br>buy≥1.39</td><td><b>Celta O0.5</b> 83.0% +3.7%<br>buy≥1.31 · team</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:30 <b>Ferencváros v Trabzonspor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.2% +2.6%<br>buy≥1.16</td><td>U3.75 79.1% +4.3%<br>buy≥1.29 · floor −2.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg Trabzonspor 0-1 Ferencvaros · Ferencváros carry 1-0 on aggregate — Trabzonspor need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:30 <b>Anderlecht v Kairat</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>— under +1%</td><td>O1.75 75.9% +3.7%<br>buy≥1.48 · floor −6.1</td></tr><tr><td colspan="3"><sub>🏆 1st leg Kairat Almaty 0-3 Anderlecht · Anderlecht carry 3-0 on aggregate — Kairat need 3 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:30 <b>Brighton v Tromsø</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 83.3% +7.3%<br>buy≥1.30</td><td>O2.25 65.0% +13.3%<br>buy≥1.51 · floor −17.0</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:30 <b>Austria Wien v Braga</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 86.9% +1.3%<br>buy≥1.22</td><td>U3.75 72.3% +1.9%<br>buy≥1.35 · floor −9.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Braga 2-0 Austria Vienna · Braga carry 2-0 on aggregate — Austria Wien need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:30 <b>Borac v Víkingur</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.8% +4.2%<br>buy≥1.22</td><td>U3.75 76.8% +6.5%<br>buy≥1.32 · floor −5.2</td></tr><tr><td colspan="3"><sub>🏆 1st leg Vikingur Reykjavik 1-3 Borac Banja Luka · Borac carry 3-1 on aggregate — Víkingur need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 20:45 <b>Rijeka v Midtjylland</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 86.7% +1.2%<br>buy≥1.23</td><td>U3.75 72.1% +1.7%<br>buy≥1.35 · floor −9.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Midtjylland 2-0 Rijeka · Midtjylland carry 2-0 on aggregate — Rijeka need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 21:00 <b>Barcelona v Athletic Club</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 78.3% +4.0%<br>buy≥1.38</td><td><b>Athletic U1.5</b> 75.3% +7.1%<br>buy≥1.44 · team</td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 21:00 <b>Partizan v Getafe</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 87.4% +1.8%<br>buy≥1.21</td><td>U3.75 73.1% +2.7%<br>buy≥1.33 · floor −8.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg Getafe 3-1 Partizan Belgrade · Getafe carry 3-1 on aggregate — Partizan need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 21:00 <b>Hibernian v Gent</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.0% +3.4%<br>buy≥1.19</td><td>U3.75 75.5% +5.2%<br>buy≥1.34 · floor −6.5</td></tr><tr><td colspan="3"><sub>🏆 1st leg KAA Gent 0-0 Hibernian · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 21:00 <b>Larne v Lincoln RI</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 91.1% +5.5%<br>buy≥1.20</td><td>U3.75 79.0% +8.7%<br>buy≥1.29 · floor −3.0</td></tr><tr><td colspan="3"><sub>🏆 1st leg Lincoln Red Imps 0-2 Larne · Larne carry 2-0 on aggregate — Lincoln RI need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 27-08 22:00 <b>MC Alger v Oran</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue Professionnelle 1 (90.5 +2.9)</td><td>— under +1%</td><td><b>MC Alger O0.5</b> 81.2% +7.2%<br>buy≥1.33 · team</td></tr></table>

<br clear="all">

## 🔵 Pending FUTURE match bettips

> [!NOTE]
> Every fixture Athena has priced that has not finished, playable or not — this and the completed block are the ENGINE's record. The typed source is `config/fixtures.tsv`; grade a fixture there and re-render with `python scripts/board.py`. The numbers after each league are its **(hit gap)** over its last 200 replayed matches — read the gap before trusting a row.

<table align="left"><tr><th align="left">🔴 LIVE 66' 1-1 <b>Al-Faisaly v Al-Fateh</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>— no tip, Al-Faisaly too little top-flight history (promoted)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 43' 1-0 <b>Rapid Vienna v Hearts</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 87.4% +1.8%<br>buy≥1.22 · <i>room for 3 · half from the 3rd</i></td><td>U3.75 73.0% +2.7%<br>buy≥1.33 · floor −9.0 · <i>room for 2</i></td></tr><tr><td colspan="3"><sub>🏆 1st leg Heart of Midlothian 2-2 Rapid Vienna · Rapid Vienna lead 3-2 on aggregate — Hearts need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 20:00 <b>Al Diriyah v Al-Kholood</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>— no tip, both promoted, too little top-flight history</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 21:00 <b>Real Madrid v Real Sociedad</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 81.2% +6.8%<br>buy≥1.33</td><td><b>Real Madrid O1.5</b> 67.0% +24.7%<br>buy≥1.62 · team</td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 21:00 <b>AEK Athens v Levski Sofia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 82.5% <b>-3.6%</b><br>buy≥1.30</td><td>— none</td></tr><tr><td colspan="3"><sub>🏆 1st leg Levski Sofia 0-0 AEK Athens · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 21:00 <b>Celje v Slovan Bratislava</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 87.6% +1.5%<br>buy≥1.21</td><td>U3.75 73.3% +2.2%<br>buy≥1.33 · floor −8.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Slovan Bratislava 1-1 NK Celje · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 21:00 <b>Lyon v Fenerbahçe</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 86.8% <b>+0.8%</b><br>buy≥1.22</td><td>U3.75 72.2% +1.1%<br>buy≥1.35 · floor −9.8</td></tr><tr><td colspan="3"><sub>🏆 1st leg Fenerbahce 1-1 Lyon · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 21:00 <b>Viking v Dinamo Zagreb</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 87.4% +1.3%<br>buy≥1.21</td><td>U3.75 73.1% +2.0%<br>buy≥1.33 · floor −8.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg Dinamo Zagreb 2-2 Viking FK · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 26-08 23:00 <b>Boyacá Chicó v Fortaleza</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>U3.0 81.9% +2.2%<br>buy≥1.35</td><td>U4.25 92.8% +1.2%<br>buy≥1.14 · lower edge</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 00:00 <b>Coquimbo v U. Católica</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga de Primera (81.5 +0.2)</td><td>O1.5 76.8% +0.5%<br>buy≥1.37</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 01:20 <b>América de Cali v Junior</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>U3.0 80.5% +0.8%<br>buy≥1.39</td><td>U2.75 60.3% +1.0%<br>buy≥1.57 · floor −14.7</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 03:30 <b>Atl. Nacional v Dep. Cali</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>U4.25 88.1% <b>−3.4%</b><br>buy≥1.20</td><td>O1.75 72.8% +5.6%<br>buy≥1.57 · floor −2.2</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 17:00 <b>KuPS v Shamrock Rovers</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 90.0% +4.5%<br>buy≥1.21</td><td>U3.75 77.3% +6.9%<br>buy≥1.31 · floor −4.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Shamrock Rovers 1-1 KuPS Kuopio · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:00 <b>Ararat-Armenia v U. Craiova</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 90.9% +2.4%<br>buy≥1.16</td><td>U3.75 78.7% +3.9%<br>buy≥1.29 · floor −3.3</td></tr><tr><td colspan="3"><sub>🏆 1st leg CSU Craiova 1-1 Ararat-Armenia · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:00 <b>Iberia v Jagiellonia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>— no tip: Iberia has no Club Elo rating</td><td>—</td></tr><tr><td colspan="3"><sub>🏆 1st leg Jagiellonia Bialystok 4-0 Iberia 1999 · Jagiellonia carry 4-0 on aggregate — Iberia need 4 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:00 <b>Jablonec v Rangers</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.9% +3.3%<br>buy≥1.19</td><td>U3.75 75.4% +5.0%<br>buy≥1.34 · floor −6.6</td></tr><tr><td colspan="3"><sub>🏆 1st leg Rangers 1-0 Jablonec · Rangers carry 1-0 on aggregate — Jablonec need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:00 <b>M. Tel-Aviv v Lugano</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.3% +3.7%<br>buy≥1.22</td><td>U3.75 76.0% +5.6%<br>buy≥1.33 · floor −6.0</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Lugano 2-1 Maccabi Tel-Aviv · Lugano carry 2-1 on aggregate — M. Tel-Aviv need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:00 <b>Qarabağ v Twente</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.8% +3.2%<br>buy≥1.19</td><td>U3.75 75.3% +4.9%<br>buy≥1.34 · floor −6.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Twente 0-1 FK Qarabag · Qarabağ carry 1-0 on aggregate — Twente need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:45 <b>Monaco v Górnik</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 79.4% +3.4%<br>buy≥1.32</td><td>O2.25 58.6% +6.8%<br>buy≥1.66 · floor −23.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg Gornik Zabrze 2-3 AS Monaco · Monaco carry 3-2 on aggregate — Górnik need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 18:45 <b>Freiburg v Motherwell</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 82.3% +6.3%<br>buy≥1.32</td><td>O2.25 63.3% +11.5%<br>buy≥1.55 · floor −18.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Motherwell 1-3 SC Freiburg · Freiburg carry 3-1 on aggregate — Motherwell need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Plzeň v Crvena zvezda</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.5% +2.9%<br>buy≥1.15</td><td>U3.75 79.7% +4.8%<br>buy≥1.28 · floor −2.3</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Kauno Žalgiris v Beşiktaş</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 89.8% +1.3%<br>buy≥1.18</td><td>U3.75 76.9% +2.0%<br>buy≥1.28 · floor −5.1</td></tr><tr><td colspan="3"><sub>🏆 1st leg Besiktas 3-0 Kauno Zalgiris · Beşiktaş carry 3-0 on aggregate — Kauno Žalgiris need 3 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Lillestrøm v Egnatia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>— no tip: Lillestrøm's Elo is 17 months stale (freshness guard)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Omonia v St. Truiden</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.6% +3.0%<br>buy≥1.15</td><td>U3.75 79.8% +5.0%<br>buy≥1.28 · floor −2.2</td></tr><tr><td colspan="3"><sub>🏆 1st leg Sint-Truidense 1-0 Omonia Nicosia · St. Truiden carry 1-0 on aggregate — Omonia need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Salzburg v Mjällby</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>— no tip: Mjällby has no Club Elo rating</td><td>—</td></tr><tr><td colspan="3"><sub>🏆 1st leg Mjällby AIF 0-1 RB Salzburg · Salzburg carry 1-0 on aggregate — Mjällby need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Hradec Králové v Panathinaikos</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.4% +2.8%<br>buy≥1.20</td><td>U3.75 74.6% +4.2%<br>buy≥1.35 · floor −7.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg Panathinaikos 2-2 FC Hradec Králové · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>København v Inter Turku</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>— no tip: Inter Turku's Elo is 3 years stale (freshness guard)</td><td>—</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Inter Turku 0-0 F.C. København · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Inter Escaldes v Drita</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.0% +3.4%<br>buy≥1.19</td><td>U3.75 75.5% +5.1%<br>buy≥1.34 · floor −6.5</td></tr><tr><td colspan="3"><sub>🏆 1st leg Drita Gjilan 2-2 Inter D'Escaldes · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Pafos v Dinamo City</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 81.9% +5.9%<br>buy≥1.32</td><td>O2.25 62.6% +10.9%<br>buy≥1.56 · floor −19.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg Dinamo City 1-1 Pafos · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Raków v Hajduk</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 84.2% <b>-1.3%</b><br>buy≥1.27</td><td>— none</td></tr><tr><td colspan="3"><sub>🏆 1st leg Hajduk Split 2-2 Raków Czestochowa · level 2-2 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Riga v Klaksvík</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 87.1% +1.5%<br>buy≥1.22</td><td>U3.75 72.6% +2.2%<br>buy≥1.34 · floor −9.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg KI Klaksvik 0-0 Riga FC · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 19:00 <b>Brann v PAOK</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 88.2% +2.6%<br>buy≥1.20</td><td>U3.75 74.4% +4.0%<br>buy≥1.36 · floor −7.6</td></tr><tr><td colspan="3"><sub>🏆 1st leg PAOK 1-1 SK Brann · level 1-1 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>Sétif v Ben Aknoun</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue Professionnelle 1 (90.5 +2.9)</td><td>U3.0 84.9% +1.8%<br>buy≥1.29</td><td>U2.75 66.5% +2.6%<br>buy≥1.43 · floor −8.5</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>AGF v Benfica</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 89.2% <b>+0.7%</b><br>buy≥1.19</td><td>U3.75 75.9% +1.1%<br>buy≥1.29 · floor −6.1</td></tr><tr><td colspan="3"><sub>🏆 1st leg Benfica 3-1 AGF · Benfica carry 3-1 on aggregate — AGF need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>CSKA Sofia v OFI</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 90.8% +2.3%<br>buy≥1.16</td><td>U3.75 78.6% +3.7%<br>buy≥1.29 · floor −3.4</td></tr><tr><td colspan="3"><sub>🏆 1st leg OFI Crete 3-0 CSKA Sofia · OFI carry 3-0 on aggregate — CSKA Sofia need 3 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>Thun v Lech</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.2% +2.7%<br>buy≥1.16</td><td>U3.75 79.2% +4.4%<br>buy≥1.29 · floor −2.8</td></tr><tr><td colspan="3"><sub>🏆 1st leg Lech Poznan 7-0 FC Thun · Lech carry 7-0 on aggregate — Thun need 7 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>Ajax v Sion</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 77.6% +1.6%<br>buy≥1.35</td><td>O2.25 55.7% +3.9%<br>buy≥1.73 · floor −26.3</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Sion 2-4 Ajax Amsterdam · Ajax carry 4-2 on aggregate — Sion need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>St. Gallen v Nordsjælland</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.0% +3.4%<br>buy≥1.19</td><td>U3.75 75.5% +5.2%<br>buy≥1.34 · floor −6.5</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Nordsjælland 1-0 St. Gallen · Nordsjælland carry 1-0 on aggregate — St. Gallen need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:00 <b>H. Tel Aviv v Atalanta</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 84.1% <b>-1.5%</b><br>buy≥1.27</td><td>— none</td></tr><tr><td colspan="3"><sub>🏆 1st leg Atalanta 0-0 Hapoel Tel Aviv · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:30 <b>Celta v Osasuna</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 75.7% +1.5%<br>buy≥1.39</td><td><b>Celta O0.5</b> 83.0% +3.7%<br>buy≥1.31 · team</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:30 <b>Ferencváros v Trabzonspor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 91.2% +2.6%<br>buy≥1.16</td><td>U3.75 79.1% +4.3%<br>buy≥1.29 · floor −2.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg Trabzonspor 0-1 Ferencvaros · Ferencváros carry 1-0 on aggregate — Trabzonspor need 1 to level it.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:30 <b>Anderlecht v Kairat</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UEL Playoff · probationary (86.4 −0.2)</td><td>U4.25 82.3% <b>-6.2%</b><br>buy≥1.30</td><td>O1.75 75.9% +3.7%<br>buy≥1.48 · floor −6.1</td></tr><tr><td colspan="3"><sub>🏆 1st leg Kairat Almaty 0-3 Anderlecht · Anderlecht carry 3-0 on aggregate — Kairat need 3 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:30 <b>Brighton v Tromsø</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>O1.5 83.3% +7.3%<br>buy≥1.30</td><td>O2.25 65.0% +13.3%<br>buy≥1.51 · floor −17.0</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:30 <b>Austria Wien v Braga</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 86.9% +1.3%<br>buy≥1.22</td><td>U3.75 72.3% +1.9%<br>buy≥1.35 · floor −9.7</td></tr><tr><td colspan="3"><sub>🏆 1st leg Braga 2-0 Austria Vienna · Braga carry 2-0 on aggregate — Austria Wien need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:30 <b>Borac v Víkingur</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.8% +4.2%<br>buy≥1.22</td><td>U3.75 76.8% +6.5%<br>buy≥1.32 · floor −5.2</td></tr><tr><td colspan="3"><sub>🏆 1st leg Vikingur Reykjavik 1-3 Borac Banja Luka · Borac carry 3-1 on aggregate — Víkingur need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 20:45 <b>Rijeka v Midtjylland</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 86.7% +1.2%<br>buy≥1.23</td><td>U3.75 72.1% +1.7%<br>buy≥1.35 · floor −9.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg FC Midtjylland 2-0 Rijeka · Midtjylland carry 2-0 on aggregate — Rijeka need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 21:00 <b>Barcelona v Athletic Club</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 78.3% +4.0%<br>buy≥1.38</td><td><b>Athletic U1.5</b> 75.3% +7.1%<br>buy≥1.44 · team</td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 21:00 <b>Partizan v Getafe</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 87.4% +1.8%<br>buy≥1.21</td><td>U3.75 73.1% +2.7%<br>buy≥1.33 · floor −8.9</td></tr><tr><td colspan="3"><sub>🏆 1st leg Getafe 3-1 Partizan Belgrade · Getafe carry 3-1 on aggregate — Partizan need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 21:00 <b>Hibernian v Gent</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 89.0% +3.4%<br>buy≥1.19</td><td>U3.75 75.5% +5.2%<br>buy≥1.34 · floor −6.5</td></tr><tr><td colspan="3"><sub>🏆 1st leg KAA Gent 0-0 Hibernian · level 0-0 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 21:00 <b>Larne v Lincoln RI</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UECL Playoff · probationary (82.2 −2.3)</td><td>U4.25 91.1% +5.5%<br>buy≥1.20</td><td>U3.75 79.0% +8.7%<br>buy≥1.29 · floor −3.0</td></tr><tr><td colspan="3"><sub>🏆 1st leg Lincoln Red Imps 0-2 Larne · Larne carry 2-0 on aggregate — Lincoln RI need 2 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 27-08 22:00 <b>MC Alger v Oran</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue Professionnelle 1 (90.5 +2.9)</td><td>U4.25 93.3% <b>−0.1%</b><br>buy≥1.13</td><td><b>MC Alger O0.5</b> 81.2% +7.2%<br>buy≥1.33 · team</td></tr></table>

<br clear="all">

## ⚪ Completed FUTURE match bettips

**Tip 1 — 24 / 29   ·   82.8%**   ·   **Tip 2 — 15 / 20   ·   75.0%**

<table align="left"><tr><th align="left">✅ 0-1 · 24-08 18:30 <b>Bologna v Lazio</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (80.5 +0.0)</td><td>U3.0 77.4% +2.3%<br>buy≥1.47</td><td>✅ U4.25 90.1% +1.4%<br>buy≥1.17 · lower edge</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-2 · 24-08 18:40 <b>Neom v Al-Qadsiah</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>O1.5 83.9% +4.3%<br>buy≥1.29</td><td>✅ <b>Al-Qadsiah O1.5</b> 57.1% +19.0%<br>buy≥1.90 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 (half win at 4) · 24-08 19:00 <b>Brøndby v Silkeborg</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Danish Superliga (84.7 +0.8)</td><td>U4.25 82.2% +1.1%<br>buy≥1.30</td><td>❌ U3.75 65.6% +1.4%<br>buy≥1.47 · floor −9.4</td></tr></table>
<table align="left"><tr><th align="left">⚪ 3-2 (no tip) · 24-08 19:00 <b>Celta Fortuna v FC Andorra</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (81.6 −5.5)</td><td>— no tip, Celta B has 1 row (reserve side, promoted)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-3 · 24-08 19:00 <b>Malmö v Djurgården</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (78.7 −4.6)</td><td>O1.5 84.8% +5.9%<br>buy≥1.28</td><td>❌ <b>Malmö O1.5</b> 60.1% +12.8%<br>buy≥1.80 · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 0-0 · 24-08 19:30 <b>Osasuna v Levante</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>O1.5 75.7% +1.4%<br>buy≥1.39</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-6 · 24-08 20:00 <b>Jong FC Utrecht v Heracles</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (81.5 −1.3)</td><td>O1.5 83.9% +2.3%<br>buy≥1.25</td><td>✅ <b>Heracles O0.5</b> 83.8% +11.1%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 24-08 20:00 <b>Jong PSV v TOP Oss</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (81.5 −1.3)</td><td>U4.25 82.4% +2.6%<br>buy≥1.30</td><td>✅ U3.75 65.9% +3.5%<br>buy≥1.46 · floor −9.1</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 (two penalties) · 24-08 20:00 <b>Al-Ittihad v Al-Hazem</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>U4.25 82.5% +0.5%<br>buy≥1.30</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">⚪ 2-0 (no tip) · 24-08 20:30 <b>Kocaelispor v Amed</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.2 −2.9)</td><td>— no tip, Amedspor has 1 row (promoted)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 · 24-08 20:45 <b>Reims v Annecy</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (79.5 −3.0)</td><td>O1.5 77.4% +5.5%<br>buy≥1.40</td><td>✅ <b>Reims O0.5</b> 81.4% +7.6%<br>buy≥1.33 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 4-0 (half win at 4) · 24-08 20:45 <b>Roma v Fiorentina</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (80.5 +0.0)</td><td>U4.25 88.4% <b>−0.3%</b><br>buy≥1.20</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-3 · 24-08 21:00 <b>Fulham v Chelsea</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (83.5 +0.1)</td><td>O1.5 81.3% +1.4%<br>buy≥1.29</td><td>✅ <b>Chelsea O0.5</b> 84.0% +9.2%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 24-08 21:15 <b>Gil Vicente v Casa Pia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (79.5 −0.9)</td><td>U4.25 86.8% +0.5%<br>buy≥1.22</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 24-08 21:30 <b>Granada v Mallorca</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (81.6 −5.5)</td><td>U3.0 78.1% +0.1%<br>buy≥1.45</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 24-08 21:30 <b>Málaga v Deportivo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (81.6 −5.5)</td><td>U3.0 79.1% +1.1%<br>buy≥1.42</td><td>✅ U2.75 58.5% +1.4%<br>buy≥1.61 · floor −16.5</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-4 · 25-08 00:30 <b>Athletic v Novorizontino</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>O1.0 89.2% +0.2%<br>buy≥1.22</td><td>✅ <b>Athletic U1.5</b> 75.2% +13.1%<br>buy≥1.44 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-0 (push at 3) · 25-08 00:30 <b>Sport Recife v América-MG</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>U3.0 83.1% +1.4%<br>buy≥1.33</td><td>❌ U2.75 63.9% +1.9%<br>buy≥1.48 · floor −11.1</td></tr></table>
<table align="left"><tr><th align="left">❌ 2-3 · 25-08 01:00 <b>Botafogo v Athletico</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão (82.7 +0.9)</td><td>U4.25 88.5% <b>−0.4%</b><br>buy≥1.20</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 · 25-08 01:30 <b>Everton v U. de Concepción</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga de Primera (81.5 +0.2)</td><td>O1.5 76.5% +0.1%<br>buy≥1.37</td><td>✅ <b>Everton O0.5</b> 83.7% +5.8%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 25-08 18:05 <b>Abha v Al-Khaleej</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>U4.25 85.8% +3.9%<br>buy≥1.28</td><td>✅ U3.75 70.7% +5.5%<br>buy≥1.42 · floor −4.3</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 25-08 18:10 <b>Al-Taawoun v Al-Fayha</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>U4.25 85.4% +3.5%<br>buy≥1.25</td><td>✅ U3.75 70.0% +4.8%<br>buy≥1.43 · floor −5.0</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 (90'+4; 5-2 aet) · 25-08 18:45 <b>Sabah v H. Be'er Sheva</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 86.5% +0.4%<br>buy≥1.23</td><td>— none</td></tr><tr><td colspan="3"><sub>🏆 1st leg Hapoel Be'er 2-1 Sabah FK · level 4-4 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">✅ 2-3 · 25-08 20:00 <b>Al-Ettifaq v Al-Nassr</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>O1.5 84.2% +4.5%<br>buy≥1.29</td><td>✅ <b>Al-Nassr O1.5</b> 56.4% +18.2%<br>buy≥1.92 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-3 · 25-08 20:00 <b>Al-Shabab v Al-Riyadh</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (82.7 −0.8)</td><td>O1.5 79.9% +0.2%<br>buy≥1.31</td><td>✅ <b>Al-Shabab O0.5</b> 84.0% +5.9%<br>buy≥1.29 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 25-08 21:00 <b>Valencia v Real Betis</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (80.5 −0.1)</td><td>U4.25 88.0% +1.1%<br>buy≥1.21</td><td>✅ U3.75 74.0% +1.7%<br>buy≥1.32 · floor −1.0</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-0 · 25-08 21:00 <b>Bodø/Glimt v NEC</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 86.6% +0.5%<br>buy≥1.23</td><td>— none</td></tr><tr><td colspan="3"><sub>🏆 1st leg NEC Nijmegen 1-3 Bodo/Glimt · Bodø/Glimt lead 6-1 on aggregate — NEC need 5 to level.</sub></td></tr></table>
<table align="left"><tr><th align="left">❌ 4-1 (90'; goal at 90', tie to ET) · 25-08 21:00 <b>LASK v Celtic</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>UCL Playoff · probationary (80.8 −3.0)</td><td>U4.25 87.2% +1.1%<br>buy≥1.22</td><td>❌ U3.75 72.7% +1.6%<br>buy≥1.34 · floor −9.3</td></tr><tr><td colspan="3"><sub>🏆 1st leg Celtic 3-0 LASK Linz · level 4-4 on aggregate — as it stands this goes to extra time.</sub></td></tr></table>
<table align="left"><tr><th align="left">✅ 3-0 (push at 3) · 26-08 00:30 <b>Atlético-GO v Botafogo-SP</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>U3.0 81.9% +0.1%<br>buy≥1.36</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 (push at 3) · 26-08 00:30 <b>Juventude v CRB</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (82.8 −1.0)</td><td>U3.0 83.3% +1.5%<br>buy≥1.32</td><td>❌ U2.75 64.1% +2.0%<br>buy≥1.48 · floor −10.9</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 26-08 03:00 <b>Cúcuta v Alianza Valledupar</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (83.0 −4.0)</td><td>U3.0 83.0% +3.3%<br>buy≥1.33</td><td>✅ U4.25 93.3% +1.8%<br>buy≥1.13 · lower edge</td></tr></table>

<br clear="all">

### 🟡 Actual placed bets

**Settled: 17 / 23  ·  ROI -3.8%  ·  flat stakes** — settled through real settlement fractions by the ledger; a push or half-win counts as a hit, a half-loss does not. Notes travel with the bet in `config/bets.tsv`.

| Result | Fixture | Lane | Odds | Return | Note |
|---|---|---|---|---|---|
| ❌ | Fulham v Chelsea | U3.75 | 1.36 | 0.00x | pre-refresh Tip 2; the refresh flipped the fixture |
| ✅ | Málaga v Deportivo | O0.5 (home) | 1.35 | 1.35x | pre-refresh team lane; later dropped off the fixture |
| ✅ | Málaga v Deportivo | O1.5 | 1.48 | 1.48x | pre-refresh Tip 1 |
| ✅ | Sport Recife v América-MG | U3.5 | 1.28 | 1.28x | same tier as the U3.0 tip |
| ❌½ | Brøndby v Silkeborg | U3.75 | 1.50 | 0.50x | Tip 2 |
| ✅ | Brøndby v Silkeborg | U4.5 | 1.26 | 1.26x | same tier, softer settlement |
| ❌ | Malmö v Djurgården | O1.5 (home) | 1.93 | 0.00x | the star lane, bought at 1.93 vs buy≥1.80 |
| ❌ | Osasuna v Levante | O1.5 | 1.30 | 0.00x | filler, under buy-from |
| ❌ | Al-Ittihad v Al-Hazem | U4.5 | 1.22 | 0.00x | filler |
| ✅ | Fulham v Chelsea | O1.5 | 1.23 | 1.23x | the tip, at break-even |
| ✅ | Bologna v Lazio | U3.5 | 1.22 | 1.22x | filler, under buy-from |
| ✅ | Cúcuta v Alianza Valledupar | U3.0 | 1.24 | 1.24x | the tip, 9 cents under buy-from |
| ✅ | Reims v Annecy | DNB (home) | 1.35 | 1.35x | experimental |
| ◦ | Al-Shabab v Al-Riyadh | DNB (home) | 1.27 | 1.00x | experimental |
| ✅ | Neom v Al-Qadsiah | DNB (away) | 1.22 | 1.22x | experimental |
| ✅ | Everton v U. de Concepción | DNB (home) | 1.22 | 1.22x | experimental |
| ✅ | Jong FC Utrecht v Heracles | DNB (away) | 1.23 | 1.23x | experimental, engine's steepest DNB verdict |
| ✅ | Jong PSV v TOP Oss | U4.5 | 1.30 | 1.30x | clean buy, +7.1% at strike |
| ✅ | Juventude v CRB | U3.5 | 1.20 | 1.20x | same tier as the flipped U3.0 tip, extra cushion; U3.0 unavailable |
| ✅ | Athletic v Novorizontino | DNB (away) | 1.63 | 1.63x | experimental, derived from the Athletic U1.5 team lane |
| ❌ | LASK v Celtic | U4.5 | 1.20 | 0.00x | first cup-lane bet, probationary; tip U4.25, book line U4.5 softer, +4.6% at strike |
| ✅ | Bodø/Glimt v NEC | U4.5 | 1.42 | 1.42x | cup lane, probationary; +22.9% EV at strike, the board price of the slate |
| — open | Rapid Vienna v Hearts | U4.5 | 1.18 | — | cup lane, softer line above U4.25 tip, +3.1% at strike |
| ◦ | Plzeň v Crvena zvezda | U4.5 | 1.14 | 1.00x | cashed out at stake, replaced with the U3.5 below |
| — open | Thun v Lech | U4.5 | 1.19 | — | cup lane, +7.0% at strike, best EV of the five |
| — open | St. Gallen v Nordsjælland | U4.5 | 1.20 | — | cup lane, +6.1% at strike |
| — open | Celta v Osasuna | DNB (home) | 1.38 | — | experimental; O0.5-pointed read, the rung the backtest flagged |
| — open | Borac v Víkingur | U4.5 | 1.17 | — | cup lane, +3.9% at strike |
| — open | Viking v Dinamo Zagreb | U3.5 | 1.45 | — | rule-6 harder line, needed ~1.41, +8.1% at strike |
| — open | Plzeň v Crvena zvezda | U3.5 | 1.43 | — | rule-6 harder line, needed ~1.32, +14.2% at strike |
| — open | Riga v Klaksvík | U3.5 | 1.47 | — | rule-6 harder line, needed ~1.41, +9.7% at strike |
| — open | CSKA Sofia v OFI | U3.5 | 1.36 | — | rule-6 harder line, needed ~1.32, +8.4% at strike |

## Engine state — 24 Aug 2026

    MU_SHRINK              0.35        per-fixture goal expectation, shrunk
                                       toward the league mean
    MU_SHRINK_BY_LEAGUE    MLS 0.15, IRL-PD 0.10
    TEAM_SHRINK            0.62        the team-total lane, shrunk separately
    BIG_MATCH_DEBIT        0.15        both sides top-6 as-of: two fat form
                                       rates price a top clash UP exactly when
                                       the occasion pushes it down; validated
                                       on two windows (−0.13 / −0.16), and the
                                       relegation mirror case died a sign-flip
    TEAM_RATE_FLOOR        0.95        floor on the shrunk per-side rate; the
                                       low end needed a level fix, not a spread
                                       one, and the two ends pull opposite ways
    MIN_WIN_PROB           0.75        probability floor, re-tuned with the shrink

    weighted calibration gap    -0.6 in-sample, -1.5 out-of-sample  (was -4.4)
    realised edge over base     +2.23                               (was +1.35)
    top market share            41%                                 (was 54%)

All 37 tippable leagues retrosimmed. Three remain cull candidates rather than
tuning targets — `IRL-PD` (residual slope -0.600, anti-correlated), `COL-PA`
(-0.06, no usable signal) and `MLS` (-5.2 out of sample).


### Recalibration — the cause found and half-fixed

#### The cause: mu is over-spread by 2.4x

Poisson was ruled out first: real totals match it to within half a point at
every rung traded, on 272,857 matches. If mu were right the probabilities would
be right. So the error is in mu, and regressing what happened on what was
predicted across ~2,000 replayed fixtures says which kind:

    actual_total = 1.640 + 0.424 * mu

**Slope 0.42 where 1.0 would be correct.** Level is fine — pooled bias +0.08
goals — so this is not the engine reading leagues too high or too low. It is
reading individual fixtures far too confidently:

    lowest mu fifth    says 1.99 goals   actually 2.54   miss +0.55
    highest mu fifth   says 3.60 goals   actually 3.26   miss -0.34

That shape explains everything at once. Every tip is selected on exactly the
extremes that are wrong, so the league average looks fine while the tip book
inherits the whole error — 85.7% claimed against 81.2% delivered across 26
leagues, and 80.4% against 69.6% on the bets actually placed, which chase the
extremes harder still.

#### The fix, and why 0.60 rather than the measured 0.42

`mu' = league_mu + k * (mu - league_mu)`, applied where mu is produced so tips,
break-even prices and buy-from thresholds all inherit it. Measured over 1,487
replays:

    k      says     hit      gap    base rate   realised edge   U4.25 share
    1.00   85.7%   81.2%    -4.5      79.8%        +1.35           39%
    0.80   85.6%   82.5%    -3.1      81.2%        +1.34           46%
    0.60   85.6%   83.9%    -1.7      82.4%        +1.50           54%
    0.42   85.8%   84.3%    -1.5      83.6%        +0.71           62%

Full shrinkage buys the last 0.2 of the gap with certainty: the base rate of
the markets it picks climbs to 83.6% and realised edge — strike minus base, the
one figure that cannot be bought by retreating to a safer rung — halves. **0.60
takes almost all the calibration gain and leaves the edge intact.** The edge
difference between 1.00 and 0.60 is inside the noise; the honest claim is that
shrinkage preserves edge while fixing calibration, not that it improves it.

Shipped as `MU_SHRINK = 0.60` in `app/data/features.py`. 101 tests pass.

#### What it actually bought

Re-running the same ten leagues at n=200:

    weighted gap   -4.4  ->  -2.3     halved
    CHN-SL  -9.6 -> -5.5      SAU-PL  -7.2 -> better than -4
    CHI-PD  -9.6 -> better    JPN-J1  -5.3 -> -3.2
    COL-PA  -5.6 -> -5.0      PER-L1  -4.2 -> -1.9
    ENG-CH  -0.9 -> +1.3      MLS     -3.1 -> -4.8  (worse)

#### The second half: the floor had to move with it

Shrinkage alone left `U4.25` taking **88-95%** of tips in five leagues, and a
book that only buys U4.25 is not a tipping engine. The cause was not the
shrinkage — it was `MIN_WIN_PROB = 0.79`, whose own justification in
`market_select` reads:

    "the highest floor that clears 80% strike while keeping the most-picked
     line under half of all calls"

That was measured against an over-spread mu. **The floor is absolute, so its
behaviour depends entirely on how spread out mu is.** Pulling every fixture
toward its league mean meant far fewer rungs clear 79%, and the selector fell
through to the safest buyable one every time. The constant did not change; what
it was applied to did, and it silently stopped meeting its own criterion.

Swept at `MU_SHRINK = 0.60` over 1,487 replays:

    floor    hit     base    realised edge   top line   mix
    0.79    83.9%   82.4%       +1.50          54%      U4.25 54  O1.5 23  U3.0 16
    0.75    81.4%   79.5%       +1.99          34%      O1.5 34   U4.25 32  U3.0 27
    0.70    77.9%   75.9%       +2.08          45%      O1.5 45   U3.0 35   U4.25 11
    0.65    73.9%   71.4%       +2.54          47%      O1.5 46   U3.0 38   O2.25 7
    0.60    70.3%   67.0%       +3.24          39%      O1.5 38   U3.0 32   O2.25 16

Lower floors keep buying edge with strike rate — 0.60 pays **13 points of hit
rate for 1.7 of edge**, which this project should not take. **0.75 is the only
setting that improves edge while holding strike above 80% and restoring the
original criterion**: the top line falls from 54% to 34% and the book becomes a
genuine three-way spread. At 0.70 concentration returns from the other side,
with `O1.5` at 45%.

Shipped as `MIN_WIN_PROB = 0.75`.

#### What both changes did, per league

    league    U4.25 share            realised edge
              before -> after        before -> after
    JPN-J1     95%  ->  35%           -0.99  ->  +0.45
    PER-L1     94%  ->  43%           -0.78  ->  +2.36
    CHI-PD     93%  ->  43%           -0.22  ->  +3.77
    ENG-CH     88%  ->  36%           +0.09  ->  +1.48
    ESP-L2     82%  ->  52%           +0.59  ->  +5.12
    TUR-SL     68%  ->  44%           +5.74  ->  +7.10

**Every league improved on both axes.** Three went from zero or negative edge to
clearly positive, and the mix diversified everywhere — `U3.0` leads in Japan,
`O1.5` in Chile and (jointly) Turkey.

**This retracts last night's gloomiest conclusion.** The claim that "the
selector adds nothing outside Turkey" was an artefact of a mis-tuned floor
funnelling every league into the same rung, not a property of the engine. With
the floor set correctly the selector beats its own base rate in all six leagues
measured.

#### Third pass: the floor had been masking how much shrinkage was warranted

Fixing the floor changed the answer to the shrink question. The first pass
shipped `MU_SHRINK = 0.60` and rejected the measured 0.42 because full
shrinkage collapsed the mix onto `U4.25` and halved edge. **That reasoning was
wrong.** The collapse was the 0.79 floor, not the shrinkage — and once the
floor came down there was no longer a reason to hold shrinkage back.

Re-swept at floor 0.75:

    MU_SHRINK    says     hit     gap    base    realised edge   top line
      0.60      83.2%   81.4%   -1.7   79.5%       +1.99           34%
      0.45      83.2%   82.4%   -0.8   80.4%       +1.97           37%
      0.35      83.2%   83.3%   +0.0   81.0%       +2.23           41%

**0.35 is best on both axes at once** — gap to zero, highest realised edge
measured — with the top line still under half of calls. Shipped.

#### Per-league result, n=250 each

    league     gap before    gap now
    SAU-PL       -4.7        +0.2
    CHI-PD       -4.0        -2.8
    JPN-J1       -3.2        -1.1
    PER-L1       -1.9        -1.1
    ENG-CH       +1.3        +2.9
    ESP-L2       -0.1        +1.1
    TUR-SL       +3.2        +5.4
    MLS          -5.1        -4.2  ->  -2.7 after per-league override

    weighted     -4.4  ->  -0.6

**Four of the five problem leagues were fixed by the global change alone.**
COL-PA and CHN-SL both dropped under the 4-point threshold; SAU-PL landed at
+0.2. Only MLS needed individual treatment.

#### The one per-league override, and why only one

`MU_SHRINK_BY_LEAGUE = {"MLS": 0.15}`. MLS residual slope is **0.325** on 262
replays — its remaining spread is still three times too wide — and it was the
only league still worse than -4 after the global fix. The arithmetic gives
0.35 x 0.325 = 0.11; it is set to **0.15**, pulled toward the global to blunt
the over-fit. Gap -4.2 -> -2.7.

This is kept deliberately sparse. Every entry is a fitted parameter on ~250
fixtures and will over-fit if added freely, so a league earns one only when it
BOTH measures far off AND still fails the retrosim at the global setting.
COL-PA measures a residual slope of **-0.06** — no usable signal left at all —
but shrinking it to the league mean already produces an acceptable gap, so it
gets no entry. Its read is worthless; the global shrink is what makes that
harmless.

MLS is also the league whose current-season history is thinnest: nine clubs
carry 20 rows each after the 2026 provider split. A weak read there is what the
data supports.

#### The team lane was never shrunk at all — fixed

Missed on the first two passes. `p_home_tt05` / `p_away_tt05` are built from
the raw per-side rates `gfh` / `gfa`, **not** from the shrunk `mu_total`, so
none of the match-total work reached them. The entire team-total lane — the one
offered as Tip 2 all weekend, which went 2/9 on Sunday — was still running on
unshrunk spread.

Measured the same way over 2,376 side-observations:

    actual_side_goals = 0.572 + 0.621 * gf

    lowest gf fifth    says 0.90 goals   actually 1.14
    highest gf fifth   says 1.92 goals   actually 1.79

Less extreme than the match total's 0.42, same defect. `TEAM_SHRINK = 0.62`,
shrinking each side toward half the league mean, applied only where the team
probabilities are derived so `mu_total` is not shrunk twice.

    residual slope   0.621  ->  0.933      (1.0 = calibrated)

    rung     says     actual     gap
    O0.5    73.4%     75.8%     +2.3
    O1.5    38.9%     40.9%     +2.0
    U1.5    61.1%     59.1%     -2.0

All three rungs now sit within 2.3 points, slightly conservative on the Over
side.

#### Full coverage: all 37 tippable leagues retrosimmed

The constants were tuned on 11 leagues. The remaining **26** — not 16, the
earlier count was wrong — have now been replayed at n=250 each. This is the
real test of whether the fix was fitted or general, because none of these
leagues influenced any constant.

    ARG-PD  +0.7    ENG-L1  +1.6    NOR-EL  +0.7    SCO-L1  +0.2
    AUT-BL  -0.3    ENG-L2  -1.1    POL-EK  -0.5    SCO-L2  +2.4
    BEL-PL  +1.7    ENG-NL  -0.7    POR-PL  -0.6    SCO-PL  -0.1
    BRA-SA  -1.6    ESP-LL  +3.4    ROU-L1  +1.9    SUI-SL  -1.4
    DEN-SL  -2.8    FIN-VL  -2.7    RUS-PL  -0.4    SWE-AL  -1.6
    FRA-L2  -0.5    GER-B2  -0.1    MEX-LMX -0.8    NED-ED  +4.5
    SCO-CH  -3.4    IRL-PD  -6.2

    batch of 13   3,178 fixtures   weighted gap  -0.6
    batch of 13   3,036 fixtures   weighted gap  +0.1

**Twenty-five of twenty-six land inside 4 points, and the two batch averages
are -0.6 and +0.1.** The constants were not over-fitted to the leagues they
were tuned on — that was the open question and it is now answered.

Across all 37 tippable leagues and ~8,700 replayed fixtures the weighted gap is
about **-0.4**.

#### The one failure, and it is not a tuning problem

`IRL-PD` came in at **-6.2**, the worst on the board. Its residual slope is
**-0.600** on 300 replays — the read is not merely weak, it is ANTI-correlated:
the more goals the engine predicts, the fewer occur. That is the worst slope
measured in any league, and a negative slope has no sensible shrink.

Set to `0.10`, as close to "use the league mean and ignore the fixture" as the
engine goes without emitting an identical tip every week. Gap **-6.2 -> -3.7**.

**Logged as a cull candidate, not a tuning success.** A calibrated tip carrying
no information is still no information, and Ireland now sits alongside COL-PA
(residual slope -0.06) in the category of leagues the engine should probably
stop tipping rather than keep tuning.

#### A separate problem the sweep exposed: withheld fixtures

Some leagues skip a large share of their fixtures rather than tipping them:

    BRA-SA  19%      NOR-EL  18%      SWE-AL  18%
    ROU-L1  15%      MEX-LMX 14%      RUS-PL  10%

That is thin history, unresolved names or no playable rung — the same family of
defects the alias and merge work has been chipping at. It costs coverage rather
than accuracy, and it has not been investigated.

#### The freshness gate should NOT be made binding — staleness costs ~1 point

Seven leagues are 86-107 days behind and are being tipped anyway, with
`league_status.py` flagging them as not cleared for futurematch and nothing
enforcing it. The obvious fix is to make the gate bite. Measured first, and the
measurement says don't.

Retrosimming those leagues answers the wrong question — replaying their own
history prices each fixture with data that was FRESH at the time. So
`scripts/staleness_cost.py` prices the same fixture twice instead: once as of
the match date, once as of the match date minus N days, forcing the form window
to end early exactly as a lagging store does. Both arms are scored against the
same real result, so the only thing that varies is the lag.

1,200 fixtures across eight leagues, each priced at every lag:

    lag      n    says     hit     gap    vs fresh
     0d   1190   81.8%   83.2%    +1.4      +0.0
    30d   1190   82.1%   81.9%    -0.2      -1.3
    60d   1190   82.4%   82.3%    -0.1      -0.9
    90d   1190   82.6%   82.8%    +0.2      -0.4
   120d   1190   82.4%   82.2%    -0.2      -1.0

**A four-month-old store costs about one point of hit rate, and calibration
holds at every lag** — the gap stays inside 1.4 points throughout, and there is
no monotone decay: 90 days scores better than 30. The live evidence agreed
without being able to prove it: the stale-league tips went 17/19 last weekend.

**Why it is so cheap is now obvious.** After shrinkage the fixture's own form
contributes only 35% of the read — the rest is the league mean, which barely
moves over a few months. Staleness can only degrade the 35%. Before
recalibration this would have cost considerably more, which is probably why the
gate was written in the first place.

**So the gate stays advisory, and that was the right call all along.** Making it
binding would drop seven leagues, including the Premier League, Serie A and
Ligue 1, to buy back roughly one point of strike rate.

**One thing this test does NOT cover.** It moves the as-of date back but keeps
the same clubs. Real season-boundary staleness also brings PROMOTED clubs the
store has never seen in that division — Hull, Ipswich, Lincoln, Racing
Santander. That is a genuinely different failure and it is already logged
separately under the cross-division and promoted-club defects, where the
history gate withholds rather than guesses. Freshness and promotion look alike
on a calendar and are not the same problem.

#### `buy from` re-derived: every published threshold was too LOW

The thresholds are `break_even x 1.05`, and break-even comes from the engine's
probability — which has moved twice (`MU_SHRINK`, `MIN_WIN_PROB`) and gained a
third input the team lane never had (`TEAM_SHRINK`). Re-pricing all 80 logged
bets with the current engine, as of their own match dates
(`scripts/rederive_buyfrom.py`):

    threshold drift, new break-even vs published
      mean  +6.8%     median +5.7%     range  -3.7% to +24.6%
      moved UP (stricter) on 76 of 80 bets

    West Ham v Charlton    O2.5    paid 1.60   1.420 -> 1.769   +24.6%
    Zürich v Basel         O2.5    paid 1.52   1.196 -> 1.471   +22.9%
    Shenzhen v Zhejiang    U3.5    paid 1.52   1.190 -> 1.431   +20.2%
    Guoan v Yukun          O2.5    paid 1.25   1.235 -> 1.448   +17.2%
    Zürich v Basel         O1.5    paid 1.83   1.269 -> 1.463   +15.2%

**The drift is larger than the margin it was supposed to protect.** A mean
+6.8% correction against a 5% cushion means the cushion never existed — the
"buy at break-even plus 5%" rule was in practice buying at roughly break-even
minus 2%. That is the mechanism behind the threshold finding dissolving at 79
settled bets, and it is now measured rather than inferred.

Re-scored on honest probabilities, the bet book looks materially worse:

    bought below break-even    27 of 82  (33%)  ->  51 of 80  (64%)
    mean break-even                              1.332

**Nearly two thirds of every bet placed was negative expected value**, not one
third.

**The margin cannot be re-derived from this data.** The sweep on recalibrated
break-even runs 27 bets at 0%, 11 at +5%, 5 at +8%, 2 at +10% — the samples
collapse before any threshold could show, and the ROI figures across them
(-0.3%, -10.3%, -10.2%, -8.5%) are noise on single digits. What can be said is
structural: with break-even now honest, a margin is a genuine cushion rather
than a correction, so **5% on the new number is a real 5%** in a way it was
never a real 5% before.

**Nothing needs regenerating in the tooling.** `two_tips.py` computes
`buy>=` from the live engine, so every threshold it prints from here is already
correct. The stale figures are the `buy≥` annotations in the fixture tables
above, and those are deliberately left as published: they record what the log
actually said at the time, and rewriting them would falsify the record. Read
any `buy≥` dated before this section as **roughly 7% too low**.

#### Season restart: it was a symptom of the over-spread mu, not a thing of its own

Before recalibration `ENG-NL` and `FRA-L2` ran -8.3 and -7.8 across the summer
break against -1.5 and -0.1 mid-season, and that was carried as an open defect.
Investigated properly (`scripts/restart_effect.py`), it does not hold up.

**"Early season" is not a measurable property of a fixture.** Two things that
are: how long a team has been idle, and how far back the ten-match form window
has to reach. Bucketing 2,982 replayed fixtures across ten leagues by both:

    by days idle (larger of the two sides)      by age of the oldest form match
      bucket     n    says    hit    gap          bucket     n    says    hit    gap
       0-10   2358   82.2%  83.5%   +1.2           0-80   1329   82.1%  82.9%   +0.8
      10-20    457   82.4%  84.5%   +2.1         80-120   1062   82.3%  83.7%   +1.4
      20-40     75   82.6%  85.3%   +2.7        120-200    400   82.5%  84.0%   +1.5
        40+     92   82.3%  79.3%   -2.9           200+    191   82.7%  85.9%   +3.2

**The form-window axis shows no degradation at all — it gets BETTER as the
window reaches further back.** That refutes the mechanism the defect was written
on. The only negative cell is teams idle 40+ days, at -2.9 on 92 fixtures, and
that is `z = -0.75`. Null.

The original two leagues do still show a gap after recalibration — `ENG-NL`
-6.1, `FRA-L2` -4.0, against +3.9 and +3.2 mid-season — but neither is
significant, and both stated probabilities sit INSIDE the Wilson interval of
what landed:

    ENG-NL   hit 75.9%  interval [67-83]  claim 82.0%  ->  inside   z = -1.71
    FRA-L2   hit 76.7%  interval [68-83]  claim 80.7%  ->  inside   z = -1.11

Two windows of 120 fixtures are consistent with the same underlying rate; the
ten-point swing between them is what noise looks like at that size. And the
defect was originally raised from exactly those two leagues, chosen because they
looked worst — the same selection trap as the 1.20-1.39 odds band.

**What actually happened:** shrinkage absorbed it. Over-spread mu hurt most
where form was least reliable, which is precisely the post-break fixtures, so
fixing the spread fixed the symptom. Nothing further is warranted.

Kept as a negative result, and `break_days` is worth re-checking once the 40+
bucket has a few hundred fixtures rather than 92 — it is the one cell pointing
the wrong way, even if it is currently indistinguishable from zero.

#### The team lane, validated where it was not fitted — it holds

`TEAM_SHRINK = 0.62` was measured on the most recent 200 fixtures of six
leagues, which is exactly the setup that made the first match-side pass look
better than it was. Re-tested against two independent moves away from the fit,
`scripts/team_validate.py`:

    cell                                slope    O0.5    O1.5    U1.5
    fitted leagues, fit window          0.933    +2.3    +2.0    -2.0
    fitted leagues, earlier window      0.830    -0.3    -1.9    +1.9
    HELD-OUT leagues, fit window        1.095    +0.3    +1.3    -1.3
    HELD-OUT leagues, earlier window    1.158    -0.0    +0.1    -0.1

**It passes on both axes.** Slopes span 0.83-1.16 around a target of 1.0, and
every rung sits inside 2.3 points. More telling than the size is the SIGN: the
gaps flip direction between windows (+2.0 then -1.9 on `O1.5`), which is noise
around zero rather than a bias the fit absorbed. The cleanest cell is the one
furthest from the fit — held-out leagues on the earlier window come in at
**-0.0, +0.1, -0.1**.

This is a better result than the match side, where MLS, COL-PA and IRL-PD still
fail out of sample.

#### Per-league, because pooled numbers hid IRL-PD once already

    league     slope    O1.5 gap        league     slope    O1.5 gap
    ENG-CH     1.063     +1.1           JPN-J1     0.380     +1.0
    ESP-L2     1.313     +1.3           MLS        0.449     +1.7
    TUR-SL     1.324     -1.4           CHI-PD     0.876     +3.5
    ENG-L1     1.356     +1.8           BEL-PL     1.176     +1.2

Slopes range widely — 0.38 to 1.36 — but **no league is broken the way IRL-PD
is on the match side**, and every rung gap lands inside 3.5 points. `MLS 0.449`
and `JPN-J1 0.380` are still over-spread on the team side, MLS in the same
direction as its match-side problem.

Deliberately not tuned per-league. The rung gaps are the thing a bet settles
on, and at +1.7 and +1.0 they are already inside the noise on 500
side-observations; fitting a constant to a slope that does not show up in the
settlements would be over-fitting for its own sake. Logged so it can be
re-checked when the sample doubles.

#### Out-of-sample: the fix generalises, but the in-sample number was optimistic

Everything above was tuned and validated on the same recent window. Re-scored on
the 250 fixtures immediately BEFORE that window, which had no influence on any
constant:

    in-sample      weighted gap  -0.6
    out-of-sample  weighted gap  -1.5

    SAU-PL  +3.3     ENG-CH  +0.3     PER-L1  -2.4
    JPN-J1  -0.4     TUR-SL  -1.5     CHI-PD  -2.5
    ESP-L2  -2.9     COL-PA  -4.7     MLS     -5.2

**-1.5 out-of-sample against -4.4 before any of this**, so roughly two thirds of
the defect is genuinely fixed rather than fitted. But it is not zero, and
`MLS -5.2` and `COL-PA -4.7` still fail out of sample — the MLS override was
fitted on the recent window and does not fully carry.

#### Where recalibration ended up

    weighted calibration gap    -4.4  ->  -0.6 in-sample, -1.5 out-of-sample
    realised edge               +1.35 -> +2.23
    top market share             39%  ->   41%   (54% mid-way, and 88-95%
                                                  per-league at the worst point)
    hit rate                    81.2% -> 83.3%

All four moved the right way at once, which is the part worth trusting — a
change that improved hit rate while concentrating the book would have been the
certainty trap again. Three constants (`MU_SHRINK`, `TEAM_SHRINK`,
`MIN_WIN_PROB`), two per-league overrides, and the coupling between the first
and last pinned in a test so neither can move alone.

### Diagnostics — 24 Aug, full re-run

Every instrument re-run on the current engine. Two things worth saying before
the numbers: **the engine reproduced its 23 Aug figures to a tenth of a point**,
which is the first time a rebuild here has been boringly stable, and **the one
open defect that looked like two defects turned out to be one, in the place
nobody was looking.**

#### The venue fix holds, and the "side split" it left behind is not real

`tt05_calibration`, 13,872 side-observations, no lane selection:

    HOME    +0.8      (was +4.1 before the fix)
    AWAY    +0.3      (was -3.6)
    BOTH    +0.5      split 0.5, from 7.7

But `team_calibration` still reported the lane-level sides split — TA +2.9,
TB −3.0 — which read as the fix not having taken. It has. `team_rung_side.py`
cross-tabulates rung against side and the margin dissolves:

    rung          HOME (TA)              AWAY (TB)
                n    says   hit  gap    n    says   hit  gap
    O0.5      884   82.1%  82.8% +0.8  350   81.7%  82.3% +0.5
    O1.5      721   60.3%  66.2% +5.9  113   59.6%  64.6% +5.0
    U1.5       61   79.9%  78.7% -1.2  813   78.6%  73.1% -5.6

**`O1.5` runs +5.9 at home and +5.0 away — the same on both sides**, so it is
not venue. The side margin was pure composition: `O1.5` is 86.5% home lanes and
`U1.5` is 93.0% away lanes, because home teams score more. Two apparent defects,
one real one, and it lives in the rung.

#### The rung defect is NOT a Poisson shape error — sixth hypothesis dead

The obvious mechanism was over-dispersion. `team_total` recovers
`gf = -ln(1 - p_tt05)` and prices every rung as Poisson(`gf`), which pins
`P(≥1)` by construction — so `O0.5` cannot be wrong however wrong the shape is,
and every shape error is pushed onto the `≥2` boundary. That predicts exactly
what was seen, including why `O0.5` is clean.

`side_shape.py` tested it selection-free on 13,872 observations. It is wrong:

    P(side scores >= 2)      poisson   actual    gap
      fitted gf 0.0-0.9       19.3%    26.0%    +6.7
      fitted gf 0.9-1.2       29.1%    30.7%    +1.6
      fitted gf 1.2-1.5       38.9%    38.5%    -0.4
      fitted gf 1.5-1.9       49.1%    49.5%    +0.4
      fitted gf 1.9-9.9       61.3%    64.9%    +3.5
      ALL                     39.1%    39.9%    +0.8

    one side's goals: mean 1.376  var 1.441  var/mean 1.047  (Poisson = 1.000)

**Pooled +0.8 and the middle bands flat.** The Poisson ladder is sound and the
dispersion is 1.047, near enough to 1.000 to price with. What the table does
show is a different thing: **both extreme bands run high**, and the two ends
turn out to need OPPOSITE corrections.

#### FIXED — the weakest sides were rated too low. `TEAM_RATE_FLOOR = 0.95`

Sweeping `TEAM_SHRINK` with band membership frozen settles which end is which:

    TEAM_SHRINK      0.62    0.70    0.78    0.86     P(>=2) gap
    gf 0.0-0.9      +6.7    +7.9    +9.2   +10.4     gets WORSE
    gf 1.9-9.9      +3.5    +2.0    +0.5    -0.9     gets better
    ALL             +0.8    +0.9    +1.0    +1.1     sd(gf) 0.304 -> 0.375

Less shrink fixes the top and wrecks the bottom, and pooled degrades all the
way, so **0.62 stays** — which independently re-confirms the sweep that set it.
The bottom band is not a spread problem at all. It is a level error, and the
regression that SET the shrink had already printed it without anyone reading it
as a separate fault: *"lowest gf fifth says 0.90 goals, actually 1.14"*. A slope
fitted with an intercept of 0.572 cannot be applied as a slope alone.

So the correction is a floor, not a scalar. Picked on the recent window and
scored on the held-back one:

    P(side scores >= 2), gf < 0.9      recent   held-back
      no floor                          +6.8       +6.5
      floor 0.95                        +1.5       +1.4

    lane        no floor           floor 0.95        volume
    U1.5     -6.7 / -4.0        -4.3 / -0.1      421->381, 453->413
    O1.5     +8.6 / +2.7        +8.5 / +2.9      untouched
    O0.5     +2.5 / -0.9        +2.4 / -0.7      untouched

**`U1.5` improves in both windows for about 9% of its lanes**, and nothing else
moves — `U1.5` needs `p ≥ 0.75`, so `P(≥2) ≤ 0.25`, so `gf ≤ 0.96`, which is
why that lane and only that lane sits inside the floored band. `p_*_tt05` is
built from `_shrink_side` and `mu_total` is not, so the match ladder cannot be
touched; a test pins that. On the live slate every `mu` is unchanged, `Casa Pia
U1.5` moved 79.2% → 75.4% and one `U1.5` dropped under its offer floor.

#### OFF THE BOARD — the whole cup family, measured at −11.4

The question "can the qualifiers be priced instead of declined?" got its
answer backwards: the qualifiers were never the risk — the main phase was.
Replayed on the 1,200 most recent stored matches per competition:

    UCL     680 tips   gap  −8.3        UCL-Q    48   −21.3
    UEL    1210 tips   gap −13.4        UEL-Q    60   −16.1
    UECL    219 tips   gap −10.0        UECL-Q   70    −7.6
    ALL    2109 tips   gap −11.4 [69−73]

Not the baselines (they were 0.17−0.36 low and are now corrected to measured
values): UEL carries half of UCL's baseline error and a BIGGER gap. The
by-market cut names the disease — `U4.25`, the rung that leans on the base
rate, is calibrated at −1.6, while every rung needing real per-fixture
information is broken (`O2.25` −23.0, `U3.0` −17.1). **Domestic form does not
transfer to European opposition**, and the engine cannot see relative
strength across leagues. Not fixable with a constant — and now proven so.
`cup_calibrate.py` (1,349 fixtures) regressed actual totals on the raw cup
mu's deviation from its baseline: **slope 0.017 ± 0.021, statistically zero**
against the domestic 0.42. The cup mu contains no per-fixture information.
The shrink sweep confirms no rescue exists: best case −3.0/−1.9 at k = 0.10,
and even k = 0 — informationless baseline tips — misses at −3.9/−2.9,
because cup totals are **over-dispersed relative to Poisson**: continental
blowouts fatten the tails, so even the base-rate rung over-promises. So
`CUP_TIPS_ENABLED = False`, pinned by a test.

**The road back is measured and half-built** (`cup_strength.py`, 25 Aug).
Every stored cup match bridges two leagues, and least squares over 4,825
clean bridges rates the leagues from goals alone — the big five emerge in
order (ENG +1.04, ESP +0.92, GER +0.84, ITA +0.68, FRA +0.50, down to IRL
−1.31) with a cup home advantage of +0.40. Where domestic form measured a
slope of 0.017, the ratings PREDICT cup totals: |gap| +0.270 ± 0.063 and sum
+0.121 ± 0.032 — mismatches mean goals, and giants attack rather than hide.
Two myths also fell: cups are NOT over-dispersed (var/mean 1.00-1.04), and
the real baseline sin was staleness — modern UCL runs 3.34 goals against the
2.70 the engine used. A strength-based mu (rolling 3-year baseline +
0.27·|gap| + 0.04·sum, trained pre-2022) validates at **+1.1 on the most
recent held-out window** with a live market mix — but sits at **−3.7 on
2022-24**, and the two-window bar is the bar: not shipped. The as-of refit was
then run (`cup_asof.py`, ratings refitted monthly on trailing bridges): the
per-competition gaps LOOKED calibrated (UCL −0.7, UEL +0.8, UECL −0.9) — and
the main-phase two-window cut exposed that as pooled cancellation, the
project's signature failure caught a fourth time: early −3.7, late +3.3,
opposite signs averaging to zero. The windows disagreeing in SIGN points at
the 2024-25 format change as a structural break; the Swiss-model era runs
hotter than everything the model was fitted on, and validating a break model
needs data the store does not yet hold. Qualifiers fail outright (−5.0). 271 clubs also carry 20+ matches of their own
European history the old path never read — the unmined strand. The club→
league map is cached in `config/club_leagues.json`.

**The final round trained entirely inside the Swiss era**
(`cup_composite.py`, 25 Aug) — the closest any cup model has come, and still
short. Two complete Swiss-format seasons exist, so nothing crosses the
break, and the strength number moved from league to CLUB (the BIG_MATCH
as-of-form machinery applied continentally, plus the previously unmined
own-European-record strand): `club_str = league_rating + 0.8·(as-of domestic
PPG − 1.45) + 0.3·own_cup_form`. Trained on one season, validated on the
other, then the roles swapped — a model that only works one way learned a
season, not a structure:

    fit 24-25 → validate 25-26   182 tips   says 85.8   hit 85.2   gap −0.6
    fit 25-26 → validate 24-25   125 tips   says 84.8   hit 80.8   gap −4.0

Forward calibrates almost perfectly; the reverse fails; pooled ≈ −2 on 307
tips, with confidence intervals ±5-6 per window at this n. The two-window
bar is both directions or nothing, so **cups stay off** — but the family's
trajectory is −11.4 → −2, the club composite beat league-only ratings in
every cut, and 2024-25 looks like the harder season in every instrument.
Coverage is the other limit: ~307 of ~1,100 Swiss-era fixtures clear the
feature gates. Each finished European matchday thickens both windows;
re-run `cup_composite.py` before the next verdict.

**The pointed gate** (`cup_pointed.py`, 25 Aug) then asked whether the
DNB-style read could validate cup lines: split the composite mu into side
expectations via the strength gap (supremacy by construction, +0.40 cup
home advantage) and gate tips on a side clearing a domestic strong-rung
floor. The gate helps in BOTH directions — pointed beats unpointed
(+0.7 vs −2.3 forward, −3.4 vs −4.6 reverse) — but the failing window
stays failing: the 24-25 miss is a level error the selection cannot fix,
so cups remain off. The side read itself, though, TRANSFERS: the pointed
side avoids defeat 77.4% in cups (79.8% at home, break-even 1.32; 69.2%
away, 1.57) — the domestic Rule 5 structure a few points weaker, priced
accordingly.

**Club Elo broke the deadlock** (`cup_elo.py`, 25 Aug — user's suggestion).
clubelo.com maintains exactly what the composite approximated: club-level
strength from every match in every competition, as-of daily. Snapshots
(mirrored via github.com/tonyelhabr/club-rankings, trimmed into
`config/club_elo.parquet`, names mapped in `config/club_elo_names.json`)
cover the whole Swiss era through the 25-26 league phase. Coverage jumps
from ~28% to **92%** (1,563 of 1,701 fixtures), and for the first time
EVERY window agrees:

    frozen   24-25 → 25-26  745 tips  gap −2.5     reverse  818  −1.7
    walked   (live shape: slopes frozen, intercept tracked monthly)
             25-26  −1.8  (halves −3.6 / −0.3)
             24-25  −2.4  (halves −3.4 / −1.5)

Real market mix (~25% O1.5 vs the U4.25 base rung), no sign flips, no
pooled cancellation. Staleness is a non-issue (Elo lagged 60 days grades
identically), so the committed snapshot refreshed every few weeks is
operationally enough. Per-competition intercepts were tried and rejected
(they absorb season noise and whipsaw between directions). The UEFA
country coefficient was considered and set aside: our bridge ratings
already measure it, club-level and in goals.

**The signed gap: the biggest cup fix yet** (`second_legs.py`,
`cup_elo_form.py`, 26 Aug). Chasing a question about two-legged ties
exposed a structural blindness: the cup mu used **|elo gap|**, so a 1900
home side against a 1600 away side priced exactly like the reverse —
venue-blind on strength, in a sport where the home side attacks. The
symptom was visible in second legs, which ran **+0.33 goals over the mu
when the stronger club hosted and −0.26 when it had hosted the first
leg**, a 0.59-goal spread the model could not express. Measured directly,
the signed term is the strongest the project has found: **+0.1015 ±
0.0205, t 4.95** on 1,563 fixtures, coefficient near-identical across
seasons (+0.097 / +0.106), monotone by tercile in both windows. In the
live shape it improves both: hit **81.8 → 82.5** and **83.9 → 85.2**.
Shipped as `B3`; the wired path now grades **84.3% pooled with a +0.1
gap** (24-25 −1.2, 25-26 +1.4).

**What a level tie actually looks like** (645 second legs at x-x, 24% of
all second legs). The goals are ordinary — **2.74 against the usual
2.67**, +0.07 and statistically nothing; the most common scoreline
bucket is three goals (24.7%), a blank is rare (8.1%). What is NOT
ordinary is the result: the home side wins **49.1%** against the usual
~45%, the away side 29.1%, and **21.7% are still level after 90 minutes
and go to extra time**. Level ties are decided at home more often than
normal, but through 1-0s and 2-1s rather than through more goals — which
is why the totals lane sees nothing. Practical corollary: in a fifth of
these the memorable goals arrive in extra time, after every over/under
has already settled on the 90 (Sabah 5-2 on 25 Aug was 3-2 at the
whistle). The one cell matching the low-block intuition — level tie with
the stronger club hosting — runs 2.37 goals, −0.34 below usual, but on
n=38 at ±0.25 that is suggestive, not established.

**The tie itself adds nothing.** Second legs were also cut by aggregate
state against the engine's own mu: home 2+ ahead +0.25 (t 1.08), 2+
behind −0.14 (t −1.01), either side 2+ ahead −0.09 / +0.12 across
windows. The lead effect that looked real against a plain baseline was
the strength gap that *created* the lead — which Elo already prices. So
the aggregate stays a printed caption, not an input.

**Elo + form: tested and declined** (`cup_elo_form.py`, 26 Aug). Since
Elo rates clubs by results, it cannot tell a 1-0 pair from a 3-2 pair —
so goal TEMPO should be orthogonal to it. It is: domestic goals-per-game
(+0.429 ± 0.165, t 2.60) and own-cup goal totals (+0.112 ± 0.053, t 2.13)
both carry signal, while every results-based term is flat (domestic PPG
t 1.15, PPG mismatch t 0.38, 90-day Elo momentum t −0.11 — Elo levels
already price momentum). But signal is not edge: graded as tips, the
terms improve NO window (−0.6→−0.8 and −6.8→−7.0; −1.2→−2.9 and
−1.4→−1.2). A term worth 0.4–2.7% of residual variance moves mu by
hundredths of a goal, less than the distance between rungs, so it
reshuffles selection at the margin and mostly picks worse. The cup lane
stays strength-only.

**WIRED IN, PROBATIONARY — 25 Aug.** The deciding test was a true dress
rehearsal: the 202 knockout fixtures played after the snapshot's last day
(Jan–May 2026), priced exactly as the live lane would — frozen Elo,
walked intercept — hit **89.1% against a stated 84.9** (+4.2). That +4.2
also killed the planned −2 debit: across five windows the gap wobbles ±4
around zero with no direction, the same wobble domestic leagues show at
these sample sizes, so the lane ships at honest probabilities.
`app/data/club_elo.py` is the boundary: results-derived only (never
odds), cups only (domestic leagues never touch it), committed snapshots
only (a refresh is a reviewed commit, never a predict-time network
call), and abstention stays first-class — national teams, unmapped
clubs, and ratings staler than 400 days produce no tip. Qualifier codes
now route through the same lane (they previously fell to the domestic
path — the −5.0). `CUP_TIPS_ENABLED` goes back to `False` if the live
gap breaks the measured band; cup tips are marked **probationary** on
the board until a real graded sample accumulates.

**The wired path replayed, and the over debit (26 Aug).** The live code
itself — `build_request` through `predict_fixture`, the path that fills
the board — replayed over the two Swiss seasons at 1,878 tips: 24-25 gap
−2.3, 25-26 gap −2.1, no flip. The by-rung cut isolated the whole miss:
U4.25 −0.6 and U3.0 +2.2 are calibrated, while **O1.5 runs −3.3 / −3.7
across the seasons and flat across every probability band** — a level
bias on the over family alone. So `club_elo.OVER_SAYS_DEBIT = 0.035`:
cup over tips now publish their probability, edge, and buy-price 3.5
points colder (selection untouched — the debit makes the numbers honest
and forces a cup over to clear the playable bar on its honest edge).
With the debit both windows close to **+0.0 / −0.1**. Cup unders and
every domestic market pass through unchanged, pinned by a test.

**Hitrate-first selection (26 Aug).** The calibrated lane still hit ~80%
against the domestic board's ~85, because edge-ranked selection favours
O1.5. Sweeping the cup probability floor on the wired path found the
whole dial at one setting: `min_win_prob 0.82` on all six cup codes
flips the mix to the calibrated base rungs at **zero volume cost**
(`choose()` falls back to the safest buyable rung, so no fixture loses
its tip) — hit rises to **81.9 / 84.7** across the seasons (≈83.3
pooled) with both windows still calibrated (−1.5 / +0.9, debited says).
Beyond 0.82 the dial saturates; ~83 is this lane's honest ceiling, a
shade under domestic, paid for in shorter under prices. Domestic floors
untouched, pinned by a test.

#### MEASURED AND DECLINED — the domestic context terms

B3's success in cups raised the obvious question: does the LEAGUE mu miss
the same kind of thing? (`domestic_context.py`, 4,189 fixtures across 16
leagues, residuals against the engine's own mu.) One term clears the
two-window bar — **ppg_gap**, how lopsided the matchup is in recent form,
+0.241 ± 0.063 (t 3.81), holding at t 3.17 and 2.23 in the two halves.
The engine under-predicts goals when a strong side meets a weak one, by
roughly +0.36 on a typical mismatch.

It still does not ship, because graded as tips it LOSES hitrate in both
directions: **86.4 → 85.9** and **86.4 → 85.7**. The same table shows
why — the domestic board already grades 86.4% against a stated 85.2-85.6,
so it is mildly under-confident already; pushing mu up on mismatched
fixtures shifts selection toward looser rungs that pick worse than the
correction gains. Signal without edge, the same verdict the cup tempo
terms got.

The shrinkage hypothesis was also refuted: if `MU_SHRINK` were
over-correcting extremes, the residual would sit where mu is far from
the league mean. It does not — near the mean +0.116 (t 1.06), middling
+0.358 (t 3.08), far +0.251 (t 2.40), strongest in the MIDDLE.

The other candidates fall short outright: table position is one window
shy (t 2.42 / 1.75), three-season **stature** — "Barcelona is always top
three", which a rolling window forgets after a bad month — is suggestive
but neither window alone (1.17 / 1.79, and it needs three prior seasons
so coverage is thin at 2,553), the signed gap is weak exactly as
predicted (domestic rates are already venue-split, so they know which
side is better), and this season's earlier meeting between the same
clubs is nothing at all (t 1.32) — the same answer the two-legged tie
question got from an entirely separate sample.

#### MEASURED AND DECLINED — defense adds almost nothing to the MATCH mu

The team-lane defense blend invited the obvious sequel: `mu_total` is also
attack-only. Measured the same way over 103,338 fixtures — combined leakiness
within a combined-attack bucket — the strand is dead: ~+1 point of O2.5 and
under a tenth of a goal at the extremes, against the team lanes' 7-11 points.
The reason is structural: at match level defense nearly double-counts, because
a team's conceded rate largely IS its opponents' scoring. A side's rate needs
its specific opponent; a match total mostly does not — which is why the match
ladder always calibrated clean while the team lanes hid the hole, and why the
residual-mu-spread hypothesis died. No knob; the Over-side edge lives in the
team lanes and long rungs, where DEFENSE_BLEND now sharpens it honestly.

#### MEASURED — the big-match effect has a season-phase structure

Cutting the 268,912-fixture measurement by season third:

    vs phase control       early      mid       late
    top-4 clash            +0.02     −0.07     −0.075
    bottom-4 clash         −0.044    −0.067    −0.104

The top-clash compression is ABSENT in the first third of a season and
switches on from mid-season — stakes must be concrete before two top sides
suppress each other. The shipped `BIG_MATCH_DEBIT` flag validated pooled and
stays as-is; phase-gating it to mid-season onward is a logged refinement
candidate that needs its own engine-relative cut (the flagged population is
too thin to split three ways today). Bottom clashes run tight at EVERY phase
with a late-season fear premium on top — a February six-pointer is the
tightest fixture type in football — but no knob ships for it: engine-relative
the effect sign-flips across windows, because form already sees bad teams'
thin rates. The engine absorbs relegation culture for free; it was only blind
to the top-clash kind.

#### SHIPPED — cross-division fallback, validated on 608 historical tips

The archive's "largest single volume fix still outstanding": a promoted club
abstains with a full season of history one division down. Form does not
transfer raw — measured over 789 club-seasons crossing a stored boundary, a
promoted side scores **×0.754** and concedes **×1.516** of its lower-division
rates, near-reciprocal with relegation (×1.345 / ×0.727), while the match
TOTAL transfers almost clean (×1.025). So the fallback pulls the club's rows
from the adjacent division with every goal rescaled by those constants, and
the fixture's own league supplies every baseline. Rescue-only, like the merge
gate: it fires only under `MIN_MATCHES` and can never move an existing tip.

Replayed on every fixture since 2015 where the live guard would fire:

    FALLBACK fixtures        608 tips   says 82.4%   hit 83.1%   gap +0.7
    control (same leagues) 18,229 tips  says 82.2%   hit 81.7%   gap -0.4

Indistinguishable from ordinary tips, which is the pass mark. Le Mans, Racing
Santander, Elversberg and Dep. A Coruña all price immediately; Celta B and
Amedspor stay abstained because their lower divisions are not stored, which is
the honest boundary of the method.

#### CLOSED — `VENUE_BLEND` swept for the first time: it is a dead knob

The last constant in the engine that had never been validated. Swept 0.0 to
0.8 on 6,891 fixtures, both windows, both lanes scored — the per-side split it
exists to fix, and the match total it is allowed to touch:

    blend        |split| recent   |split| held-back   match gap
    0.00              0.2              1.2              1.50 / 0.32
    0.20              0.1              1.3              1.50 / 0.32
    0.35  (live)      0.1              1.4              1.49 / 0.33
    0.50              0.2              1.6              1.49 / 0.33
    0.80              0.6              1.9              1.50 / 0.34

Nothing moves outside noise (each side's gap carries ~0.9 points of standard
error at this n). The reason is structural: the residual de-bias is written as
`edge * (1 - blend)`, so the two mechanisms are redundant — turn the blend off
and the de-bias runs at full strength and closes the split by itself; turn it
up and the de-bias hands the job to the venue-specific rates, which do it
slightly worse on held-back data. The de-bias is doing the work. The blend is
along for the ride, and no value in the range beats 0.35 by anything a bet
would notice. **0.35 stays, and the constant is no longer unvalidated.**

#### Two things the window split made WEAKER, not stronger

Pooling hid this, and both correct claims made earlier in this section:

- **The `≥1` under-statement does not replicate.** Recent +11.4, held-back +2.8
  on n≈300 a side. Only the `≥2` signal survives the split (+6.8 / +6.5), which
  is the one the rungs are cut on — but "both thresholds are under-stated" was
  a pooled-data artefact and is withdrawn.
- **`O1.5` at +5.8 is softer than it looks.** Recent +8.6, held-back +2.7. That
  is about 1.8 standard errors apart on n≈420 each, so the rung's over-delivery
  is partly window noise. `U1.5` by contrast holds (−6.7 / −4.0, under 1 SE),
  which is why the floor was validated on `U1.5` and not on the pair.

#### The match lane reproduced exactly

`edge_bands`, 7,576 tips across 62 leagues:

    stated edge        n    says    hit    gap    base   REAL    23 Aug
    under +1%       2704   83.3%  84.7%   +1.4   84.4%  +0.3      +1.4
    +1 to +2%        807   82.9%  83.1%   +0.2   81.4%  +1.7      +0.2
    +2 to +3.5%     1193   82.0%  81.9%   -0.1   79.2%  +2.7      -0.1
    over +3.5%      2872   81.6%  79.1%   -2.5   74.8%  +4.3      -2.5

Identical to the archived run. Every market still calibrated (`O1.5` +0.2,
`U3.0` −0.5, `U4.25` −0.5). Realised edge over base **+2.4**.

**This is now priced rather than noted** — see rule 3. `mu_mismatch` re-measured
at 0.2 points (tail −2.9 → −2.7), confirming it is real, tiny, and not the tail.

#### Team histories: four clubs served less than a rolling window

Scanning every club active in the last 120 days against `ROLLING_MATCHES = 10`:

    1081 clubs   full window
     129 clubs   5-9 rows — priced on a PARTIAL window
     192 clubs   under 5 — withheld outright

Most of the thin ones are cup competitions (`UCL-Q`, `UEL-Q`, `UECL-Q`), where
a club genuinely plays 2–8 matches and there is nothing to fix. The domestic
cases are real:

- **`ROU-L1` — eleven clubs sit at exactly 5 rows.** They price, on half a
  rolling window. Three of them (`Dinamo Bucuresti`, `FC Botosani`,
  `Farul Constanta`) have 78–79 further rows under an accented spelling that
  the merge table cannot reach, because `_MERGE_GATE = 5` and `5 < 5` is false.
  The gate is one off from rescuing them.
- **`MEX-LMX` is effectively dark** — fourteen clubs at 4 rows, withheld. Only
  one has a twin spelling, so this is missing data rather than naming.

An accent-variant scan across all 61 leagues found 18 split clubs, 14 of which
serve the THINNER half. Most are harmless (`Malmö FF` serves 17 rows against a
twin that stops in April 2025 — the rolling window never reaches back that far).
The four that matter are listed above.

### Diagnostics — 23 Aug, full sweep

Run across every league: freshness, configuration, and a per-league retrosim
scoring 120–260 fixtures each strictly as-of. `scripts/retrosim.py`,
`scripts/league_status.py`.

#### 1. The engine is systematically overconfident, by about 4 points

Twenty-six leagues, ~3,000 priced fixtures replayed:

    weighted   says 85.7%   actually hit 81.8%   gap -3.9

The live log's 84.5% on 97 tips is a small, favourable sample. **The engine's
stated probability is not what it delivers**, and the `MIN_WIN_PROB = 0.79`
floor is really buying something closer to 0.75.

#### 2. Four leagues are genuinely broken, and it is not a small effect

Re-run at n≈260 so the intervals mean something. In all four the stated
probability sits OUTSIDE the 95% interval of what actually landed:

    league     n    says     hit      gap    95% interval
    CHN-SL   251   84.9%   75.3%    -9.6      [70-80]
    CHI-PD   255   85.3%   75.7%    -9.6      [70-81]
    SAU-PL   258   85.5%   78.3%    -7.2      [73-83]
    COL-PA   260   86.4%   80.8%    -5.6      [76-85]

    ENG-CH   259   85.1%   84.2%    -0.9      [79-88]   control
    ESP-L2   259   85.1%   84.9%    -0.2      [80-89]   control
    TUR-SL   258   84.9%   85.7%    +0.8      [81-89]   control

The controls are near-perfect, so this is not the engine being globally
miscalibrated in a way that excuses the four. **China, Chile, Saudi and
Colombia are individually bad**, and all four are currently being tipped —
Saudi has 6 settled tips in the live log at 50%, Colombia 3 settled and 3
pending, Chile 1 settled and 2 pending.

The live log agreed before the retrosim ran: Saudi 3/6, J1 6/9, Peru 3/5 were
the three worst there, and the retrosim independently puts SAU-PL at -7.2,
JPN-J1 at -5.3 and PER-L1 at -4.2 on 120+ fixtures each. Two independent reads,
same answer.

#### 3. Season-restart is a separate effect and it is real

Some leagues look bad only in the most recent window. Scoring the 120 matches
BEFORE the last 120:

    league     last 120    prior 120
    ENG-NL       -8.3        -1.5      restart effect
    FRA-L2       -7.8        -0.1      restart effect
    CHN-SL      -10.7        -9.3      persistent
    CHI-PD       -8.1       -11.9      persistent
    SAU-PL       -7.6        -6.9      persistent
    COL-PA       -6.9        -6.0      persistent

`ENG-NL` and `FRA-L2` recover completely mid-season. Their last-120 window
straddles the summer break, where the rolling form window reaches across it and
describes teams that no longer exist in that shape. **The engine is materially
worse in the first weeks of a season and does not know it** — that is most of
today's slate.

#### 4. Configuration: 13 of 52 leagues are actually tuned

    tuned (a dial moved off default)   13   ENG-CH, ENG-PL, ESP-L2, ESP-LL,
                                            FRA-L1, FRA-L2, GER-B2, GER-BL,
                                            ITA-SA, ITA-SB, NED-ED, POR-PL, UCL
    registered only                    39   means filled in, every dial default
    no config at all                    9   CHI-PD, SAU-PL, PER-L1, AUT-BL, ...

**All three unconfigured leagues that are being tipped underperform** — CHI-PD
-9.6, SAU-PL -7.2, PER-L1 -4.2. But configuration is not sufficient: CHN-SL and
COL-PA are registered and still run -9.6 and -5.6, while TUR-SL is equally
untuned and calibrates perfectly. Being unconfigured is a risk marker, not the
cause.

#### 5. Seven leagues are being tipped on stale data

`league_status.py` already flags these as not cleared for futurematch, and the
tip path does not enforce it:

    ENG-PL   90 days stale      ITA-SA   90 days
    FRA-L1   97 days            ITA-SB  106 days
    GER-BL   98 days            GRE-SL   93 days
    COPA-L   86 days

Four of them were tipped this weekend (Brentford, Genoa/Parma/Inter/Udinese,
Toulouse/Nice/Troyes, five Serie B fixtures). Those tips ran on form ending in
May 2026. **They went 17/19 — better than the fresh leagues** — so this is a
governance gap rather than a demonstrated harm, but nineteen fixtures prove
nothing and the status tool's own verdict is being ignored.

#### What this changes

Nothing is being switched off mid-slate. Ranked by what the evidence supports:

1. **Stop tipping CHN-SL, CHI-PD, SAU-PL, COL-PA** until re-calibrated. Four
   leagues, ~1,000 fixtures of evidence, intervals excluding their own claim.
2. **Make the freshness gate binding** rather than advisory.
3. **Damp confidence early in a season** — the restart effect is worth 6–8
   points in the leagues where it shows.
4. Re-check the global -3.9 after 1–3; some of it is those four leagues.

### Do name failures cost markets? Mostly no — measured

`scripts/name_audit.py` runs every upcoming fixture through the resolver the
engine actually uses, so an abstain can be sorted into the two causes that hide
behind one message:

    GENUINE   the club really is new. Wisla Plock has 3 rows because it was
              promoted this month, and no name work invents a fourth.
    NAME      the history exists under a spelling the resolver cannot reach.

Across 4,161 upcoming fixtures, **78 are blocked — 1.9%**, and after this pass
**not one of them is a name**. Two were, and are now fixed: `NEC` resolves to
`Nijmegen` (716 rows) and `Fortuna Sittard` to `For Sittard` (334 rows), both
recovered by alias. What remains is a different problem wearing the same error
message:

    Le Mans FC          Ligue 1 fixture list, 328 rows sitting in FRA-L2
    SV 07 Elversberg    Bundesliga fixture list, 102 rows sitting in GER-B2

**Both are promoted clubs, and no alias can fix them** — the spelling is right,
the history is one division down. That is a cross-division lookup, and it
recurs for roughly three clubs per league every summer.

**The audit's blind spot is the vocabulary that actually fails in practice.** It
compares the fixture feed against the results store, and 24 of 62 leagues ship a
fixture list at all. The names typed off a screenshot are a *third* naming
system that neither half contains — `København` for `FC Copenhagen`, `Başakşehir`
for `Buyuksehyr`, `BP`, `VSK`, `ADO`. Those cannot be audited ahead of time;
`config/team_aliases.json` is the accumulating record of them, and it only grows
when a fixture is actually missed. An alias is consulted only when the raw name
resolves to nothing, so adding one can rescue a withheld fixture and can never
change a tip already issued.

**Split clubs are the larger residue and need judgement, not automation.** The
audit proposes candidate groups; many are false positives that would be actively
harmful to merge — `Nacional` (Uruguay) against `Club Nacional Potosí`
(Bolivia), `Santos FC` against `Santos Laguna`, `Celta` against `Celta B`. They
are reported for confirmation and never applied automatically.

- **OPEN — Allsvenskan is missing most of the 2025 season.** Found while
  checking why eight Swedish clubs appear under two spellings. The two spellings
  are real but harmless on their own — one provider covers 2023 to May 2025
  (`IFK Göteborg`), another covers 2026 (`Goteborg`) — and the resolver already
  picks the current one. The gap underneath is the problem:

      2023  240 rows      2024  240 rows      2025  53 rows      2026  135 rows

  A full Allsvenskan season is 240 matches, so **roughly 187 matches of 2025 are
  simply absent**. Every Swedish tip is therefore priced on ~17 matches per club
  — the current season alone — including the team lanes, which carry the largest
  published edges on the slate. Not wrong, but far thinner than the row counts
  suggest, and worth knowing before sizing a Swedish bet.

### FIXED — the team lane was split by venue, and four checks missed it

Found on 23 Aug while answering a question about `TEAM_SHRINK`. Two separate
bugs, both in how a side's scoring rate reaches the team lane, and both invisible
to every aggregate check this project runs.

**1. Both sides were shrunk toward `league_mu / 2`.** The code asserted that half
the league mean is the per-side mean. It is not — home teams average **1.502**
goals and away teams **1.154**, against a shared target of **1.328**. Every one of
twelve leagues checked missed by the same **±0.174**, so home rates were dragged
down and away rates pushed up, everywhere.

**2. `VENUE_BLEND` left the input biased before shrinkage ran.** Both rates start
from a team's last ten matches home AND away, with only 35% replaced by
venue-specific form, so `gfh` lands about **0.113 goals** under the true home mean
and `gfa` the same amount over. Correcting the shrink target could not reach
this: the bias is already in the input.

Measured on **13,872 side-observations with no selection in the sample at all** —
every priceable fixture contributes `p_home_tt05` against whether the home side
scored, and `p_away_tt05` against the away side:

    original (shared shrink target)     HOME +4.1   AWAY -3.6   split 7.7
    per-side shrink target              HOME +2.5   AWAY -1.8   split 4.3
    + venue de-bias                     HOME +0.8   AWAY +0.3   split 0.5

**Why four aggregate checks passed it.** Pooled, +4.1 and -3.6 average to +0.2,
so the team lane reported near-perfect calibration. The full-lane retrosim, the
`TEAM_SHRINK` sweep in both directions over ~6,900 fixtures, the by-probability
calibration test and the by-rung breakdown all returned it clean. The defect only
appears when the data is cut by the axis nobody was cutting on.

The venue correction is applied **symmetrically**, so `mu_total = gfh + gfa` is
exactly unchanged and the match lane — calibrated to a gap of ~0 — does not move
to fix the team lane. Pinned by `test_venue_debias_leaves_mu_total_unchanged`.

**It was withholding lanes, not just mispricing them.** `Porto v Arouca`
published no second tip at all under the old engine — Porto sat at 0.779 against
an `O0.5` floor of 0.80. Corrected, it is 0.807 and the lane appears at +5.1%.
Counted across 1,349 fixtures that produce a lane under either engine:

    gained by the fix     479   35.5%
    lost to the fix       196   14.5%
    unchanged             674   50.0%

    lanes offered, old engine   870
    lanes offered, new engine  1153      +32.5%

**Half the team-lane population changed.** The fix is not a refinement of prices
on a stable set of offers — it moved which fixtures produce a lane at all, and
raised team-lane volume by about a third. Lanes were being suppressed where a
home side sat just under a floor, and manufactured where an away side was held
just above one.

**Every team lane published before this was mispriced**: home lanes under-stated
by ~4 points, away lanes over-stated by ~4. Today's published numbers stand as
the record of what was actually issued; the corrected engine applies from the
next slate on.

**The added lanes are real, not filler.** Scored separately, since 674 kept
lanes would otherwise carry the average and hide whatever the additions do:

    bucket        n    says     hit     gap    z
    KEPT        674   71.5%   76.4%    +4.9   +3.00
    GAINED      479   78.2%   76.8%    -1.4   -0.73
    LOST        196   80.1%   75.5%    -4.6   -1.50

**GAINED hits 76.8% against KEPT's 76.4%** — a 0.4 point difference on a
standard error of 2.5, which is no difference at all — and its calibration gap is
inside noise. The extra third of volume performs like the lanes that were already
there. The removed lanes point the other way at -4.6, which is short of
significance but consistent with them having been offers that should not have
qualified.

Pooled lane calibration on the corrected engine is **+0.4 on 2,942 lanes**.

**Left open, and larger than the volume question.** By rung the same run gives
`O0.5` +0.7 (n=1,234), `O1.5` **+5.8** (n=834) and `U1.5` **-5.3** (n=874), both
at z = 3.5. `U1.5` and `O1.5` are complements for one side, so that is a single
error seen twice: too much mass on exactly one goal. Side-level over-dispersion
was the obvious cause and is measured and rejected — 293,114 side-observations
give var/mean 1.087, but the rung effect is 0.62 points and points the wrong way.
Leading candidate is now selection, since `candidates()` takes a max over rungs
and sides and these two rungs select opposite tails. Not diagnosed, not acted on.

**Practical consequence:** `O0.5` is the trustworthy team rung. `U1.5` currently
over-states by ~5 points, so its published `buy≥` is about 5% too low.

### Known data defects

- **FIXED — the resolver matched the wrong club, confidently, three times on
  one slate.** Found on 24 Aug by auditing every name on the board against the
  store before publishing, rather than trusting the tips that came back:

      Celta Fortuna     ->  Celta                  Celta Vigo's 2004-2012 rows
      U. de Concepción  ->  Deportes Concepcion    a different club entirely
      América-MG        ->  América (MG)           a spelling retired in 2013

  None abstained. All three priced and published a probability, and the first
  was carrying **+12.1% on Tip 1 and +23.9% on Tip 2** — the largest stated
  edge on the slate, built on a first team's history for a reserve side's
  fixture. Corrected, `Celta Fortuna` resolves to `Celta B` and abstains with
  one row; `Everton v U. de Concepción` fell from +2.6% to +0.1% and left the
  playable block entirely.

  The cause was the alias table's ordering, not the alias table's contents.
  `_aliased` consulted `config/team_aliases.json` **only when the raw name
  matched nothing**, on the reasoning that an alias could then never change a
  tip the engine already issues. That protected the wrong failure: the resolver
  does not fail by returning blanks, it fails by returning the wrong club, and
  no alias could reach any of the three. An alias now overrules the resolver,
  guarded by an exact membership test against the store's own name list — the
  guard used to be a resolver call, which answers "yes, `Celta B` exists" in a
  store that only carries `Celta`.

- **OPEN — two clubs are split across spellings by era, like Cienciano was.**
  `Deportes Concepcion` (19 rows, 2026) and `Deportes Concepción` (79 rows,
  2005-2008) are one club in CHI-PD under two spellings; `América (MG)` (114
  rows to 2013) and `América Mineiro` (289 rows to 2026) are one club in
  BRA-SB. Both currently work because the recent spelling carries the recent
  rows, which is the half that matters — but a resolver landing on the older
  half prices a fixture on decade-old form, which is exactly what happened
  above. The fix is a merge at load time, not an alias.

- **OPEN — half-time scores are censored on 0-0 finishes in 23 leagues.** In
  ALG-L1, ARG-PD, BRA-SA, COPA-L, CRO-1L, CZE-FL, DEN-SL, EGY-PL, MAR-BP,
  MEX-LMX, MLS, NOR-EL, POL-EK, ROU-L1, RSA-PL, RUS-PL, SUI-SL, SWE-AL, UCL,
  UCL-Q, UECL-Q, UEL-Q and UKR-PL, **every match that finished 0-0 is missing
  its half-time score** — 2,524 matches in total, and not one of them kept a
  half-time row. Drop the nulls and you have deleted exactly the goalless
  results, so any half-time question answers itself: every one of those leagues
  reported that a 0-0 at the break produced a second-half goal **100.0% of the
  time**. It is survivorship, not football.

  Nothing in the tip path reads `hthg`/`htag`, so no published tip is affected.
  It matters for live questions, which is where it was found — pricing an
  `O0.5` bought at half time. `scripts/ht_zero.py` detects the censoring by
  comparing 0-0 finishes inside the half-time subset against the league as a
  whole and excludes any league that shows none, rather than trying to repair
  rows that cannot be recovered. Twenty-three clean leagues remain, 57,092
  goalless halves, which is enough.

- **FIXED — team-name resolution was non-deterministic across processes.** The
  most serious bug found in this log, and it had been silently live the whole
  time. `_team_names` returned `list(set(...))`, and inside `_match_team` two
  spellings of one club collapse to the same lookup key, so exactly one of them
  wins. Python randomises the string hash seed per process, so WHICH one won
  changed from run to run:

      run 1:  Montréal -> CF Montreal   (20 rows, to 2026-08-20)   mu 2.34
      run 2:  Montréal -> CF Montréal   (149 rows, to 2025-05-03)  mu 1.62
      run 3:  Montréal -> CF Montreal                              mu 2.34

  The same fixture priced at `U3.0 79.1%` or `U3.0 91.8%` depending on nothing
  but which process ran it. Every downstream number — probability, edge,
  break-even, buy-from — inherited the coin flip. It was caught only because a
  bet-pricing script disagreed with the tip table it was pricing against.

  **Scope: 16 colliding name groups across 10 leagues**, every one a coin flip
  before the fix — ARG-PD (4), ROU-L1 (3), SWE-AL (2), and one each in ARG-CLP,
  BRA-SB, CHI-PD, COPA-L, MEX-LMX, MLS and UEL. MLS additionally has 6
  collisions at the CANONICAL level (`Charlotte`/`Charlotte FC`, `Inter
  Miami`/`Inter Miami CF`, `Minnesota United`/`Minnesota United FC`, …), all of
  which resolved arbitrarily.

  **The fix has two halves.** `_team_names` now returns names ordered most
  recent match first, then row count, then name — deterministic, and it picks
  the current half of a split club, which is what a project that ranks the last
  two seasons above deep history should want. `_match_team` builds its lookup
  maps with `setdefault` instead of a dict comprehension so the FIRST (most
  preferred) spelling wins rather than the last. `_compute_features` had the
  same `set()` union and now preserves order too.

  **Blast radius on the live slate: 1 tip of 13.** Re-running MLS
  deterministically changed only `San Jose v Minnesota` — `U4.25` moved from
  82.5% / +2.1% to **86.6% / +6.2%**, because `Minnesota` had been landing on
  the 2025 variant. The other twelve are byte-identical. The published table has
  been corrected.

  Worth stating plainly: this is a reason to treat every figure in this log that
  predates the fix as carrying an unknown amount of this noise. The tips
  themselves were mostly unaffected — 12 of 13 — but "mostly" is doing real work
  in that sentence.


- **No recency bound on team history — now measured, and it is the biggest one.** `_find_team_rows` takes a club's last ten matches with no limit on how old they are, so a side returning to a competition after years away is priced on ancient form. The history gate counts matches, not their age. Auditing every upcoming fixture put a number on it:

      form older than    fixtures withheld
        120 days             37 of 155   (24%)
        300 days             21 of 155   (14%)

  The worst are not marginal. Coventry v **Hull** would be priced off form **9,226 days** old; Real Madrid v **Málaga** off 3,017 days, from a club last in LaLiga in 2018. Hull v Man United is the case already caught by hand: Hull last played in the Premier League in **2017**, and the engine produces a confident U4.25 at 84% off ten matches from March–May 2017.

  Two distinct causes sit underneath. Some clubs are simply long gone from the division. Others — **Hull, Ipswich** — are promoted sides whose recent form is real but filed in *another league's store*: Hull have **690 rows in ENG-CH** ending 2026-05-02, and a Premier League fixture never looks there. That half is not a naming bug and not a staleness bug; it needs cross-division lookup.

  **This is the next fix, and it is deferred deliberately.** Unlike the alias layer it *withholds* tips, so it changes engine output on fixtures already tipped and bet. It waits until the 26 open bets settle.

- **Feed names vs store names — FIXED for 20 clubs.** Results come from football-data.co.uk, which files clubs under trading names (`Man United`, `QPR`, `Nott'm Forest`, `M'gladbach`); fixtures arrive with full legal names (`Manchester United FC`, `Queens Park Rangers FC`). Fuzzy matching cannot bridge an abbreviation — `qpr` and `queens park rangers` share no text to score — so the resolver returned nothing and the fixture was withheld. It affected **38 of 164 upcoming fixtures (23%)**, including Man Utd, Man City, Inter, Atlético, Athletic Club, Lyon, PSV, Sporting CP, Gladbach and Eintracht Frankfurt. `PSV` failed for a separate reason: `psv` is in the generic club-token list, so canonicalising deleted the only identifying word in the name. `config/team_aliases.json` now maps the 20 the store already carries; each target was read off the store's own name list rather than guessed. Le Mans and Elversberg are omitted because they have no rows under any spelling — newly promoted, a data gap not a naming one.

- **Every remaining no-tip is a promoted club.** After the alias layer, exactly **10 of 177** upcoming fixtures produce no tip, and all ten trace to five clubs newly arrived in their division — their history is real, it is just filed in a tier this store does not carry:

      Lincoln City        1 match    up from League One
      Racing Santander    1 match    up from Segunda
      Académico Viseu     2 matches  up from Liga Portugal 2
      Le Mans             0 rows     up from Ligue 2
      Elversberg          0 rows     up from 2. Bundesliga

  **Accepted as-is.** Refusing a promoted club is the gate behaving correctly, and a tip built on one or two matches would be worse than no tip. Not to be confused with the split-name problem below, which fails the other way round — it *issues* a tip off three or four matches instead of withholding one.

  **One more feed-side abbreviation surfaced on the overnight Colombian slate.** `Ind. Medellín v Cúcuta` returned no tip, and only the first half of the name was at fault: the resolver strips accents perfectly well — `Cucuta` and `Cúcuta` both find `Cúcuta Deportivo` (448 rows, last 2026-08-19) — but `Ind.` is an abbreviation of `Independiente` and shares no scoreable text with it, exactly the `QPR` failure in a different alphabet. One entry added; the fixture prices at `U3.0` 80.1%. Worth stating that this is the *second* time an abbreviated prefix has cost a fixture, and both times the store already held a full, recent history.

  None of these is a naming fault, and no alias can help — there is nothing in the store to alias *to*. `Marítimo v Académico Viseu` is correct as logged: the store holds exactly two rows mentioning Viseu (Benfica 2-2 on 09 Aug, Viseu 0-1 Santa Clara on 15 Aug) and nothing under any other spelling. The fix is a second-tier source per country plus cross-division lookup — the same feature Hull and Ipswich need, approached from the other side.

- **MLS is split nine ways, and it is the benign form of the defect.** The 2026
  provider break hits `Atlanta`, `Montréal`, `Charlotte`, `DC United`, `Inter
  Miami`, `Minnesota`, `New York City`, `New York Red Bulls` and `St. Louis`.
  Each club now has a short-name variant carrying exactly **20 rows ending
  2026-08-20** and a long-name variant carrying the deep history and stopping
  **2025-05-03**:

      D.C. United        679 rows -> 2025-05-03      DC United      20 -> 2026-08-20
      New York RB        673 rows -> 2025-05-03      NY Red Bulls   20 -> 2026-08-20
      Minnesota Utd FC   277 rows -> 2025-05-03      Minnesota Utd  20 -> 2026-08-20

  **Checked before pricing the slate, and every feed name resolves to the
  CURRENT variant**, not the stale one — the worst case here would have been
  `Montréal` landing on `Impact de Montréal`, 305 rows ending 2020-11-20, and it
  does not. So the split costs *depth*, not *recency*, and this project ranks
  recency first: 20 rows is comfortably above the history gate and the form
  window takes the last 10, all of which are 2026 matches.

  **Deliberately not merged.** The served variant holds 20 rows, four times the
  merge gate, so folding the history in WOULD change tips that are already
  issued and backed — the opposite of the safety property the Poland and
  Switzerland merges were accepted on. It also only adds seasons the project
  calls "nice to have". Worth doing when the board is clear, not mid-slate.

  Two names failed to resolve at all and are now aliased: `LA Galaxy` →
  `Los Angeles Galaxy` and `NY Red Bulls` → `New York Red Bulls`. Note that
  `Los Angeles` alone correctly resolves to `Los Angeles FC`, so the Galaxy
  entry is load-bearing rather than cosmetic.

- **FIXED — accent folding missed every letter that does not decompose.**
  `_strip_accents` relied on NFD splitting a letter into base plus combining
  mark, then dropped the mark. That works for `é`, `å`, `ş`. It does nothing at
  all for a letter whose modification lives inside the codepoint: Scandinavian
  `ø` and `æ`, Polish `ł`, Croatian `đ`, German `ß`, Icelandic `þ`/`ð`. NFD
  leaves them unchanged and the filter never sees them.

  So `Sønderjyske` did not match `Sonderjyske` and `Widzew Łódź` did not match
  `Widzew Lodz` — the accent-insensitive pass, which exists precisely for this,
  was blind to a whole class of European club names. A translation table now
  runs before NFD.

- **FIXED — Denmark merged, and it was the worst split found.** Ten clubs, and
  unlike MLS the CURRENT variant is tiny: every 2026 name carries **3 or 4
  rows** against 32–65 in the stale one:

      Sonderjyske      4 rows -> 2026-08-17    SønderjyskE      32 -> 2025-05-24
      Nordsjaelland    3 rows -> 2026-08-16    FC Nordsjælland  64 -> 2025-05-25
      Midtjylland      3 rows -> 2026-08-16    FC Midtjylland   64 -> 2025-05-25

  This is the dangerous configuration. The current half sits under the merge
  gate so on its own it would be withheld — but the resolver was matching the
  STALE half on an exact-name hit, which is above the gate, so Denmark was
  quietly issuing tips off **fifteen-month-old form**. Worse than a no-tip and
  invisible without looking.

  All ten now merge (`Aarhus`, `Brondby`, `FC Copenhagen`, `Lyngby`,
  `Midtjylland`, `Nordsjaelland`, `Odense`, `Silkeborg`, `Sonderjyske`,
  `Viborg`), each satisfying the gate rule that the primary is under 5 rows.
  `Sønderjyske` now resolves to 36 rows ending 2026-08-17. Not merged: Aalborg,
  Hvidovre and Vejle have no current variant — relegated, correctly left alone —
  and Randers FC was never split.

  Timing was checked first: **no Danish fixture appears anywhere in this log**,
  so the merge cannot disturb a tip already issued or backed.

- **`Piast v Legia` — confirmed a true split, and it is league-wide.** `Legia` holds 4 rows (2026-07-24 → 2026-08-14) and `Legia Warszawa` holds 68 (2023-07-21 → 2025-05-24). One club, 72 matches, and the engine sees 4. The same 2026-provider break splits **26 further clubs across Denmark, Mexico, Poland, Russia and Switzerland** — `CF Monterrey` 587 rows vs `Monterrey` 4, `CF Pachuca` 558 vs `Pachuca` 4, `FC Zürich` 220 vs `Zurich` 3. Those leagues have no fixtures in the current window, so nothing is being lost today, but every one of them would tip off three or four matches the moment their fixtures load.

  Worth noting for sequencing: a merge restricted to groups whose served variant is **below the 5-match gate** carries the same safety property the alias layer was accepted on — such a fixture has no tip to change, so the merge can only add. The stale-serving cases (Chapecoense, SC Internacional) sit above the gate and would still need the full fix.

- **Era-split team names** — 15 leagues, ~73 names. `KS Cracovia` (2023-25) vs `Cracovia` (2026), `IK Sirius` vs `Sirius`, `AIK Solna` vs `AIK`. Cause: 2026 seasons arriving from a different provider. Effect: thin predictions and false refusals. Cracovia was refused on 4 matches when 72 exist. Detector is fuzzy and overcounts — needs a manual pass.
- **Stale stores** — Premier League and Serie B end in May 2026.
- **No-tips resolved** — Al Faisaly 0-2 Neom, Al Hazem 0-1 Al Diriyah, Cracovia **3-2** Wieczysta. An earlier version of this note said all three finished under three goals; that was wrong. Cracovia was read at 1-2 with fourteen minutes left and finished on five, which would have beaten a U4.25 and lost a U3.0. Two of three would have been safe Unders, not three. Still not counted either way — a refusal is not a bet — and the error is left visible because grading declined fixtures from partial scores is exactly the habit that turns a no-tip rule into a tip.

---

## Most recent key updates

**Team-name aliases — 35 fixtures recovered, 0 tips changed.** 23% of upcoming fixtures had a team the engine could not resolve at all and were silently withheld. `config/team_aliases.json` maps 20 feed names onto the store names already present. An alias is consulted **only after the raw name fails**, so it can add a withheld fixture and never alter one already priced — verified by pricing all 164 upcoming fixtures with aliases off and on: **35 newly priced, 0 changed, 0 lost, 120 identical.**

**Split names — 120 clubs, 21 leagues, 81 served the wrong side.** Separate from the alias problem and worse, because it does not withhold a tip, it *issues* one off stale form. Chapecoense is served 114 rows ending **2021-12-09** while 136 exist and the freshest is 2026-08-16; SC Internacional 114 rows ending 2025-12-07 against a 327-row union. Six Brazilian clubs playing this week are priced on form that stops eight months ago. Likely also the explanation for the two ENG-PL tips that never reproduced. **Fix deferred until the open bets settle**, since merging the variants changes live tips.

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

