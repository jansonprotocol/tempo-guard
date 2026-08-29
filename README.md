# ATHENA — TEMPO GUARD · PRE-ALFA 1


## CURRENT CONFIRMED HITRATE: 82.9%

    lane                        Tip 1              Tip 2
    all matches             58 / 70   82.9%     33 / 53   62.3%
    played lanes  >+1%      27 / 37   73.0%     33 / 53   62.3%
    placed bets             39 / 46   84.8%    ROI +6.1%

**All matches** is the engine: every fixture priced, bet or not. **Played lanes** is the same count over the lanes with real edge — what was buyable, tracked in its own block below. **Placed bets** is the book. Rendered by `python scripts/board.py` from `config/fixtures.tsv`, never typed · over/under markets only · live tips, not backtests

**Session #4 opened 28 Aug 2026.** Session #3 (24–27 Aug, the cup run) is
archived whole at **[archive/2026-08-24-session-3-cup-run/](archive/2026-08-24-session-3-cup-run/)** —
final read: Tip 1 81.9% on 72 settled, playable lanes 84.3%, bets ROI −7.5%.
Every straggler is settled and graded in the archive; this board is purely Session #4.

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

**Playable — 60 / 90   ·   66.7%**   ·   **Tip 1 — 27 / 37   ·   73.0%**   ·   **Tip 2 — 33 / 53   ·   62.3%**

<table align="left"><tr><th align="left">✅ 0-2 · 28-08 01:10 <b>Llaneros v Millonarios</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (84.3 capped)</td><td>— under +1%</td><td>✅ O1.75 69.5% +2.3%<br>buy≥1.62 · floor −5.5</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 03:15 <b>Internacional de Bogotá v Deportivo Pasto</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (84.3 capped)</td><td>— under +1%</td><td>✅ <b>Internacional de Bogotá O0.5</b> 82.9% +7.5%<br>buy≥1.31 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 13:35 <b>Dalian Yingbo v Guoan</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>O1.5 83.0% +1.1%<br>buy≥1.26 (+4.5% margin)</td><td>❌ <b>Guoan O1.5</b> 56.2% +17.2%<br>buy≥1.49 (−16.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 2-3 · 28-08 13:35 <b>Shenhua v Taishan</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>— under +1%</td><td>✅ <b>Taishan O0.5</b> 81.8% +9.2%<br>buy≥1.31 (+6.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 28-08 14:00 <b>Shenzhen v Port</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 83.9% +4.6%<br>buy≥1.30 (+8.4% margin)</td><td>❌ <b>Port O1.5</b> 57.3% +18.4%<br>buy≥1.48 (−15.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 28-08 17:50 <b>Al-Riyadh v Neom</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>— under +1%</td><td>❌ <b>Neom O0.5</b> 81.4% +8.5%<br>buy≥1.29 (+5.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-2 · 28-08 18:00 <b>Wisła Płock v Korona</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>U3.0 79.2% +9.0%<br>buy≥1.47 (+8.9% margin)</td><td>✅ U2.75 58.7% +10.6%<br>buy≥1.39 (−8.9% margin) · floor −19.3</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 28-08 18:00 <b>Al-Fayha v Abha</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>U4.25 85.9% +4.0%<br>buy≥1.28 (+8.5% margin)</td><td>❌ U3.75 70.8% +5.6%<br>buy≥1.26 (−3.5% margin) · floor −4.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-5 · 28-08 18:30 <b>Braunschweig v Hertha</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>O1.5 83.5% +2.9%<br>buy≥1.26 (+5.3% margin)</td><td>✅ <b>Hertha O0.5</b> 81.9% +8.5%<br>buy≥1.32 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 28-08 18:30 <b>Bochum v Osnabrück</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>U4.25 82.7% +1.9%<br>buy≥1.29 (+5.1% margin)</td><td>❌ <b>Bochum O1.5</b> 56.4% +8.3%<br>buy≥1.51 (−15.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 28-08 19:00 <b>Racing Santander v Elche</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>U3.0 76.1% +3.6%<br>buy≥1.49 (+3.8% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 28-08 20:00 <b>Clermont v Sochaux</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>— under +1%</td><td>✅ <b>Sochaux U1.5</b> 75.4% +6.8%<br>buy≥1.43 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 28-08 20:00 <b>Montpellier v Boulogne</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>— under +1%</td><td>✅ <b>Boulogne U1.5</b> 75.4% +6.8%<br>buy≥1.43 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 28-08 20:00 <b>Nancy v Dunkerque</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>— under +1%</td><td>✅ U3.75 76.5% +1.5%<br>buy≥1.29 (+5.2% margin) · floor −1.5</td></tr></table>
<table align="left"><tr><th align="left">◦ 0-3 · 28-08 20:00 <b>Laval v Grenoble</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U3.0 79.9% +5.0%<br>buy≥1.46 (+9.5% margin)</td><td>❌ U2.75 59.6% +6.1%<br>buy≥1.40 (−6.9% margin) · floor −18.4</td></tr></table>
<table align="left"><tr><th align="left">❌ 2-3 · 28-08 20:00 <b>Groningen v Fortuna Sittard</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>U4.25 85.2% +5.8%<br>buy≥1.29 (+8.5% margin)</td><td>❌ U3.75 69.7% +7.9%<br>buy≥1.27 (−3.9% margin) · floor −5.3</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-4 · 28-08 20:00 <b>Den Bosch v Vitesse</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>O1.5 83.0% +1.4%<br>buy≥1.25 (+4.0% margin)</td><td>✅ <b>Vitesse O0.5</b> 81.1% +8.4%<br>buy≥1.31 (+5.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 6-1 · 28-08 20:00 <b>Volendam v Dordrecht</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>— under +1%</td><td>✅ <b>Volendam O1.5</b> 57.5% +6.1%<br>buy≥1.47 (−15.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 28-08 20:00 <b>Jong Ajax v Helmond Sport</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 84.1% +4.4%<br>buy≥1.29 (+8.2% margin)</td><td>❌ U3.75 69.6% +7.2%<br>buy≥1.28 (−3.8% margin) · floor −5.4</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-2 · 28-08 20:00 <b>Jong AZ v MVV</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 81.2% +1.4%<br>buy≥1.29 (+2.6% margin)</td><td>❌ <b>Jong AZ O1.5</b> 56.0% +4.6%<br>buy≥1.48 (−16.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:00 <b>Waalwijk v Jong PSV</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>— under +1%</td><td>✅ <b>Waalwijk O1.5</b> 58.1% +6.7%<br>buy≥1.47 (−14.7% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:00 <b>Roda v NAC Breda</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 81.6% +1.8%<br>buy≥1.29 (+2.9% margin)</td><td>❌ U3.75 64.8% +2.4%<br>buy≥1.26 (−11.1% margin) · floor −10.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 28-08 20:00 <b>TOP Oss v Jong FC Utrecht</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 81.6% +1.8%<br>buy≥1.29 (+2.9% margin)</td><td>✅ U3.75 64.8% +2.4%<br>buy≥1.26 (−11.1% margin) · floor −10.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-5 · 28-08 20:00 <b>Al-Khaleej v Al-Hilal</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>O1.5 82.3% +2.5%<br>buy≥1.25 (+2.7% margin)</td><td>✅ <b>Al-Hilal O1.5</b> 64.5% +26.3%<br>buy≥1.41 (−9.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 28-08 20:00 <b>Al-Nassr v Al-Taawoun</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>O1.5 82.8% +3.0%<br>buy≥1.24 (+3.1% margin)</td><td>✅ <b>Al-Nassr O1.5</b> 60.5% +15.4%<br>buy≥1.44 (−13.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 5-1 · 28-08 20:30 <b>Bayern v Stuttgart</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 86.7% +4.0%<br>buy≥1.23 (+9.0% margin)</td><td>✅ <b>Bayern O1.5</b> 72.6% +22.1%<br>buy≥1.36 (−1.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 20:30 <b>Gençlerbirliği v Erzurumspor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>— under +1%</td><td>✅ <b>Erzurumspor U1.5</b> 75.1% +8.8%<br>buy≥1.39 (+4.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 20:30 <b>Legia v Śląsk</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>— under +1%</td><td>❌ <b>Legia O1.5</b> 55.0% +9.5%<br>buy≥1.59 (−12.7% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:45 <b>Lille v PSG</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>O1.5 80.1% +2.8%<br>buy≥1.31 (+4.9% margin)</td><td>✅ O2.25 57.7% +4.1%<br>buy≥1.35 (−12.2% margin) · floor −17.3</td></tr></table>
<table align="left"><tr><th align="left">✅ 4-0 · 28-08 20:45 <b>Genk v Beveren</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>— under +1%</td><td>✅ <b>Beveren U1.5</b> 75.4% +9.8%<br>buy≥1.34 (+0.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-4 · 28-08 21:00 <b>Crystal Palace v Man City</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 84.3% +4.5%<br>buy≥1.28 (+8.1% margin)</td><td>✅ <b>Man City O1.5</b> 69.8% +30.5%<br>buy≥1.38 (−3.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">◦ 1-2 · 28-08 21:00 <b>Wrexham v Birmingham</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U3.0 76.3% +2.0%<br>buy≥1.47 (+2.8% margin)</td><td>❌ U2.75 55.0% +2.4%<br>buy≥1.35 (−16.8% margin) · floor −20.0</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-4 · 28-08 21:15 <b>Rio Ave v Sporting CP</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>O1.5 79.6% +4.5%<br>buy≥1.35 (+7.7% margin)</td><td>✅ <b>Sporting CP O0.5</b> 80.0% +11.7%<br>buy≥1.35 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 1-0 · 28-08 21:30 <b>Alavés v Villarreal</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>O1.5 76.3% +2.1%<br>buy≥1.32 (+0.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-3 · 28-08 22:00 <b>Comerciantes Unidos v Cajamarca</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga 1 (75.5 −4.1)</td><td>O1.5 76.3% +2.7%<br>buy≥1.38 (+5.2% margin)</td><td>✅ <b>Comerciantes Unidos O0.5</b> 83.5% +3.6%<br>buy≥1.32 (+10.5% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 01:30 <b>Novorizontino v Sport Recife</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (84.0 −2.6)</td><td>— under +1%</td><td>✅ O1.75 67.4% +2.5%<br>buy≥1.47 (−8.5% margin) · floor −7.6</td></tr></table>
<table align="left"><tr><th align="left">◦ 2-1 · 29-08 01:30 <b>Náutico v Athletic</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (84.0 −2.6)</td><td>U3.0 83.1% +1.4%<br>buy≥1.32 (+4.3% margin)</td><td>❌ U2.75 63.9% +2.0%<br>buy≥1.25 (−11.7% margin) · floor −11.1</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 29-08 11:00 <b>Gamba Osaka v Sanfrecce</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>— under +1%</td><td>❌ <b>Sanfrecce O0.5</b> 83.6% +14.6%<br>buy≥1.24 (+3.6% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 29-08 11:00 <b>Mito Hollyhock v Machida Zelvia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>— under +1%</td><td>✅ <b>Machida Zelvia O0.5</b> 82.0% +13.0%<br>buy≥1.25 (+2.4% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 1-5 · 29-08 11:00 <b>Qingdao Hainiu v Wuhan Three Towns</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 81.3% +1.9%<br>buy≥1.30 (+3.2% margin)</td><td>✅ <b>Wuhan Three Towns O0.5</b> 80.8% +8.2%<br>buy≥1.31 (+6.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 4-2 · 29-08 12:00 <b>Kawasaki v Chiba</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 90.0% +1.7%<br>buy≥1.19 (+5.0% margin)</td><td>❌ <b>Chiba U1.5</b> 75.2% +8.0%<br>buy≥1.29 (−3.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 12:00 <b>Nagoya v Okayama</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>— under +1%</td><td>✅ O1.75 76.2% +3.7%<br>buy≥1.34 (−2.2% margin) · floor −5.8</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-2 · 29-08 12:00 <b>Tokyo Verdy v Kashima</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>— under +1%</td><td>✅ <b>Kashima O0.5</b> 80.1% +11.1%<br>buy≥1.26 (+0.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 29-08 12:00 <b>Urawa Reds v Yokohama FM</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 90.0% +1.7%<br>buy≥1.18 (+5.0% margin)</td><td>❌ U3.75 78.2% +3.8%<br>buy≥1.19 (−0.6% margin) · floor −3.8</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 57' 1-1 <b>Radomiak v Cracovia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>— under +1%</td><td>U3.75 75.5% +5.2%<br>buy≥1.32 (+6.9% margin) · floor −2.5 · <i>room for 1</i></td></tr></table>
<table align="left"><tr><th align="left">❌ 5-3 · 29-08 13:00 <b>Heidenheim v Dresden</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>U4.25 82.1% +1.2%<br>buy≥1.30 (+4.8% margin)</td><td>✅ <b>Heidenheim O0.5</b> 83.4% +3.2%<br>buy≥1.26 (+5.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-4 · 29-08 13:00 <b>Cottbus v Fürth</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>U4.25 84.8% +3.9%<br>buy≥1.30 (+9.0% margin)</td><td>❌ U3.75 69.8% +6.0%<br>buy≥1.29 (−2.2% margin) · floor −5.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-5 · 29-08 13:00 <b>Karlsruhe v Wolfsburg</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>O1.5 82.1% +1.4%<br>buy≥1.28 (+4.8% margin)</td><td>✅ O2.25 60.8% +2.2%<br>buy≥1.27 (−13.4% margin) · floor −14.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 29-08 13:00 <b>Tianjin Jinmen Tiger v Qingdao West Coast</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 80.5% +1.2%<br>buy≥1.30 (+2.6% margin)</td><td>✅ U3.75 63.4% +1.5%<br>buy≥1.27 (−11.9% margin) · floor −11.6</td></tr></table>
<table align="left"><tr><th align="left">✅ 6-0 · 29-08 13:00 <b>Zhejiang v Yunnan Yukun</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>O1.5 83.9% +2.0%<br>buy≥1.23 (+5.0% margin)</td><td>❌ <b>Yunnan Yukun O1.5</b> 61.3% +22.4%<br>buy≥1.45 (−11.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 29-08 13:30 <b>Liverpool v Nottingham Forest</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 84.2% +4.4%<br>buy≥1.28 (+8.0% margin)</td><td>✅ <b>Nottingham Forest O1.5</b> 57.3% +18.0%<br>buy≥1.47 (−15.8% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 · 29-08 13:30 <b>Middlesbrough v West Brom</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 76.5% +3.8%<br>buy≥1.39 (+6.3% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 4-1 · 29-08 13:30 <b>Wolves v Stoke</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 76.8% +4.1%<br>buy≥1.39 (+6.5% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 29-08 14:00 <b>Annecy v Metz</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>— under +1%</td><td>✅ O1.75 77.2% +5.2%<br>buy≥1.47 (+8.7% margin) · floor −0.8</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-3 · 29-08 15:00 <b>Häcken v Västerås SK</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (83.0 +1.3)</td><td>— under +1%</td><td>❌ U3.75 70.3% +3.4%<br>buy≥1.24 (−5.3% margin) · floor −4.7</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 29-08 15:00 <b>Elfsborg v Degerfors</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (83.0 +1.3)</td><td>— under +1%</td><td>✅ U3.75 71.4% +4.5%<br>buy≥1.28 (−1.3% margin) · floor −3.6</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 90'+5' 3-2 <b>Köln v Hoffenheim</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>— under +1%</td><td><b>Köln O1.5</b> 56.4% +5.9%<br>buy≥1.49 (−16.3% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 29-08 15:30 <b>Mainz v Paderborn</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>U4.25 81.3% +3.1%<br>buy≥1.29 (+3.0% margin)</td><td>❌ <b>Mainz O1.5</b> 58.3% +7.8%<br>buy≥1.47 (−14.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-0 · 29-08 15:30 <b>Leipzig v Mönchengladbach</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 85.9% +3.2%<br>buy≥1.23 (+5.5% margin)</td><td>✅ <b>Leipzig O1.5</b> 65.8% +15.3%<br>buy≥1.41 (−7.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 90'+7' 3-2 <b>Elversberg v Leverkusen</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 84.3% +1.5%<br>buy≥1.25 (+5.1% margin) · <i>✓ landed</i></td><td><b>Leverkusen O1.5</b> 69.1% +27.6%<br>buy≥1.39 (−4.1% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 69' 1-0 <b>Bournemouth v Everton</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>— under +1%</td><td><b>Bournemouth O0.5</b> 80.7% +2.0%<br>buy≥1.26 (+1.9% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 67' 0-0 <b>Coventry v Hull</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>U4.25 85.4% +3.6%<br>buy≥1.29 (+8.5% margin) · <i>room for 4 · half from the 4th</i></td><td><b>Coventry O1.5</b> 55.8% +6.7%<br>buy≥1.48 (−17.3% margin) · team · <i>needs 2 more</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 70' 1-1 <b>Blackburn v QPR</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U3.0 76.1% +1.8%<br>buy≥1.47 (+2.6% margin) · <i>next goal hurts</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 68' 2-0 <b>Bristol City v Portsmouth</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 75.8% +3.2%<br>buy≥1.35 (+2.4% margin) · <i>✓ landed</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 68' 1-0 <b>Charlton v Preston</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U3.0 81.1% +6.9%<br>buy≥1.43 (+8.9% margin) · <i>room for 1</i></td><td>U2.75 61.2% +8.6%<br>buy≥1.36 (−7.7% margin) · floor −13.8 · <i>room for 1</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 67' 4-1 <b>Norwich v Burnley</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 76.9% +4.2%<br>buy≥1.39 (+6.6% margin) · <i>✓ landed</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 71' 1-1 <b>Watford v West Ham</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 79.9% +7.3%<br>buy≥1.36 (+8.6% margin) · <i>✓ landed</i></td><td><b>West Ham O1.5</b> 55.7% +24.2%<br>buy≥1.56 (−13.4% margin) · team · <i>needs 1 more</i></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 16:00 <b>Vålerenga v Molde</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eliteserien (82.9 −1.5)</td><td>O1.5 83.2% +3.1%<br>buy≥1.26 (+5.1% margin)</td><td><b>Vålerenga O1.5</b> 55.1% +7.1%<br>buy≥1.51 (−16.8% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 64' 2-2 <b>La Louvière v Mechelen</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>— under +1%</td><td><b>La Louvière O0.5</b> 83.0% +5.1%<br>buy≥1.29 (+7.0% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 1-0 <b>Excelsior v Sparta Rotterdam</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>U4.25 82.7% +3.4%<br>buy≥1.28 (+3.6% margin) · <i>room for 3 · half from the 3rd</i></td><td>U3.75 66.3% +4.4%<br>buy≥1.29 (−7.0% margin) · floor −8.7 · <i>room for 2</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 1-1 <b>De Graafschap v Almere City</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>O1.5 84.2% +2.6%<br>buy≥1.24 (+4.9% margin) · <i>✓ landed</i></td><td><b>Almere City O0.5</b> 80.1% +7.4%<br>buy≥1.31 (+5.0% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 0-0 <b>Alverca v Santa Clara</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>U3.0 76.1% +4.7%<br>buy≥1.51 (+4.8% margin) · <i>room for 2</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 1-1 <b>Arouca v Marítimo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>O1.5 76.5% +1.4%<br>buy≥1.33 (+1.9% margin) · <i>✓ landed</i></td><td><b>Arouca O0.5</b> 80.5% +5.2%<br>buy≥1.35 (+8.4% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 24' 0-0 <b>Levante v Real Betis</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>O1.5 78.2% +3.9%<br>buy≥1.35 (+5.5% margin) · <i>needs 2 more</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 26' 0-0 <b>Sabadell v Almería</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>— under +1%</td><td><b>Sabadell U1.5</b> 75.4% +17.0%<br>buy≥1.36 (+2.4% margin) · team · <i>room for 1</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 12' 0-0 <b>Strasbourg v Lens</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>O1.5 79.9% +2.6%<br>buy≥1.31 (+4.7% margin) · <i>needs 2 more</i></td><td><b>Lens O0.5</b> 80.9% +11.9%<br>buy≥1.34 (+8.5% margin) · team · <i>needs 1 more</i></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 17:30 <b>Halmstad v Kalmar</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (83.0 +1.3)</td><td>U3.0 75.6% +8.7%<br>buy≥1.48 (+2.3% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:00 <b>Viking v Aalesund</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eliteserien (82.9 −1.5)</td><td>O1.5 85.8% +5.7%<br>buy≥1.27 (+9.1% margin)</td><td><b>Viking O1.5</b> 76.6% +28.5%<br>buy≥1.35 (+3.2% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 82.9% +23.3% · buy≥1.27 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:00 <b>Konyaspor v Kocaelispor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>— under +1%</td><td>U3.75 72.7% +4.4%<br>buy≥1.30 (+2.0% margin) · floor −9.3</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:00 <b>Lausanne v Zürich</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Swiss Super League (82.6 +0.7)</td><td>— under +1%</td><td><b>Lausanne O1.5</b> 56.0% +5.5%<br>buy≥1.51 (−15.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 78.0% +2.9% · buy≥1.35 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:00 <b>Servette v Luzern</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Swiss Super League (82.6 +0.7)</td><td>O1.5 80.1% +1.3%<br>buy≥1.29 (+3.1% margin)</td><td><b>Servette O1.5</b> 59.9% +9.4%<br>buy≥1.47 (−11.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 79.1% +4.0% · buy≥1.33 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:05 <b>Al-Kholood v Al-Ahli</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>— under +1%</td><td><b>Al-Ahli O0.5</b> 81.9% +8.9%<br>buy≥1.29 (+5.6% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:30 <b>Tottenham v Newcastle</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 84.7% +4.8%<br>buy≥1.28 (+8.3% margin)</td><td><b>Newcastle O1.5</b> 58.1% +18.8%<br>buy≥1.46 (−15.0% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 79.0% +3.5% · buy≥1.33 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:30 <b>Monza v Udinese</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>— under +1%</td><td><b>Monza U1.5</b> 75.4% +15.1%<br>buy≥1.34 (+1.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:30 <b>Dortmund v Hamburg</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>— under +1%</td><td><b>Dortmund O1.5</b> 57.3% +6.8%<br>buy≥1.48 (−15.3% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 74.5% +18.1% · buy≥1.41 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 18:45 <b>AZ v Go Ahead Eagles</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>O1.5 84.4% +2.5%<br>buy≥1.24 (+4.9% margin)</td><td>O2.25 64.5% +4.0%<br>buy≥1.27 (−8.7% margin) · floor −10.5</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 77.6% +3.1% · buy≥1.35 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 19:00 <b>Sampdoria v Juve Stabia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 78.5% +3.0%<br>buy≥1.43 (+4.2% margin)</td><td>U2.75 57.8% +3.7%<br>buy≥1.37 (−11.6% margin) · floor −17.2</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 19:00 <b>Entella v Cesena</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 77.0% +1.5%<br>buy≥1.45 (+3.0% margin)</td><td>U2.75 55.9% +1.8%<br>buy≥1.34 (−16.2% margin) · floor −19.1</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 69.3% +9.0% · buy≥1.51 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 19:00 <b>Real Sociedad v Espanyol</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>O1.5 75.3% +1.1%<br>buy≥1.33 (−0.1% margin)</td><td><b>Real Sociedad O0.5</b> 82.2% +2.9%<br>buy≥1.28 (+5.1% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 72.7% +10.0% · buy≥1.44 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:00 <b>Al-Fateh v Al-Ittihad</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>U4.25 82.9% +1.1%<br>buy≥1.27 (+3.2% margin)</td><td>U3.75 66.6% +1.5%<br>buy≥1.24 (−10.2% margin) · floor −8.4</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:00 <b>Los Chankas v Juan Pablo II</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Peruvian Liga 1 (75.5 −4.1)</td><td>U3.0 75.1% +1.9%<br>buy≥1.53 (+4.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:15 <b>Zagłębie v Pogoń</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>— under +1%</td><td>U3.75 73.8% +3.5%<br>buy≥1.33 (+5.5% margin) · floor −4.2</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:30 <b>Nürnberg v Bielefeld</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>— under +1%</td><td><b>Bielefeld O0.5</b> 81.0% +7.6%<br>buy≥1.33 (+7.3% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 78.0% +3.0% · buy≥1.35 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:30 <b>Galatasaray v Göztepe</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>— under +1%</td><td><b>Göztepe U1.5</b> 74.0% +7.7%<br>buy≥1.39 (+3.1% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 71.5% +10.0% · buy≥1.47 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:30 <b>Young Boys v Basel</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Swiss Super League (82.6 +0.7)</td><td>O1.5 83.3% +4.5%<br>buy≥1.21 (+8.5% margin)</td><td><b>Basel O1.5</b> 67.2% +34.7%<br>buy≥1.42 (−4.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 81.1% +6.0% · buy≥1.29 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:45 <b>Auxerre v Angers</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>— under +1%</td><td><b>Angers U1.5</b> 75.2% +11.5%<br>buy≥1.38 (+4.2% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 74.7% +17.0% · buy≥1.41 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:45 <b>Lorient v Troyes</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>— under +1%</td><td>U3.75 73.9% +5.2%<br>buy≥1.30 (+3.0% margin) · floor −1.1</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 71.8% +14.2% · buy≥1.46 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:45 <b>Lyon v Le Havre</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>— under +1%</td><td><b>Lyon O0.5</b> 82.2% +5.2%<br>buy≥1.32 (+8.9% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 71.5% +13.8% · buy≥1.47 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:45 <b>Juventus v Parma</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>— under +1%</td><td><b>Parma U1.5</b> 75.4% +9.4%<br>buy≥1.34 (+1.3% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 67.0% +11.1% · buy≥1.57 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 20:45 <b>Cercle Brugge v Lommel</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>O1.5 80.2% +4.0%<br>buy≥1.31 (+4.8% margin)</td><td><b>Cercle Brugge O1.5</b> 60.4% +16.7%<br>buy≥1.45 (−12.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 80.1% +7.1% · buy≥1.31 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 21:00 <b>Ascoli v Carrarese</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 78.5% +3.0%<br>buy≥1.43 (+4.2% margin)</td><td><b>Carrarese U1.5</b> 75.4% +7.0%<br>buy≥1.39 (+5.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 21:00 <b>Padova v Verona</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 79.2% +3.7%<br>buy≥1.46 (+8.1% margin)</td><td>U2.75 58.6% +4.6%<br>buy≥1.37 (−10.7% margin) · floor −16.4</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 72.3% +4.7% · buy≥1.45 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 21:00 <b>Avellino v Vicenza</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 77.5% +2.0%<br>buy≥1.45 (+3.4% margin)</td><td><b>Vicenza U1.5</b> 75.4% +7.0%<br>buy≥1.39 (+5.0% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 66.5% +6.2% · buy≥1.58 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 21:00 <b>Zwolle v NEC</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>O1.5 83.9% +2.0%<br>buy≥1.25 (+4.5% margin)</td><td><b>NEC O1.5</b> 60.7% +19.2%<br>buy≥1.44 (−12.4% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 78.4% +3.8% · buy≥1.34 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 21:30 <b>Sevilla v Atlético Madrid</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>U3.0 77.8% +5.3%<br>buy≥1.46 (+5.2% margin)</td><td>U2.75 56.9% +6.3%<br>buy≥1.35 (−14.1% margin) · floor −18.1</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 65.6% +3.0% · buy≥1.60 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🟢 29-08 21:30 <b>Girona v Las Palmas</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>— under +1%</td><td><b>Las Palmas U1.5</b> 75.4% +2.2%<br>buy≥1.32 (−0.7% margin) · team</td></tr></table>

<br clear="all">

## 🔵 Pending FUTURE match bettips

> [!NOTE]
> Every fixture Athena has priced that has not finished, playable or not — this and the completed block are the ENGINE's record. The typed source is `config/fixtures.tsv`; grade a fixture there and re-render with `python scripts/board.py`. The numbers after each league are its **(hit gap)** over its last 200 replayed matches — read the gap before trusting a row.

<table align="left"><tr><th align="left">🔴 LIVE 57' 1-1 <b>Radomiak v Cracovia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>U4.25 86.0% <b>+0.5%</b><br>buy≥1.22 (+7.2% margin) · <i>room for 2 · half from the 2nd</i></td><td>U3.75 75.5% +5.2%<br>buy≥1.32 (+6.9% margin) · floor −2.5 · <i>room for 1</i></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 14:45 <b>Motor Lublin v Piast Gliwice</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>U4.25 85.7% <b>+0.1%</b><br>buy≥1.27 (+7.1% margin)</td><td>— none</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 76.5% +4.0% · buy≥1.37 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 90'+5' 3-2 <b>Köln v Hoffenheim</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 83.3% <b>+0.6%</b><br>buy≥1.25 (+4.5% margin) · <i>✓ landed</i></td><td><b>Köln O1.5</b> 56.4% +5.9%<br>buy≥1.49 (−16.3% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 90'+2' 2-3 <b>Union Berlin v Frankfurt</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>U4.25 78.9% <b>+0.7%</b><br>buy≥1.31 (+1.1% margin) · <i>✗ gone</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 90'+7' 3-2 <b>Elversberg v Leverkusen</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 84.3% +1.5%<br>buy≥1.25 (+5.1% margin) · <i>✓ landed</i></td><td><b>Leverkusen O1.5</b> 69.1% +27.6%<br>buy≥1.39 (−4.1% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 69' 1-0 <b>Bournemouth v Everton</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 80.0% <b>+0.1%</b><br>buy≥1.27 (+1.4% margin) · <i>needs 1 more</i></td><td><b>Bournemouth O0.5</b> 80.7% +2.0%<br>buy≥1.26 (+1.9% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 67' 0-0 <b>Coventry v Hull</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>U4.25 85.4% +3.6%<br>buy≥1.29 (+8.5% margin) · <i>room for 4 · half from the 4th</i></td><td><b>Coventry O1.5</b> 55.8% +6.7%<br>buy≥1.48 (−17.3% margin) · team · <i>needs 2 more</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 70' 1-1 <b>Blackburn v QPR</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U3.0 76.1% +1.8%<br>buy≥1.47 (+2.6% margin) · <i>next goal hurts</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 69' 0-1 <b>Bolton v Lincoln</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>— no tip: Invalid value '[3.016 1.508 0.754 2.262 2.262]' for dtype 'int64'</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 68' 2-0 <b>Bristol City v Portsmouth</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 75.8% +3.2%<br>buy≥1.35 (+2.4% margin) · <i>✓ landed</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 68' 2-2 <b>Cardiff v Sheffield United</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U4.25 83.6% <b>−4.6%</b><br>buy≥1.22 (+6.2% margin) · <i>half safe</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 68' 1-0 <b>Charlton v Preston</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U3.0 81.1% +6.9%<br>buy≥1.43 (+8.9% margin) · <i>room for 1</i></td><td>U2.75 61.2% +8.6%<br>buy≥1.36 (−7.7% margin) · floor −13.8 · <i>room for 1</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 67' 4-1 <b>Norwich v Burnley</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 76.9% +4.2%<br>buy≥1.39 (+6.6% margin) · <i>✓ landed</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 65' 4-1 <b>Southampton v Millwall</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U4.25 83.6% <b>−4.5%</b><br>buy≥1.23 (+6.2% margin) · <i>✗ gone</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 71' 1-1 <b>Watford v West Ham</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 79.9% +7.3%<br>buy≥1.36 (+8.6% margin) · <i>✓ landed</i></td><td><b>West Ham O1.5</b> 55.7% +24.2%<br>buy≥1.56 (−13.4% margin) · team · <i>needs 1 more</i></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 16:00 <b>Vålerenga v Molde</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eliteserien (82.9 −1.5)</td><td>O1.5 83.2% +3.1%<br>buy≥1.26 (+5.1% margin)</td><td><b>Vålerenga O1.5</b> 55.1% +7.1%<br>buy≥1.51 (−16.8% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 64' 2-2 <b>La Louvière v Mechelen</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>U4.25 85.7% <b>+0.3%</b><br>buy≥1.21 (+5.2% margin) · <i>half safe</i></td><td><b>La Louvière O0.5</b> 83.0% +5.1%<br>buy≥1.29 (+7.0% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 1-0 <b>Excelsior v Sparta Rotterdam</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>U4.25 82.7% +3.4%<br>buy≥1.28 (+3.6% margin) · <i>room for 3 · half from the 3rd</i></td><td>U3.75 66.3% +4.4%<br>buy≥1.29 (−7.0% margin) · floor −8.7 · <i>room for 2</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 1-1 <b>De Graafschap v Almere City</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>O1.5 84.2% +2.6%<br>buy≥1.24 (+4.9% margin) · <i>✓ landed</i></td><td><b>Almere City O0.5</b> 80.1% +7.4%<br>buy≥1.31 (+5.0% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 0-0 <b>Alverca v Santa Clara</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>U3.0 76.1% +4.7%<br>buy≥1.51 (+4.8% margin) · <i>room for 2</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE HT 1-1 <b>Arouca v Marítimo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>O1.5 76.5% +1.4%<br>buy≥1.33 (+1.9% margin) · <i>✓ landed</i></td><td><b>Arouca O0.5</b> 80.5% +5.2%<br>buy≥1.35 (+8.4% margin) · team · <i>✓ landed</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 24' 0-0 <b>Levante v Real Betis</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>O1.5 78.2% +3.9%<br>buy≥1.35 (+5.5% margin) · <i>needs 2 more</i></td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 26' 0-0 <b>Sabadell v Almería</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>U4.25 84.2% <b>−6.2%</b><br>buy≥1.18 (+5.4% margin) · <i>room for 4 · half from the 4th</i></td><td><b>Sabadell U1.5</b> 75.4% +17.0%<br>buy≥1.36 (+2.4% margin) · team · <i>room for 1</i></td></tr></table>
<table align="left"><tr><th align="left">🔴 LIVE 12' 0-0 <b>Strasbourg v Lens</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>O1.5 79.9% +2.6%<br>buy≥1.31 (+4.7% margin) · <i>needs 2 more</i></td><td><b>Lens O0.5</b> 80.9% +11.9%<br>buy≥1.34 (+8.5% margin) · team · <i>needs 1 more</i></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 17:30 <b>Halmstad v Kalmar</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (83.0 +1.3)</td><td>U3.0 75.6% +8.7%<br>buy≥1.48 (+2.3% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 17:30 <b>Wisła Kraków v Wieczysta</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>— no tip: engine abstained (thin history or an unresolved name)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:00 <b>Viking v Aalesund</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eliteserien (82.9 −1.5)</td><td>O1.5 85.8% +5.7%<br>buy≥1.27 (+9.1% margin)</td><td><b>Viking O1.5</b> 76.6% +28.5%<br>buy≥1.35 (+3.2% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 82.9% +23.3% · buy≥1.27 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:00 <b>Konyaspor v Kocaelispor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>U4.25 84.9% <b>+0.7%</b><br>buy≥1.23 (+6.1% margin)</td><td>U3.75 72.7% +4.4%<br>buy≥1.30 (+2.0% margin) · floor −9.3</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:00 <b>Lausanne v Zürich</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Swiss Super League (82.6 +0.7)</td><td>O1.5 79.8% <b>+1.0%</b><br>buy≥1.29 (+2.8% margin)</td><td><b>Lausanne O1.5</b> 56.0% +5.5%<br>buy≥1.51 (−15.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 78.0% +2.9% · buy≥1.35 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:00 <b>Servette v Luzern</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Swiss Super League (82.6 +0.7)</td><td>O1.5 80.1% +1.3%<br>buy≥1.29 (+3.1% margin)</td><td><b>Servette O1.5</b> 59.9% +9.4%<br>buy≥1.47 (−11.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 79.1% +4.0% · buy≥1.33 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:05 <b>Al-Kholood v Al-Ahli</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>O1.5 79.9% <b>+0.1%</b><br>buy≥1.26 (+0.8% margin)</td><td><b>Al-Ahli O0.5</b> 81.9% +8.9%<br>buy≥1.29 (+5.6% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:15 <b>Kortrijk v Charleroi</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>U4.25 85.9% <b>+0.5%</b><br>buy≥1.24 (+5.3% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:30 <b>Tottenham v Newcastle</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 84.7% +4.8%<br>buy≥1.28 (+8.3% margin)</td><td><b>Newcastle O1.5</b> 58.1% +18.8%<br>buy≥1.46 (−15.0% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 79.0% +3.5% · buy≥1.33 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:30 <b>Fiorentina v Frosinone</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>U4.25 85.8% <b>−2.9%</b><br>buy≥1.18 (+5.4% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:30 <b>Monza v Udinese</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>U4.25 85.8% <b>−2.9%</b><br>buy≥1.18 (+5.4% margin)</td><td><b>Monza U1.5</b> 75.4% +15.1%<br>buy≥1.34 (+1.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:30 <b>Sassuolo v Torino</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>U4.25 85.9% <b>−2.8%</b><br>buy≥1.20 (+5.4% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:30 <b>Dortmund v Hamburg</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 82.9% <b>+0.1%</b><br>buy≥1.26 (+4.2% margin)</td><td><b>Dortmund O1.5</b> 57.3% +6.8%<br>buy≥1.48 (−15.3% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 74.5% +18.1% · buy≥1.41 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 18:45 <b>AZ v Go Ahead Eagles</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>O1.5 84.4% +2.5%<br>buy≥1.24 (+4.9% margin)</td><td>O2.25 64.5% +4.0%<br>buy≥1.27 (−8.7% margin) · floor −10.5</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 77.6% +3.1% · buy≥1.35 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 19:00 <b>Sampdoria v Juve Stabia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 78.5% +3.0%<br>buy≥1.43 (+4.2% margin)</td><td>U2.75 57.8% +3.7%<br>buy≥1.37 (−11.6% margin) · floor −17.2</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 19:00 <b>Entella v Cesena</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 77.0% +1.5%<br>buy≥1.45 (+3.0% margin)</td><td>U2.75 55.9% +1.8%<br>buy≥1.34 (−16.2% margin) · floor −19.1</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 69.3% +9.0% · buy≥1.51 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 19:00 <b>Real Sociedad v Espanyol</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>O1.5 75.3% +1.1%<br>buy≥1.33 (−0.1% margin)</td><td><b>Real Sociedad O0.5</b> 82.2% +2.9%<br>buy≥1.28 (+5.1% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 72.7% +10.0% · buy≥1.44 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 19:00 <b>Albacete v Real Oviedo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>U4.25 84.2% <b>−6.2%</b><br>buy≥1.18 (+5.4% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 19:00 <b>Leganés v Eldense</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>U3.0 77.8% <b>+0.0%</b><br>buy≥1.30 (+1.2% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 19:00 <b>Académico Viseu v Porto</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>— no tip: engine abstained (thin history or an unresolved name)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:00 <b>Guingamp v Nantes</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 84.0% <b>−4.7%</b><br>buy≥1.22 (+7.1% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:00 <b>Al-Ettifaq v Al Diriyah</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>— no tip: engine abstained (thin history or an unresolved name)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:00 <b>Al-Fateh v Al-Ittihad</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>U4.25 82.9% +1.1%<br>buy≥1.27 (+3.2% margin)</td><td>U3.75 66.6% +1.5%<br>buy≥1.24 (−10.2% margin) · floor −8.4</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:00 <b>Los Chankas v Juan Pablo II</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Peruvian Liga 1 (75.5 −4.1)</td><td>U3.0 75.1% +1.9%<br>buy≥1.53 (+4.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:15 <b>Zagłębie v Pogoń</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>U4.25 86.1% <b>+0.6%</b><br>buy≥1.23 (+7.2% margin)</td><td>U3.75 73.8% +3.5%<br>buy≥1.33 (+5.5% margin) · floor −4.2</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:30 <b>Nürnberg v Bielefeld</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>O1.5 81.5% <b>+0.9%</b><br>buy≥1.28 (+4.4% margin)</td><td><b>Bielefeld O0.5</b> 81.0% +7.6%<br>buy≥1.33 (+7.3% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 78.0% +3.0% · buy≥1.35 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:30 <b>Galatasaray v Göztepe</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>U4.25 81.3% <b>−2.9%</b><br>buy≥1.32 (+5.3% margin)</td><td><b>Göztepe U1.5</b> 74.0% +7.7%<br>buy≥1.39 (+3.1% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 71.5% +10.0% · buy≥1.47 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:30 <b>Gaziantep v Rizespor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>U4.25 83.2% <b>−1.0%</b><br>buy≥1.29 (+5.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:30 <b>Young Boys v Basel</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Swiss Super League (82.6 +0.7)</td><td>O1.5 83.3% +4.5%<br>buy≥1.21 (+8.5% margin)</td><td><b>Basel O1.5</b> 67.2% +34.7%<br>buy≥1.42 (−4.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 81.1% +6.0% · buy≥1.29 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>Auxerre v Angers</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>U4.25 83.3% <b>−1.1%</b><br>buy≥1.24 (+5.8% margin)</td><td><b>Angers U1.5</b> 75.2% +11.5%<br>buy≥1.38 (+4.2% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 74.7% +17.0% · buy≥1.41 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>Lorient v Troyes</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>U4.25 83.2% <b>−1.2%</b><br>buy≥1.22 (+5.7% margin)</td><td>U3.75 73.9% +5.2%<br>buy≥1.30 (+3.0% margin) · floor −1.1</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 71.8% +14.2% · buy≥1.46 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>Lyon v Le Havre</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>O1.5 78.2% <b>+0.9%</b><br>buy≥1.32 (+3.3% margin)</td><td><b>Lyon O0.5</b> 82.2% +5.2%<br>buy≥1.32 (+8.9% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 71.5% +13.8% · buy≥1.47 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>Brest v Toulouse</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>U4.25 83.5% <b>−0.9%</b><br>buy≥1.26 (+5.8% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>Juventus v Parma</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>U3.0 75.1% <b>+0.0%</b><br>buy≥1.31 (−2.1% margin)</td><td><b>Parma U1.5</b> 75.4% +9.4%<br>buy≥1.34 (+1.3% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 67.0% +11.1% · buy≥1.57 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>Cercle Brugge v Lommel</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>O1.5 80.2% +4.0%<br>buy≥1.31 (+4.8% margin)</td><td><b>Cercle Brugge O1.5</b> 60.4% +16.7%<br>buy≥1.45 (−12.7% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 80.1% +7.1% · buy≥1.31 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 20:45 <b>OH Leuven v Standard Liège</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>— no tip: engine abstained (thin history or an unresolved name)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 21:00 <b>Ascoli v Carrarese</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 78.5% +3.0%<br>buy≥1.43 (+4.2% margin)</td><td><b>Carrarese U1.5</b> 75.4% +7.0%<br>buy≥1.39 (+5.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 21:00 <b>Padova v Verona</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 79.2% +3.7%<br>buy≥1.46 (+8.1% margin)</td><td>U2.75 58.6% +4.6%<br>buy≥1.37 (−10.7% margin) · floor −16.4</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 72.3% +4.7% · buy≥1.45 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 21:00 <b>Avellino v Vicenza</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 77.5% +2.0%<br>buy≥1.45 (+3.4% margin)</td><td><b>Vicenza U1.5</b> 75.4% +7.0%<br>buy≥1.39 (+5.0% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 66.5% +6.2% · buy≥1.58 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 21:00 <b>Zwolle v NEC</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>O1.5 83.9% +2.0%<br>buy≥1.25 (+4.5% margin)</td><td><b>NEC O1.5</b> 60.7% +19.2%<br>buy≥1.44 (−12.4% margin) · team</td></tr><tr><td colspan="3"><sub>Tip 3 · 12 78.4% +3.8% · buy≥1.34 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 21:30 <b>Sevilla v Atlético Madrid</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>U3.0 77.8% +5.3%<br>buy≥1.46 (+5.2% margin)</td><td>U2.75 56.9% +6.3%<br>buy≥1.35 (−14.1% margin) · floor −18.1</td></tr><tr><td colspan="3"><sub>Tip 3 · DNB1 65.6% +3.0% · buy≥1.60 · probation — result lane, outside every tally until it earns in</sub></td></tr></table>
<table align="left"><tr><th align="left">🔵 29-08 21:30 <b>Girona v Las Palmas</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>U4.25 84.2% <b>−6.2%</b><br>buy≥1.18 (+5.4% margin)</td><td><b>Las Palmas U1.5</b> 75.4% +2.2%<br>buy≥1.32 (−0.7% margin) · team</td></tr></table>

<br clear="all">

## ⚪ Completed FUTURE match bettips

**Tip 1 — 58 / 70   ·   82.9%**   ·   **Tip 2 — 33 / 53   ·   62.3%**

<table align="left"><tr><th align="left">✅ 0-2 · 28-08 01:10 <b>Llaneros v Millonarios</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (84.3 capped)</td><td>U4.25 90.0% <b>−1.5%</b><br>buy≥1.19</td><td>✅ O1.75 69.5% +2.3%<br>buy≥1.62 · floor −5.5</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 03:15 <b>Internacional de Bogotá v Deportivo Pasto</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Categoría Primera A (84.3 capped)</td><td>U4.25 90.0% <b>−1.5%</b><br>buy≥1.18</td><td>✅ <b>Internacional de Bogotá O0.5</b> 82.9% +7.5%<br>buy≥1.31 · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 13:35 <b>Dalian Yingbo v Guoan</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>O1.5 83.0% +1.1%<br>buy≥1.26 (+4.5% margin)</td><td>❌ <b>Guoan O1.5</b> 56.2% +17.2%<br>buy≥1.49 (−16.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 2-3 · 28-08 13:35 <b>Shenhua v Taishan</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 79.7% <b>+0.4%</b><br>buy≥1.31 (+1.9% margin)</td><td>✅ <b>Taishan O0.5</b> 81.8% +9.2%<br>buy≥1.31 (+6.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 28-08 14:00 <b>Shenzhen v Port</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 83.9% +4.6%<br>buy≥1.30 (+8.4% margin)</td><td>❌ <b>Port O1.5</b> 57.3% +18.4%<br>buy≥1.48 (−15.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">⚪ — no source · 28-08 17:00 <b>Shooting Stars SC v Inter Lagos</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>NPFL (88.7 −1.8)</td><td>— no tip: Inter Lagos has no history in the data</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 28-08 17:50 <b>Al-Riyadh v Neom</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>U4.25 82.6% <b>+0.7%</b><br>buy≥1.27 (+2.9% margin)</td><td>❌ <b>Neom O0.5</b> 81.4% +8.5%<br>buy≥1.29 (+5.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-2 · 28-08 18:00 <b>Wisła Płock v Korona</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>U3.0 79.2% +9.0%<br>buy≥1.47 (+8.9% margin)</td><td>✅ U2.75 58.7% +10.6%<br>buy≥1.39 (−8.9% margin) · floor −19.3</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 28-08 18:00 <b>Al-Fayha v Abha</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>U4.25 85.9% +4.0%<br>buy≥1.28 (+8.5% margin)</td><td>❌ U3.75 70.8% +5.6%<br>buy≥1.26 (−3.5% margin) · floor −4.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-5 · 28-08 18:30 <b>Braunschweig v Hertha</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>O1.5 83.5% +2.9%<br>buy≥1.26 (+5.3% margin)</td><td>✅ <b>Hertha O0.5</b> 81.9% +8.5%<br>buy≥1.32 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 28-08 18:30 <b>Bochum v Osnabrück</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>U4.25 82.7% +1.9%<br>buy≥1.29 (+5.1% margin)</td><td>❌ <b>Bochum O1.5</b> 56.4% +8.3%<br>buy≥1.51 (−15.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">⚪ 2-3 (no tip) · 28-08 19:00 <b>Horsens v Viborg FF</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Danish Superliga (82.2 −0.9)</td><td>— no tip: Horsens has 4 stored matches (promoted; no Danish lower-division source)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 28-08 19:00 <b>Racing Santander v Elche</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>U3.0 76.1% +3.6%<br>buy≥1.49 (+3.8% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 28-08 20:00 <b>Clermont v Sochaux</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 83.9% <b>−4.7%</b><br>buy≥1.21 (+7.0% margin)</td><td>✅ <b>Sochaux U1.5</b> 75.4% +6.8%<br>buy≥1.43 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 28-08 20:00 <b>Montpellier v Boulogne</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 83.9% <b>−4.7%</b><br>buy≥1.21 (+7.0% margin)</td><td>✅ <b>Boulogne U1.5</b> 75.4% +6.8%<br>buy≥1.43 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 28-08 20:00 <b>Nancy v Dunkerque</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 83.9% <b>−4.7%</b><br>buy≥1.20 (+7.0% margin)</td><td>✅ U3.75 76.5% +1.5%<br>buy≥1.29 (+5.2% margin) · floor −1.5</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:00 <b>Rodez v Pau</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 84.0% <b>−4.6%</b><br>buy≥1.23 (+7.1% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">◦ 0-3 · 28-08 20:00 <b>Laval v Grenoble</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U3.0 79.9% +5.0%<br>buy≥1.46 (+9.5% margin)</td><td>❌ U2.75 59.6% +6.1%<br>buy≥1.40 (−6.9% margin) · floor −18.4</td></tr></table>
<table align="left"><tr><th align="left">❌ 2-3 · 28-08 20:00 <b>Groningen v Fortuna Sittard</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eredivisie (84.6 −0.7)</td><td>U4.25 85.2% +5.8%<br>buy≥1.29 (+8.5% margin)</td><td>❌ U3.75 69.7% +7.9%<br>buy≥1.27 (−3.9% margin) · floor −5.3</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-4 · 28-08 20:00 <b>Den Bosch v Vitesse</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>O1.5 83.0% +1.4%<br>buy≥1.25 (+4.0% margin)</td><td>✅ <b>Vitesse O0.5</b> 81.1% +8.4%<br>buy≥1.31 (+5.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 6-1 · 28-08 20:00 <b>Volendam v Dordrecht</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>O1.5 82.0% <b>+0.4%</b><br>buy≥1.26 (+3.2% margin)</td><td>✅ <b>Volendam O1.5</b> 57.5% +6.1%<br>buy≥1.47 (−15.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 28-08 20:00 <b>Jong Ajax v Helmond Sport</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 84.1% +4.4%<br>buy≥1.29 (+8.2% margin)</td><td>❌ U3.75 69.6% +7.2%<br>buy≥1.28 (−3.8% margin) · floor −5.4</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-2 · 28-08 20:00 <b>Jong AZ v MVV</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 81.2% +1.4%<br>buy≥1.29 (+2.6% margin)</td><td>❌ <b>Jong AZ O1.5</b> 56.0% +4.6%<br>buy≥1.48 (−16.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:00 <b>Waalwijk v Jong PSV</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>O1.5 82.2% <b>+0.6%</b><br>buy≥1.26 (+3.4% margin)</td><td>✅ <b>Waalwijk O1.5</b> 58.1% +6.7%<br>buy≥1.47 (−14.7% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:00 <b>Roda v NAC Breda</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 81.6% +1.8%<br>buy≥1.29 (+2.9% margin)</td><td>❌ U3.75 64.8% +2.4%<br>buy≥1.26 (−11.1% margin) · floor −10.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 28-08 20:00 <b>TOP Oss v Jong FC Utrecht</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Eerste Divisie (84.3 +2.1)</td><td>U4.25 81.6% +1.8%<br>buy≥1.29 (+2.9% margin)</td><td>✅ U3.75 64.8% +2.4%<br>buy≥1.26 (−11.1% margin) · floor −10.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-5 · 28-08 20:00 <b>Al-Khaleej v Al-Hilal</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>O1.5 82.3% +2.5%<br>buy≥1.25 (+2.7% margin)</td><td>✅ <b>Al-Hilal O1.5</b> 64.5% +26.3%<br>buy≥1.41 (−9.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 28-08 20:00 <b>Al-Nassr v Al-Taawoun</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Saudi Pro League (85.4 +1.3)</td><td>O1.5 82.8% +3.0%<br>buy≥1.24 (+3.1% margin)</td><td>✅ <b>Al-Nassr O1.5</b> 60.5% +15.4%<br>buy≥1.44 (−13.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">◦ 0-3 · 28-08 20:30 <b>Cremonese v Modena</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie B (79.5 +2.3)</td><td>U3.0 74.4% <b>−1.0%</b><br>buy≥1.50 (+0.9% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 5-1 · 28-08 20:30 <b>Bayern v Stuttgart</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 86.7% +4.0%<br>buy≥1.23 (+9.0% margin)</td><td>✅ <b>Bayern O1.5</b> 72.6% +22.1%<br>buy≥1.36 (−1.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 20:30 <b>Gençlerbirliği v Erzurumspor</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Trendyol Süper Lig (80.3 −2.8)</td><td>U4.25 84.7% <b>+0.5%</b><br>buy≥1.20 (+6.1% margin)</td><td>✅ <b>Erzurumspor U1.5</b> 75.1% +8.8%<br>buy≥1.39 (+4.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 28-08 20:30 <b>Legia v Śląsk</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ekstraklasa (77.2 −7.1)</td><td>U4.25 84.3% <b>−1.2%</b><br>buy≥1.29 (+6.8% margin)</td><td>❌ <b>Legia O1.5</b> 55.0% +9.5%<br>buy≥1.59 (−12.7% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 28-08 20:45 <b>Lille v PSG</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 1 (80.3 −1.3)</td><td>O1.5 80.1% +2.8%<br>buy≥1.31 (+4.9% margin)</td><td>✅ O2.25 57.7% +4.1%<br>buy≥1.35 (−12.2% margin) · floor −17.3</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 28-08 20:45 <b>Milan v Venezia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Serie A (84.2 capped)</td><td>U3.0 75.1% <b>+0.0%</b><br>buy≥1.30 (−2.1% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 4-0 · 28-08 20:45 <b>Genk v Beveren</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Belgian Pro League (84.7 +0.8)</td><td>U4.25 85.8% <b>+0.4%</b><br>buy≥1.23 (+5.3% margin)</td><td>✅ <b>Beveren U1.5</b> 75.4% +9.8%<br>buy≥1.34 (+0.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-4 · 28-08 21:00 <b>Crystal Palace v Man City</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 84.3% +4.5%<br>buy≥1.28 (+8.1% margin)</td><td>✅ <b>Man City O1.5</b> 69.8% +30.5%<br>buy≥1.38 (−3.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">◦ 1-2 · 28-08 21:00 <b>Wrexham v Birmingham</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U3.0 76.3% +2.0%<br>buy≥1.47 (+2.8% margin)</td><td>❌ U2.75 55.0% +2.4%<br>buy≥1.35 (−16.8% margin) · floor −20.0</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 28-08 21:00 <b>Tenerife v Sporting Gijón</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga 2 (82.7 capped)</td><td>U3.0 77.8% <b>+0.0%</b><br>buy≥1.36 (+1.2% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-4 · 28-08 21:15 <b>Rio Ave v Sporting CP</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga Portugal (80.4 −2.0)</td><td>O1.5 79.6% +4.5%<br>buy≥1.35 (+7.7% margin)</td><td>✅ <b>Sporting CP O0.5</b> 80.0% +11.7%<br>buy≥1.35 (+8.0% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 1-0 · 28-08 21:30 <b>Alavés v Villarreal</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>LaLiga (81.7 −1.4)</td><td>O1.5 76.3% +2.1%<br>buy≥1.32 (+0.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-3 · 28-08 22:00 <b>Comerciantes Unidos v Cajamarca</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga 1 (75.5 −4.1)</td><td>O1.5 76.3% +2.7%<br>buy≥1.38 (+5.2% margin)</td><td>✅ <b>Comerciantes Unidos O0.5</b> 83.5% +3.6%<br>buy≥1.32 (+10.5% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 00:30 <b>Goiás v São Bernardo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (84.0 −2.6)</td><td>O1.0 88.5% <b>−0.6%</b><br>buy≥1.23 (+6.1% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 29-08 00:30 <b>Cobresal v Palestino</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Liga de Primera (81.9 −3.3)</td><td>U4.25 84.6% <b>−0.8%</b><br>buy≥1.27 (+5.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 01:30 <b>Novorizontino v Sport Recife</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (84.0 −2.6)</td><td>O1.0 88.5% <b>−0.6%</b><br>buy≥1.22 (+6.1% margin)</td><td>✅ O1.75 67.4% +2.5%<br>buy≥1.47 (−8.5% margin) · floor −7.6</td></tr></table>
<table align="left"><tr><th align="left">◦ 2-1 · 29-08 01:30 <b>Náutico v Athletic</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Brasileirão Série B (84.0 −2.6)</td><td>U3.0 83.1% +1.4%<br>buy≥1.32 (+4.3% margin)</td><td>❌ U2.75 63.9% +2.0%<br>buy≥1.25 (−11.7% margin) · floor −11.1</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 29-08 11:00 <b>Gamba Osaka v Sanfrecce</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 81.9% <b>−6.3%</b><br>buy≥1.23 (−0.8% margin)</td><td>❌ <b>Sanfrecce O0.5</b> 83.6% +14.6%<br>buy≥1.24 (+3.6% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 29-08 11:00 <b>Mito Hollyhock v Machida Zelvia</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 87.2% <b>−1.0%</b><br>buy≥1.19 (+3.0% margin)</td><td>✅ <b>Machida Zelvia O0.5</b> 82.0% +13.0%<br>buy≥1.25 (+2.4% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 1-5 · 29-08 11:00 <b>Qingdao Hainiu v Wuhan Three Towns</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 81.3% +1.9%<br>buy≥1.30 (+3.2% margin)</td><td>✅ <b>Wuhan Three Towns O0.5</b> 80.8% +8.2%<br>buy≥1.31 (+6.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-1 · 29-08 11:30 <b>Shimizu v Kashiwa</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 88.3% <b>+0.1%</b><br>buy≥1.19 (+3.8% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-3 · 29-08 11:30 <b>Nagasaki v FC Tokyo</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 88.2% <b>−0.0%</b><br>buy≥1.19 (+3.8% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">❌ 4-2 · 29-08 12:00 <b>Kawasaki v Chiba</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 90.0% +1.7%<br>buy≥1.19 (+5.0% margin)</td><td>❌ <b>Chiba U1.5</b> 75.2% +8.0%<br>buy≥1.29 (−3.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 12:00 <b>Kyoto Sanga v Fukuoka</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 88.6% <b>+0.3%</b><br>buy≥1.19 (+4.0% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 12:00 <b>Nagoya v Okayama</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 85.4% <b>−2.9%</b><br>buy≥1.21 (+1.7% margin)</td><td>✅ O1.75 76.2% +3.7%<br>buy≥1.34 (−2.2% margin) · floor −5.8</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-2 · 29-08 12:00 <b>Tokyo Verdy v Kashima</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 89.3% <b>+1.0%</b><br>buy≥1.18 (+4.5% margin)</td><td>✅ <b>Kashima O0.5</b> 80.1% +11.1%<br>buy≥1.26 (+0.9% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-2 · 29-08 12:00 <b>Urawa Reds v Yokohama FM</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 90.0% +1.7%<br>buy≥1.18 (+5.0% margin)</td><td>❌ U3.75 78.2% +3.8%<br>buy≥1.19 (−0.6% margin) · floor −3.8</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-0 · 29-08 12:00 <b>Vissel Kobe v Cerezo Osaka</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>J1 League (90.0 +1.4)</td><td>U4.25 88.6% <b>+0.4%</b><br>buy≥1.18 (+4.0% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">❌ 5-3 · 29-08 13:00 <b>Heidenheim v Dresden</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>U4.25 82.1% +1.2%<br>buy≥1.30 (+4.8% margin)</td><td>✅ <b>Heidenheim O0.5</b> 83.4% +3.2%<br>buy≥1.26 (+5.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">❌ 3-4 · 29-08 13:00 <b>Cottbus v Fürth</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>U4.25 84.8% +3.9%<br>buy≥1.30 (+9.0% margin)</td><td>❌ U3.75 69.8% +6.0%<br>buy≥1.29 (−2.2% margin) · floor −5.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-5 · 29-08 13:00 <b>Karlsruhe v Wolfsburg</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>2. Bundesliga (82.3 −0.5)</td><td>O1.5 82.1% +1.4%<br>buy≥1.28 (+4.8% margin)</td><td>✅ O2.25 60.8% +2.2%<br>buy≥1.27 (−13.4% margin) · floor −14.2</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 29-08 13:00 <b>Tianjin Jinmen Tiger v Qingdao West Coast</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>U4.25 80.5% +1.2%<br>buy≥1.30 (+2.6% margin)</td><td>✅ U3.75 63.4% +1.5%<br>buy≥1.27 (−11.9% margin) · floor −11.6</td></tr></table>
<table align="left"><tr><th align="left">✅ 6-0 · 29-08 13:00 <b>Zhejiang v Yunnan Yukun</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>O1.5 83.9% +2.0%<br>buy≥1.23 (+5.0% margin)</td><td>❌ <b>Yunnan Yukun O1.5</b> 61.3% +22.4%<br>buy≥1.45 (−11.1% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-2 · 29-08 13:30 <b>Liverpool v Nottingham Forest</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Premier League (84.7 +1.1)</td><td>O1.5 84.2% +4.4%<br>buy≥1.28 (+8.0% margin)</td><td>✅ <b>Nottingham Forest O1.5</b> 57.3% +18.0%<br>buy≥1.47 (−15.8% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-3 · 29-08 13:30 <b>Derby v Swansea</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>U4.25 83.6% <b>−4.6%</b><br>buy≥1.21 (+6.2% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-1 · 29-08 13:30 <b>Middlesbrough v West Brom</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 76.5% +3.8%<br>buy≥1.39 (+6.3% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 4-1 · 29-08 13:30 <b>Wolves v Stoke</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Championship (79.0 −3.0)</td><td>O1.5 76.8% +4.1%<br>buy≥1.39 (+6.5% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-1 · 29-08 13:35 <b>Chengdu Rongcheng v Tieren</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>O1.5 81.9% <b>+0.0%</b><br>buy≥1.27 (+3.7% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-1 · 29-08 14:00 <b>Annecy v Metz</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 84.4% <b>−4.2%</b><br>buy≥1.29 (+7.2% margin)</td><td>✅ O1.75 77.2% +5.2%<br>buy≥1.47 (+8.7% margin) · floor −0.8</td></tr></table>
<table align="left"><tr><th align="left">❌ 1-4 · 29-08 14:00 <b>Red Star v Reims</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Ligue 2 (75.9 −6.5)</td><td>U4.25 84.0% <b>−4.6%</b><br>buy≥1.23 (+7.1% margin)</td><td>— none</td></tr></table>
<table align="left"><tr><th align="left">⚪ 1-1 (no tip) · 29-08 14:00 <b>Henan v Chongqing Tongliang Long</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Chinese Super League (83.7 +1.7)</td><td>— no tip: engine abstained (thin history or an unresolved name)</td><td>—</td></tr></table>
<table align="left"><tr><th align="left">✅ 1-3 · 29-08 15:00 <b>Häcken v Västerås SK</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (83.0 +1.3)</td><td>U4.25 83.5% <b>+0.4%</b><br>buy≥1.25 (+5.1% margin)</td><td>❌ U3.75 70.3% +3.4%<br>buy≥1.24 (−5.3% margin) · floor −4.7</td></tr></table>
<table align="left"><tr><th align="left">✅ 2-0 · 29-08 15:00 <b>Elfsborg v Degerfors</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Allsvenskan (83.0 +1.3)</td><td>U4.25 83.4% <b>+0.3%</b><br>buy≥1.23 (+5.1% margin)</td><td>✅ U3.75 71.4% +4.5%<br>buy≥1.28 (−1.3% margin) · floor −3.6</td></tr></table>
<table align="left"><tr><th align="left">✅ 0-0 · 29-08 15:30 <b>Mainz v Paderborn</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>U4.25 81.3% +3.1%<br>buy≥1.29 (+3.0% margin)</td><td>❌ <b>Mainz O1.5</b> 58.3% +7.8%<br>buy≥1.47 (−14.3% margin) · team</td></tr></table>
<table align="left"><tr><th align="left">✅ 3-0 · 29-08 15:30 <b>Leipzig v Mönchengladbach</b></th><th align="left">Tip 1</th><th align="left">Tip 2</th></tr><tr><td>Bundesliga (84.0 −0.9)</td><td>O1.5 85.9% +3.2%<br>buy≥1.23 (+5.5% margin)</td><td>✅ <b>Leipzig O1.5</b> 65.8% +15.3%<br>buy≥1.41 (−7.0% margin) · team</td></tr></table>

<br clear="all">

### 🟡 Actual placed bets

**Settled: 39 / 46  ·  ROI +6.1%  ·  flat stakes** — settled through real settlement fractions by the ledger; a push or half-win counts as a hit, a half-loss does not. Notes travel with the bet in `config/bets.tsv`.

| Result | Fixture | Lane | Odds | Return | Note |
|---|---|---|---|---|---|
| ✅ | Clermont v Sochaux | U1.5 (away) | 1.41 | 1.41x | widened: team under on the away side |
| ◦ | Montpellier v Boulogne | DNB (home) | 1.26 | 1.00x | rule 5 shape, home favourite |
| ✅ | Laval v Grenoble | U3.5 | 1.24 | 1.24x | rule-6 harder line off the U3.0 tip (79.9%) |
| ✅ | Den Bosch v Vitesse | X2 (away) | 1.46 | 1.46x | own read: double chance Vitesse or draw |
| ✅ | Volendam v Dordrecht | O1.5 (home) | 1.62 | 1.62x | widened: Volendam team over |
| ✅ | Volendam v Dordrecht | O1.5 | 1.18 | 1.18x | softer companion to the O1.5 tip |
| ✅ | Jong AZ v MVV | U4.5 | 1.28 | 1.28x | softer line above the U4.25 tip |
| ✅ | Roda v NAC Breda | U4.5 | 1.20 | 1.20x | softer line above the U4.25 tip |
| ✅ | TOP Oss v Jong FC Utrecht | U4.5 | 1.22 | 1.22x | softer line above the U4.25 tip |
| ❌ | Groningen v Fortuna Sittard | U4.5 | 1.24 | 0.00x | softer line above the U4.25 tip (+5.8%) |
| ◦ | Legia v Śląsk | DNB (home) | 1.17 | 1.00x | rule 5 shape on the sub-bar fixture |
| ✅ | Milan v Venezia | U3.5 | 1.37 | 1.37x | softer line above the U3.0 tip (+5.2%) |
| ❌ | Al-Riyadh v Neom | DNB (away) | 1.42 | 0.00x | rule 5 shape, away side |
| ◦ | Dalian Yingbo v Guoan | DNB (away) | 1.53 | 1.00x | rule 5 shape, away side |
| ✅ | Shenhua v Taishan | O0.5 (away) | 1.18 | 1.18x | = Tip 2 team lane (81.8% +9.2%, buy≥1.32 — taken under it, widened) |
| ✅ | Shenzhen v Port | U4.5 | 1.20 | 1.20x | softer line above the U4.25 tip (+5.3%) |
| ✅ | Braunschweig v Hertha | O1.5 | 1.19 | 1.19x | = Tip 1 rung (83.5% +2.9%, buy≥ not met — widened) |
| ❌ | Bochum v Osnabrück | DNB (home) | 1.25 | 0.00x | rule 5 shape, home favourite |
| ✅ | Gençlerbirliği v Erzurumspor | U1.5 (away) | 1.36 | 1.36x | widened: away team under beside the U4.25 tip (89.5%) |
| ✅ | Genk v Beveren | U4.5 | 1.20 | 1.20x | softer line above the U4.25 tip (86.7% +1.3%) |
| ✅ | Crystal Palace v Man City | O1.5 (away) | 1.61 | 1.61x | widened: Man City team over |
| ✅ | Crystal Palace v Man City | O1.5 | 1.20 | 1.20x | = Tip 1 rung (84.3% +4.5%), under buy≥ — widened |
| ❌ | Comerciantes Unidos v Cajamarca | DNB (home) | 1.24 | 0.00x | rule 5 shape, home side of the O1.5 tip |
| ❌ | Jong Ajax v Helmond Sport | U4.5 | 1.21 | 0.00x | R6 softer line above the U4.25 tip (84.1% +4.4%, buy≥1.29) |
| ✅ | Lille v PSG | O1.5 | 1.22 | 1.22x | = Tip 1 rung (80.1% +2.8%), under buy≥1.31 — widened |
| ✅ | Náutico v Athletic | U3.5 | 1.20 | 1.20x | R6 softer line above the U3.0 tip (83.1% +1.4%, buy≥1.32) |
| ✅ | Gamba Osaka v Sanfrecce | U4.5 | 1.19 | 1.19x | R6 softer above the U4.25 tip |
| ✅ | Mito Hollyhock v Machida Zelvia | X2 | 1.18 | 1.18x | Machida or draw — own read on the promoted host |
| ✅ | Qingdao Hainiu v Wuhan Three Towns | O0.5 (away) | 1.18 | 1.18x | Three Towns to score — team lane own read |
| ❌ | Kawasaki v Chiba | U4.5 | 1.18 | 0.00x | R6 softer above the U4.25 tip (90.0% +1.7%) |
| ✅ | Nagoya v Okayama | O1.5 | 1.35 | 1.35x | own read against the sub-bar U-tip |
| ✅ | Tokyo Verdy v Kashima | O0.5 (away) | 1.18 | 1.18x | Kashima to score — team lane own read |
| ✅ | Heidenheim v Dresden | 12 | 1.23 | 1.23x | either side outright — own read DC |
| ❌ | Cottbus v Fürth | U4.5 | 1.23 | 0.00x | R6 softer above the U4.25 tip (+3.9%) |
| ✅ | Zhejiang v Yunnan Yukun | 12 | 1.21 | 1.21x | either side outright — own read DC |
| ✅ | Liverpool v Nottingham Forest | O1.5 | 1.18 | 1.18x | = Tip 1 rung (84.2% +4.4%), under buy≥1.28 — widened |
| ✅ | Middlesbrough v West Brom | O1.5 | 1.24 | 1.24x | = Tip 1 rung (76.5% +3.8%), under buy≥1.39 — widened |
| ✅ | Häcken v Västerås SK | U4.5 | 1.30 | 1.30x | R6 softer above the U4.25 tip |
| ✅ | Köln v Hoffenheim | O0.5 (home) | 1.20 | 1.20x | Köln to score — team lane own read |
| ◦ | Mainz v Paderborn | DNB (home) | 1.26 | 1.00x | rule 5 shape, Mainz side |
| ✅ | Leipzig v Mönchengladbach | O1.5 (home) | 1.42 | 1.42x | Leipzig team over — own read |
| ✅ | Elversberg v Leverkusen | O1.5 (away) | 1.42 | 1.42x | Leverkusen team over — own read |
| — open | Bournemouth v Everton | O1.5 | 1.24 | — | vs the board's lane — widened |
| — open | Coventry v Hull | 1X | 1.20 | — | Coventry or draw — own read DC |
| — open | Blackburn v QPR | U3.5 | 1.26 | — | R6 off the U-tip |
| ✅ | Bristol City v Portsmouth | O1.5 | 1.30 | 1.30x | vs the board's lane — widened |
| — open | Charlton v Preston | U3.5 | 1.22 | — | R6 off the U-tip |
| ✅ | Norwich v Burnley | O1.5 | 1.24 | 1.24x | vs the board's lane — widened |
| — open | Watford v West Ham | O1.5 (away) | 1.67 | — | West Ham team over — own read, the slip's big price |
| ✅ | La Louvière v Mechelen | O0.5 (home) | 1.30 | 1.30x | La Louvière to score — team lane own read |
| — open | Excelsior v Sparta Rotterdam | U4.5 | 1.21 | — | R6 softer off the U-tip |
| ✅ | Arouca v Marítimo | O1.5 | 1.34 | 1.34x | vs the board's lane — widened |
| — open | Levante v Real Betis | O1.5 | 1.25 | — | vs the board's lane — widened |
| — open | Strasbourg v Lens | O1.5 | 1.19 | — | vs the board's lane — widened |
| — open | Halmstad v Kalmar | U3.5 | 1.32 | — | R6 off the U-tip |
| — open | Lausanne v Zürich | O1.5 (home) | 1.62 | — | Lausanne team over — own read |
| — open | Servette v Luzern | 1X | 1.25 | — | Servette or draw — own read DC |
| — open | Al-Kholood v Al-Ahli | O1.5 | 1.19 | — | vs the board's lane — widened |
| — open | Tottenham v Newcastle | O0.5 (away) | 1.25 | — | Newcastle to score — team lane own read |
| — open | Monza v Udinese | X2 | 1.36 | — | Udinese or draw — own read DC |
| — open | AZ v Go Ahead Eagles | O2.5 | 1.35 | — | harder line off the O-tip |
| — open | Real Sociedad v Espanyol | O1.5 | 1.24 | — | vs the board's lane — widened |
| — open | Al-Fateh v Al-Ittihad | U4.5 | 1.23 | — | R6 softer off the U-tip |
| — open | Nürnberg v Bielefeld | O1.5 | 1.20 | — | vs the board's lane — widened |
| — open | Galatasaray v Göztepe | U4.5 | 1.21 | — | R6 softer off the U-tip |
| — open | Lyon v Le Havre | O1.5 | 1.20 | — | vs the board's lane — widened |
| — open | Juventus v Parma | U3.5 | 1.44 | — | R6 off the U-tip |
| — open | Cercle Brugge v Lommel | O1.5 (home) | 1.44 | — | Cercle team over — own read |

<!-- HYPOTHESES:START -->

## The ledger of everything tried

Every feature suggestion and hypothesis put through the bar — 28 verified, 14 unfinished, 22 declined. Typed in `config/hypotheses.tsv`; this table and the app's Patches page both render from it, so they cannot disagree.

### 🟢 Verified and helping — 28

Cleared two separate time windows and is live in the engine today.

| | Date | Area | Hypothesis | Verdict |
|---|---|---|---|---|
| 🟢 | 08-29 | engine | **Team-lane side attribution fails on regime breaks — the bettor's hypothesis from the Yukun/Forest cards: recent staff and squad churn shows up as a form streak the engine mistakes for quality** | Measured on 8,404 replayed team-lane offers across 52 leagues (scripts/team_attrib.py): ALIGNED divergence — the last-8 scoring stub vs the long record, signed toward the rung — is monotone with overclaim: deflated +3.3, stable +1.2, mild inflation −1.7, strong (0.8+) −3.2 with both half-windows negative (−3.9/−2.5). The proposed data inputs measured differently: DEPTH of record (thin history) showed no consistent effect and shipped nothing, and league tables would have argued FOR the Yukun pick ("chasing the top"). STREAK_FROM 0.35 / STREAK_SLOPE 0.05 ships on the published number; treated region closes to +0.4/−1.0 per half; ~2.7% of offers stop printing. One-sided as always — deflated reads keep their honest underclaim. Honest limit: the debit fixes the population, not every anecdote — Yukun's own offer drew only 1.9 points because its edge came mostly from Zhejiang's base rate |
| 🟢 | 08-28 | engine | **The Playable-hit column's collapse leagues — the bettor spotted IRL-PD 83.2 falling to 68.6 on 35 lanes, CRO-1L and MEX-LMX similar** | Confirmed and diagnosed on a 29,763-tip current-engine replay (retrosim --dump). Board-wide the playable layer is honest — says 82.3, hit 82.1, gap −0.1 on 15,813 lanes — and no playable-bar height beats +1% (raising it LOWERS hit in both halves), so the bar stands. The collapse is seven specific leagues (CRO-1L, MEX-LMX, IRL-PD, MAR-BP, COL-PA, ESP-L2, ITA-SA) whose positive-edge lanes overclaim >4 points in BOTH half-windows — pooled −6.4/−7.5 — flat across edge size, while their consensus lanes underclaim +3.4: there, disagreeing with the league consensus IS the model's error. The implied proportional debit (k≈2.0/2.3) empties their playable set anyway, so the honest ship is the CONSENSUS CAP: published probability capped at the market's league-baseline chance, edge can never exceed zero, no lane there ever badges playable. Board playable hit rises 82.1 → 82.6 (82.4→82.9 / 81.8→82.3 per half). Selection untouched, as always |
| 🟢 | 08-28 | engine | **A tip claiming far more than its league's proven base rate is bragging — the bettor's hypothesis, confirmed one-sided** | Measured on 32,493 replayed tips against each league's own hit rate: within ±2 points the claims are honest (gap −0.1); past +2 every extra claimed point delivers ~−1.2 back (slope −1.27/−1.04, both windows), and the wide-high band ran 5.6 hot in BOTH halves — while wide-LOW tips deliver their claims or better, so only one side is diseased. Filtering would lift playable hit just 86.4 → 86.7 (the braggarts still hit ~84); the real leak was the CLAIM, so REL_SAYS_DEBIT ships instead: 1.1 × the excess above league+2, published number only, selection raw. Every band closes to ±1.1 in both windows |
| 🟢 | 08-27 | board | **Proof in hindsight: the losing era was volume, not the engine's ceiling** | The pre-calibration weekend — 69.9% across 73 bets taken 'just going with it', ROI −10.1% — replayed through today's engine grades 84.2% taking every Tip 1 (avg buy≥ 1.30) and 83.3% at the playable bar. On the first calibrated slate the +1% bar filters 63 lanes to 42 and hits 95.0%: selection keeps the best winners. In-sample caveat attached — these weeks are the windows the constants were tuned on, so this is a ceiling estimate; the live confirmed number (84.6% on tips published before kickoff) landing in the same neighbourhood is the corroboration |
| 🟢 | 08-27 | engine | **The weakest leagues can be lifted the way the cups were — but only under an ROI constraint** | Raised floors climb selection to safer rungs and lift hitrate in both windows everywhere — but the first pass measured only hitrate, and GRE-SL's 80.9 → 91.7 turned out to cost 66% → 6% of PLAYABLE volume (buy≥ 1.37 → 1.19): safe rungs nobody can buy edge on. Final rule: highest floor keeping ≥2/3 of playable volume. Six leagues ship 0.82, one 0.80, seven 0.78; GRE-SL and PER-L1 revert; MLS (worse at any floor) and ITA-SB (nothing to climb to) never shipped |
| 🟢 | 08-27 | engine | **Above says 90% the domestic board is too sure of itself** | Measured on 32,493 tips across 45 leagues: below 90% the gap is −0.2 (honest); at 90%+ it is −1.2 ± 0.3, holds in both windows (−1.0 / −1.5) and is STRONGER in the played population (−1.8 / −1.4). Flat inside the band, so HIGH_SAYS_DEBIT 0.012 ships on the published number — selection stays raw |
| 🟢 | 08-27 | board | **Per-league gaps at n=200 are mostly noise wearing a number** | The worst sixteen rows, re-measured at ~780 each: ROU-L1 −11.6 → −2.8, and every one of them collapsed into a band of −2.8 to +1.7 around a pooled −1.7. One row in the whole table cleared two standard errors. The table now replays up to 800 fixtures capped at two seasons |
| 🟢 | 08-27 | interface | **One update is never one match** | The sweep checks every unsettled fixture against ESPN, grades on the 90 with extra time peeled off, and refuses to leave neighbours stale — the human failure it replaces was updating one score and leaving three settled bets showing as open |
| 🟢 | 08-27 | interface | **ESPN files by the competition's local day** | A Chilean 18:00 sits on our previous date, so the sweep asked for one day and was told nothing was there; neighbouring days are now merged, which covers the Americas below us and Asia above |
| 🟢 | 08-27 | interface | **Club identity across sources** | NFD leaves ø, đ, ł and ß whole (Lillestrøm was not Lillestrom), and a translated name shares no letters with its original (Red Star Belgrade is Crvena zvezda) — folded and aliased through the nickname file |
| 🟢 | 08-27 | board | **The +1% edge bar separates rather than thins** | This run's settled Tip 1: 75.0% held (says 82.7, gap −7.7) · 86.7% at +1–3% · 100% at +3%+. The withheld band is the only one grading BELOW its own stated probability |
| 🟢 | 08-26 | interface | **The renderer must verify itself** | Every fixture on every surface, tallies matching, ledger complete, generated script parsing — a mismatch refuses to finish. Written after a syntax error blanked every page while the file still looked fine |
| 🟢 | 08-26 | interface | **A live lane should say what the score did to it** | Landed, needs N more, room for N, half gone — read from the ledger's own settlement so a push is never called a loss |
| 🟢 | 08-26 | cups | **The cup mu is blind to WHICH side is strong** | It priced from \|elo gap\|, symmetric, so a strong home side and a strong away side got the same number. B3 is the strongest term the cup lane has produced (t 4.95) and improves both windows live: 83.3 → 84.3 |
| 🟢 | 08-26 | cups | **Cup over-tips are systematically hot** | Measured 3.5 points hot in both seasons and flat across bands, so a flat debit is the right shape — OVER_SAYS_DEBIT ships, windows close at +0.0 / −0.1 |
| 🟢 | 08-26 | cups | **Hitrate-first selection beats edge-first on cups** | A probability floor of 0.82 on all six cup codes lifts ~80 → ~83 at zero volume cost |
| 🟢 | 08-26 | ledger | **A cashed-out position settles at cashout, not at the result** | The column existed and was not being read; a position cashed at stake settles 1.00x immediately |
| 🟢 | 08-25 | rules | **Rule 5, DNB confluence** | Backtested on 6,354 pointed pairs: the pointed-team lane is real at 78%, and the home+strong rung avoids defeat 83.9% — leg 4 stops being judgement and becomes a number |
| 🟢 | 08-25 | rules | **Rule 6, the line-translation ladder** | Softer line first, printed line second, harder half-line only at buy≥ +0.07–0.10 |
| 🟢 | 08-25 | engine | **Team lanes should see the opponent's defense** | DEFENSE_BLEND 0.5 — worth +7 to +11 points on team lanes |
| 🟢 | 08-25 | engine | **Top-six clashes are priced too high** | BIG_MATCH_DEBIT 0.15 goals, validated on both windows |
| 🟢 | 08-25 | data | **A league can be launched same-day** | NED-D2 taken from check data → calibrate → futurematch in one sitting, and went 4-for-4 on its first slate |
| 🟢 | 08-24 | engine | **A promoted club can be priced from its lower-division history** | Cross-division fallback at measured exchange rates (×0.754 scored, ×1.516 conceded), validated on 608 historical tips |
| 🟢 | 08-24 | engine | **The published buy≥ thresholds were all too low** | Re-derived from settlement rather than from stated probability, with the winner's-curse haircut folded into buy_from — hand rules became code |
| 🟢 | 08-24 | engine | **The team lane was split by venue and four checks missed it** | Found and fixed; the "side split" it appeared to leave behind was then measured and is not real |
| 🟢 | 08-24 | engine | **mu is over-spread** | The founding recalibration: MU_SHRINK to 0.60 with the floor moved to match, the single largest accuracy change the engine has had |
| 🟢 | 08-24 | engine | **The weakest sides are rated too low** | TEAM_RATE_FLOOR 0.95 |
| 🟢 | 08-27 | engine | **IRL-PD and MAR-BP price on a mu that carries no information** | At n≈800 IRL-PD's residual slope is −2.8 (t −3.0) and NEGATIVE IN BOTH WINDOWS — mu is anti-correlated with outcomes there; MAR-BP reads −0.8 (t −2.2). Their hitrates survive because the league mean carries them, but the stated probabilities pretend knowledge. RESOLVED 28 Aug by the consensus cap: both are in CONSENSUS_CAP_LEAGUES, so their published numbers never claim more than the league baseline and no lane there badges playable — the cull happened at the claim layer, and the tips stay on the board honestly |

### 🟠 Unfinished — 14

Measured but not concluded, or shipped on **probation** and still waiting on live results.

| | Date | Area | Hypothesis | Verdict |
|---|---|---|---|---|
| 🟠 | 08-29 | engine | **Tip 3 — the result lane (1X/X2/12, sharpening to DNB when one side dominates), the bettor's proposal, SHIPPED ON PROBATION** | Measured first on 15,048 fixtures across 52 leagues (scripts/result_lanes.py): the per-side expectations through two Poissons price the result market honestly once the home side is tilted 1.10 (raw home wins ran +3.2 hot in both halves; tilted, all six outcome gaps close within 1.4). The feared Poisson draw bias never appears — TEAM_SHRINK compresses the sides and pumps the draw by roughly what Poisson drops it. The strongest-DC band that supplies three quarters of prints grades +0.1/−0.5; every stronger band and every DNB band UNDERCLAIMS (+1.3 to +8.5) — the lane's failure mode is modesty. Prints from DC_FLOOR 0.72 with edge ≥2 over the league's as-of base rate; DNB upgrade at 0.65 of the no-draw mass. Outside every tally and the hero until live results earn it in, exactly the cup lane's probation path |
| 🟠 | 08-29 | engine | **A push should count as a win at the ROOT, not just in the tallies — the bettor's rule: the standing offset plays U3.0 as U3.5, so the actual play wins where the printed rung pushes** | The board's counters, playable tallies and hero window now grade ◦ as a hit (shipped 29 Aug). The deeper change — hit_weight("push")=1.0, which would fold push totals into winning_totals and hence into the PUBLISHED probabilities and selection ties — is a real engine change: U3.0 would collapse into the U3.25/U3.5 tier, tips would reshuffle, and every baseline (retrosim table, REL bases, floors) needs re-derivation on two windows before it ships. Until then the claims exclude the push and the tally credits it, which overstates hit vs says by the push rate (~1-2 points on U3.0-heavy lanes) — known, accepted, and queued |
| 🟠 | 08-28 | engine | **The buy≥ gate prices REL-debited and capped lanes from the raw engine number** | _undebited() reverses the cup-over and high-says debits but not REL or the consensus cap, so a debited lane's printed buy≥ sits a few percent below its honest break-even. Capped lanes all land sub-bar where nothing is bought, and REL rarely bites hard on board lanes — small, but the gate run backwards should someday reverse everything the gate applies |
| 🟠 | 08-28 | ledger | **The widened staking strategy — taking lanes under buy≥ when probability still clears** | The bettor's own call, now instrumented: 18 positions logged on the Session #4 slate, 8 on engine-priced rungs of which 1 sits above buy≥, 3 between break-even and buy≥, and 4 marginally under stated break-even (−0.01 to −0.04). The played-lane record historically grades 1–2 points ABOVE its stated probability, which covers most of that shortfall — but not with a margin. The ledger measures it live: alignment column plus stored odds settle the question in a week of results. Systematized 28 Aug as the buy-blend (BUY_BLEND_BELOW/ABOVE): the printed buy≥ itself now reaches toward the league's playable record and prints the margin it gave up, so the widened strategy is priced on the card instead of eyeballed against it |
| 🟠 | 08-27 | data | **Official tables can diverge from results tables (points deductions)** | CSL 2026: Shenhua 28 pts official vs 35 by results, Taishan 33 vs 39, Guoan 36 vs 40 — every stored result verified correct against ESPN, so the gap is federation deductions the walked table cannot see. Reads now say 'by results'; no engine impact (mu never reads standings, and the pos_signed term was measured and declined). Open question: whether any consumer ever NEEDS the official table |
| 🟠 | 08-27 | cups | **The playoff DECIDER round may run hotter than the lane prices** | Live probation evidence from the 27 Aug slate: U4.25 tips stating 87–91 covered 20/28 (71%) across the run's cup fixtures — six 5+ goal matches in one night — while our O1.5 tips landed at their stated rate (calibrated, not underrated). One knockout round, ~2 SE, rhymes with the qualifiers' standing −3.2/−3.5 gaps. Untested: whether elimination-night second legs are structurally hotter than earlier rounds. Measure before the next playoff window |
| 🟠 | 08-27 | engine | **Balance means hit × buy≥, and five leagues fail it** | EV at the printed buy-from across the table runs 1.02–1.15. The extremes people worry about are HONEST — ITA-SB (77.2 hit, 1.46 odds) sits near the top at 1.127, JPN-J1 (88.6, 1.20) clears 1.063 — but the overconfident rows cannot cover their own price: MLS 1.017, ESP-L2 1.025, MEX-LMX 1.027, UEL-Q 1.033, FRA-L2 1.055. Their fix is per-league calibration or a cull, not floor tuning. ESP-L2 and MEX-LMX resolved 28 Aug by the consensus cap (no playable lanes to misprice); MLS, UEL-Q and FRA-L2 still pending |
| 🟠 | 08-27 | engine | **TUR-SL and ESP-L2 are overconfident in their RECENT two seasons** | The only two rows that survive the noise-aware re-measure: TUR-SL −5.2 (n 391), ESP-L2 −4.3 (n 525) — both read fine over a longer trail (−2.6 / −2.8 at n≈740), so the miss is concentrated in the current window. Cause not yet diagnosed |
| 🟠 | 08-27 | cups | **The whole Club Elo cup lane** | PROBATIONARY. 92% coverage, all windows same-signed, a 202-fixture dress rehearsal at 89.1% and a pooled gap of +0.1 after B3 — but cups have failed this project twice before, so it stays labelled until a full slate of live results confirms it |
| 🟠 | 08-26 | cups | **Level-aggregate second legs are decided at home, not through goals** | Profiled on 645 ties: goals ordinary (2.74 v 2.67 usual) but the home side wins 49.1% and 21.7% go to extra time. Real, and printed as context beside the tip — it is not yet a term in the mu |
| 🟠 | 08-26 | cups | **First-leg caution has faded** | Second legs run 2.81 goals against first legs' 2.59 and hold both windows, but the caution effect is gone (older −0.19, t −4.28; newer +0.02). Recorded, not acted on |
| 🟠 | 08-25 | engine | **The big-match effect has a season-phase structure** | The structure is real and measured; no constant has been cut from it yet |
| 🟠 | 08-27 | cups | **UEFA country coefficients as a cup strength source** | Proposed and never measured — Club Elo was taken instead because it prices CLUBS directly rather than their federations. Still open if the Elo lane fails probation |
| 🟠 | 08-27 | data | **ALG-L1 has no ESPN coverage** | Every slug returns 400, so Algerian fixtures cannot be swept and must be graded by hand. No second provider wired in yet |

### 🔴 Declined — 22

Tested and rejected, with the number that killed it. Kept deliberately — a dead idea that stays written down does not get re-proposed.

| | Date | Area | Hypothesis | Verdict |
|---|---|---|---|---|
| 🔴 | 08-26 | engine | **Domestic context: the mismatch term** | The fourth "signal without edge" in a day. Clears the residual bar outright (+0.241 ± 0.063, t 3.81, both windows) and still LOSES hitrate when converted to tips: 86.4 → 85.9 and 86.4 → 85.7 |
| 🔴 | 08-26 | engine | **Domestic context: league table position** | +0.103 ± 0.035, t 2.94 — but windows t 2.42 / 1.75, one window short |
| 🔴 | 08-26 | engine | **Domestic context: three-season stature** | "Barcelona is always top three" as a number. t 2.06 pooled, windows 1.17 / 1.79 — fails the bar |
| 🔴 | 08-26 | engine | **Domestic context: which side is better (signed PPG)** | The domestic echo of the cup fix. t 2.40 pooled, windows 1.92 / 1.46 — the hole B3 filled in cups does not exist here, because domestic mu is already built from each side's own rate at its own venue |
| 🔴 | 08-26 | engine | **Domestic context: the reverse fixture** | t 1.32. Nothing |
| 🔴 | 08-26 | engine | **MU_SHRINK is over-correcting the extremes** | Refuted directly. If it were, the residual would concentrate where mu sits far from the league mean; instead it is strongest in the MIDDLE (near +0.116, middling +0.358, far +0.251) |
| 🔴 | 08-26 | cups | **The aggregate score should move the mu** | The apparent lead effect was the strength gap that created the lead — against the engine's own mu the aggregate adds nothing (t ≈ 1) |
| 🔴 | 08-26 | cups | **First and second legs should be priced differently** | Profiled across 5,341 legs. Against the engine's own mu neither leg is detectable, so no leg term |
| 🔴 | 08-26 | cups | **Elo plus domestic form** | Goal-tempo terms carry real signal (t 2.6, 2.1) but no window improves when graded; results-based form and Elo momentum are flat outright |
| 🔴 | 08-25 | cups | **A Swiss-era club composite** | The closest any pre-Elo cup model came, and still short: −0.6 / −4.0 |
| 🔴 | 08-25 | cups | **League bridge ratings** | Real ratings, genuinely informative, and one window short — the recurring cup failure signature |
| 🔴 | 08-25 | cups | **Domestic form carries cup signal** | Slope 0.017. Zero |
| 🔴 | 08-25 | cups | **The pointed gate can rescue cups** | It helps in BOTH directions (+0.7 vs −2.3 forward, −3.4 vs −4.6 reverse) and still fails, because the reverse window's miss is a LEVEL error and no selection rule fixes a mis-levelled mu |
| 🔴 | 08-25 | cups | **Tightening cup EDGE improves cup tips** | It hurts — the winner's curse. High printed edge often means the mu is wrong, not that the price is generous. Tightening PROBABILITY is what worked |
| 🔴 | 08-25 | rules | **The O0.5 tag on a pointed team** | The trap in Rule 5: it looks like the safest rung on the board and grades worst |
| 🔴 | 08-24 | engine | **VENUE_BLEND is a live knob** | Swept for the first time and closed: dead at 0.35 |
| 🔴 | 08-24 | engine | **Defense belongs in the MATCH mu too** | ~+1 point, and it double-counts what the rates already carry |
| 🔴 | 08-24 | engine | **Season restart is an effect of its own** | It was a symptom of the over-spread mu; once MU_SHRINK landed, it disappeared |
| 🔴 | 08-24 | engine | **The rung defect is a Poisson shape error** | The sixth hypothesis for that defect, and dead like the five before it |
| 🔴 | 08-24 | engine | **The freshness gate should be made binding** | Staleness costs about a point; making the gate binding costs more than that in withheld fixtures |
| 🔴 | 08-24 | engine | **A relegation variant of the big-match debit** | Measured alongside the top-six version and declined |
| 🔴 | 08-23 | cups | **The cup family as originally built** | Measured at −11.4 and taken off the board entirely — the failure that started the whole cup investigation |

<!-- HYPOTHESES:END -->

## Engine state — 28 Aug 2026

Every constant below is checked against the live code on each render
(`scripts/board.py verify`), so this block cannot drift from the engine it
describes.

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
    DEFENSE_BLEND          0.50        the team lane finally sees the opponent's
                                       defense; worth +7 to +11 points there.
                                       Measured for the MATCH mu too and
                                       declined — ~+1 point, and it double-counts
    VENUE_BLEND            0.35        swept for the first time and CLOSED: a
                                       validated dead knob, kept at its value
                                       so the sweep is not repeated
    MIN_WIN_PROB           0.75        probability floor, re-tuned with the shrink
    RESULT_TILT            1.10        Tip 3, the result lane (PROBATION):
    DC_FLOOR               0.72        the per-side expectations priced into
    DNB_FROM               0.65        1X/X2/12 through two Poissons, home
                                       tilted 1.10 (raw H ran +3.2 hot in
                                       both halves; tilted, all six outcome
                                       gaps close within 1.4). Strongest DC
                                       prints from 0.72, sharpens to DNB when
                                       one side holds 0.65 of the no-draw
                                       mass. Measured on 15,048 fixtures —
                                       never overclaims, and stays OUT of
                                       every tally until live results earn
                                       it in, exactly as the cup lane did
    STREAK_FROM            0.35        the team-lane streak debit: when the
    STREAK_SLOPE           0.05        credited side's last-8 scoring stub
                                       diverges from its long record in the
                                       direction that flatters the read (hot
                                       under an over, cold under an under),
                                       the published number is debited 0.05
                                       per goal of excess above 0.35 —
                                       measured monotone on 8,404 offers,
                                       −3.2 in the top band in BOTH halves;
                                       depth of record measured dead. The
                                       bettor's regime-break hypothesis
    BUY_BLEND_BELOW        0.4         the buy≥ price is computed on a BLEND of
    BUY_BLEND_ABOVE        0.8         the tip and its league's playable record
                                       (the bettor's rule, 28 Aug): below the
                                       record the tip carries 0.4 and the league
                                       0.6 — lower-probability lanes become
                                       reachable at real-world odds; above it
                                       the tip carries 0.8 — the easy lanes are
                                       asked a little extra. Every card prints
                                       the margin its price still holds over the
                                       tip's own break-even, negative included
    HIGH_SAYS_DEBIT        0.012       above says 90% the domestic board reads a
                                       measured 1.2 points hot (32,493 tips, both
                                       windows, stronger in the played set) — the
                                       PUBLISHED probability, edge and buy≥ carry
                                       the debit; selection stays raw
    HIGH_SAYS_FROM         0.90        where that band starts

The cup lane — **probationary**, and priced from Club Elo rather than from
form, because domestic form carries no cup signal at all (slope 0.017):

    B1                     0.161       |elo gap| — how lopsided the tie is
    B2                     0.017       elo sum — how good both clubs are
    B3                     0.101       SIGNED gap: WHICH side is the stronger
                                       one. The mu used |gap| alone and so
                                       priced a strong home side exactly like a
                                       strong away side; strongest cup term yet
                                       (t 4.95), and both windows improve
    B0_FALLBACK           −0.747       pooled intercept, used until 100 trailing
                                       rows exist; then walked monthly
    OVER_SAYS_DEBIT        0.035       cup over-tips measured 3.5 points hot in
                                       both seasons and flat across bands
    MAX_STALE_DAYS         400         no tip when a club's Elo is older than
                                       this — an abstention, not a guess
    REL_SAYS_FROM          0.02        the relative-overreach debit: a claim
    REL_SAYS_SLOPE         1.1         more than 2 points above the league's
                                       own base rate is pulled back at 1.1 per
                                       excess point — wide-high claims ran 5.6
                                       hot in both windows; published number
                                       only, selection stays raw

The consensus cap — in seven leagues the model's disagreement with the
league consensus IS the error (positive-edge lanes read a flat 6–7 points
hot in both half-windows, pooled −6.4 / −7.5 on the 29,763-tip current-
engine replay, while their consensus lanes underclaim +3.4), so the
published probability there is capped at the market's league-baseline
chance and no lane can ever badge playable:

    CONSENSUS_CAP_LEAGUES  COL-PA, CRO-1L, ESP-L2, IRL-PD, ITA-SA, MAR-BP, MEX-LMX
    min_win_prob           0.82        on all six cup codes (~80 → ~83 at zero
                                       volume cost) and, since 27 Aug, raised
                                       per-league on fourteen weak domestic
                                       leagues (six at 0.82, one at 0.80, seven
                                       at 0.78) under an ROI constraint: the
                                       highest floor keeping ≥2/3 of PLAYABLE
                                       volume. GRE-SL and PER-L1 reverted — a
                                       lifted hitrate on rungs nobody can buy
                                       edge on is a mirage — and MLS / ITA-SB
                                       never shipped (the floor cannot help
                                       them)

    weighted calibration gap    -0.6 in-sample, -1.5 out-of-sample  (was -4.4)
    realised edge over base     +2.23                               (was +1.35)
    top market share            41%                                 (was 54%)

All 37 tippable leagues retrosimmed. The former cull candidates (`IRL-PD`,
`COL-PA`, `MAR-BP`) were resolved on 28 Aug by the consensus cap above —
culled at the claim layer, tips still on the board honestly. `MLS` (-5.2
out of sample) remains the one open calibration question.


### Recalibration — archived

The full recalibration record, every diagnostic, and the complete session
logs live untouched in [archive/](archive/) — pre-calibration
(20–23 Aug), the first calibrated slate (23–24 Aug), and the cup run
(24–27 Aug). Nothing in an archive is ever edited.


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

## The three operating commands

Each command has a contract: what it reads, what it must write, and the
verify that refuses a half-done job. Nothing on any surface is ever
updated by hand.

**1 · Update fixtures** — `python scripts/sweep.py`
Fetch every unsettled fixture's state from ESPN (all of them, never one —
hard-locked). In play → `LIVE 63' 2-1` with per-lane room/landed states;
in extra time → settled NOW on the 90-minute score from the goal
narration; finished → Tip 1 graded into the status, Tip 2 into its own
cell. Writes `config/fixtures.tsv`, then renders README and app — where
found bets, ROI and both hitrate tiles are DERIVED from the ledger and
the graded rows, so settling a fixture settles its bets everywhere at
once. Ends in `board.verify()`: every fixture on every surface, tallies
matching, no row four hours stale.

**2 · Futurematch** — `python scripts/futurematch.py slate.tsv | --reprice`
Run Athena on new fixtures and put them on the board (an abstention is
added too, with its reason in the tip cell), or re-price every pending
row after an engine change — board rows are typed at slate time and do
not move by themselves. Writes the rows, renders both surfaces, verifies.

**3 · Calibrations and tests** — the instruments in `backend/scripts/`
A measurement writes nothing but its verdict (docstring, patch note,
hypotheses ledger). A SHIPPED change must be followed by, in order:
`retrosim --write` for the affected leagues (refreshes
`league_hitrates.tsv` — the badges, the Retrosim page and the REL
debit's base rates), then `futurematch.py --reprice` (pending cards onto
the new engine), then the render. The Engine state block is checked
against the live constants on every render, so a shipped constant that
skips the README fails the build.

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

