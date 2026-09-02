# ARCHIVE — Sessions #4 and #5 · 28 Aug – 1 Sep 2026

Session #4 was the first full run on the calibrated floors (28–31 Aug).
Session #5 was the two build days that followed on the same board (1–2 Sep):
the odds layer, the guard, the decline rule, and the diagnostics pass that
found seven missing seasons. Frozen together because they share one board.
`log.md` is the full README exactly as it last rendered, `fixtures.tsv` and
`bets.tsv` are the raw rows.

Two things deliberately NOT frozen here, at the bettor's instruction: the
Londrina v Juventude U3.5 at 1.17 (2 Sep 00:30, won 0-0) stays on the live
book as the first row of Session #6, and every open position carries over.

## Final numbers

    lane                        Tip 1              Tip 2
    all matches            216 / 267  80.9%    135 / 196  68.9%
    played lanes  >+1%     103 / 132  78.0%
    placed bets            113 / 143  79.0%    ROI −0.3%

## What these sessions built

- **The odds layer.** Live bookmaker prices from a 26-book feed, the card
  showing what the market pays rather than what the engine wants, and the
  line that reaches the slip quoted (`U3.0` printed, `U3.5` struck).
- **The guard.** Every starred lane carries one of five labels — super green
  to super red — from the card's tier and a confluence score built from the
  board's own searches, strictly as-of. Frozen thresholds, validated on two
  windows over 62,528 replayed picks.
- **The decline rule.** Each label implies a break-even; the card says PLAY
  only when the best quote clears it by 6%, and never on a red tier. The
  first rule in the project's history with a positive return at real
  prices — and the measurement that showed the return lives in the panel:
  +1.71% best-of-ten-books, +0.62% at one book, −0.69% at the average.
- **STRONG.** A play whose confluence score sits in the top quartile in
  Europe: 81.0% and +8.87% on the 1,008 bets the bar fires on, against
  72.8% and −0.67% for the rest.
- **The name fold.** One club, one name, store-wide: 552 folds, hidden rows
  from 22% to 0.8%, primary chosen by latest-played so a board name never
  resolves to a dead spelling. A correctness fix, measured as such.
- **Seven seasons the store never had.** The diagnostics pass found the git
  source had abandoned FRA-L2, ESP-L2, GER-B2 and TUR-SL 2025-26 in November
  and never published SUI-SL, SWE-AL or NOR-EL. 1,921 results filled from
  football-data; forty cards re-priced; both Swiss unders on the book turned
  into overs on the card.
- **Five guards that had drifted** — a red test suite, a forward log counting
  twice, a ledger with its own settlement rules, a clock pinned to UTC+2, two
  non-atomic writes — and a matcher that joined reserve sides to their parent
  club. All closed.
- **Three measurements that decline ideas with numbers:** an age-aware
  fallback (count 82.10 / age 82.06 / union 81.71), a blended probability as
  guard (every card-tracking bar negative, the five coarse labels +0.93%),
  and per-league price bars.
- **The bankroll replay.** €50 at 4% through two seasons at real closing
  prices: the whole book is a coin flip with a near-halving drawdown, the
  strong lane compounds, and a card that only clears the bar because the
  price drifted out late loses in both seasons at both books.

## The honest ending

The tips ran 80.9% and the book closed at −0.3% on 143 positions — flat,
which is the best a book has done here, and still not the number. Three
findings survive into Session #6 as the way the board is played: the market
disagrees most exactly where Athena is most wrong, so the bar is a category
rate the market cannot see through card by card; the edge is in the panel
and the strong lane, not in volume; and a card is decided at first sight, not
at the close.
