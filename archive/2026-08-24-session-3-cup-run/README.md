# ARCHIVE — Session #3, the cup run · 24–27 Aug 2026

The era that reopened the cups. Frozen at archive time (23:00 CEST, 27 Aug);
`log.md` is the full board exactly as it last rendered, `fixtures.tsv` and
`bets.tsv` are the raw rows.

## Final numbers

    lane                        Tip 1              Tip 2
    all matches             57 / 69   82.6%     39 / 55   70.9%
    played lanes  >+1%      41 / 48   85.4%
    placed bets             25 / 35   71.4%    ROI -7.5%

Larne v Lincoln RI settled minutes after the era closed — ✅ 0-2 on the
90, both tips in, the open U3.5 at 1.30 won — and is graded here in its
own era rather than polluting the next one; the numbers above include
it. Still unresolved at archive time: Barcelona v Athletic, Partizan v
Getafe and Hibernian v Gent (in play on the closing night, carried onto
the Session #4 board as marked stragglers), plus Sétif v Ben Aknoun and
MC Alger v Oran, whose scores never surfaced (ESPN carries no Algerian
league).

## What this era built

- **The cups came back.** Off the board at −11.4 when the era began;
  reopened on a probationary Club Elo lane (B1/B2/B3, the over debit, the
  0.82 floor), which then graded 12/16 on its first full playoff night.
- **Rules 5 and 6 became numbers** — the DNB confluence backtested on
  6,354 pairs, the line-translation ladder codified.
- **The board became a web app** — five pages, four tabs, Ask Athena,
  sortable everything, self-verifying renderer, an updater hard-locked to
  sweep every fixture and settle extra time on the 90.
- **The calibration day (27 Aug)** — the retrosim page forced every number
  to be defended: the n=200 table was mostly noise, HIGH_SAYS_DEBIT
  shipped, fourteen weak leagues got per-league floors under an ROI
  constraint (GRE-SL and PER-L1 reverted when the buy-from column exposed
  the mirage), and board-wide retrosim hit went 82.4 → 83.7.
- **Four "signal without edge" declines** in a single day — tie state,
  Elo+form, domestic context, leg distinction — all recorded red in the
  hypotheses ledger with the numbers that killed them.
- **Grading integrity earned the hard way**: the Plzeň 90-minute score was
  mis-graded off ESPN's incomplete keyEvents feed, caught by the bettor,
  corrected within the hour, and the reader rebuilt on running-score
  narration with every ET row re-verified.

## The honest ending

The era closed at ROI −8.6% on 34 settled positions while the tips ran
82.4%. The gap is the story the next session inherits: hitrate is
necessary, prices make it sufficient. The floors, the debits and the
buy-from column all exist so Session #4 starts with better claims than
Session #3 did — and the proof-in-hindsight replay says the discipline is
worth roughly the whole gap between the losing era and today.
