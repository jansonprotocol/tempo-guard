# Working agreements — ATHENA: Tempo Guard

Standing instructions from the bettor, in his own words where possible.
This file exists because a session ends and an instruction given in chat
dies with it. Add to it when he states a rule; do not invent entries.

## How to report to him

**WRONG LANE IS AN ALERT, NOT A PARAGRAPH.** (24 Sep, after the Georgia v
Northern Ireland warning was buried mid-answer and he only found it after
the bet lost: "that was a bit drowning in the text. Whenever you see me
taking a wrong lane, put that immediately on top and again on the bottom,
also maybe bold with an alert symbol to make it catch my attention.")

WHEN IT APPLIES: in chat, when he sends a bet slip to be logged or
checked. It is not a repo-wide alarm and not something to raise
unprompted.

WHAT A WRONG LANE IS, and it is only this: **the bet is on the OPPOSITE
SIDE to the card.** He buys an over where the card prices an under, or
the reverse. Georgia v Northern Ireland, 25 Sep, is the case that made
the rule — the card was Under 4.25 and he bought Over 1.5. (He narrowed
it himself, 25 Sep: "it's really about taking a completely wrong lane,
the opposite like Ireland with over instead of under.")

WHAT IT IS NOT. None of these earns the alert; they belong in the body
of the reply like any other observation:

- a card whose edge is under the +1% playable bar — priced but not
  selected;
- a different LINE on the same side (U4.5 against the card's U4.25);
- a price that sits under the buy-from bar.

When a wrong lane is present, the reply OPENS with it and CLOSES with
it, bold, with an alert symbol, same fixture and same sentence both
times. Everything else goes between. When the slip is clean, say so in
one line up front so the silence reads as a check rather than an
omission.

**THE SECOND ALERT: DOES THE GAP BAND AGREE?** (4 Oct: "Remove the over
the bar alert. Replace with: gap bands agree or don't.") It replaces the
more-than-3%-over-the-bar alert of 25-27 Sep, which is retired: the
over-3% gap is no longer flagged on a slip at all.

Every leg's card carries a band check under its VS BAR line: the
like-for-like record of its gap band (same band, best price within
±0.10), my bets and all cards, read by `webapp.band_call`:

- **agrees** — "backs it": every line with 3+ behind it at 80%+;
- **does not agree** — "vetoes": no line at 80%, and one under 75%;
- in between — "close" (75-80%, none under 75), "split" (one line 80%+,
  one under), "thin" (too few to read).

A leg whose band DOES NOT AGREE gets the alert treatment: named at the
top and again at the bottom, bold, with the symbol. Legs where it agrees
are said so in one line; close, split and thin go in the body. Both
alerts can fire on one slip; name each once at each end.

THIS IS A FLAG, NOT A DECLINE, AND IT IS UNPROVEN. The band check is
tracked only (config/tier_band_log.tsv, the "Does the band back the
tier?" table): on its first as-of read, 4 Oct, vetoed tiered cards had
landed 16 of 17 and backed ones 82 of 94 — no edge yet. Say so whenever
it fires; it never argues against a bet already placed.

## The books he can actually reach

"1xBet and Tonybet and often Unibet are the prices I most likely see
and can find" (27 Sep). Pinnacle holds the best price on 36% of stamped
cards and he cannot use it, so EVERY ROI computed from the feed's best
price is optimistic for him. Measured: where the best price sat at a
book he can reach, the cards return 5 to 10 points WORSE per band than
where it sat somewhere he cannot. Quote feed-best ROI as what the
market offered, never as what he would have got.

## What he has asked not to be told

- **Bankroll.** "I hold bank roll on 2 accounts. So ignore my bankroll
  statusses." A screenshot shows one book's wallet, never the roll.
  Never infer a bankroll or size a stake from a balance.

## Facts that decide how a bet is logged

- **The singles unit is €1.15** unless he says otherwise. Column 8 of
  config/bets.tsv is the stake and is required.
- **Price the line he actually bought**, not the card's rung, when they
  differ. The buy-from bar must be recomputed on his line.

## Archiving a session

- **Keep the odds with the cards.** (1 Oct, on finding the older archives
  hold boards and bets but no prices: "whenever this session gets
  archived keep the odds these cards had in store as well".) An archived
  session folder carries, beside its README, bets and fixtures:
  `config/forward_log.tsv` (every card's first-sight best price, book,
  consensus and play bar — the record the band tables are built on),
  `config/odds_quotes.tsv` (the last quote snapshot, with the three best
  books), and `config/bets.tsv` (the prices actually paid). Copied whole,
  never trimmed, so a later build can be re-priced against the real
  quotes of the time rather than prices derived from the 2.5 market.

## Grading by hand

- **Three sources, never one.** Never set a score from a single page;
  prefer sources that name the scorers. This applies to dates as much as
  to scores — two of the three cards hand-checked on 24 Sep turned out to
  be mis-dated rather than ungraded.

## Engine and repo rules that predate this file

- **Odds never enter the prediction path.** The feed prices cards; it
  never informs the engine.
- `config/league_hitrates.tsv` and `config/guard_slices.tsv` are ENGINE
  inputs. Do not recompute them for display.
- **No model identifiers** in commit messages, PR titles or bodies, code
  comments, or anything else pushed to the repository. Chat replies only.
- **Work directly in `main`.** (28 Sep: "always directly work in main".)
  Check out `main`, commit there, push to `main`. No session branch.
- PRE-ALFA 2: the engine and its rules are not touched while the run is
  live, except where he explicitly overrides it. Measure freely; changing
  a rule is his call.
