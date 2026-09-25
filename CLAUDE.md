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

## What he has asked not to be told

- **Bankroll.** "I hold bank roll on 2 accounts. So ignore my bankroll
  statusses." A screenshot shows one book's wallet, never the roll.
  Never infer a bankroll or size a stake from a balance.

## Facts that decide how a bet is logged

- **The singles unit is €1.15** unless he says otherwise. Column 8 of
  config/bets.tsv is the stake and is required.
- **Price the line he actually bought**, not the card's rung, when they
  differ. The buy-from bar must be recomputed on his line.

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
- Push every commit to **both** `main` and the working branch.
- PRE-ALFA 2: the engine and its rules are not touched while the run is
  live, except where he explicitly overrides it. Measure freely; changing
  a rule is his call.
