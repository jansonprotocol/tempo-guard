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

A WRONG LANE is any of these, and each is worth the alert on its own:

- the bet is on the **opposite side** to the card (he buys an over, the
  card prices an under, or the reverse);
- the bet is a **lane Athena does not offer** — the rung he bought
  prices at or below its own bar, or at a negative edge;
- the bet is on a **card Athena does not select** — tip 1's edge is under
  the +1% playable bar, so the board prices the fixture but never badges
  it;
- the **line differs** from the card's and settles differently at some
  scoreline (U4.5 against the card's U4.25, U3.5 against U3.0). Name the
  scoreline where they part, and say which way it cuts.

When one applies, the reply OPENS with it and CLOSES with it, bold, with
an alert symbol. Same fixture, same sentence, top and bottom. Everything
else goes between. When nothing applies, say so in one line up front, so
the silence is legible as a check rather than an omission.

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
