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

**WEAK MEANS NO PLAY.** (4 Oct: "Medium actually looks weak to me ... label
it weak"; then "everything else is also weak, all below 75% ... leaving it
now for data, but weak basically means no plays.") The tier once called
MEDIUM is shown as WEAK, and so is any untiered play under its tier's
floor; both land about 72-75% on counted cards. On the board they read
"WEAK · NO PLAY" but stay in the record for data. **Each WEAK gap band
moves to DECLINED the day it reaches 50 settled cards**, one band at a time
(4 Oct: "weak needs to move to declined whenever one of its bands gets 50
cards; one band can go earlier than the other") — set by
scripts/tier_optimize.py, shown as "n/50" on the pill. A card already a
play or carrying a bet keeps its place (a play stays a play). When a slip leg is on a WEAK card, say so in the body; it
is not an alert.

**STRONG STOPS AT 3% UNDER THE BAR.** (11 Oct: "Move this 3-5 under
strong to weak and replace with Unlabeled all 7 rates 0-3 under".) Split
by band, STRONG's old 0-5% under half was two different cards: 0-3%
under landed 30 of 33 (+11.2%), 3-5% under 11 of 16 (-16.8%,
break-even 1.45). So `STRONG_UNDER` is 3, and an all-seven card in the
3-5% under band at a price STRONG would once have taken is **WEAK**,
never a play, still counted. Its place goes to the all-seven 0-3% under
cards under the floor (21 cards, 100%, +14.0%): they are **★ STRONG
FROM**, a named profile and a buy-from price, NOT the play pill — his
call, 11 Oct, when asked whether to waive the floor for them instead.
THE 1.17 STRONG FLOOR IS HELD BY HAND for that reason
(`tier_optimize.FLOOR_HELD`): with the 3-5% band gone the tier's
break-even falls to about 1.11, and a 1.11 floor would sweep those very
cards into STRONG. Drop the entry to let the optimizer measure it again.

**LIVE INPLAY GOOD: THE HALF OF STRONG WATCH HE CAN ACTUALLY BUY.**
(14 Oct, after he described the entry: "with overs that means waiting up
to about 20-25 minutes with 0-0 ... with unders I actually need goals ...
those never reached my sample".) A STRONG WATCH card is bought in play,
and the wait itself selects the match. Simulated against config/live_log.tsv
over the 560 settled STRONG WATCH and FROM cards, the 85.5% headline is
not his: OVERS he can only buy while it is goalless, and the matches
where a goal came first — which he never buys — land 96.3%, leaving him
38 of 48 (79.2%, break-even 1.26); UNDERS he can only buy once goals go
in, and those are the matches that overrun (by 30' the games finishing 5+
average 1.15 goals against 0.52, 32% already 2+ against 7%), so buying an
under BECAUSE goals went in selects the blow-ups. NOTHING on the card
separates them — colour, claim and band differ by under two points.
So the pill reads **LIVE INPLAY GOOD · from X** on an over (from 1.30) or
a green under claiming 85%+ (29 of 32, 90.6%, from 1.15); every other
STRONG WATCH card keeps its own label and board, and ONLY these reach the
Running tab. webapp.live_good; tracked as its own row on the Profiles page.

**WEAK IS A DECLINE.** (14 Oct: "Make weak = declined. Never going to
play those. Only 3-6% over 11, 90.9%, +23.7% from the profile board shows
potential, but too thin to conclude now. The workflow might promote it
later.") WEAK had been no-play-but-counted since 4 Oct, each band
declining itself at 50 cards; it lands 75.2% overall. Now the whole tier
is declined at once and leaves the record, EXCEPT `WEAK_KEPT_BAND`
(3-6% over), which keeps its place and keeps counting so the optimizer
can promote it if the record holds. Both routes into WEAK are covered —
the MEDIUM tier and an all-seven card in a band STRONG gave up —
because `medband_state` now answers for both.

AND NO WEAK CARD ON THE PLAYABLE BOARD, kept band included (14 Oct, on
the three MLS cards still reading "WEAK - NO PLAY" there: "I want all
weak from the playable board. We still track it record ofcourse. This
band: 3-6% over 11 90.9% +23.7%, has potential. But not for now.").
`webapp.weak_noplay` is the test the pill makes - a play that is not
STRONG - so both routes into WEAK leave Playable and Watch and read from
Athena lanes instead, still priced, graded, counted and carrying their
n/50. Playable now holds STRONG and MILD only. board.verify partitions
the pending tabs the same way, WEAK and the non-live_good STRONG WATCH
cards included, or it would call the app's own tabs a contradiction.
THE OPTIMIZER CANNOT PROMOTE THE KEPT BAND. scripts/tier_optimize.py
only writes a band state (good / skip / hard / none), and `good` is a
mark on the pill, nothing more - there is no path from a WEAK band to a
play. Keeping the band counted keeps the record running so he can decide;
the promotion itself would be a new rule and his call.

**THE BAND MOVER: PROFILES MOVE BOTH WAYS, BY THEMSELVES.** (14 Oct:
"Build that rule: 85%+ positive ROI on 30+ promotes. This means it can
check all profiles inside decline (same as we have on the other boards).
Also the other way around, when now playable or watch profiles in their
bandsample start dropping below a good hitrate (ignore ROI in WATCH/LIVE
profiles) they can move to declined.") The band tables had only ever run
on the play boards, so a declined profile's record was measured and never
acted on. `scripts/tier_optimize.py` now reads EVERY profile by gap band
(`webapp.profile_name`, the one classifier the Profiles page also reads)
and writes `move` rows to config/tier_params.tsv:

- **promote** a no-play cell (WEAK, or unlabelled) at **85%+ with a
  positive ROI on 30+** settled cards — `MOVE_UP_HIT`, `MOVE_MIN_N`. It
  leaves decline, returns to the record and reads **PROMOTED · PLAY**
  wherever its price clears the play bar.
- **decline** a **play** cell under **80%** (`MOVE_DOWN_HIT`) whose ROI is
  at or under zero, and a **watch or live** cell under 80% **whatever its
  ROI** — his rule, and the right one: those pills are bought live at a
  price the feed never printed, so their feed ROI says nothing.
- Nothing is permanent. The state is recomputed every run with the mover
  and the WEAK hard rule OFF (`webapp._MOVES_OFF`), so a declined cell
  keeps counting and comes back by itself, and a promoted cell that slips
  reverts the same way.

WHICH CARDS A CELL IS MEASURED ON. Only the ones a promotion would
actually free: a red card never (the tier says avoid — releasing a red
profile is scripts/profile_review.py's job), and nor does a card declined
by the O1.5 band, a strike combo or an unsafe tag, because promoting its
cell would not free it. O1.5 alone holds 30 of the 41 cards sitting in
WEAK's 3-6% over band; counting them would have read that cell as 78.0%
instead of the 90.9% its own 11 cards have.

ON THE FIRST RUN, 14 Oct, NOTHING MOVED: no cell is over or under a bar on
30+ cards. The 3-6% over band he is waiting on stands at 11, 90.9%,
+23.7% — it promotes itself the day it reaches 30 if it holds. Every
no-play cell, and any cell the mover declines, is listed with its record
under **inside decline** at the foot of the Profiles page, because the
table above it counts only the record and a declined cell has no row
there at all.

**★ STRONG FROM JOINS LIVE INPLAY GOOD.** (14 Oct: "those are also cards
i might go in live with".) Its unders need NO green/claim test: that test
exists because a STRONG WATCH card is picked on price alone, while STRONG
FROM has already passed all seven rates at 80%+. It also sits 0-3% under
the bar, not 5-15%, so drift can carry it to the price and goals are not
the only way in — which is the whole reason the STRONG WATCH unders were
a trap. Thin: 24 settled cards, 23 landing, 9 of 10 on the reachable live
subset. It keeps its own ★ STRONG FROM row on the Profiles page.

**MILD: THE SOFT MIDDLE OF STRONG.** (14 Oct, on the one marker left in
the STRONG losses: "82-85% 35/42 = 83.3% +5.5% > Make that MILD".) Split
by the card's own claim, STRONG is two different cards: under 82% lands
88.6%, 85%+ lands 92.3%, and the 82-85% middle lands 35 of 42 — 83.3%,
break-even **1.20**, +5.5% at a feed price. It is where what STRONG still
loses sits: 54% of the losses against 39% of the wins. MILD IS STILL A
PLAY and stays in the record; the pill stops calling it strong and names
the 1.20 its own rate needs. webapp.mild; its own row on Profiles, search
word `tier mild`. The optimizer still measures MILD inside STRONG for the
floor — splitting that is a separate decision.

**THE 10%+ OVER BAND LEFT STRONG.** (14 Oct: "move over 10% from STRONG
to weak. ROI and hitrate are negative".) 18 of 25, 72.0%, break-even
1.39, and +0.5% only at a feed price he cannot reach. It is also where
the losses sat: 35% of every STRONG loss against 17% of the wins. It
joins 3-5% under in `STRONG_WEAK_BANDS`, and the strong test is gated on
both at source. STRONG now reads 90 of 103, 87.4%, break-even 1.14.

**HIS METHOD: THE CLASS RATE, THEN WAIT FOR THE PRICE.** (5 Oct: "All is
moot. As we have found out how all cards are performing, that's the only
number that counts. Each time and band is measured, that's the only rate
I'm counting on. So whatever happens, I'm just waiting for the good prices
for those classes.") He buys a card's locked profile and gap-band record
at whatever moment the price is right, pre-match or live. Do not argue an
in-play bet against the live game state; judge it on the card's class and
band record and the price paid, like any other leg.

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
- **Everything is locked at first sight.** (4 Oct: "everything must get
  locked as soon as a card comes in. That gives a clear line that always
  is rated and measured with: max price from the moment the card gets in.
  Otherwise we keep doubting how much was profile A or B ... no matter
  where the card ends up.") A card's lane is rated on the feed's best
  price and the play bar stamped the FIRST time the board saw it
  (config/forward_log.tsv): tier, gap band, play or no play, the
  price-reading declines and the record all read that, before kickoff and
  after, and never move — whatever the price does later, and whether or
  not he holds a bet. The live quote is shown as information only, and a
  card's "from" price is buy guidance, not a way to change its profile.
  This replaced the same day's "a play stays a play" lock
  (config/play_lock.tsv is kept as history, no longer read).
  ONE EXCEPTION (4 Oct): a card no book has quoted that he bets on
  pre-kickoff takes the price he paid as its first-sight stamp
  (webapp.stamp_bets, book "<book> (his bet)").
- **Unquoted leagues are priced from Unibet (NL).** (4 Oct: "can't you use
  that for any other missing league without quotes?") Where the odds feed
  has no market, scripts/kambi.py reads the Total Goals ladder from
  Unibet's own (Kambi) offering API, inside the normal quote refresh: book
  "Unibet (NL)", one book, .5 lines only. Leagues in kambi.PATHS: NED-D2,
  COL-PA, PER-L1, INT-CONCACAF.
- **TOTO is a book on every card it prices.** (4 Oct: "TonyBet can
  sometimes price better ... also TOTO has good ones.") scripts/toto.py
  reads TOTO's Total Goals ladder (sport-api.toto.nl) for 40 board leagues
  inside the quote refresh; where it beats the feed it becomes the lane's
  best price and book, so a new card's first-sight stamp can be TOTO's.
  TonyBet is not read yet (its API filter could not be confirmed).
- PRE-ALFA 2: the engine and its rules are not touched while the run is
  live, except where he explicitly overrides it. Measure freely; changing
  a rule is his call.
