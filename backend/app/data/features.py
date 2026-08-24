"""
As-of feature computation — the engine's input layer.

Given a fixture (league, home, away, date) this produces the point-in-time
feature bundle `evaluate_athena` consumes, derived from each team's last
`ROLLING_MATCHES` games *strictly before* the match date. Nothing after the
cutoff is ever read, so retrosim and calibration are free of lookahead bias.

Ported from the FBref snapshot reader; the feature maths and its tuned
constants are unchanged. What changed is where matches come from: parquet
snapshots in `data/` (see app.data.store) rather than a Postgres table.

Features produced
-----------------
    p_two_plus              Poisson P(total goals >= 2)
    p_home_tt05             P(home team scores)      — team-total over 0.5
    p_away_tt05             P(away team scores)
    tempo_index             normalised expected goal volume
    sot_proj_total          projected shots on target (estimated, see note)
    support_idx_over_delta  expected goals vs league average
    deg_pressure            recent defensive deterioration across both teams
    home_det / away_det     per-team volatility (std dev of match totals)
    det_boost               combined volatility
    eps_stability           consistency of goal totals (inverse CV)

NOTE ON SHOTS ON TARGET. openfootball publishes goals, not shot counts, so
`sot_proj_total` is derived from expected goals via a league-typical
shots-per-goal ratio (SOT_PER_GOAL). It is an estimate, not a measurement. It
feeds only the strict O2.5 add-on gate; every other feature is goal-derived and
therefore exact.
"""
from __future__ import annotations

import math
import re
import unicodedata
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Tuple

import pandas as pd
from rapidfuzz import fuzz, process

from app.data import aliases, store

# ── Constants (unchanged from the tuned FBref implementation) ─────────────────
ROLLING_MATCHES = 10
MIN_MATCHES     = 5     # calibration may lower this to reduce early-season skips

# Fuzzy cutoff on token_set_ratio over *canonicalised* names (see _canonical).
# Canonicalisation removes the club-suffix noise that separates "Arsenal" from
# "Arsenal FC", so genuine variants land at or near 100 while different clubs
# score far below. Measured on real openfootball names: true variants >= 100,
# nearest wrong-club pair ("Sunderland" vs "Sheffield United") 38.5 — so 88 sits
# in a wide safe band.
#
# This deliberately favours failing loudly over matching loosely. An earlier
# WRatio-based version silently resolved "Coventry City FC" to "Chelsea FC",
# which would have produced a confident tip built on the wrong club's form.
# A missing tip is recoverable; a wrong one is not.
FUZZY_CUTOFF    = 88

# Club-name decoration that carries no identity: legal forms, sponsor prefixes
# and the like. Stripped before comparison so "AFC Bournemouth", "Bournemouth"
# and "Bournemouth FC" collapse to the same key.
_CLUB_TOKENS = {
    "fc", "afc", "cf", "sc", "ac", "bc", "sk", "fk", "sv", "vfb", "vfl", "tsg",
    "rc", "as", "ss", "ssc", "us", "aj", "ogc", "rcd", "cd", "ud", "sd", "cfc",
    "club", "calcio", "nk", "if", "bk", "de", "cp", "sl", "psv", "bv", "ssv",
    "fsv", "msv", "spvgg", "kv", "rsc", "kaa", "aa", "asd", "acf", "aca",
}

VENUE_BLEND = 0.35      # weight of venue-specific scoring rate in gfh/gfa
VENUE_MIN   = 3         # minimum venue-specific games before blending

# Shots on target per goal — league-typical ratio used to estimate SoT from
# expected goals, when the source carries no shot counts.
SOT_PER_GOAL = 3.2

# Weight given to the shot-implied scoring rate when blending it with the
# observed goal rate. See _blended_scoring_rate for the measurement behind it.
SHOT_BLEND = 0.60

# Tempo normalisation. mu_total (expected match goals) realistically spans about
# 1.5-4.5; mapping that onto 0-1 keeps the signal spread out instead of pinned
# at a ceiling. See the tempo_index note in _compute_features.
TEMPO_BASE = 1.5
TEMPO_SPAN = 3.0

# ── Shrinkage of the per-fixture goal expectation ─────────────────────────────
# Ten teams' worth of recent scoring rates make a confident-looking mu, and it
# is far too confident. Regressing what actually happened on what was predicted
# across ~2,000 replayed fixtures gives:
#
#     actual_total = 1.640 + 0.424 * mu
#
# A slope of 1.0 would mean the spread is right. 0.42 means the engine's
# per-fixture reads are about two and a half times too extreme. By quintile:
#
#     lowest mu fifth     says 1.99 goals   actually 2.54   miss +0.55
#     highest mu fifth    says 3.60 goals   actually 3.26   miss -0.34
#
# Level is fine — pooled bias is +0.08 goals. It is purely SPREAD, and that is
# the worst possible shape, because every tip is selected on exactly the
# extremes that are wrong. It explains the whole symptom: the engine claimed
# 85.7% across 26 leagues and delivered 81.2%, and on the bets actually placed
# — which skew to the extremes harder still — it claimed 80.4% and delivered
# 69.6%.
#
# Poisson is not the culprit: on 272,857 matches real totals match Poisson to
# within half a point at every rung traded (scripts/dispersion.py). If mu were
# right the probabilities would be right.
#
# HOW 0.35 WAS ARRIVED AT, AND WHY IT IS COUPLED TO market_select.MIN_WIN_PROB.
# The first pass shipped 0.60, rejecting the measured 0.42 because full
# shrinkage collapsed the market mix onto U4.25 and halved realised edge. That
# reasoning was wrong, and instructively so: the collapse was not caused by
# shrinkage, it was caused by the ABSOLUTE probability floor of 0.79. Pulling
# fixtures toward the league mean meant fewer rungs cleared 79%, so the selector
# fell through to the safest buyable one. Lowering the floor to 0.75 removed the
# funnel, and with it removed the reason to hold shrinkage back.
#
# Re-swept at floor 0.75 over 1,487 replays (scripts/shrink_ab.py):
#
#     MU_SHRINK    says     hit     gap   base    realised edge   top line
#       0.60       83.2%   81.4%   -1.7  79.5%       +1.99          34%
#       0.45       83.2%   82.4%   -0.8  80.4%       +1.97          37%
#       0.35       83.2%   83.3%   +0.0  81.0%       +2.23          41%
#
# 0.35 is best on BOTH axes at once — the calibration gap closes to zero and
# realised edge is the highest measured — while the top line stays under half of
# calls. Neither constant can be tuned without the other; test_modules pins them
# together for that reason.
#
# Across ten leagues at n=250 this took the weighted gap from -4.4 to **-0.6**.
MU_SHRINK = 0.35

# Per-league overrides, for leagues whose residual slope stays far from 1.0
# after the global shrink. Re-measure with scripts/calibrate_mu.py: a residual
# slope of b means this league's remaining spread is still b times too wide, so
# its shrink should be MU_SHRINK * b.
#
# Deliberately sparse. Every entry here is a fitted parameter on ~250 fixtures
# and will over-fit if added freely, so a league only earns one when it BOTH
# measures far off AND still fails the retrosim at the global setting.
#
#   IRL-PD  residual slope **-0.600** on 300 replays — the read is not merely
#           weak, it is ANTI-correlated: the more goals the engine predicts, the
#           fewer occur. The worst slope measured in any league. A negative
#           slope has no sensible k, so this is set to 0.10, which is as close
#           to "use the league mean and ignore the fixture" as the engine goes
#           without producing an identical tip every week. Flagged as a cull
#           candidate rather than a tuning success: a calibrated tip carrying no
#           information is still no information.
#
#   MLS   residual slope 0.325 on 262 replays, and the only league still worse
#         than -4 points after the global fix (-4.2). 0.35 * 0.325 = 0.11;
#         set to 0.15, pulled toward the global to blunt the over-fit. MLS is
#         also the league whose current-season history is thinnest — nine clubs
#         carry 20 rows each after the 2026 provider split — so a weak read
#         there is what the data supports.
MU_SHRINK_BY_LEAGUE: Dict[str, float] = {
    "MLS": 0.15,
    "IRL-PD": 0.10,
}

# The team-total lane needs its OWN shrink, and this was missed on the first
# pass. `p_home_tt05` / `p_away_tt05` are built from the raw per-side rates
# `gfh` / `gfa`, not from the shrunk mu_total, so the match-total fix never
# reached them — the whole team lane was still running on unshrunk spread.
#
# Measured the same way, regressing a side's actual goals on its predicted rate
# over 2,376 side-observations:
#
#     actual_side_goals = 0.572 + 0.621 * gf
#
#     lowest gf fifth    says 0.90 goals   actually 1.14
#     highest gf fifth   says 1.92 goals   actually 1.79
#
# Less extreme than the match total's 0.42 but the same defect, and it lands on
# the lane that has been offered as Tip 2. Per league: JPN-J1 0.149, MLS 0.378,
# ENG-CH 0.644, CHI-PD 0.710, ESP-L2 0.806, TUR-SL 0.823.
#
# Shrunk toward the per-side league mean, which is league_mu / 2. Applied only
# where the team probabilities are derived, so mu_total is not shrunk twice.
TEAM_SHRINK = 0.62

# A FLOOR on the shrunk per-side rate, and it is not a second shrink — the two
# ends of the range need opposite corrections and one scalar cannot give them.
# Swept on 13,872 selection-free side-observations, membership frozen:
#
#     TEAM_SHRINK      0.62    0.70    0.78    0.86
#     gf 0.0-0.9      +6.7    +7.9    +9.2   +10.4     P(>=2), worse
#     gf 1.9-9.9      +3.5    +2.0    +0.5    -0.9     P(>=2), better
#
# Less shrink fixes the top and wrecks the bottom, so 0.62 stays. The bottom is
# not a spread problem at all: the weakest sides are simply rated too low, which
# the regression that SET the shrink already showed and nobody read as a
# separate fault — "lowest gf fifth says 0.90 goals, actually 1.14". A slope
# fitted with an intercept of 0.572 cannot be applied as a slope alone without
# leaving exactly this residual at the low end.
#
# What the floor is worth, held-back window scored with the value picked on the
# recent one, and both rungs it could touch measured alongside:
#
#     P(side scores >= 2), gf < 0.9      recent   held-back
#       no floor                          +6.8       +6.5
#       floor 0.95                        +1.5       +1.4
#
#     lane        no floor              floor 0.95
#     U1.5     -6.7 / -4.0            -4.3 / -0.1     n 421->381, 453->413
#     O1.5     +8.6 / +2.7            +8.5 / +2.9     untouched
#     O0.5     +2.5 / -0.9            +2.4 / -0.7     untouched
#
# It replicates in both windows, costs about 9% of `U1.5` lanes, and leaves the
# other two rungs alone — `U1.5` needs p >= 0.75, so P(>=2) <= 0.25, so gf <=
# 0.96, which is why that lane and only that lane sits inside the floored band.
#
# 1.00 scores marginally better on the shape metric (-0.4 / -0.5) but was never
# measured at lane level, and 0.95 moves half as many observations for a gain
# already validated. The conservative one ships.
TEAM_RATE_FLOOR = 0.95


# Fraction of a league's goals scored by the home side. Both sides used to be
# shrunk toward `league_mu / 2`, on the stated reasoning that half the league
# mean IS the per-side mean. It is not: home teams average 1.502 goals and away
# teams 1.154 across the twelve leagues checked, against a shared target of
# 1.328. Every league showed the same +0.174 / -0.174 miss, so home rates were
# dragged down and away rates pushed up, systematically and in the same
# direction everywhere.
#
# Measured on 2,158 offered lanes, that is worth a SEVEN point split: home lanes
# delivered 78.8% against 74.7% claimed (+4.1), away lanes 73.0% against 75.9%
# (-2.9). It hid because the two halves cancel — pooled, the team lane reports a
# gap of +0.3 and looks perfectly calibrated.
DEFAULT_HOME_SHARE = 0.565
_HOME_SHARE_CACHE: dict[tuple[Optional[str], Optional[int]], float] = {}


def _home_share(df: pd.DataFrame, league_code: Optional[str],
                cutoff: Optional[datetime]) -> float:
    """Share of this league's goals scored at home, counted before `cutoff`.

    Windowed and cached exactly like `_league_conversion`, and for the same
    reason: `_compute_features` is handed the whole unfiltered league frame, so
    an unguarded mean here would read seasons that had not happened yet.
    """
    year = cutoff.year if cutoff is not None else None
    key = (league_code, year)
    if league_code is not None and key in _HOME_SHARE_CACHE:
        return _HOME_SHARE_CACHE[key]

    rows = df
    if cutoff is not None and len(rows):
        rows = rows[rows["date"] < datetime(year, 1, 1)]
    h = float(rows["hg"].fillna(0).sum()) if len(rows) else 0.0
    a = float(rows["ag"].fillna(0).sum()) if len(rows) else 0.0
    share = h / (h + a) if (h + a) > 200 else DEFAULT_HOME_SHARE
    # Guard against a thin or freak window inverting the split.
    share = min(0.65, max(0.50, share))

    if league_code is not None:
        _HOME_SHARE_CACHE[key] = share
    return share


# ── The top-clash debit ───────────────────────────────────────────────────────
#
# Born from the 0-0 Manchester derby retrosim and measured before being
# believed. Stage 1, selection-free over 268,912 stored fixtures with league
# tables computed strictly as-of: big matches run under their own league's
# mean, monotone in stakes (top6 −0.02, top4 −0.05, 1v2 −0.11). Stage 2, the
# ENGINE-relative residual on 24 leagues and two separate windows:
#
#     actual − mu        recent           held-back
#     control            +0.027           +0.025
#     both top-6         −0.131 ±0.085    −0.155 ±0.091    replicates
#     both top-4         −0.217 ±0.129    −0.363 ±0.141    replicates
#     both bottom-4      −0.180           +0.237           SIGN FLIP — dead
#
# The engine reads two fat attacking rates and prices a top clash UP exactly
# when the occasion pushes it down — form cannot see stakes. The relegation
# mirror case died the two-window death: bad teams already arrive with thin
# rates, so the engine absorbs that one on its own.
#
# 0.15 is the pooled top-6 effect shaded conservative (−0.15 to −0.17 across
# both windows, z ≈ 2.5 against control). One tier, not two: top-4 measures
# stronger but noisier, and a second constant can earn its place with more
# data. Applied to the MATCH mu only — the team lanes were not measured.
#
# The flag replicates the measurement exactly, quirk included: points within
# the CALENDAR year to date, both sides with six-plus rounds played, both in
# the top six. For autumn rounds of a European season that is the season table;
# for spring rounds it is a half-season form table. That is what validated on
# both windows, so that is what ships — refining the boundary is a new
# measurement, not a free edit.
BIG_MATCH_DEBIT = 0.15
_TOP_CLASH_MIN_ROUNDS = 6
_TOP_CLASH_TOP_N = 6
_TOP_CLASH_CACHE: dict[tuple, frozenset] = {}


def _is_top_clash(df: pd.DataFrame, h_norm: str, a_norm: str,
                  cutoff: datetime, league_code: str) -> bool:
    key = (league_code, cutoff.date() if hasattr(cutoff, "date") else cutoff)
    top = _TOP_CLASH_CACHE.get(key)
    if top is None:
        year_start = datetime(cutoff.year, 1, 1)
        block = df[(df["date"] >= year_start) & (df["date"] < cutoff)]
        pts: dict[str, int] = {}
        played: dict[str, int] = {}
        for r in block.itertuples():
            if pd.isna(r.hg) or pd.isna(r.ag):
                continue
            h, a = _norm(str(r.home)), _norm(str(r.away))
            hw = 3 if r.hg > r.ag else 1 if r.hg == r.ag else 0
            pts[h] = pts.get(h, 0) + hw
            pts[a] = pts.get(a, 0) + (3 if hw == 0 else 1 if hw == 1 else 0)
            played[h] = played.get(h, 0) + 1
            played[a] = played.get(a, 0) + 1
        # Measured on full-size leagues. In a frame with few clubs — a cup
        # fallback, a tiny sample — "top six" is everybody and means nothing,
        # so the flag stands down rather than firing on all of them.
        if len(pts) < 2 * _TOP_CLASH_TOP_N:
            top = frozenset()
        else:
            table = sorted(pts, key=lambda t: -pts[t])[:_TOP_CLASH_TOP_N]
            top = frozenset(t for t in table
                            if played.get(t, 0) >= _TOP_CLASH_MIN_ROUNDS)
        if len(_TOP_CLASH_CACHE) > 4096:
            _TOP_CLASH_CACHE.clear()
        _TOP_CLASH_CACHE[key] = top
    return h_norm in top and a_norm in top


def _shrink_side(gf: float, league_mu: Optional[float],
                 share: float = 0.5) -> float:
    """Shrink one side's scoring rate toward that SIDE's league mean.

    `share` is the fraction of league goals this side scores — see
    `_home_share`. Passing 0.5 reproduces the old behaviour of shrinking both
    sides toward the same midpoint.
    """
    if not league_mu or league_mu <= 0:
        return gf
    target = league_mu * share
    # The floor is applied last, so it binds on the SHRUNK rate rather than the
    # raw one — it is a statement about what the engine ends up believing, not
    # about what the form data said before shrinkage.
    return max(TEAM_RATE_FLOOR, target + TEAM_SHRINK * (gf - target))


def _shrink_mu(mu: float, league_mu: Optional[float],
               league_code: Optional[str] = None) -> float:
    """Pull a fixture's goal expectation toward its league mean. See MU_SHRINK."""
    if not league_mu or league_mu <= 0:
        return mu
    k = MU_SHRINK_BY_LEAGUE.get(league_code or "", MU_SHRINK)
    return max(0.2, league_mu + k * (mu - league_mu))


# ── Recency window for league-wide aggregates ─────────────────────────────────
# League means and shot-conversion rates describe "what this league is like
# now", so they are computed over a trailing window rather than all of stored
# history. Two separate reasons, both measured:
#
# DRIFT. Scoring levels move. As of 2026 the all-time mean sits 0.215 goals
# above Serie A's recent level and 0.223 below Ligue 1's. The sharp lane
# standardises fixtures against this mean, so an error here shifts every
# z-score and can fire the lane the wrong way.
#
# UNIT BREAKS. Worse than drift, the source's shot-on-target definition
# changed. England records 12.43 SoT per game in 2010-2015 and 8.64 in
# 2015-2023 — teams did not stop shooting by 30%, the counting rule changed.
# Pooling across that break returned a conversion of 0.257 for England when
# the modern value is ~0.318, so the shot blend was fed a constant roughly 20%
# too low on every recent fixture. A trailing window sits inside one regime and
# sidesteps the break without needing to know where it is.
#
# Three years is long enough for a stable rate and short enough to stay within
# a single recording convention.
RECENT_WINDOW_DAYS = 365 * 3

# Below this many matches the window is too thin to describe a league, and the
# aggregate falls back to all history before the cutoff. Thin-but-real beats
# precise-but-empty, and the fallback is still strictly as-of.
MIN_LEAGUE_SAMPLE = 150

INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}

# Historical goals/game baselines for competitions whose own history is too
# short or too uneven to derive a stable league average from.
INTL_GOAL_AVERAGES: Dict[str, float] = {
    "UCL": 2.70, "UEL": 2.50, "UECL": 2.40, "EC": 2.25, "WC": 2.30,
}

def _domestic_fallback() -> List[str]:
    """
    Domestic leagues searched when resolving a cup team's recent club form.

    Derived from the registry rather than hardcoded. An earlier fixed list of
    twelve leagues silently starved the cup competitions as coverage grew: 67 of
    113 Europa League clubs could not be resolved at all, including Olympiakos,
    Fenerbahçe, Slavia Praha and Bodø/Glimt — whose domestic leagues were in
    fact loaded, just absent from the list.

    Ordered by stored match count so the deepest leagues are searched first;
    the search keeps the largest frame it finds, so this only affects speed.
    """
    from app.data import sources

    codes = [c for c, s in sources.LEAGUES.items() if not s.international]
    return sorted(codes, key=lambda c: -len(store.load_results(c)))


# ── Name normalisation ────────────────────────────────────────────────────────

# Latin letters that are NOT an accented base plus a combining mark, so NFD
# leaves them untouched and the Mn filter below never sees them. Without these
# the accent-insensitive match silently fails on whole leagues: `Sønderjyske`
# would not match `Sonderjyske`, `Widzew Łódź` not `Widzew Lodz`. The `å`, `é`,
# `ş` family DO decompose and need no entry here.
_UNDECOMPOSED = str.maketrans({
    "ø": "o", "æ": "ae", "œ": "oe", "ł": "l", "đ": "d", "ð": "d",
    "þ": "th", "ß": "ss", "ħ": "h", "ŧ": "t", "ı": "i", "ĸ": "k",
})


def _strip_accents(s: str) -> str:
    """
    Fold a name to plain ASCII letters for matching.

    Two passes are needed. NFD splits an accented letter into base plus
    combining mark and the mark is dropped; but a letter whose glyph carries the
    modification INSIDE the codepoint — Scandinavian `ø`, Polish `ł`, Croatian
    `đ` — has no decomposition at all and survives NFD unchanged. Those are
    translated explicitly first.
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s.translate(_UNDECOMPOSED))
        if unicodedata.category(c) != "Mn"
    )


def _norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def _norm_accent(s: Optional[str]) -> str:
    return _strip_accents(_norm(s or ""))


def _canonical(name: str) -> str:
    """
    Reduce a club name to its identifying core: lowercased, accent-free,
    punctuation-free, with generic club tokens removed.

        "AFC Bournemouth"          -> "bournemouth"
        "Brighton & Hove Albion FC"-> "brighton hove albion"
        "Atlético Madrid"          -> "atletico madrid"

    Falls back to the undecorated name if stripping would empty the string
    (e.g. a club literally named "PSV").
    """
    s = _strip_accents(name.lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = [t for t in s.split() if t not in _CLUB_TOKENS]
    return " ".join(tokens) if tokens else " ".join(s.split())


def _match_team(target: str, candidates: List[str]) -> Optional[str]:
    """
    Resolve a team name against the names present in a dataset.

    Exact, then accent-insensitive, then canonical-exact, then a strict fuzzy
    pass over canonical forms. Returns None rather than guessing when nothing
    clears FUZZY_CUTOFF — callers treat that as "no data for this team", which
    surfaces as a skipped fixture instead of a tip built on the wrong club.
    """
    t_norm = _norm(target)
    t_accent = _norm_accent(target)
    # setdefault, not a dict comprehension: `candidates` arrives most-preferred
    # first (see _team_names), and a comprehension would let the LAST colliding
    # spelling win instead of the first. That is the difference between picking
    # a club's current form and its abandoned historical variant.
    norm_map: dict[str, str] = {}
    accent_map: dict[str, str] = {}
    for c in candidates:
        norm_map.setdefault(_norm(c), c)
        accent_map.setdefault(_norm_accent(c), c)

    if t_norm in norm_map:
        return norm_map[t_norm]
    if t_accent in accent_map:
        return accent_map[t_accent]

    # Canonical exact — resolves pure club-suffix drift with no fuzziness at all.
    canon_map: dict[str, str] = {}
    for c in candidates:
        canon_map.setdefault(_canonical(c), c)
    t_canon = _canonical(target)
    if t_canon in canon_map:
        return canon_map[t_canon]

    keys = list(canon_map.keys())
    if not keys:
        return None
    best = process.extractOne(t_canon, keys, scorer=fuzz.token_set_ratio,
                              score_cutoff=FUZZY_CUTOFF)
    return canon_map[best[0]] if best else None


# ── Helpers ───────────────────────────────────────────────────────────────────

def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _poisson_p0(mu: float) -> float:
    return math.exp(-max(0.001, float(mu)))


def _cutoff(match_date: date) -> datetime:
    return datetime.combine(match_date, datetime.min.time()) - timedelta(seconds=1)


def _team_names(df: pd.DataFrame) -> List[str]:
    """
    Every club name in the frame, MOST PREFERRED FIRST.

    This used to be `list(set(...))`, and that was a live defect rather than an
    untidiness. Two spellings of one club — `CF Montreal` and `CF Montréal`,
    `DC United` and `D.C. United` — collapse to the same key inside
    `_match_team`, so exactly one of them wins the lookup. Set iteration order
    depends on the per-process string hash seed, so WHICH one won changed from
    run to run: the same fixture priced at mu 2.34 in one process and 1.62 in
    the next, off 20 rows of current form or 149 rows stopping in May 2025.
    Every downstream number inherited that coin flip.

    Ordering is by most recent match first, then row count, then the name, so
    the winner is both stable and the right half of a split club — this project
    ranks the current and previous season above deep history. A thin-but-recent
    variant that beats a fat-but-stale one on this ordering will usually fail
    the history gate downstream and withhold the fixture, which is the safe
    failure: no tip beats a tip built on year-old form.

    The real repair for a split club is a merge in config/team_merges.json.
    This only makes the unmerged case deterministic.
    """
    if df.empty:
        return []
    home = df[["home", "date"]].rename(columns={"home": "team"})
    away = df[["away", "date"]].rename(columns={"away": "team"})
    both = pd.concat([home, away])
    both["team"] = both["team"].astype(str)
    agg = both.groupby("team")["date"].agg(["max", "count"])
    agg = agg.sort_values(["max", "count"], ascending=[False, False])
    return list(agg.index)


# ── Per-frame index cache ─────────────────────────────────────────────────────
# Replaying a league used to be quadratic: every match re-normalised both team
# columns with a Python-level .map() over the whole frame and rebuilt the team
# name list before fuzzy-matching. On a single season that was tolerable; across
# 27 seasons of history it made a replay unusable.
#
# Instead each frame is indexed once — normalised name columns, the set of names
# present, and a per-team row index — and every subsequent lookup is a dict hit
# plus a date filter.
#
# The cache is keyed by id(df) and *keeps a reference to the frame itself*. That
# reference is load-bearing: without it a frame could be collected and a new one
# allocated at the same address, and the stale index would silently be served
# for the wrong data. Holding the frame keeps every live id unique.
#
# (Stashing the index on df.attrs looks tidier but breaks pandas: attrs
# propagate to slices, and concat compares attrs dicts, which raises as soon as
# the dict contains array values.)
_INDEX_CACHE: dict[int, dict] = {}
_MAX_CACHED_FRAMES = 8


def _frame_index(df: pd.DataFrame) -> dict:
    cached = _INDEX_CACHE.get(id(df))
    if cached is not None and cached["n_rows"] == len(df):
        return cached

    home_norm = df["home"].astype(str).map(_norm)
    away_norm = df["away"].astype(str).map(_norm)
    names = _team_names(df)

    # team_norm -> positional rows where that team played, home or away
    by_team: dict[str, "pd.Index"] = {}
    by_home: dict[str, "pd.Index"] = {}
    by_away: dict[str, "pd.Index"] = {}
    for norm_name in set(home_norm) | set(away_norm):
        at_home = home_norm == norm_name
        at_away = away_norm == norm_name
        by_team[norm_name] = df.index[at_home | at_away]
        by_home[norm_name] = df.index[at_home]
        by_away[norm_name] = df.index[at_away]

    # Sorted dates plus a running goal total, so the as-of league mean is a
    # binary search rather than a full re-filter of the frame per fixture.
    order = df["date"].values.argsort()
    dates_sorted = df["date"].values[order]
    goals = (df["hg"].fillna(0) + df["ag"].fillna(0)).values[order]
    cum_goals = goals.cumsum()

    index = {
        "n_rows": len(df),
        "dates_sorted": dates_sorted,
        "cum_goals": cum_goals,
        "frame": df,             # keeps id(df) alive and unique — see above
        "home_norm": home_norm,
        "away_norm": away_norm,
        "names": names,
        "by_team": by_team,
        "by_home": by_home,
        "by_away": by_away,
        "resolved": {},          # raw team name -> matched dataset name
    }
    if len(_INDEX_CACHE) >= _MAX_CACHED_FRAMES:
        _INDEX_CACHE.clear()
    _INDEX_CACHE[id(df)] = index
    return index


def _league_mean_asof(df: pd.DataFrame, cutoff: datetime) -> tuple[float, int]:
    """
    Mean total goals over the RECENT_WINDOW_DAYS before `cutoff`, and how many
    matches that was, in O(log n).

    Windowed rather than cumulative. Averaging a league's entire stored history
    answers "what has this league ever been like", when what every caller
    actually wants is "what is it like now" — and for England that difference
    is 9,800 matches back to 1993 versus the current level.

    Falls back to all history before the cutoff when the window is too thin to
    mean anything, which mostly affects a league's first stored seasons.
    """
    import numpy as np

    idx = _frame_index(df)
    ds, cg = idx["dates_sorted"], idx["cum_goals"]

    hi = int(np.searchsorted(ds, np.datetime64(cutoff), side="left"))
    if hi <= 0:
        return 0.0, 0

    start = cutoff - timedelta(days=RECENT_WINDOW_DAYS)
    lo = int(np.searchsorted(ds, np.datetime64(start), side="left"))
    n = hi - lo
    if n >= MIN_LEAGUE_SAMPLE:
        total = float(cg[hi - 1] - (cg[lo - 1] if lo > 0 else 0.0))
        return total / n, n

    return float(cg[hi - 1] / hi), hi


def _resolve_in_frame(df: pd.DataFrame, team: str) -> Optional[str]:
    """Match a team name against a frame, memoised per frame."""
    idx = _frame_index(df)
    if team not in idx["resolved"]:
        idx["resolved"][team] = _match_team(team, idx["names"])
    return idx["resolved"][team]


def _aliased(league_code: str, df: pd.DataFrame, team: str) -> str:
    """
    The name this league's store files `team` under.

    An alias WINS over the resolver, and that is a deliberate reversal. The
    table used to be consulted only when the raw name matched nothing, on the
    reasoning that it could then never change a tip the engine already issues.
    That protected the wrong failure. The resolver's mistakes are not blanks,
    they are confident matches on the wrong club:

        Celta Fortuna     -> Celta           Celta Vigo, 2004-2012
        U. de Concepción  -> Deportes Concepcion    a different club entirely
        América-MG        -> América (MG)    a spelling retired in 2013

    All three priced, none abstained, and one of them was carrying the largest
    stated edge on its slate. A missing tip is recoverable; a tip built on
    another club's form is not — so the hand-written statement "this feed name
    IS that store name" has to be able to overrule a guess.

    The staleness guard is what keeps that safe, and it still runs first: an
    alias pointing at a name the store does not carry is ignored, and the
    resolver is left to do what it would have done anyway. That guard is now an
    EXACT membership test rather than a resolver call, which matters more than
    it looks — asked whether `Celta B` exists in a store that only has `Celta`,
    the resolver says yes. Every target in the table was read off the store's
    own name list, so exact is the test that was always meant.
    """
    mapped = aliases.get(league_code, team)
    if mapped and mapped != team and mapped in _frame_index(df)["names"]:
        return mapped
    return team


def _find_team_rows(df: pd.DataFrame, team: str, cutoff: datetime) -> pd.DataFrame:
    """Last ROLLING_MATCHES for a team (home or away) strictly before cutoff."""
    if df.empty:
        return df

    matched = _resolve_in_frame(df, team)
    if matched is None:
        return pd.DataFrame()

    idx = _frame_index(df)
    rows = df.loc[idx["by_team"].get(_norm(matched), df.index[:0])]
    rows = rows[rows["date"] < cutoff]
    return rows.sort_values("date", ascending=False).head(ROLLING_MATCHES)


def _find_venue_rows(
    df: pd.DataFrame, team: str, cutoff: datetime, venue: str,
) -> pd.DataFrame:
    """Last ROLLING_MATCHES for a team in a specific venue context."""
    if df.empty:
        return df

    matched = _resolve_in_frame(df, team)
    if matched is None:
        return pd.DataFrame()

    idx = _frame_index(df)
    # Was a full-frame boolean comparison, run twice per fixture, so its cost
    # scaled with the league's entire history rather than the team's own
    # matches. Indexed like by_team for the same reason.
    key = "by_home" if venue == "home" else "by_away"
    rows = df.loc[idx[key].get(_norm(matched), df.index[:0])]
    rows = rows[rows["date"] < cutoff]
    return rows.sort_values("date", ascending=False).head(ROLLING_MATCHES)


# ── Rolling metrics ───────────────────────────────────────────────────────────

def _goals_per_game(rows: pd.DataFrame, team_norm: str, metric: str = "scored") -> float:
    if rows.empty:
        return 0.0
    total = 0
    for _, r in rows.iterrows():
        is_home = _norm(str(r["home"])) == team_norm
        hg = int(r["hg"]) if pd.notnull(r["hg"]) else 0
        ag = int(r["ag"]) if pd.notnull(r["ag"]) else 0
        if metric == "scored":
            total += hg if is_home else ag
        else:
            total += ag if is_home else hg
    return total / len(rows)


def _sot_per_game(rows: pd.DataFrame, team_norm: str) -> Optional[float]:
    """
    Measured shots on target per game for a team, or None when the source did
    not carry shot counts for these matches.

    A team's shots on target are the home column when it played at home and the
    away column when it played away, so the columns cannot simply be averaged.
    """
    if rows.empty or "hst" not in rows.columns or "ast" not in rows.columns:
        return None

    import numpy as np
    is_home = rows["home"].astype(str).map(_norm).values == team_norm
    val = np.where(is_home, rows["hst"].values, rows["ast"].values).astype(float)
    ok = ~np.isnan(val)
    total = float(val[ok].sum())
    counted = int(ok.sum())

    # Require most of the window to have data; a couple of stray rows would
    # make the rate noisier than the estimate it replaces.
    if counted < max(3, len(rows) // 2):
        return None
    return total / counted


def _match_totals(rows: pd.DataFrame) -> List[int]:
    if rows.empty:
        return []
    return (rows["hg"].fillna(0) + rows["ag"].fillna(0)).astype(int).tolist()


def _compute_deg_pressure(
    h_rows: pd.DataFrame, a_rows: pd.DataFrame,
    h_norm: str, a_norm: str, n_recent: int = 3,
) -> float:
    """
    Recent defensive deterioration for both teams: the trend in goals conceded
    (last 3 games vs the rolling 10). Positive means both sides are conceding
    more lately, raising the goal outlook. Range 0.0–0.80.
    """
    def trend(rows: pd.DataFrame, norm: str) -> float:
        if len(rows) < n_recent + 1:
            return 0.0
        recent = _goals_per_game(rows.head(n_recent), norm, "conceded")
        rolling = _goals_per_game(rows, norm, "conceded")
        return recent - rolling

    combined = (trend(h_rows, h_norm) + trend(a_rows, a_norm)) / 2.0
    return round(_clip(combined * 0.6, 0.0, 0.80), 3)


def _compute_team_det(rows: pd.DataFrame) -> float:
    """Per-team volatility: normalised std dev of match goal totals. 0.10–0.80."""
    if len(rows) < 4:
        return 0.30
    totals = _match_totals(rows)
    if not totals:
        return 0.30
    mean_g = sum(totals) / len(totals)
    std = (sum((x - mean_g) ** 2 for x in totals) / len(totals)) ** 0.5
    return round(_clip(std / 2.5, 0.10, 0.80), 3)


# Goals per shot on target, keyed by league. A whole-league aggregate has no
# business being recomputed per fixture.
# Keyed by (league_code, year): the rate is a windowed, point-in-time value,
# so a key without a date would serve one season's rate to every other.
_CONVERSION_CACHE: dict[tuple, Optional[float]] = {}


def _league_conversion(
    df: pd.DataFrame,
    league_code: Optional[str] = None,
    cutoff: Optional[datetime] = None,
) -> Optional[float]:
    """
    Goals per shot on target for a league, over the window ending at `cutoff`,
    or None when shots are unavailable or too sparse to trust.

    Needed because conversion is not universal: England converts about 0.318 of
    its shots on target, Germany 0.335. Using one global rate would make German
    attacks look weak and English ones strong purely as an artefact.

    THIS USED TO READ THE FUTURE. `_compute_features` passes the whole
    unfiltered league frame, so the rate was computed over every stored season
    — including seasons after the fixture being predicted — and cached under
    the league alone with no date in the key. Both halves are fixed here: the
    window ends at the cutoff, and the cache key carries the year.

    It also used to pool across the source's 2015 change in how shots on target
    are counted, which alone put England's rate ~20% below its true modern
    value. See RECENT_WINDOW_DAYS.

    The window is anchored to the start of the cutoff's year rather than to the
    cutoff itself. That keeps the result identical for every fixture in a given
    season regardless of the order they are computed in — an order-dependent
    cache could otherwise serve a January fixture a rate derived from December.
    """
    if "hst" not in df.columns or "ast" not in df.columns:
        return None

    # Keyed by (league, year), not by frame. Keying it to the frame index
    # looked harmless but was quadratic: asof_features passes a freshly
    # date-filtered slice for every fixture, so the index missed every time and
    # rebuilt the entire team-name map once per match.
    year = cutoff.year if cutoff is not None else None
    key = (league_code, year)
    if league_code is not None and key in _CONVERSION_CACHE:
        return _CONVERSION_CACHE[key]

    rows = df[df["hst"].notna() & df["ast"].notna()]
    if cutoff is not None and len(rows):
        end = datetime(year, 1, 1)
        rows = rows[(rows["date"] < end)
                    & (rows["date"] >= end - timedelta(days=RECENT_WINDOW_DAYS))]

    sot = float((rows["hst"] + rows["ast"]).sum()) if len(rows) else 0.0
    goals = float((rows["hg"] + rows["ag"]).sum()) if len(rows) else 0.0
    conv = (goals / sot) if sot > 50 else None

    if league_code is not None:
        _CONVERSION_CACHE[key] = conv
    return conv


def _blended_scoring_rate(
    rows: pd.DataFrame, team_norm: str, goals_rate: float, conversion: Optional[float],
) -> float:
    """
    Blend a team's goal rate with what its shot volume implies it should score.

    Goals are a noisy record of chances taken; shots on target are a steadier
    record of chances made. A side scoring 2.0 from 4 shots on target is
    converting at a rate it is unlikely to sustain, and one scoring 1.0 from 6
    is doing the reverse — mixing the two anticipates the regression.

    Measured over 10,421 matches with shot data, blending lifts AUC against
    "2+ goals" from 0.554 to 0.565 and correlation with the actual total from
    +0.139 to +0.157. The optimum is a broad plateau between weights of 0.5 and
    0.8 rather than a spike, so SHOT_BLEND sits in the middle of it.

    Falls back to the unmodified goal rate whenever shots are missing.
    """
    if conversion is None:
        return goals_rate
    sot = _sot_per_game(rows, team_norm)
    if sot is None:
        return goals_rate
    return (1.0 - SHOT_BLEND) * goals_rate + SHOT_BLEND * (sot * conversion)


def _projected_sot(
    H: pd.DataFrame, A: pd.DataFrame,
    h_norm: str, a_norm: str,
    mu_total: float,
) -> tuple[float, bool]:
    """
    Projected shots on target for the fixture, and whether it was measured.

    Prefers each side's actual recent shots-on-target rate. Falls back to
    deriving the figure from expected goals via SOT_PER_GOAL when the source
    carried no shot counts — which is the case for every openfootball-only
    league, and for any season football-data does not cover.

    Returns (total, measured).
    """
    h_sot = _sot_per_game(H, h_norm)
    a_sot = _sot_per_game(A, a_norm)

    if h_sot is not None and a_sot is not None:
        return round(_clip(h_sot + a_sot, 2.0, 24.0), 2), True

    return round(_clip(mu_total * SOT_PER_GOAL, 6.0, 16.0), 2), False


def _compute_eps_stability(h_rows: pd.DataFrame, a_rows: pd.DataFrame) -> float:
    """Combined consistency from the coefficient of variation. 0.35–0.95."""
    all_rows = pd.concat([h_rows, a_rows]).drop_duplicates()
    if len(all_rows) < 4:
        return 0.65
    totals = _match_totals(all_rows)
    if not totals:
        return 0.65
    mean_g = sum(totals) / len(totals)
    if mean_g < 0.5:
        return 0.65
    std = (sum((x - mean_g) ** 2 for x in totals) / len(totals)) ** 0.5
    cv = std / mean_g
    return round(_clip(1.0 - _clip(cv * 0.55, 0.05, 0.60), 0.35, 0.95), 3)


# ── Core feature computation ──────────────────────────────────────────────────

def _compute_features(
    H: pd.DataFrame, A: pd.DataFrame,
    hname: str, aname: str,
    full_df: pd.DataFrame,
    league_code: Optional[str] = None,
    league_mu: Optional[float] = None,
    H_home: Optional[pd.DataFrame] = None,
    A_away: Optional[pd.DataFrame] = None,
    cutoff: Optional[datetime] = None,
) -> Dict[str, float]:
    # Concatenate rather than union two sets: `_team_names` returns a preference
    # ORDER, and a set would discard it and reintroduce the hash-seed coin flip
    # this function's callers were just fixed for.
    seen: set[str] = set()
    names = [n for n in (*_team_names(H), *_team_names(A))
             if not (n in seen or seen.add(n))]
    h_norm = _norm(_match_team(hname, names) or hname)
    a_norm = _norm(_match_team(aname, names) or aname)

    gfh = _goals_per_game(H, h_norm, "scored")
    gfa = _goals_per_game(A, a_norm, "scored")

    # Venue blend: a home side's attack is better described by its home scoring
    # rate, an away side's by its away rate.
    venue_h = venue_a = 0.0
    if H_home is not None and len(H_home) >= VENUE_MIN:
        gfh = gfh * (1 - VENUE_BLEND) + _goals_per_game(H_home, h_norm, "scored") * VENUE_BLEND
        venue_h = VENUE_BLEND
    if A_away is not None and len(A_away) >= VENUE_MIN:
        gfa = gfa * (1 - VENUE_BLEND) + _goals_per_game(A_away, a_norm, "scored") * VENUE_BLEND
        venue_a = VENUE_BLEND

    # Blend in what each side's shot volume implies it should be scoring.
    # Applied after the venue split so the two adjustments compose.
    conversion = _league_conversion(full_df, league_code, cutoff)
    gfh = _blended_scoring_rate(H, h_norm, gfh, conversion)
    gfa = _blended_scoring_rate(A, a_norm, gfa, conversion)
    shots_blended = conversion is not None

    # Residual venue de-bias. Both scoring rates start from a team's form over
    # its last ten matches HOME AND AWAY, and only VENUE_BLEND of that is
    # replaced by venue-specific form. So `gfh` keeps leaning on a venue-neutral
    # number and lands about 0.113 goals under the true home mean, with `gfa`
    # the same amount over — measured across twelve leagues, and present before
    # any shrinkage runs. Correcting the shrink TARGET could not reach it,
    # because the bias is already in the input.
    #
    # Applied symmetrically so `mu_total = gfh + gfa` is exactly unchanged: the
    # match lane is calibrated to a gap of ~0 and must not move to fix the team
    # lane. Only the split between the two sides shifts.
    if league_mu and league_mu > 0:
        edge = league_mu * (_home_share(full_df, league_code, cutoff) - 0.5)
        c = edge * (1 - (venue_h + venue_a) / 2)
        gfh, gfa = max(0.05, gfh + c), max(0.05, gfa - c)

    mu_total = max(0.2, gfh + gfa)
    mu_total = _shrink_mu(mu_total, league_mu, league_code)
    # Two top sides suppress each other in a way form cannot see — the engine
    # reads two fat attacking rates and prices the fixture UP when the occasion
    # pushes it down. See _is_top_clash for the measurement; applied after the
    # shrink because it is an occasion effect, not a spread error, and to
    # mu_total only because only the match residual was measured.
    if (league_code and cutoff is not None
            and _is_top_clash(full_df, h_norm, a_norm, cutoff, league_code)):
        mu_total = max(0.2, mu_total - BIG_MATCH_DEBIT)
    p0 = math.exp(-mu_total)
    p1 = mu_total * p0
    p_two_plus = 1.0 - (p0 + p1)

    sot_total, sot_measured = _projected_sot(H, A, h_norm, a_norm, mu_total)

    if league_code and league_code in INTL_GOAL_AVERAGES:
        league_mu = INTL_GOAL_AVERAGES[league_code]
    elif league_mu is None:
        # Only reached by the cup fallback, which assembles frames by hand.
        league_mu = float(
            (full_df["hg"].fillna(0) + full_df["ag"].fillna(0)).mean() or 2.5
        )

    _hshare = _home_share(full_df, league_code, cutoff)

    return {
        "p_two_plus":             round(float(p_two_plus), 3),
        "p_home_tt05":            round(float(1.0 - _poisson_p0(
            _shrink_side(gfh, league_mu, _hshare))), 3),
        "p_away_tt05":            round(float(1.0 - _poisson_p0(
            _shrink_side(gfa, league_mu, 1.0 - _hshare))), 3),
        # Tempo is normalised so a typical fixture lands near the middle of the
        # range. The previous mapping (mu/3.0 clipped at 0.9) saturated: league
        # means sit at 2.4-3.2 goals, so ~63% of matches pinned to the ceiling
        # and the signal was effectively constant. That starved every module
        # gated on low tempo — gate_b, ulr and mfr could fire on a handful of
        # matches per season rather than acting as real controls.
        # TEMPO_BASE is roughly the lowest realistic match total, TEMPO_SPAN the
        # working range above it.
        "tempo_index":            round(_clip((mu_total - TEMPO_BASE) / TEMPO_SPAN,
                                              0.05, 0.95), 3),
        # Measured where the source carries shot counts, estimated otherwise.
        # See _projected_sot — the two are on noticeably different scales, so
        # which one is in play matters to the O2.5 gate that consumes it.
        "sot_proj_total":         sot_total,
        "sot_measured":           sot_measured,
        "shots_blended":          shots_blended,
        "support_idx_over_delta": round(_clip((mu_total - league_mu) * 0.12, -0.15, 0.15), 3),
        "deg_pressure":           _compute_deg_pressure(H, A, h_norm, a_norm),
        "home_det":               _compute_team_det(H),
        "away_det":               _compute_team_det(A),
        "det_boost":              round((_compute_team_det(H) + _compute_team_det(A)) / 2.0, 3),
        "eps_stability":          _compute_eps_stability(H, A),
        # The raw goal expectation and its league reference. Both were
        # already computed here and thrown away; the market selector needs
        # them to compare this fixture against a typical one.
        "mu_total":               round(float(mu_total), 3),
        # Diagnostics
        "h_scoring_rate":         round(gfh, 3),
        "a_scoring_rate":         round(gfa, 3),
        "league_mu":              round(league_mu, 3),
        "n_home_matches":         len(H),
        "n_away_matches":         len(A),
    }


# ── International fallback ────────────────────────────────────────────────────

def _asof_features_intl(
    home_team: str, away_team: str, match_date: date,
    league_code: str, min_matches: int,
) -> Dict[str, float]:
    """
    Cup fixtures: clubs' recent form comes from their domestic leagues, since a
    cup campaign alone is far too few matches to compute rolling form from.
    """
    cutoff = _cutoff(match_date)

    def best_frame(team: str) -> tuple[pd.DataFrame, Optional[pd.DataFrame]]:
        best_rows, best_full = pd.DataFrame(), None
        for code in _domestic_fallback():
            df = store.load_results(code)
            if df.empty:
                continue
            rows = _find_team_rows(df, _aliased(code, df, team), cutoff)
            if len(rows) > len(best_rows):
                best_rows, best_full = rows, df
            # A full rolling window is the most that will ever be used, so once
            # one league supplies it there is nothing better to find. Without
            # this the search scans all ~50 leagues for every cup team.
            if len(best_rows) >= ROLLING_MATCHES:
                break
        return best_rows, best_full

    H, full_H = best_frame(home_team)
    A, _ = best_frame(away_team)

    if len(H) < min_matches or len(A) < min_matches:
        return {}

    return _compute_features(H, A, home_team, away_team,
                             full_H if full_H is not None else H,
                             league_code=league_code)


# ── Main entry point ──────────────────────────────────────────────────────────

def asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    min_matches: int = MIN_MATCHES,
) -> Dict[str, float]:
    """
    Compute the feature bundle for a fixture as of its match date.

    Returns {} when there is not enough history — callers treat that as
    "no prediction possible" rather than guessing.
    """
    if league_code in INTL_LEAGUE_CODES:
        return _asof_features_intl(home_team, away_team, match_date,
                                   league_code, min_matches)

    df = store.load_results(league_code)
    if df.empty:
        return {}

    cutoff = _cutoff(match_date)
    league_mu_asof, n_before = _league_mean_asof(df, cutoff)
    if n_before == 0:
        return {}

    # Done once, before any lookup, so the rolling rows, the venue rows and the
    # per-team nudges all key off the same name.
    home_team = _aliased(league_code, df, home_team)
    away_team = _aliased(league_code, df, away_team)

    H = _find_team_rows(df, home_team, cutoff)
    A = _find_team_rows(df, away_team, cutoff)
    H_home = _find_venue_rows(df, home_team, cutoff, "home")
    A_away = _find_venue_rows(df, away_team, cutoff, "away")

    # A club its own league cannot describe may have just arrived from the
    # division above or below — see the fallback's header for the measured
    # exchange rate. Rescue-only: this branch is unreachable for any club the
    # league already has `min_matches` rows for.
    if len(H) < min_matches:
        got = _cross_division_rows(league_code, home_team, cutoff,
                                   min_matches, "home")
        if got is not None:
            home_team, H, H_home = got
    if len(A) < min_matches:
        got = _cross_division_rows(league_code, away_team, cutoff,
                                   min_matches, "away")
        if got is not None:
            away_team, A, A_away = got

    if len(H) < min_matches or len(A) < min_matches:
        return {}

    return _compute_features(
        H, A, home_team, away_team, df,
        league_code=league_code,
        league_mu=league_mu_asof,
        H_home=H_home,
        A_away=A_away,
        cutoff=cutoff,
    )


# ── Cross-division fallback ───────────────────────────────────────────────────
#
# A promoted club abstains with a full season of history one division down —
# Le Mans with 328 rows in FRA-L2, Racing Santander with 336 in ESP-L2 — and
# recurs for ~3 clubs per league every August. No alias or merge can reach it,
# because the rows genuinely live in another competition's file.
#
# The reason it was never simply "look one division down" is that the form does
# not transfer raw. Measured over 789 club-seasons that crossed a stored
# boundary (15+ matches before, 10+ after):
#
#     PROMOTED  (415)   goals for  x0.754    against x1.516    total x1.025
#     RELEGATED (374)   goals for  x1.345    against x0.727    total x0.948
#
# A promoted side scores a quarter less and concedes half again more, so its
# raw lower-division rates would overstate the team lane badly. But the two
# directions are near-reciprocal (0.754 up against 1/1.345 = 0.743 down), which
# is what one stable exchange rate between adjacent divisions looks like — so
# the correction is those constants applied to the rows, and the fixture's own
# league then supplies every baseline (league_mu, base rates, shrink targets)
# exactly as it would for any other club. The match TOTAL transfers almost
# clean (x1.025), which is why the scaled read is usable at all: this engine
# prices totals.
#
# GUARDED like the merge table: the fallback fires only when the club's own
# league yields fewer than `min_matches` rows, so it can convert an abstention
# into a tip and can never move a tip the engine already issues.
DIVISION_LADDERS = [
    ["ENG-PL", "ENG-CH", "ENG-L1", "ENG-L2", "ENG-NL"],
    ["ESP-LL", "ESP-L2"],
    ["NED-ED", "NED-D2"],
    ["GER-BL", "GER-B2"],
    ["ITA-SA", "ITA-SB"],
    ["FRA-L1", "FRA-L2"],
    ["SCO-PL", "SCO-CH", "SCO-L1", "SCO-L2"],
    ["BRA-SA", "BRA-SB"],
    ["SUI-SL", "SUI-CL"],
]
PROMOTED_SCORED = 0.754
PROMOTED_CONCEDED = 1.516


def _adjacent_divisions(league_code: str) -> List[Tuple[str, bool]]:
    """(sibling code, promoted) — promoted=True when the sibling sits BELOW
    the fixture's league, so a club found there is moving up into it."""
    for ladder in DIVISION_LADDERS:
        if league_code in ladder:
            i = ladder.index(league_code)
            out = []
            if i + 1 < len(ladder):
                out.append((ladder[i + 1], True))
            if i > 0:
                out.append((ladder[i - 1], False))
            return out
    return []


def _cross_division_rows(
    league_code: str, team: str, cutoff: datetime, min_matches: int,
    venue: str,
) -> Optional[Tuple[str, pd.DataFrame, pd.DataFrame]]:
    """(matched name, rows, venue rows) from an adjacent division, with every
    goal rescaled to the fixture league's level. None when the club is not
    there either — the abstention then stands, which is the honest end."""
    for code, promoted in _adjacent_divisions(league_code):
        df = store.load_results(code)
        if df.empty:
            continue
        matched = _resolve_in_frame(df, _aliased(code, df, team))
        if matched is None:
            continue
        rows = _find_team_rows(df, matched, cutoff)
        if len(rows) < min_matches:
            continue
        sf = PROMOTED_SCORED if promoted else 1.0 / PROMOTED_SCORED
        cf = PROMOTED_CONCEDED if promoted else 1.0 / PROMOTED_CONCEDED

        def scale(frame: pd.DataFrame) -> pd.DataFrame:
            out = frame.copy()
            is_home = out["home"].astype(str) == matched
            out.loc[is_home, "hg"] = out.loc[is_home, "hg"] * sf
            out.loc[is_home, "ag"] = out.loc[is_home, "ag"] * cf
            out.loc[~is_home, "ag"] = out.loc[~is_home, "ag"] * sf
            out.loc[~is_home, "hg"] = out.loc[~is_home, "hg"] * cf
            return out

        return (matched, scale(rows),
                scale(_find_venue_rows(df, matched, cutoff, venue)))
    return None


def validate_match_existed(
    league_code: str, home_team: str, away_team: str, match_date: date,
) -> Tuple[bool, Optional[str]]:
    """Confirm a fixture really was played on that date before retro-simulating it."""
    df = store.load_results(league_code)
    if df.empty:
        return False, f"No data stored for {league_code}. Run `athena data load` first."

    day = df[df["date"].dt.date == match_date]
    if day.empty:
        return False, f"No {league_code} matches found on {match_date}."

    names = _team_names(day)
    mh = _match_team(home_team, names)
    ma = _match_team(away_team, names)
    if not mh:
        return False, f"{home_team} did not play in {league_code} on {match_date}."
    if not ma:
        return False, f"{away_team} did not play in {league_code} on {match_date}."

    exists = any(
        _norm(str(r["home"])) == _norm(mh) and _norm(str(r["away"])) == _norm(ma)
        for _, r in day.iterrows()
    )
    if not exists:
        return False, (
            f"{home_team} and {away_team} did not play each other on {match_date} "
            f"(both played, but not one another)."
        )
    return True, None


def actual_result(
    league_code: str, home_team: str, away_team: str, match_date: date,
) -> Optional[tuple[int, int]]:
    """Actual (home_goals, away_goals) for a played match, or None."""
    df = store.load_results(league_code)
    if df.empty:
        return None
    day = df[df["date"].dt.date == match_date]
    if day.empty:
        return None
    names = _team_names(day)
    mh = _match_team(home_team, names)
    ma = _match_team(away_team, names)
    if not mh or not ma:
        return None
    for _, r in day.iterrows():
        if _norm(str(r["home"])) == _norm(mh) and _norm(str(r["away"])) == _norm(ma):
            return int(r["hg"]), int(r["ag"])
    return None
