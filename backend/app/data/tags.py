"""
Team tags — a readable description of who is playing, computed as of match day.

WHAT THESE ARE FOR
==================
Tags explain a tip. They do not currently produce one.

That distinction is deliberate and it is what the measurements support. Three of
these tags — attack, defence and form — are derived from goals scored and
conceded, which is exactly what the engine's `mu` already reads. Naming a side
"leaky defence" adds no information the goal model does not have; it puts words
on a number. The residual test run against 388 teams found a split-half
correlation of +0.206 for team tendency *after* rolling form is accounted for,
with wide league-to-league scatter (Sweden +0.62, Premier League -0.15). Modest,
and negative in some leagues.

So these are published alongside a tip so a human can see the shape of the
fixture at a glance, and they are NOT fed back into market selection. Wiring
them into the prediction would be refitting a signal the engine already uses,
which is the mistake this codebase has made before.

Two of them are different, and are the reason the module exists at all:

  possession   not derivable from goals or shots. A side can dominate the ball
               and score once, or sit deep and score three, and nothing in `mu`
               separates those. Carried by ESPN on 100% of finished matches.

  table        league position, points, and distance from the relegation line.
               Computable from stored results and completely absent from the
               engine — it has no concept of what a match is worth to either
               side. The season-stage effect measured earlier (+0.15 goals in
               closing rounds, consistent across 26 of 29 leagues) is the same
               family of signal and did survive measurement.

Both are candidates for a future feature. Neither is one yet, and this module
does not pretend otherwise.

EVERYTHING IS AS-OF
===================
Every tag reads only matches strictly before the fixture date, the same rule the
feature layer follows. A tag computed from the full season would describe how
the year turned out, not what was knowable on the morning of the match.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Optional

import numpy as np
import pandas as pd

from app.data import store

# Matches of history required before a tag is offered. Below this the sample
# describes noise, and a confident-looking label on four games is worse than
# no label at all.
MIN_MATCHES = 6

# Rolling window, matching the feature layer so tags and predictions describe
# the same stretch of football.
WINDOW = 10

# Thresholds are expressed in standard deviations from the league's own mean,
# so "elite attack" means elite *for this competition* rather than against some
# global scale. A 1.6-goal side is poor in the Bundesliga and ordinary in
# Argentina.
STRONG_Z = 0.75
ELITE_Z = 1.40


@dataclass
class TeamTags:
    team: str
    matches: int
    attack: Optional[str] = None
    defence: Optional[str] = None
    possession: Optional[str] = None
    form: Optional[str] = None
    table: Optional[str] = None
    # Raw values behind the labels, so a reader can check the tag rather than
    # trust it.
    detail: dict = field(default_factory=dict)

    def labels(self) -> list[str]:
        return [t for t in (self.attack, self.defence, self.possession,
                            self.form, self.table) if t]

    def __str__(self) -> str:
        return ", ".join(self.labels()) if self.labels() else "no read"


def _band(z: float, high: str, mid: str, low: str, neutral: str = "") -> str:
    if z >= ELITE_Z:
        return high
    if z >= STRONG_Z:
        return mid
    if z <= -ELITE_Z:
        return low
    if z <= -STRONG_Z:
        return neutral or low
    return ""


def _team_rows(df: pd.DataFrame, team: str, cutoff: datetime) -> pd.DataFrame:
    m = df[(df["date"] < cutoff) & ((df["home"] == team) | (df["away"] == team))]
    return m.sort_values("date").tail(WINDOW)


def _scored_conceded(rows: pd.DataFrame, team: str) -> tuple[float, float]:
    at_home = (rows["home"] == team).values
    hg = rows["hg"].fillna(0).values
    ag = rows["ag"].fillna(0).values
    scored = np.where(at_home, hg, ag)
    conceded = np.where(at_home, ag, hg)
    return float(scored.mean()), float(conceded.mean())


def _points(rows: pd.DataFrame, team: str) -> int:
    at_home = (rows["home"] == team).values
    hg = rows["hg"].fillna(0).values
    ag = rows["ag"].fillna(0).values
    own = np.where(at_home, hg, ag)
    opp = np.where(at_home, ag, hg)
    return int((own > opp).sum() * 3 + (own == opp).sum())


def _standings(df: pd.DataFrame, season: str, cutoff: datetime) -> dict[str, tuple[int, int]]:
    """
    Points and position for every team in a season, from matches before cutoff.

    Rebuilt per call rather than cached: the table on 3 March is not the table
    on 10 March, and a cached one would quietly leak later results backwards.
    """
    played = df[(df["season"] == season) & (df["date"] < cutoff)]
    if played.empty:
        return {}
    pts: dict[str, int] = {}
    for team in pd.concat([played["home"], played["away"]]).unique():
        rows = played[(played["home"] == team) | (played["away"] == team)]
        pts[team] = _points(rows, team)
    order = sorted(pts, key=lambda t: -pts[t])
    return {t: (pts[t], i + 1) for i, t in enumerate(order)}


def for_team(league_code: str, team: str, match_date: date) -> TeamTags:
    """Tags for one side, using only what was knowable before `match_date`."""
    df = store.load_results(league_code)
    if df.empty:
        return TeamTags(team=team, matches=0)

    cutoff = datetime.combine(match_date, datetime.min.time())
    rows = _team_rows(df, team, cutoff)
    n = len(rows)
    if n < MIN_MATCHES:
        return TeamTags(team=team, matches=n)

    tags = TeamTags(team=team, matches=n)

    # League scale, from the same recent window so the comparison is fair.
    recent = df[df["date"] < cutoff].tail(600)
    if recent.empty:
        return tags
    lg_scored = float((recent["hg"].fillna(0) + recent["ag"].fillna(0)).mean()) / 2
    per_team = []
    for t in pd.concat([recent["home"], recent["away"]]).unique():
        tr = recent[(recent["home"] == t) | (recent["away"] == t)]
        if len(tr) >= MIN_MATCHES:
            per_team.append(_scored_conceded(tr, t))
    if len(per_team) < 6:
        return tags
    sc_sd = float(np.std([p[0] for p in per_team])) or 0.3
    cd_sd = float(np.std([p[1] for p in per_team])) or 0.3

    scored, conceded = _scored_conceded(rows, team)
    tags.detail["scored"] = round(scored, 2)
    tags.detail["conceded"] = round(conceded, 2)

    a_z = (scored - lg_scored) / sc_sd
    d_z = (lg_scored - conceded) / cd_sd     # positive = concedes fewer
    tags.attack = _band(a_z, "elite attack", "strong attack", "poor attack", "weak attack")
    tags.defence = _band(d_z, "elite defence", "solid defence", "leaky defence", "porous defence")

    # ── Possession: the one signal not derived from goals ─────────────
    if "hpos" in df.columns:
        at_home = (rows["home"] == team).values
        hp = rows["hpos"].values.astype(float)
        ap = rows["apos"].values.astype(float)
        own = np.where(at_home, hp, ap)
        own = own[~np.isnan(own)]
        if len(own) >= MIN_MATCHES:
            pos = float(own.mean())
            tags.detail["possession"] = round(pos, 1)
            if pos >= 57:
                tags.possession = "possession-dominant"
            elif pos >= 53:
                tags.possession = "controls the ball"
            elif pos <= 43:
                tags.possession = "direct / low possession"
            elif pos <= 47:
                tags.possession = "cedes possession"

    # ── Form: results, not goals. Deliberately different from mu. ─────
    ppg = _points(rows, team) / n
    tags.detail["ppg"] = round(ppg, 2)
    if ppg >= 2.0:
        tags.form = "in form"
    elif ppg >= 1.6:
        tags.form = "good form"
    elif ppg <= 0.7:
        tags.form = "poor form"
    elif ppg <= 1.0:
        tags.form = "struggling"

    # ── Table position ────────────────────────────────────────────────
    season = df[df["date"] < cutoff]["season"].dropna()
    if len(season):
        table = _standings(df, season.iloc[-1], cutoff)
        if team in table and len(table) >= 8:
            pts, pos = table[team]
            total = len(table)
            tags.detail["position"] = pos
            tags.detail["points"] = pts
            # These positions are computed from RESULTS alone. Federations
            # dock points (CSL 2026: Shenhua sit 9th official but 4th by
            # results), so this is not always the official table — and for
            # goal modelling the results version is the meaningful one.
            # The wording says which it is, and stays league-agnostic:
            # "promotion/europe hunt" read as nonsense in a Chinese or
            # Saudi top flight.
            if pos <= max(2, total // 8):
                tags.table = f"top by results ({pos})"
            elif pos <= total // 3:
                tags.table = f"chasing the top ({pos})"
            elif pos > total - max(2, total // 8):
                tags.table = f"bottom of the results table ({pos})"
    return tags


def for_fixture(league_code: str, home: str, away: str,
                match_date: date) -> tuple[TeamTags, TeamTags]:
    return (for_team(league_code, home, match_date),
            for_team(league_code, away, match_date))
