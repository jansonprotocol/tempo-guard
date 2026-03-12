# backend/app/models/team_config.py
"""
Per-team calibration nudges — stored per league.

over_nudge  / under_nudge:
  Additive adjustment to support_delta when this team appears in a match.
  Applied as: (home_nudge + away_nudge) / 2 → added to support_delta.
  Range: -0.05 to +0.05
  Positive = push toward over, Negative = push toward under.

det_nudge:
  Per-team adjustment to the raw DET (Detonation) score.
  Applied individually to home_det and away_det before the pipeline runs.
  Range: -0.15 to +0.15
  Positive = this team is more volatile than the model computes (e.g. Man City,
             Liverpool — top attackers, high variance output). Pushing det up
             makes BILATERAL_CHAOS_ESCALATOR fire more easily when they meet
             other volatile teams.
  Negative = this team suppresses volatility (e.g. low-block, structured teams).

deg_nudge:
  Per-team adjustment to the raw DEG (Degradation) pressure.
  Applied to the combined deg_pressure before the pipeline runs.
  Range: -0.10 to +0.10
  Positive = this team's decline is more severe than rolling averages show
             (often needed for newly relegated sides, rotation-heavy squads).
  Negative = this team is more resilient than short-term form suggests.

squad_power / atk_power / mid_power / def_power / gk_power:
  (v2.0) Player-derived zonal strength scores on a 0–100 scale.
  Written by player_index.py after scrape_players.py populates
  PlayerSeasonStats. These are the CURRENT squad assessment —
  historical values live in SquadSnapshot for backtesting.

  squad_power = weighted blend: ATK×0.30 + MID×0.25 + DEF×0.30 + GK×0.15

  Used in fbref_base.py to compute player_power_composite, which feeds
  into support_delta at a configurable blend weight (default 30%).

  None = not yet computed (player data not scraped for this team).
  When None, the pipeline falls back to macro-only features (v1.x behaviour).

Example:
  Man City  over_nudge=-0.04  det_nudge=+0.12  deg_nudge=-0.05
    → Tends to suppress total goals (strong defense) but is individually volatile.
    → When paired with another high-det team, bilateral chaos fires.

  Burnley   over_nudge=+0.03  det_nudge=+0.04  deg_nudge=+0.06
    → Leaky defense, consistent structural decline across seasons.

These are derived purely from ATHENA's historical miss/hit patterns,
NOT from subjective team quality — calibration only.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, UniqueConstraint

from app.database.base import Base


class TeamConfig(Base):
    __tablename__ = "team_configs"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    league_code = Column(String,  nullable=False, index=True)
    team        = Column(String,  nullable=False, index=True)

    # ── Support delta nudges (existing) ───────────────────────────────
    over_nudge  = Column(Float, default=0.0)   # additive on support_delta (over calls)
    under_nudge = Column(Float, default=0.0)   # additive on support_delta (under calls)

    # ── Module-level nudges (existing) ────────────────────────────────
    det_nudge   = Column(Float, default=0.0)   # adjustment to this team's DET score
    deg_nudge   = Column(Float, default=0.0)   # adjustment to this team's DEG pressure

    # ── Diagnostics (existing) ────────────────────────────────────────
    over_hit_rate   = Column(Float,   default=None)
    under_hit_rate  = Column(Float,   default=None)
    over_matches    = Column(Integer, default=0)
    under_matches   = Column(Integer, default=0)
    avg_det         = Column(Float,   default=None)  # team's historical avg DET
    avg_deg         = Column(Float,   default=None)  # team's historical avg DEG

    # ── v2.0: Player-derived squad power scores (0–100) ──────────────
    # Written by player_index.py after player data is scraped.
    # None = not yet computed → pipeline uses macro-only features.
    squad_power     = Column(Float, default=None)
    atk_power       = Column(Float, default=None)
    mid_power       = Column(Float, default=None)
    def_power       = Column(Float, default=None)
    gk_power        = Column(Float, default=None)

    last_calibrated = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("league_code", "team", name="uq_team_league"),
    )

    def __repr__(self):
        return (
            f"<TeamConfig {self.league_code}/{self.team} "
            f"over={self.over_nudge:+.3f} under={self.under_nudge:+.3f} "
            f"det={self.det_nudge:+.3f} deg={self.deg_nudge:+.3f} "
            f"squad={self.squad_power}>"
        )
