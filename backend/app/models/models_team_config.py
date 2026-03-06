# backend/app/models/team_config.py
"""
Per-team calibration nudges — stored per league.

over_nudge  / under_nudge:
  Additive adjustment to support_delta when this team appears in a match.
  Applied as: (home_nudge + away_nudge) / 2 → added to support_delta.
  Range: -0.05 to +0.05
  Positive = push toward over, Negative = push toward under.

Example:
  Man City  over_nudge=-0.04 (strong defense, tends to suppress goals)
  Burnley   over_nudge=+0.03 (leaky defense, tends toward high-scoring)

These are derived purely from ATHENA's historical miss/hit patterns,
NOT from subjective team quality — calibration only.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import Column, DateTime, Float, Integer, String, UniqueConstraint

from app.database.db import Base


class TeamConfig(Base):
    __tablename__ = "team_configs"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    league_code    = Column(String,  nullable=False, index=True)
    team           = Column(String,  nullable=False, index=True)

    # Calibration nudges — additive on support_delta
    over_nudge     = Column(Float, default=0.0)   # when called over
    under_nudge    = Column(Float, default=0.0)   # when called under

    # Diagnostics
    over_hit_rate  = Column(Float, default=None)  # team's over hit rate in window
    under_hit_rate = Column(Float, default=None)  # team's under hit rate in window
    over_matches   = Column(Integer, default=0)   # sample size for over nudge
    under_matches  = Column(Integer, default=0)   # sample size for under nudge

    last_calibrated = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("league_code", "team", name="uq_team_league"),
    )

    def __repr__(self):
        return (
            f"<TeamConfig {self.league_code}/{self.team} "
            f"over={self.over_nudge:+.3f} under={self.under_nudge:+.3f}>"
        )
