# backend/app/models/models_players.py
"""
ATHENA v2.0 — Player-level intelligence layer.
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column, DateTime, Date, Float, Integer, String, Text,
    UniqueConstraint, ForeignKey, Boolean
)

from app.database.base import Base


class Player(Base):
    __tablename__ = "players"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    fbref_id      = Column(String,  nullable=False, unique=True, index=True)
    name          = Column(String,  nullable=False)
    current_team  = Column(String,  nullable=True, index=True)
    league_code   = Column(String,  nullable=True, index=True)
    position      = Column(String,  nullable=True)
    date_of_birth = Column(Date,    nullable=True)
    last_scraped  = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return f"<Player {self.fbref_id} {self.name} ({self.position}) {self.current_team}>"


class PlayerSeasonStats(Base):
    __tablename__ = "player_season_stats"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    player_id       = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    season          = Column(String,  nullable=False)
    league_code     = Column(String,  nullable=False, index=True)

    matches_played  = Column(Integer, default=0)
    minutes         = Column(Integer, default=0)
    last_match_count = Column(Integer, default=0)

    # Attack
    goals_per90     = Column(Float, default=0.0)
    assists_per90   = Column(Float, default=0.0)
    xg_per90        = Column(Float, default=0.0)
    xa_per90        = Column(Float, default=0.0)
    shots_per90     = Column(Float, default=0.0)  # from shooting table
    goals_minus_xg  = Column(Float, default=0.0)  # finishing over/underperformance
    sca_per90       = Column(Float, default=0.0)

    # Midfield
    progressive_passes_per90  = Column(Float, default=0.0)
    progressive_carries_per90 = Column(Float, default=0.0)
    pass_completion_pct       = Column(Float, default=0.0)

    # Defense
    tackles_won_per90    = Column(Float, default=0.0)
    interceptions_per90  = Column(Float, default=0.0)
    blocks_per90         = Column(Float, default=0.0)
    clearances_per90     = Column(Float, default=0.0)
    aerials_won_pct      = Column(Float, default=0.0)

    # GK
    save_pct             = Column(Float, default=0.0)
    cs_pct               = Column(Float, default=0.0)
    psxg_minus_ga        = Column(Float, default=0.0)

    # Computed
    power_index        = Column(Float, default=None)
    performance_delta  = Column(Float, default=None)   # player power - team squad_power

    last_updated   = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("player_id", "season", "league_code", name="uq_player_season_league"),
    )

    def __repr__(self):
        return (f"<PlayerSeasonStats player={self.player_id} {self.season} "
                f"{self.league_code} MP={self.matches_played} power={self.power_index}>")


class SquadSnapshot(Base):
    __tablename__ = "squad_snapshots"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    team           = Column(String,  nullable=False, index=True)
    league_code    = Column(String,  nullable=False, index=True)
    snapshot_date  = Column(Date,    nullable=False, index=True)
    player_ids     = Column(Text, nullable=True)
    squad_power    = Column(Float, default=None)
    atk_power      = Column(Float, default=None)
    mid_power      = Column(Float, default=None)
    def_power      = Column(Float, default=None)
    gk_power       = Column(Float, default=None)
    created_at     = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("team", "league_code", "snapshot_date", name="uq_squad_snapshot_date"),
    )

    def __repr__(self):
        return f"<SquadSnapshot {self.league_code}/{self.team} {self.snapshot_date} squad={self.squad_power}>"


# ── NEW: PlayerMatchStats model for point-in-time reconstruction ──────────────

class PlayerMatchStats(Base):
    __tablename__ = "player_match_stats"
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    player_id = Column(Integer, ForeignKey("players.id", ondelete="CASCADE"), nullable=False, index=True)
    match_date = Column(Date, nullable=False, index=True)
    league_code = Column(String, nullable=False, index=True)
    opponent = Column(String, nullable=True)
    is_home = Column(Boolean, default=True)
    
    # Match stats
    minutes = Column(Integer, default=0)
    goals = Column(Integer, default=0)
    assists = Column(Integer, default=0)
    shots = Column(Integer, default=0)
    shots_on_target = Column(Integer, default=0)
    passes_completed = Column(Integer, default=0)
    passes_attempted = Column(Integer, default=0)
    tackles = Column(Integer, default=0)
    interceptions = Column(Integer, default=0)
    blocks = Column(Integer, default=0)
    saves = Column(Integer, default=0)
    clean_sheet = Column(Boolean, default=False)
    yellow_cards = Column(Integer, default=0)
    red_cards = Column(Integer, default=0)
    
    # Advanced stats
    xg = Column(Float, default=0.0)
    xa = Column(Float, default=0.0)
    
    created_at = Column(DateTime, default=datetime.utcnow)
    
    __table_args__ = (
        UniqueConstraint("player_id", "match_date", "league_code", name="uq_player_match"),
    )
    
    def __repr__(self):
        return (f"<PlayerMatchStats player={self.player_id} {self.match_date} "
                f"{self.league_code} mins={self.minutes}>")
