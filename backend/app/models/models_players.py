# backend/app/models/models_players.py
"""
ATHENA v2.0 — Player-level intelligence layer.

Three tables that sit alongside the existing team_configs / league_configs
without touching any existing table definitions.

Player
  One row per player. Keyed on fbref_id (the hex slug in their FBref URL).
  Example: Mo Salah = e06683e8 → fbref.com/en/players/e06683e8/...

  position uses a simple 4-value enum: GK / DEF / MID / FWD.
  FBref lists positions as "FW", "MF", "DF", "GK" — the scraper normalises
  these to our 3-letter codes on insert.

PlayerSeasonStats
  One row per player per season per league.  Stores aggregated per-90 stats
  rather than individual match rows — this is the 10-match interval design.

  The scraper fetches the squad page, reads the player's matches_played,
  and only writes/updates this row when matches_played has increased by ≥10
  since last_match_count.  This keeps the table small (~6,000 rows for the
  top 6 leagues) while still capturing meaningful form shifts.

  Stats are stored as per-90 values (FBref's native unit).  The power index
  engine (player_index.py, Phase 2) converts these into z-scores → 0–100.

  Zone coverage:
    Attack:    goals, assists, xG, xA, shot-creating actions
    Midfield:  progressive passes, progressive carries, pass completion %
    Defense:   tackles won, interceptions, blocks, clearances, aerial win %
    GK:        save %, clean sheet %, post-shot xG minus goals allowed

SquadSnapshot
  Freezes a team's squad composition + power scores at a specific date.
  Written every time scrape_players.py runs (one snapshot per team per day).

  player_ids is a JSON-encoded list of Player.id values — the full squad
  at that point in time.  Zonal power scores are pre-computed so the
  calibration engine doesn't need to re-derive them during backtesting.

  The calibration loop (routes_calibration.py) uses the most recent snapshot
  with snapshot_date ≤ match_date to get point-in-time squad strength.
  If no snapshot exists, it falls back to current TeamConfig values
  (identical to v1.x behaviour — zero regression).
"""
from __future__ import annotations

from datetime import datetime

from sqlalchemy import (
    Column, DateTime, Date, Float, Integer, String, Text,
    UniqueConstraint, ForeignKey,
)

from app.database.base import Base


# ── Player ───────────────────────────────────────────────────────────────────

class Player(Base):
    __tablename__ = "players"

    id            = Column(Integer, primary_key=True, autoincrement=True)
    fbref_id      = Column(String,  nullable=False, unique=True, index=True)
    name          = Column(String,  nullable=False)
    current_team  = Column(String,  nullable=True, index=True)
    league_code   = Column(String,  nullable=True, index=True)
    position      = Column(String,  nullable=True)       # GK / DEF / MID / FWD
    date_of_birth = Column(Date,    nullable=True)
    last_scraped  = Column(DateTime, default=datetime.utcnow)

    def __repr__(self):
        return (
            f"<Player {self.fbref_id} {self.name} "
            f"({self.position}) {self.current_team}>"
        )


# ── PlayerSeasonStats ────────────────────────────────────────────────────────

class PlayerSeasonStats(Base):
    __tablename__ = "player_season_stats"

    id              = Column(Integer, primary_key=True, autoincrement=True)
    player_id       = Column(Integer, ForeignKey("players.id"), nullable=False, index=True)
    season          = Column(String,  nullable=False)   # e.g. "2025-2026" or "2026"
    league_code     = Column(String,  nullable=False, index=True)

    # ── Appearance counters ───────────────────────────────────────────
    matches_played  = Column(Integer, default=0)
    minutes         = Column(Integer, default=0)

    # ── Scraper bookkeeping ───────────────────────────────────────────
    # last_match_count: the matches_played value when stats were last
    # written.  The scraper only updates this row when the new
    # matches_played >= last_match_count + 10 (the 10-match interval).
    last_match_count = Column(Integer, default=0)

    # ── Attack stats (per 90) ─────────────────────────────────────────
    goals_per90     = Column(Float, default=0.0)
    assists_per90   = Column(Float, default=0.0)
    xg_per90        = Column(Float, default=0.0)
    xa_per90        = Column(Float, default=0.0)
    sca_per90       = Column(Float, default=0.0)   # shot-creating actions

    # ── Midfield stats (per 90) ───────────────────────────────────────
    progressive_passes_per90  = Column(Float, default=0.0)
    progressive_carries_per90 = Column(Float, default=0.0)
    pass_completion_pct       = Column(Float, default=0.0)   # 0–100 scale

    # ── Defensive stats (per 90) ──────────────────────────────────────
    tackles_won_per90    = Column(Float, default=0.0)
    interceptions_per90  = Column(Float, default=0.0)
    blocks_per90         = Column(Float, default=0.0)
    clearances_per90     = Column(Float, default=0.0)
    aerials_won_pct      = Column(Float, default=0.0)   # 0–100 scale

    # ── Goalkeeper stats ──────────────────────────────────────────────
    save_pct             = Column(Float, default=0.0)   # 0–100 scale
    cs_pct               = Column(Float, default=0.0)   # clean sheet % (0–100)
    psxg_minus_ga        = Column(Float, default=0.0)   # post-shot xG minus goals allowed

    # ── Computed power index (written by player_index.py) ─────────────
    power_index    = Column(Float, default=None)   # 0–100, None = not yet computed
    performance_delta = Column(Float, default=None)

    # ── Meta ──────────────────────────────────────────────────────────
    last_updated   = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "player_id", "season", "league_code",
            name="uq_player_season_league",
        ),
    )

    def __repr__(self):
        return (
            f"<PlayerSeasonStats player={self.player_id} "
            f"{self.season} {self.league_code} "
            f"MP={self.matches_played} power={self.power_index}>"
        )


# ── SquadSnapshot ────────────────────────────────────────────────────────────

class SquadSnapshot(Base):
    __tablename__ = "squad_snapshots"

    id             = Column(Integer, primary_key=True, autoincrement=True)
    team           = Column(String,  nullable=False, index=True)
    league_code    = Column(String,  nullable=False, index=True)
    snapshot_date  = Column(Date,    nullable=False, index=True)

    # JSON-encoded list of Player.id values at this point in time.
    # e.g. "[12, 45, 67, 89, ...]"
    # Using Text instead of JSON type for SQLite/Postgres compatibility.
    player_ids     = Column(Text, nullable=True)

    # ── Pre-computed zonal power scores (0–100) ──────────────────────
    # Written by player_index.py → calculate_squad_power().
    # These are the values the calibration engine reads during backtesting
    # so it doesn't need to recompute from individual player stats.
    squad_power    = Column(Float, default=None)
    atk_power      = Column(Float, default=None)
    mid_power      = Column(Float, default=None)
    def_power      = Column(Float, default=None)
    gk_power       = Column(Float, default=None)

    # ── Meta ──────────────────────────────────────────────────────────
    created_at     = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint(
            "team", "league_code", "snapshot_date",
            name="uq_squad_snapshot_date",
        ),
    )

    def __repr__(self):
        return (
            f"<SquadSnapshot {self.league_code}/{self.team} "
            f"{self.snapshot_date} squad={self.squad_power}>"
        )
