# backend/app/admin.py
"""ATHENA Admin Dashboard — sqladmin views only."""
from fastapi import FastAPI
from sqladmin import Admin, ModelView
from sqlalchemy import Column, Integer, String, DateTime
from app.database.db import engine
from app.database.base import Base
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog


# --- Model for the stats fetch cache table (created in main.py migration) ---
class StatsFetchCache(Base):
    __tablename__ = "stats_fetch_cache"
    league_code = Column(String, primary_key=True)
    last_fetched = Column(DateTime, nullable=False)
    created_at = Column(DateTime)


# --- Admin Views ---
   
class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    name = "League Config"
    name_plural = "League Configs"
    icon = "fa-solid fa-globe"
    column_list = [LeagueConfig.id, LeagueConfig.league_code, LeagueConfig.description,
                   LeagueConfig.base_over_bias, LeagueConfig.base_under_bias,
                   LeagueConfig.tempo_factor, LeagueConfig.strength_coefficient,
                   LeagueConfig.form_delta_sensitivity]
    column_searchable_list = [LeagueConfig.league_code, LeagueConfig.description]
    column_default_sort = ("league_code", False)
    form_columns = ["league_code", "description", "display_name", "country_code",
                    "base_over_bias", "base_under_bias", "tempo_factor",
                    "safety_mode", "aggression_level", "volatility",
                    "deg_sensitivity", "det_sensitivity", "eps_sensitivity",
                    "form_delta_sensitivity",
                    "strength_coefficient"]
    can_create = True
    can_edit = True
    can_delete = False
    can_view_details = True


class TeamAdmin(ModelView, model=Team):
    name = "Team"
    name_plural = "Teams"
    icon = "fa-solid fa-shield-halved"
    column_list = [Team.id, Team.team_key, Team.display_name, Team.league_code, Team.country]
    column_searchable_list = [Team.team_key, Team.display_name, Team.league_code]
    column_default_sort = ("league_code", False)
    column_details_list = ["id", "team_key", "display_name", "league_code", "country", "aliases"]
    form_columns = ["team_key", "display_name", "league_code", "country"]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True


class TeamAliasAdmin(ModelView, model=TeamAlias):
    name = "Team Alias"
    name_plural = "Team Aliases"
    icon = "fa-solid fa-tags"
    column_list = [TeamAlias.id, TeamAlias.alias_key, "team"]
    column_searchable_list = [TeamAlias.alias_key]
    column_default_sort = ("alias_key", False)
    form_columns = ["alias_key", "team"]
    can_create = True
    can_edit = True
    can_delete = True


class TeamConfigAdmin(ModelView, model=TeamConfig):
    name = "Team Config"
    name_plural = "Team Configs"
    icon = "fa-solid fa-sliders"
    column_list = [
        TeamConfig.id, TeamConfig.league_code, TeamConfig.team,
        TeamConfig.squad_power, TeamConfig.atk_power, TeamConfig.mid_power,
        TeamConfig.def_power, TeamConfig.gk_power,
        TeamConfig.over_nudge, TeamConfig.under_nudge,
        TeamConfig.det_nudge, TeamConfig.deg_nudge,
        TeamConfig.good_form_nudge, TeamConfig.neutral_form_nudge, TeamConfig.poor_form_nudge,
    ]
    column_searchable_list = [TeamConfig.league_code, TeamConfig.team]
    column_default_sort = [("league_code", False), ("team", False)]
    form_columns = [
        "league_code", "team",
        "over_nudge", "under_nudge", "det_nudge", "deg_nudge",
        "good_form_nudge", "neutral_form_nudge", "poor_form_nudge",
        "form_good_threshold", "form_poor_threshold",
        "squad_power", "atk_power", "mid_power", "def_power", "gk_power",
    ]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True


class PlayerAdmin(ModelView, model=Player):
    name = "Player"
    name_plural = "Players"
    icon = "fa-solid fa-person-running"
    column_list = [Player.id, Player.name, Player.current_team, Player.league_code, Player.position]
    column_searchable_list = [Player.name, Player.current_team, Player.league_code]
    column_default_sort = ("name", False)
    form_columns = ["fbref_id", "name", "current_team", "league_code", "position"]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50

class PlayerMatchStatsAdmin(ModelView, model=PlayerMatchStats):
    name = "Player Match Stats"
    name_plural = "Player Match Stats"
    icon = "fa-solid fa-clock"
    column_list = [
        PlayerMatchStats.id,
        "player_name",  # we'll define this as a custom column
        PlayerMatchStats.match_date,
        PlayerMatchStats.league_code,
        PlayerMatchStats.opponent,
        PlayerMatchStats.minutes,
        PlayerMatchStats.goals,
        PlayerMatchStats.assists,
        PlayerMatchStats.xg,
        PlayerMatchStats.xa,
    ]
    column_searchable_list = [
        PlayerMatchStats.league_code,
        PlayerMatchStats.opponent,
    ]
    column_default_sort = ("match_date", True)
    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True
    page_size = 50

    async def player_name(self, instance):
        # Retrieve player name from the Player table
        db = self.session
        player = db.query(Player).filter(Player.id == instance.player_id).first()
        return player.name if player else "-"

class PlayerSeasonStatsAdmin(ModelView, model=PlayerSeasonStats):
    name = "Player Stats"
    name_plural = "Player Stats"
    icon = "fa-solid fa-chart-line"
    
    async def player_name(self, instance):
        # Retrieve player name from relationship (assuming a 'player' relationship exists)
        # If not, fallback to query. We'll use a direct query for reliability.
        db = self.session
        player = db.query(Player).filter(Player.id == instance.player_id).first()
        return player.name if player else "-"
    
    column_list = [
        PlayerSeasonStats.id,
        "player_name",  # custom column
        PlayerSeasonStats.season,
        PlayerSeasonStats.league_code,
        PlayerSeasonStats.matches_played,
        PlayerSeasonStats.minutes,
        PlayerSeasonStats.power_index,
        PlayerSeasonStats.performance_delta,
    ]
    column_searchable_list = [
        PlayerSeasonStats.league_code,
        PlayerSeasonStats.season,
        # Note: can't directly search custom column, but you could add a filter later
    ]
    column_default_sort = ("power_index", True)
    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True
    page_size = 50


class SquadSnapshotAdmin(ModelView, model=SquadSnapshot):
    name = "Squad Snapshot"
    name_plural = "Squad Snapshots"
    icon = "fa-solid fa-camera"
    column_list = [SquadSnapshot.id, SquadSnapshot.team, SquadSnapshot.league_code,
                   SquadSnapshot.snapshot_date, SquadSnapshot.squad_power]
    column_searchable_list = [SquadSnapshot.team, SquadSnapshot.league_code]
    column_default_sort = ("snapshot_date", True)
    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True


class FBrefFixtureAdmin(ModelView, model=FBrefFixture):
    name = "Fixture"
    name_plural = "Fixtures"
    icon = "fa-solid fa-calendar"
    column_list = [FBrefFixture.id, FBrefFixture.league_code, FBrefFixture.home_team,
                   FBrefFixture.away_team, FBrefFixture.match_date, FBrefFixture.match_time]
    column_searchable_list = [FBrefFixture.league_code, FBrefFixture.home_team, FBrefFixture.away_team]
    column_default_sort = ("match_date", True)
    form_columns = ["league_code", "home_team", "away_team", "match_date", "match_time"]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


class PredictionLogAdmin(ModelView, model=PredictionLog):
    name = "Prediction"
    name_plural = "Predictions"
    icon = "fa-solid fa-bullseye"
    column_list = [PredictionLog.id, PredictionLog.league_code,
                   PredictionLog.home_team, PredictionLog.away_team,
                   PredictionLog.match_date, PredictionLog.market,
                   PredictionLog.confidence, PredictionLog.status,
                   PredictionLog.actual_score, PredictionLog.variance_flag]
    column_searchable_list = [PredictionLog.league_code, PredictionLog.home_team,
                              PredictionLog.away_team, PredictionLog.status]
    column_default_sort = ("match_date", True)
    can_create = False
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


class StatsFetchCacheAdmin(ModelView, model=StatsFetchCache):
    name = "Stats Fetch Cache"
    name_plural = "Stats Fetch Cache"
    icon = "fa-solid fa-clock"
    column_list = [StatsFetchCache.league_code, StatsFetchCache.last_fetched, StatsFetchCache.created_at]
    column_searchable_list = [StatsFetchCache.league_code]
    column_default_sort = ("last_fetched", True)
    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True


def setup_admin(app: FastAPI):
    admin = Admin(app, engine, title="ATHENA Admin")
    admin.add_view(LeagueConfigAdmin)
    admin.add_view(TeamAdmin)
    admin.add_view(TeamAliasAdmin)
    admin.add_view(TeamConfigAdmin)
    admin.add_view(PlayerAdmin)
    admin.add_view(PlayerSeasonStatsAdmin)
    admin.add_view(SquadSnapshotAdmin)
    admin.add_view(FBrefFixtureAdmin)
    admin.add_view(PredictionLogAdmin)
    admin.add_view(StatsFetchCacheAdmin)
    return admin
