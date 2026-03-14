# backend/app/admin.py
"""
ATHENA Admin Dashboard — sqladmin configuration.

Provides admin views for all core tables with:
  - Team management with inline alias editing
  - Fixture team validation (flags unresolved teams)
  - Player/squad power browsing
  - Prediction log with status filters
"""
from fastapi import FastAPI
from sqladmin import Admin, ModelView
from app.database.db import engine
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog


# ── League Config ────────────────────────────────────────────────────────────

class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    name = "League Config"
    name_plural = "League Configs"
    icon = "fa-solid fa-globe"

    column_list = [
        LeagueConfig.id,
        LeagueConfig.league_code,
        LeagueConfig.description,
        LeagueConfig.base_over_bias,
        LeagueConfig.base_under_bias,
        LeagueConfig.tempo_factor,
        LeagueConfig.strength_coefficient,
    ]
    column_searchable_list = [LeagueConfig.league_code, LeagueConfig.description]
    column_sortable_list = [LeagueConfig.league_code, LeagueConfig.strength_coefficient]
    column_default_sort = ("league_code", False)

    form_columns = [
        "league_code", "description", "display_name", "country_code",
        "base_over_bias", "base_under_bias", "tempo_factor",
        "safety_mode", "aggression_level", "volatility",
        "deg_sensitivity", "det_sensitivity", "eps_sensitivity",
        "strength_coefficient",
    ]

    can_create = True
    can_edit = True
    can_delete = False
    can_view_details = True


# ── Team (Master) ────────────────────────────────────────────────────────────

class TeamAdmin(ModelView, model=Team):
    name = "Team"
    name_plural = "Teams"
    icon = "fa-solid fa-shield-halved"

    column_list = [
        Team.id,
        Team.team_key,
        Team.display_name,
        Team.league_code,
        Team.country,
    ]
    column_searchable_list = [Team.team_key, Team.display_name, Team.league_code]
    column_sortable_list = [Team.team_key, Team.league_code, Team.display_name]
    column_default_sort = ("league_code", False)

    # Detail view shows aliases — use this to verify
    column_details_list = [
        "id", "team_key", "display_name", "league_code", "country", "aliases",
    ]

    # Edit form includes aliases for inline management
    # Click Edit → scroll to Aliases → add/remove aliases directly
    form_columns = ["team_key", "display_name", "league_code", "country", "aliases"]

    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True


class TeamAliasAdmin(ModelView, model=TeamAlias):
    """
    Manage team name aliases here. To add an alias:
    1. Click 'Create'
    2. Type the alias_key (normalised name, e.g. 'fc fredericia')
    3. Select the team it belongs to
    4. Save

    This is the correct place to manage aliases — NOT the Team edit page.
    """
    name = "Team Alias"
    name_plural = "Team Aliases"
    icon = "fa-solid fa-tags"

    column_list = [
        TeamAlias.id,
        TeamAlias.alias_key,
        "team",
    ]
    column_searchable_list = [TeamAlias.alias_key]
    column_sortable_list = [TeamAlias.alias_key]
    column_default_sort = ("alias_key", False)

    form_columns = ["alias_key", "team"]

    can_create = True
    can_edit = True
    can_delete = True


# ── Team Config (Calibration) ────────────────────────────────────────────────

class TeamConfigAdmin(ModelView, model=TeamConfig):
    name = "Team Config"
    name_plural = "Team Configs"
    icon = "fa-solid fa-sliders"

    column_list = [
        TeamConfig.id,
        TeamConfig.league_code,
        TeamConfig.team,
        TeamConfig.squad_power,
        TeamConfig.atk_power,
        TeamConfig.mid_power,
        TeamConfig.def_power,
        TeamConfig.gk_power,
        TeamConfig.over_nudge,
        TeamConfig.under_nudge,
    ]
    column_searchable_list = [TeamConfig.league_code, TeamConfig.team]
    column_sortable_list = [
        TeamConfig.league_code, TeamConfig.team, TeamConfig.squad_power,
    ]
    column_default_sort = [("league_code", False), ("team", False)]

    form_columns = [
        "league_code", "team",
        "over_nudge", "under_nudge", "det_nudge", "deg_nudge",
        "squad_power", "atk_power", "mid_power", "def_power", "gk_power",
    ]

    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True


# ── Player ───────────────────────────────────────────────────────────────────

class PlayerAdmin(ModelView, model=Player):
    name = "Player"
    name_plural = "Players"
    icon = "fa-solid fa-person-running"

    column_list = [
        Player.id,
        Player.name,
        Player.current_team,
        Player.league_code,
        Player.position,
        Player.fbref_id,
    ]
    column_searchable_list = [Player.name, Player.current_team, Player.league_code]
    column_sortable_list = [Player.name, Player.current_team, Player.league_code, Player.position]
    column_default_sort = ("name", False)

    form_columns = [
        "fbref_id", "name", "current_team", "league_code", "position",
    ]

    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


# ── Player Season Stats ─────────────────────────────────────────────────────

class PlayerSeasonStatsAdmin(ModelView, model=PlayerSeasonStats):
    name = "Player Stats"
    name_plural = "Player Stats"
    icon = "fa-solid fa-chart-line"

    column_list = [
        PlayerSeasonStats.id,
        PlayerSeasonStats.player_id,
        PlayerSeasonStats.season,
        PlayerSeasonStats.league_code,
        PlayerSeasonStats.matches_played,
        PlayerSeasonStats.minutes,
        PlayerSeasonStats.power_index,
        PlayerSeasonStats.performance_delta,
    ]
    column_searchable_list = [PlayerSeasonStats.league_code, PlayerSeasonStats.season]
    column_sortable_list = [
        PlayerSeasonStats.matches_played, PlayerSeasonStats.power_index,
        PlayerSeasonStats.performance_delta, PlayerSeasonStats.league_code,
    ]
    column_default_sort = ("power_index", True)

    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True
    page_size = 50


# ── Squad Snapshot ───────────────────────────────────────────────────────────

class SquadSnapshotAdmin(ModelView, model=SquadSnapshot):
    name = "Squad Snapshot"
    name_plural = "Squad Snapshots"
    icon = "fa-solid fa-camera"

    column_list = [
        SquadSnapshot.id,
        SquadSnapshot.team,
        SquadSnapshot.league_code,
        SquadSnapshot.snapshot_date,
        SquadSnapshot.squad_power,
        SquadSnapshot.atk_power,
        SquadSnapshot.def_power,
    ]
    column_searchable_list = [SquadSnapshot.team, SquadSnapshot.league_code]
    column_sortable_list = [
        SquadSnapshot.team, SquadSnapshot.league_code,
        SquadSnapshot.snapshot_date, SquadSnapshot.squad_power,
    ]
    column_default_sort = ("snapshot_date", True)

    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True


# ── FBref Fixtures ───────────────────────────────────────────────────────────

class FBrefFixtureAdmin(ModelView, model=FBrefFixture):
    name = "Fixture"
    name_plural = "Fixtures"
    icon = "fa-solid fa-calendar"

    column_list = [
        FBrefFixture.id,
        FBrefFixture.league_code,
        FBrefFixture.home_team,
        FBrefFixture.away_team,
        FBrefFixture.match_date,
        FBrefFixture.match_time,
    ]
    column_searchable_list = [
        FBrefFixture.league_code, FBrefFixture.home_team, FBrefFixture.away_team,
    ]
    column_sortable_list = [
        FBrefFixture.league_code, FBrefFixture.match_date,
        FBrefFixture.home_team, FBrefFixture.away_team,
    ]
    column_default_sort = ("match_date", True)

    form_columns = [
        "league_code", "home_team", "away_team", "match_date", "match_time",
    ]

    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


# ── Prediction Log ───────────────────────────────────────────────────────────

class PredictionLogAdmin(ModelView, model=PredictionLog):
    name = "Prediction"
    name_plural = "Predictions"
    icon = "fa-solid fa-bullseye"

    column_list = [
        PredictionLog.id,
        PredictionLog.league_code,
        PredictionLog.home_team,
        PredictionLog.away_team,
        PredictionLog.match_date,
        PredictionLog.market,
        PredictionLog.confidence,
        PredictionLog.status,
        PredictionLog.actual_score,
        PredictionLog.variance_flag,
    ]
    column_searchable_list = [
        PredictionLog.league_code, PredictionLog.home_team,
        PredictionLog.away_team, PredictionLog.market, PredictionLog.status,
    ]
    column_sortable_list = [
        PredictionLog.match_date, PredictionLog.league_code,
        PredictionLog.status, PredictionLog.confidence,
    ]
    column_default_sort = ("match_date", True)

    can_create = False
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


# ── Setup ────────────────────────────────────────────────────────────────────

def setup_admin(app: FastAPI):
    admin = Admin(
        app,
        engine,
        title="ATHENA Admin",
    )
    admin.add_view(LeagueConfigAdmin)
    admin.add_view(TeamAdmin)
    admin.add_view(TeamAliasAdmin)
    admin.add_view(TeamConfigAdmin)
    admin.add_view(PlayerAdmin)
    admin.add_view(PlayerSeasonStatsAdmin)
    admin.add_view(SquadSnapshotAdmin)
    admin.add_view(FBrefFixtureAdmin)
    admin.add_view(PredictionLogAdmin)
    return admin
