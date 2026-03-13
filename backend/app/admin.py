from fastapi import FastAPI
from sqladmin import Admin, ModelView
from sqladmin.filters import ColumnFilter
from app.database.db import engine
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog


class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    column_list = ["id", "league_code", "base_over_bias", "base_under_bias", "tempo_factor"]
    column_searchable_list = ["league_code"]
    column_filters = ["league_code"]
    column_default_sort = [("league_code", False)]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True


class TeamAdmin(ModelView, model=Team):
    column_list = ["id", "team_key", "display_name", "league_code", "country"]
    column_searchable_list = ["team_key", "display_name", "league_code"]
    column_filters = ["league_code", "country"]
    column_default_sort = [("team_key", False)]
    can_create = True
    can_edit = True
    can_delete = True


class TeamAliasAdmin(ModelView, model=TeamAlias):
    column_list = ["id", "alias_key", "team"]
    column_searchable_list = ["alias_key"]
    # Fixed: Use ColumnFilter for relationship filtering
    column_filters = [
        "alias_key",  # simple column filter
        ColumnFilter(
            column=TeamAlias.team,
            name="team__league_code",
            options={"field": Team.league_code}
        )
    ]
    column_default_sort = [("alias_key", False)]
    can_create = True
    can_edit = True
    can_delete = True


class TeamConfigAdmin(ModelView, model=TeamConfig):
    column_list = ["id", "league_code", "team", "over_nudge", "under_nudge", "squad_power"]
    column_searchable_list = ["league_code", "team"]
    column_filters = ["league_code"]
    column_default_sort = [("league_code", False), ("team", False)]
    can_create = True
    can_edit = True
    can_delete = True


class PlayerAdmin(ModelView, model=Player):
    column_list = ["id", "name", "current_team", "league_code", "position"]
    column_searchable_list = ["name", "current_team", "league_code"]
    column_filters = ["league_code", "position"]
    column_default_sort = [("name", False)]
    can_create = True
    can_edit = True
    can_delete = True


class PlayerSeasonStatsAdmin(ModelView, model=PlayerSeasonStats):
    column_list = ["id", "player_id", "season", "league_code", "matches_played", "power_index"]
    column_searchable_list = ["league_code", "season"]
    column_filters = ["league_code", "season"]
    column_default_sort = [("season", True), ("matches_played", True)]
    can_create = True
    can_edit = True
    can_delete = True


class SquadSnapshotAdmin(ModelView, model=SquadSnapshot):
    column_list = ["id", "team", "league_code", "snapshot_date", "squad_power"]
    column_searchable_list = ["team", "league_code"]
    column_filters = ["league_code", "snapshot_date"]
    column_default_sort = [("snapshot_date", True)]
    can_create = True
    can_edit = True
    can_delete = True


class FBrefFixtureAdmin(ModelView, model=FBrefFixture):
    column_list = ["id", "league_code", "home_team", "away_team", "match_date", "match_time", "round_type"]
    column_searchable_list = ["league_code", "home_team", "away_team", "round_type"]
    column_filters = ["league_code", "match_date", "round_type"]
    column_default_sort = [("match_date", True)]
    can_create = True
    can_edit = True
    can_delete = True


class PredictionLogAdmin(ModelView, model=PredictionLog):
    column_list = [
        "id", "league_code", "home_team", "away_team", 
        "match_date", "market", "status", "variance_flag"
    ]
    column_searchable_list = ["league_code", "home_team", "away_team", "market", "status"]
    column_filters = ["league_code", "status", "variance_flag", "match_date"]
    column_default_sort = [("match_date", True)]
    can_create = True
    can_edit = True
    can_delete = True


def setup_admin(app: FastAPI):
    admin = Admin(app, engine)
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
