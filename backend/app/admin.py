from fastapi import FastAPI
from sqladmin import Admin, ModelView
from sqladmin.fields import Select2TagsField  # Use this instead of Select2TagsWidget
from wtforms import Field
from app.database.db import engine
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog


class TeamAdmin(ModelView, model=Team):
    column_list = ["id", "team_key", "display_name", "league_code", "country", "alias_list"]
    column_searchable_list = ["team_key", "display_name", "league_code", "country"]
    column_filters = ["league_code", "country"]
    column_default_sort = [("team_key", False)]
    
    # Show aliases as a comma-separated list
    async def alias_list(self, instance):
        if instance.aliases:
            return ", ".join([a.alias_key for a in instance.aliases][:5]) + ("..." if len(instance.aliases) > 5 else "")
        return "-"
    
    # Form configuration
    form_columns = ["team_key", "display_name", "league_code", "country"]
    
    can_create = True
    can_edit = True
    can_delete = True


class TeamAliasAdmin(ModelView, model=TeamAlias):
    column_list = ["id", "alias_key", "team"]
    column_searchable_list = ["alias_key", "team__display_name", "team__league_code"]
    column_filters = ["team__league_code"]
    column_default_sort = [("alias_key", False)]
    
    # Form configuration for easy alias creation
    form_columns = ["alias_key", "team"]
    
    can_create = True
    can_edit = True
    can_delete = True


# ... (rest of your admin classes remain the same)


class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    column_list = ["id", "league_code", "base_over_bias", "base_under_bias", "tempo_factor"]
    column_searchable_list = ["league_code"]
    column_filters = []  # Empty for now
    column_default_sort = [("league_code", False)]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True


class TeamConfigAdmin(ModelView, model=TeamConfig):
    column_list = ["id", "league_code", "team", "over_nudge", "under_nudge", "squad_power"]
    column_searchable_list = ["league_code", "team"]
    column_filters = []  # Empty for now
    column_default_sort = [("league_code", False), ("team", False)]
    can_create = True
    can_edit = True
    can_delete = True


class PlayerAdmin(ModelView, model=Player):
    column_list = ["id", "name", "current_team", "league_code", "position"]
    column_searchable_list = ["name", "current_team", "league_code"]
    column_filters = []  # Empty for now
    column_default_sort = [("name", False)]
    can_create = True
    can_edit = True
    can_delete = True


class PlayerSeasonStatsAdmin(ModelView, model=PlayerSeasonStats):
    column_list = ["id", "player_id", "season", "league_code", "matches_played", "power_index"]
    column_searchable_list = ["league_code", "season"]
    column_filters = []  # Empty for now
    column_default_sort = [("season", True), ("matches_played", True)]
    can_create = True
    can_edit = True
    can_delete = True


class SquadSnapshotAdmin(ModelView, model=SquadSnapshot):
    column_list = ["id", "team", "league_code", "snapshot_date", "squad_power"]
    column_searchable_list = ["team", "league_code"]
    column_filters = []  # Empty for now
    column_default_sort = [("snapshot_date", True)]
    can_create = True
    can_edit = True
    can_delete = True


class FBrefFixtureAdmin(ModelView, model=FBrefFixture):
    column_list = ["id", "league_code", "home_team", "away_team", "match_date", "match_time", "round_type"]
    column_searchable_list = ["league_code", "home_team", "away_team", "round_type"]
    column_filters = []  # Empty for now
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
    column_filters = []  # Empty for now
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
