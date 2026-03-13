from fastapi import FastAPI
from sqladmin import Admin, ModelView
from app.database.db import engine
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog
from sqladmin.widgets import Select2TagsWidget  # Add this import at the top


class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    column_list = ["id", "league_code", "base_over_bias", "base_under_bias", "tempo_factor"]
    column_searchable_list = ["league_code"]
    column_filters = []  # Empty for now
    column_default_sort = [("league_code", False)]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True

class TeamAdmin(ModelView, model=Team):
    column_list = ["id", "team_key", "display_name", "league_code", "country", "alias_count"]
    column_searchable_list = ["team_key", "display_name", "league_code", "country"]
    column_filters = ["league_code", "country"]
    column_default_sort = [("team_key", False)]
    
    # Add a custom column to show alias count
    async def alias_count(self, instance):
        return len(instance.aliases) if instance.aliases else 0
    alias_count.column_labels = "Aliases"
    
    # Form configuration for editing
    form_columns = ["team_key", "display_name", "league_code", "country", "aliases"]
    
    # Use a better widget for aliases - searchable multi-select
    form_overrides = {
        "aliases": Select2TagsWidget  # This makes aliases searchable
    }
    
    # Allow adding new aliases directly in the form
    form_args = {
        "aliases": {
            "render_kw": {
                "placeholder": "Type to search or add new aliases...",
                "data-tags": "true",  # Allow creating new tags
                "data-token-separators": "[',']"  # Separate by comma
            }
        }
    }
    
    can_create = True
    can_edit = True
    can_delete = True
    
    # Inline view for aliases (alternative approach)
    inline_models = [TeamAlias]



class TeamAliasAdmin(ModelView, model=TeamAlias):
    column_list = ["id", "alias_key", "team_id"]
    column_searchable_list = ["alias_key"]
    column_filters = []  # Empty for now
    column_default_sort = [("alias_key", False)]
    can_create = True
    can_edit = True
    can_delete = True


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
