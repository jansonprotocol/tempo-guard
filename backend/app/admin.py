from fastapi import FastAPI
from sqladmin import Admin, ModelView
from app.database.db import engine
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog

class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    column_list = ["id", "league_code", "base_over_bias", "base_under_bias", "tempo_factor"]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True

class TeamAdmin(ModelView, model=Team):
    column_list = ["id", "team_key", "display_name", "league_code", "country"]
    can_create = True
    can_edit = True
    can_delete = True

class TeamAliasAdmin(ModelView, model=TeamAlias):
    column_list = ["id", "alias_key", "team_id"]
    can_create = True
    can_edit = True
    can_delete = True

class TeamConfigAdmin(ModelView, model=TeamConfig):
    column_list = ["id", "league_code", "team", "over_nudge", "under_nudge", "squad_power"]
    can_create = True
    can_edit = True
    can_delete = True

class PlayerAdmin(ModelView, model=Player):
    column_list = ["id", "name", "current_team", "league_code", "position"]
    can_create = True
    can_edit = True
    can_delete = True

class PlayerSeasonStatsAdmin(ModelView, model=PlayerSeasonStats):
    column_list = ["id", "player_id", "season", "league_code", "matches_played", "power_index"]
    can_create = True
    can_edit = True
    can_delete = True

class SquadSnapshotAdmin(ModelView, model=SquadSnapshot):
    column_list = ["id", "team", "league_code", "snapshot_date", "squad_power"]
    can_create = True
    can_edit = True
    can_delete = True

class FBrefFixtureAdmin(ModelView, model=FBrefFixture):
    column_list = ["id", "league_code", "home_team", "away_team", "match_date", "match_time"]
    can_create = True
    can_edit = True
    can_delete = True

class PredictionLogAdmin(ModelView, model=PredictionLog):
    column_list = ["id", "league_code", "home_team", "away_team", "match_date", "market", "status"]
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
