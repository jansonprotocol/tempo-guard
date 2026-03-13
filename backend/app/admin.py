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
    column_searchable_list = ["id", "league_code", "base_over_bias", "base_under_bias", "tempo_factor"]
    column_default_sort = [("league_code", False)]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True

class TeamAdmin(ModelView, model=Team):
    column_list = ["id", "team_key", "display_name", "league_code", "country"]
    column_searchable_list = ["id", "team_key", "display_name", "league_code", "country"]
    column_default_sort = [("league_code", False)]
    can_create = True
    can_edit = True
    can_delete = True

from sqladmin.actions import Action
from sqlalchemy import text

    async def merge_teams(self, request):
        """Custom action to merge selected teams."""
        pks = request.query_params.get("pks", "").split(",")
        if len(pks) != 2:
            return JSONResponse({"error": "Select exactly two teams to merge"}, status_code=400)

        team_a, team_b = await self.get_objects(pks)
        # Determine master (e.g., shorter name)
        master = team_a if len(team_a.display_name) <= len(team_b.display_name) else team_b
        variant = team_b if master == team_a else team_a

        # Perform merge in all tables (similar to your fix_duplicates.py)
        async with self.session as db:
            # Update players
            await db.execute(
                text("UPDATE players SET current_team = :master WHERE current_team = :variant AND league_code = :league"),
                {"master": master.team_key, "variant": variant.team_key, "league": master.league_code}
            )
            # Update fixtures
            await db.execute(
                text("UPDATE fbref_fixtures SET home_team = :master WHERE home_team = :variant AND league_code = :league"),
                {"master": master.team_key, "variant": variant.team_key, "league": master.league_code}
            )
            # ... similar for away_team, team_configs, squad_snapshots ...

            # Delete the variant team
            await db.delete(variant)
            await db.commit()

        return JSONResponse({"success": f"Merged {variant.display_name} into {master.display_name}"})

    # Register the action
    actions = [Action(
        name="merge_teams",
        label="Merge Selected Teams",
        add_in_list=True,
        callback=merge_teams,
        confirmation="Merge the two selected teams? This cannot be undone."
    )]

class TeamAliasAdmin(ModelView, model=TeamAlias):
    column_list = ["id", "alias_key", "team_key", "team_id"]
    column_searchable_list = ["id", "alias_key", "team_id"]
    column_default_sort = [("team_key", False)]
    can_create = True
    can_edit = True
    can_delete = True

class TeamConfigAdmin(ModelView, model=TeamConfig):
    column_list = ["id", "league_code", "team", "over_nudge", "under_nudge", "squad_power"]
    column_searchable_list = ["id", "league_code", "team", "over_nudge", "under_nudge", "squad_power"]
    column_default_sort = [("team", False)]
    can_create = True
    can_edit = True
    can_delete = True

class PlayerAdmin(ModelView, model=Player):
    column_list = ["id", "name", "current_team", "league_code", "position"]
    column_searchable_list = ["id", "name", "current_team", "league_code", "position"]
    column_default_sort = [("current_team", False)]
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
    column_searchable_list = ["league_code", "home_team", "away_team", "match_date", "match_time"]
    column_default_sort = [("match_date", True)]
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
