# backend/app/admin.py
"""
ATHENA Admin Dashboard — sqladmin + custom alias manager.
"""
from fastapi import FastAPI, Request, Depends, Form
from fastapi.responses import HTMLResponse, RedirectResponse
from sqladmin import Admin, ModelView
from sqlalchemy.orm import Session
from app.database.db import engine, SessionLocal
from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.database.models_predictions import FBrefFixture, PredictionLog
from app.util.text_norm import normalize_team


def _get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ══════════════════════════════════════════════════════════════════════════════
# SQLADMIN MODEL VIEWS
# ══════════════════════════════════════════════════════════════════════════════

class LeagueConfigAdmin(ModelView, model=LeagueConfig):
    name = "League Config"
    name_plural = "League Configs"
    icon = "fa-solid fa-globe"
    column_list = [
        LeagueConfig.id, LeagueConfig.league_code, LeagueConfig.description,
        LeagueConfig.base_over_bias, LeagueConfig.base_under_bias,
        LeagueConfig.tempo_factor, LeagueConfig.strength_coefficient,
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


class TeamAdmin(ModelView, model=Team):
    name = "Team"
    name_plural = "Teams"
    icon = "fa-solid fa-shield-halved"
    column_list = [
        Team.id, Team.team_key, Team.display_name, Team.league_code, Team.country,
    ]
    column_searchable_list = [Team.team_key, Team.display_name, Team.league_code]
    column_sortable_list = [Team.team_key, Team.league_code, Team.display_name]
    column_default_sort = ("league_code", False)
    column_details_list = [
        "id", "team_key", "display_name", "league_code", "country", "aliases",
    ]
    # No aliases in form — use /alias-manager instead (link shown in list)
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
    column_sortable_list = [TeamAlias.alias_key]
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
    ]
    column_searchable_list = [TeamConfig.league_code, TeamConfig.team]
    column_sortable_list = [TeamConfig.league_code, TeamConfig.team, TeamConfig.squad_power]
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


class PlayerAdmin(ModelView, model=Player):
    name = "Player"
    name_plural = "Players"
    icon = "fa-solid fa-person-running"
    column_list = [
        Player.id, Player.name, Player.current_team,
        Player.league_code, Player.position, Player.fbref_id,
    ]
    column_searchable_list = [Player.name, Player.current_team, Player.league_code]
    column_sortable_list = [Player.name, Player.current_team, Player.league_code]
    column_default_sort = ("name", False)
    form_columns = ["fbref_id", "name", "current_team", "league_code", "position"]
    can_create = True
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


class PlayerSeasonStatsAdmin(ModelView, model=PlayerSeasonStats):
    name = "Player Stats"
    name_plural = "Player Stats"
    icon = "fa-solid fa-chart-line"
    column_list = [
        PlayerSeasonStats.id, PlayerSeasonStats.player_id,
        PlayerSeasonStats.season, PlayerSeasonStats.league_code,
        PlayerSeasonStats.matches_played, PlayerSeasonStats.minutes,
        PlayerSeasonStats.power_index, PlayerSeasonStats.performance_delta,
    ]
    column_searchable_list = [PlayerSeasonStats.league_code, PlayerSeasonStats.season]
    column_sortable_list = [
        PlayerSeasonStats.matches_played, PlayerSeasonStats.power_index,
        PlayerSeasonStats.performance_delta,
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
    column_list = [
        SquadSnapshot.id, SquadSnapshot.team, SquadSnapshot.league_code,
        SquadSnapshot.snapshot_date, SquadSnapshot.squad_power,
    ]
    column_searchable_list = [SquadSnapshot.team, SquadSnapshot.league_code]
    column_sortable_list = [SquadSnapshot.team, SquadSnapshot.snapshot_date, SquadSnapshot.squad_power]
    column_default_sort = ("snapshot_date", True)
    can_create = False
    can_edit = False
    can_delete = True
    can_view_details = True


class FBrefFixtureAdmin(ModelView, model=FBrefFixture):
    name = "Fixture"
    name_plural = "Fixtures"
    icon = "fa-solid fa-calendar"
    column_list = [
        FBrefFixture.id, FBrefFixture.league_code,
        FBrefFixture.home_team, FBrefFixture.away_team,
        FBrefFixture.match_date, FBrefFixture.match_time,
    ]
    column_searchable_list = [FBrefFixture.league_code, FBrefFixture.home_team, FBrefFixture.away_team]
    column_sortable_list = [FBrefFixture.league_code, FBrefFixture.match_date]
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
    column_list = [
        PredictionLog.id, PredictionLog.league_code,
        PredictionLog.home_team, PredictionLog.away_team,
        PredictionLog.match_date, PredictionLog.market,
        PredictionLog.confidence, PredictionLog.status,
        PredictionLog.actual_score, PredictionLog.variance_flag,
    ]
    column_searchable_list = [
        PredictionLog.league_code, PredictionLog.home_team,
        PredictionLog.away_team, PredictionLog.status,
    ]
    column_sortable_list = [PredictionLog.match_date, PredictionLog.league_code, PredictionLog.status]
    column_default_sort = ("match_date", True)
    can_create = False
    can_edit = True
    can_delete = True
    can_view_details = True
    page_size = 50


# ══════════════════════════════════════════════════════════════════════════════
# CUSTOM ALIAS MANAGER PAGE
# ══════════════════════════════════════════════════════════════════════════════

ALIAS_PAGE_HTML = """
<!DOCTYPE html>
<html>
<head>
  <title>Manage Team Aliases — ATHENA Admin</title>
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <style>
    * { box-sizing: border-box; margin: 0; padding: 0; }
    body { font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif; background: #f5f7fa; color: #1a1a2e; }
    .top-bar { background: #1a1a2e; color: #fff; padding: 12px 24px; display: flex; align-items: center; gap: 16px; }
    .top-bar a { color: #7eb8f7; text-decoration: none; font-size: 13px; }
    .top-bar h1 { font-size: 16px; font-weight: 600; }
    .container { max-width: 800px; margin: 24px auto; padding: 0 16px; }
    .card { background: #fff; border-radius: 8px; border: 1px solid #e2e8f0; padding: 24px; margin-bottom: 16px; }
    .card h2 { font-size: 16px; margin-bottom: 16px; color: #334155; }
    label { display: block; font-size: 13px; font-weight: 600; color: #475569; margin-bottom: 4px; }
    input, select { width: 100%; padding: 10px 12px; border: 1px solid #d1d5db; border-radius: 6px; font-size: 14px; margin-bottom: 12px; }
    input:focus, select:focus { outline: none; border-color: #3b82f6; box-shadow: 0 0 0 3px rgba(59,130,246,0.1); }
    .btn { padding: 8px 16px; border: none; border-radius: 6px; font-size: 13px; font-weight: 600; cursor: pointer; }
    .btn-primary { background: #3b82f6; color: #fff; }
    .btn-primary:hover { background: #2563eb; }
    .btn-danger { background: #ef4444; color: #fff; font-size: 11px; padding: 4px 10px; }
    .btn-danger:hover { background: #dc2626; }
    .alias-list { list-style: none; }
    .alias-item { display: flex; align-items: center; justify-content: space-between; padding: 8px 12px; border: 1px solid #e2e8f0; border-radius: 6px; margin-bottom: 6px; background: #f8fafc; }
    .alias-key { font-family: monospace; font-size: 14px; color: #1e293b; }
    .empty-msg { color: #94a3b8; font-size: 13px; font-style: italic; padding: 12px 0; }
    .team-info { display: flex; align-items: center; gap: 12px; margin-bottom: 16px; padding: 12px 16px; background: #f0f9ff; border: 1px solid #bae6fd; border-radius: 6px; }
    .team-info .name { font-size: 18px; font-weight: 700; color: #0c4a6e; }
    .team-info .league { font-size: 12px; color: #0369a1; font-family: monospace; }
    .add-row { display: flex; gap: 8px; align-items: flex-end; }
    .add-row input { flex: 1; margin-bottom: 0; }
    .search-results { max-height: 300px; overflow-y: auto; }
    .team-btn { display: block; width: 100%; text-align: left; padding: 10px 12px; border: 1px solid #e2e8f0; border-radius: 6px; margin-bottom: 4px; background: #fff; cursor: pointer; font-size: 14px; }
    .team-btn:hover { background: #f0f9ff; border-color: #3b82f6; }
    .team-btn .league-tag { font-size: 11px; color: #64748b; font-family: monospace; margin-left: 8px; }
    .msg { padding: 8px 12px; border-radius: 6px; font-size: 13px; margin-bottom: 12px; }
    .msg-ok { background: #f0fdf4; border: 1px solid #86efac; color: #166534; }
    .msg-err { background: #fef2f2; border: 1px solid #fca5a5; color: #991b1b; }
    .hidden { display: none; }
  </style>
</head>
<body>
  <div class="top-bar">
    <a href="/admin">← Back to Admin</a>
    <h1>Manage Team Aliases</h1>
  </div>
  <div class="container">
    <!-- Step 1: Find team -->
    <div class="card" id="search-card">
      <h2>1. Find a team</h2>
      <label>Search by name or league</label>
      <input type="text" id="team-search" placeholder="Type team name..." oninput="searchTeams()">
      <div id="search-results" class="search-results"></div>
    </div>

    <!-- Step 2: Manage aliases (shown after team selected) -->
    <div class="card hidden" id="alias-card">
      <div class="team-info" id="team-info"></div>
      <div id="msg"></div>

      <h2>Current Aliases</h2>
      <ul class="alias-list" id="alias-list"></ul>

      <h2 style="margin-top:20px">Add New Alias</h2>
      <div class="add-row">
        <input type="text" id="new-alias" placeholder="Type alias (e.g. fc fredericia)..." onkeydown="if(event.key==='Enter')addAlias()">
        <button class="btn btn-primary" onclick="addAlias()">Add</button>
      </div>
    </div>
  </div>

  <script>
    let currentTeamId = null;
    let allTeams = [];

    // Load all teams on page load
    fetch('/alias-api/teams')
      .then(r => r.json())
      .then(data => { allTeams = data.teams || []; });

    function searchTeams() {
      const q = document.getElementById('team-search').value.toLowerCase().trim();
      const el = document.getElementById('search-results');
      if (q.length < 2) { el.innerHTML = ''; return; }

      const matches = allTeams.filter(t =>
        t.display_name.toLowerCase().includes(q) ||
        t.team_key.toLowerCase().includes(q) ||
        t.league_code.toLowerCase().includes(q)
      ).slice(0, 20);

      el.innerHTML = matches.map(t =>
        `<button class="team-btn" onclick="selectTeam(${t.id})">
          ${t.display_name} <span class="league-tag">${t.league_code}</span>
        </button>`
      ).join('') || '<div class="empty-msg">No teams found</div>';
    }

    function selectTeam(teamId) {
      currentTeamId = teamId;
      const team = allTeams.find(t => t.id === teamId);
      document.getElementById('search-results').innerHTML = '';
      document.getElementById('team-search').value = '';
      document.getElementById('team-info').innerHTML =
        `<div><div class="name">${team.display_name}</div><div class="league">${team.league_code} · ID: ${team.id}</div></div>`;
      document.getElementById('alias-card').classList.remove('hidden');
      document.getElementById('msg').innerHTML = '';
      loadAliases();
    }

    function loadAliases() {
      fetch(`/alias-api/team/${currentTeamId}/aliases`)
        .then(r => r.json())
        .then(data => {
          const list = document.getElementById('alias-list');
          if (!data.aliases || data.aliases.length === 0) {
            list.innerHTML = '<div class="empty-msg">No aliases yet — add one below</div>';
            return;
          }
          list.innerHTML = data.aliases.map(a =>
            `<li class="alias-item">
              <span class="alias-key">${a.alias_key}</span>
              <button class="btn btn-danger" onclick="deleteAlias(${a.id})">Remove</button>
            </li>`
          ).join('');
        });
    }

    function addAlias() {
      const input = document.getElementById('new-alias');
      const val = input.value.trim();
      if (!val) return;

      fetch(`/alias-api/team/${currentTeamId}/aliases`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ alias_key: val }),
      })
      .then(r => r.json())
      .then(data => {
        const msg = document.getElementById('msg');
        if (data.error) {
          msg.innerHTML = `<div class="msg msg-err">${data.error}</div>`;
        } else {
          msg.innerHTML = `<div class="msg msg-ok">Added alias: ${data.alias_key}</div>`;
          input.value = '';
          loadAliases();
        }
      });
    }

    function deleteAlias(aliasId) {
      fetch(`/alias-api/alias/${aliasId}`, { method: 'DELETE' })
        .then(r => r.json())
        .then(data => {
          const msg = document.getElementById('msg');
          if (data.error) {
            msg.innerHTML = `<div class="msg msg-err">${data.error}</div>`;
          } else {
            msg.innerHTML = `<div class="msg msg-ok">Removed alias</div>`;
            loadAliases();
          }
        });
    }
  </script>
</body>
</html>
"""


# ══════════════════════════════════════════════════════════════════════════════
# SETUP — registers sqladmin views + custom alias routes
# ══════════════════════════════════════════════════════════════════════════════

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

    # ── Custom alias manager page ────────────────────────────────────

    @app.get("/alias-manager", response_class=HTMLResponse)
    async def manage_aliases_page():
        return ALIAS_PAGE_HTML

    # ── Alias API endpoints (used by the custom page) ────────────────

    @app.get("/alias-api/teams")
    def api_list_teams():
        db = SessionLocal()
        try:
            teams = db.query(Team).order_by(Team.league_code, Team.display_name).all()
            return {
                "teams": [
                    {"id": t.id, "team_key": t.team_key,
                     "display_name": t.display_name, "league_code": t.league_code}
                    for t in teams
                ]
            }
        finally:
            db.close()

    @app.get("/alias-api/team/{team_id}/aliases")
    def api_get_aliases(team_id: int):
        db = SessionLocal()
        try:
            aliases = (
                db.query(TeamAlias)
                .filter(TeamAlias.team_id == team_id)
                .order_by(TeamAlias.alias_key)
                .all()
            )
            return {
                "team_id": team_id,
                "aliases": [{"id": a.id, "alias_key": a.alias_key} for a in aliases],
            }
        finally:
            db.close()

    @app.post("/alias-api/team/{team_id}/aliases")
    async def api_add_alias(team_id: int, request: Request):
        body = await request.json()
        raw_key = body.get("alias_key", "").strip()
        if not raw_key:
            return {"error": "alias_key is required"}

        alias_key = normalize_team(raw_key)

        db = SessionLocal()
        try:
            # Check team exists
            team = db.query(Team).filter(Team.id == team_id).first()
            if not team:
                return {"error": f"Team ID {team_id} not found"}

            # Check alias doesn't already exist
            existing = db.query(TeamAlias).filter(
                TeamAlias.alias_key == alias_key
            ).first()
            if existing:
                owner = db.query(Team).filter(Team.id == existing.team_id).first()
                owner_name = owner.display_name if owner else "unknown"
                if existing.team_id == team_id:
                    return {"error": f"Alias '{alias_key}' already exists for this team"}
                return {"error": f"Alias '{alias_key}' is already assigned to {owner_name}"}

            db.add(TeamAlias(team_id=team_id, alias_key=alias_key))
            db.commit()
            return {"ok": True, "alias_key": alias_key, "team": team.display_name}
        except Exception as e:
            db.rollback()
            return {"error": str(e)}
        finally:
            db.close()

    @app.delete("/alias-api/alias/{alias_id}")
    def api_delete_alias(alias_id: int):
        db = SessionLocal()
        try:
            alias = db.query(TeamAlias).filter(TeamAlias.id == alias_id).first()
            if not alias:
                return {"error": "Alias not found"}
            db.delete(alias)
            db.commit()
            return {"ok": True}
        except Exception as e:
            db.rollback()
            return {"error": str(e)}
        finally:
            db.close()

    return admin
