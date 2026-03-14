# backend/app/api/routes_alias_manager.py
"""
Custom alias management page + API.
Registered as a normal FastAPI router — no sqladmin dependency.
"""
from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from app.database.db import SessionLocal
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

router = APIRouter()


@router.get("/alias-manager", response_class=HTMLResponse)
async def alias_manager_page():
    return ALIAS_PAGE_HTML


@router.get("/alias-api/teams")
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


@router.get("/alias-api/team/{team_id}/aliases")
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


@router.post("/alias-api/team/{team_id}/aliases")
async def api_add_alias(team_id: int, request: Request):
    body = await request.json()
    raw_key = body.get("alias_key", "").strip()
    if not raw_key:
        return {"error": "alias_key is required"}

    alias_key = normalize_team(raw_key)

    db = SessionLocal()
    try:
        team = db.query(Team).filter(Team.id == team_id).first()
        if not team:
            return {"error": f"Team ID {team_id} not found"}

        existing = db.query(TeamAlias).filter(TeamAlias.alias_key == alias_key).first()
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


@router.delete("/alias-api/alias/{alias_id}")
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
    <a href="/admin">&larr; Back to Admin</a>
    <h1>Manage Team Aliases</h1>
  </div>
  <div class="container">
    <div class="card" id="search-card">
      <h2>1. Find a team</h2>
      <label>Search by name or league</label>
      <input type="text" id="team-search" placeholder="Type team name..." oninput="searchTeams()">
      <div id="search-results" class="search-results"></div>
    </div>
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
    fetch('/alias-api/teams').then(r => r.json()).then(data => { allTeams = data.teams || []; });

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
        `<div><div class="name">${team.display_name}</div><div class="league">${team.league_code} &middot; ID: ${team.id}</div></div>`;
      document.getElementById('alias-card').classList.remove('hidden');
      document.getElementById('msg').innerHTML = '';
      loadAliases();
    }

    function loadAliases() {
      fetch(`/alias-api/team/${currentTeamId}/aliases`).then(r => r.json()).then(data => {
        const list = document.getElementById('alias-list');
        if (!data.aliases || data.aliases.length === 0) {
          list.innerHTML = '<div class="empty-msg">No aliases yet</div>';
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
      }).then(r => r.json()).then(data => {
        const msg = document.getElementById('msg');
        if (data.error) {
          msg.innerHTML = `<div class="msg msg-err">${data.error}</div>`;
        } else {
          msg.innerHTML = `<div class="msg msg-ok">Added: ${data.alias_key}</div>`;
          input.value = '';
          loadAliases();
        }
      });
    }

    function deleteAlias(aliasId) {
      fetch(`/alias-api/alias/${aliasId}`, { method: 'DELETE' }).then(r => r.json()).then(data => {
        const msg = document.getElementById('msg');
        if (data.error) {
          msg.innerHTML = `<div class="msg msg-err">${data.error}</div>`;
        } else {
          msg.innerHTML = `<div class="msg msg-ok">Removed</div>`;
          loadAliases();
        }
      });
    }
  </script>
</body>
</html>
"""
