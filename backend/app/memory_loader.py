import os
import json
from sqlalchemy.orm import Session

from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

# ---------------------------------------------------------------------------
# PATH HANDLING — 100% SAFE FOR YOUR STRUCTURE (/backend/app)
# ---------------------------------------------------------------------------

BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # → /backend/app
SEED_DIR = os.path.join(BASE_DIR, "seed")               # → /backend/app/seed

def _seed_path(filename: str) -> str:
    return os.path.join(SEED_DIR, filename)


# ---------------------------------------------------------------------------
# JSON LOADER HELPERS
# ---------------------------------------------------------------------------


def load_teams(db: Session):
    path = _seed_path("teams.json")
    data = _read_json(path)
    if not data:
        print("[memory_loader] No teams loaded.")
        return

    from sqlalchemy.dialects.postgresql import insert as pg_insert  # for ON CONFLICT

    created, updated = 0, 0

    for entry in data:
        display_name = (entry.get("display_name") or "").strip()
        league_code = (entry.get("league_code") or "").strip()
        if not display_name or not league_code:
            continue

        team_key = normalize_team(display_name)
        existing = db.query(Team).filter(Team.team_key == team_key).first()

        if existing is None:
            # CREATE new team
            team = Team(
                team_key=team_key,
                display_name=display_name,
                league_code=league_code,
                country=(entry.get("country") or "").strip(),
            )
            db.add(team)
            db.flush()  # get team.id
            # Insert aliases using ON CONFLICT DO NOTHING
            for alias in entry.get("aliases", []):
                alias_key = normalize_team(alias)
                if alias_key and alias_key != team_key:
                    stmt = pg_insert(TeamAlias).values(
                        team_id=team.id,
                        alias_key=alias_key
                    ).on_conflict_do_nothing(index_elements=['alias_key'])
                    db.execute(stmt)
            created += 1
        else:
            # UPDATE existing team
            existing.display_name = display_name
            existing.league_code = league_code
            if entry.get("country") is not None:
                existing.country = (entry.get("country") or "").strip()

            # Delete all existing aliases for this team (they will be replaced)
            db.query(TeamAlias).filter(TeamAlias.team_id == existing.id).delete()
            db.flush()

            # Insert new aliases with ON CONFLICT DO NOTHING
            for alias in entry.get("aliases", []):
                alias_key = normalize_team(alias)
                if alias_key and alias_key != team_key:
                    stmt = pg_insert(TeamAlias).values(
                        team_id=existing.id,
                        alias_key=alias_key
                    ).on_conflict_do_nothing(index_elements=['alias_key'])
                    db.execute(stmt)
            updated += 1

    db.commit()
    print(f"[memory_loader] Teams loaded → created={created}, updated={updated}")
