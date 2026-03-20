import os
import json
from sqlalchemy.orm import Session

from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

# ---------------------------------------------------------------------------
# PATH HANDLING — 100% SAFE FOR YOUR STRUCTURE (/backend/app)
# ---------------------------------------------------------------------------

# This file lives in: /app/backend/app/memory_loader.py  (Render)
# Or locally:         /backend/app/memory_loader.py
# So the seed folder is ALWAYS located at: /backend/app/seed/ (same level)
BASE_DIR = os.path.dirname(os.path.abspath(__file__))   # → /backend/app
SEED_DIR = os.path.join(BASE_DIR, "seed")               # → /backend/app/seed

def _seed_path(filename: str) -> str:
    """
    Build an absolute path to a JSON seed file located in /backend/app/seed.
    """
    return os.path.join(SEED_DIR, filename)


# ---------------------------------------------------------------------------
# JSON LOADER HELPERS
# ---------------------------------------------------------------------------

def _read_json(path: str):
    if not os.path.exists(path):
        print(f"[memory_loader] ERROR: Seed file not found → {path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"[memory_loader] Loaded seed file: {path} ({len(data)} items)")
            return data
    except Exception as e:
        print(f"[memory_loader] ERROR reading JSON {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# LEAGUE CONFIG LOADER
# ---------------------------------------------------------------------------

def load_league_configs(db: Session):
    path = _seed_path("league_configs.json")
    data = _read_json(path)

    if not data:
        print("[memory_loader] No league configs loaded.")
        return

    created, updated = 0, 0

    for entry in data:
        code = entry.get("league_code")
        if not code:
            continue

        existing = db.query(LeagueConfig).filter(
            LeagueConfig.league_code == code
        ).first()

        if existing is None:
            # CREATE
            item = LeagueConfig(
                league_code=code,
                base_over_bias=float(entry.get("base_over_bias", 0.0)),
                base_under_bias=float(entry.get("base_under_bias", 0.0)),
                tempo_factor=float(entry.get("tempo_factor", 1.0)),
                safety_mode=bool(entry.get("safety_mode", True)),
                aggression_level=float(entry.get("aggression_level", 0.5)),
                volatility=float(entry.get("volatility", 0.5)),
                description=(entry.get("description") or "").strip(),
                strength_coefficient=float(entry.get("strength_coefficient", 1.0)),
            )
            db.add(item)
            created += 1

        else:
            # UPDATE
            existing.base_over_bias = float(entry.get("base_over_bias", existing.base_over_bias))
            existing.base_under_bias = float(entry.get("base_under_bias", existing.base_under_bias))
            existing.tempo_factor = float(entry.get("tempo_factor", existing.tempo_factor))
            existing.safety_mode = bool(entry.get("safety_mode", existing.safety_mode))
            existing.aggression_level = float(entry.get("aggression_level", existing.aggression_level))
            existing.volatility = float(entry.get("volatility", existing.volatility))
            existing.description = (entry.get("description") or existing.description or "").strip()
            existing.strength_coefficient = float(entry.get("strength_coefficient", existing.strength_coefficient or 1.0))
            updated += 1

    db.commit()
    print(f"[memory_loader] League configs loaded → created={created}, updated={updated}")


# ---------------------------------------------------------------------------
# TEAM LOADER
# ---------------------------------------------------------------------------

def load_teams(db: Session):
    path = _seed_path("teams.json")
    data = _read_json(path)

    if not data:
        print("[memory_loader] No teams loaded.")
        return

    created, updated = 0, 0

    for entry in data:
        display_name = (entry.get("display_name") or "").strip()
        league_code = (entry.get("league_code") or "").strip()

        if not display_name or not league_code:
            continue

        team_key = normalize_team(display_name)

        existing = db.query(Team).filter(
            Team.team_key == team_key
        ).first()

        if existing is None:
            # CREATE
            team = Team(
                team_key=team_key,
                display_name=display_name,
                league_code=league_code,
                country=(entry.get("country") or "").strip(),
            )

            for alias in entry.get("aliases", []):
                alias_key = normalize_team(alias)
                if alias_key and alias_key != team_key:
                    team.aliases.append(TeamAlias(alias_key=alias_key))

            db.add(team)
            created += 1

        else:
            # UPDATE
            existing.display_name = display_name
            existing.league_code = league_code

            if entry.get("country") is not None:
                existing.country = (entry.get("country") or "").strip()

            # Replace aliases
            existing.aliases.clear()
            for alias in entry.get("aliases", []):
                alias_key = normalize_team(alias)
                if alias_key and alias_key != team_key:
                    existing.aliases.append(TeamAlias(alias_key=alias_key))

            updated += 1

    db.commit()
    print(f"[memory_loader] Teams loaded → created={created}, updated={updated}")
