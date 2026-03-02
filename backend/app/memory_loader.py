import os
import json
from sqlalchemy.orm import Session

from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team

# ---------------------------------------------------------------------------
# PATH HANDLING (works on Render, Docker, and locally)
# ---------------------------------------------------------------------------

# Absolute directory of THIS file
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Seed folder is always located in: backend/app/seed/
SEED_DIR = os.path.join(BASE_DIR, "..", "seed")

def _seed_path(filename: str) -> str:
    """
    Build an absolute path to a seed file.
    This fixes 100% of the path issues you were having on Render.
    """
    return os.path.join(SEED_DIR, filename)


# ---------------------------------------------------------------------------
# SEED LOADER UTILITIES
# ---------------------------------------------------------------------------

def _read_json(path: str):
    """
    Read a JSON seed file with graceful logging.
    Returns list/dict or None if missing.
    """
    if not os.path.exists(path):
        print(f"[memory_loader] ERROR: Seed file not found → {path}")
        return None

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
            print(f"[memory_loader] Loaded seed: {path} ({len(data)} entries)")
            return data
    except Exception as e:
        print(f"[memory_loader] ERROR reading JSON {path}: {e}")
        return None


# ---------------------------------------------------------------------------
# LEAGUE CONFIG SEEDER
# ---------------------------------------------------------------------------

def load_league_configs(db: Session):
    """
    Upsert league configs from seed/league_configs.json.
    Fields include:
      league_code, base_over_bias, base_under_bias, tempo_factor,
      safety_mode, aggression_level, volatility, description
    """
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
            # CREATE NEW
            item = LeagueConfig(
                league_code=code,
                base_over_bias=float(entry.get("base_over_bias", 0.0)),
                base_under_bias=float(entry.get("base_under_bias", 0.0)),
                tempo_factor=float(entry.get("tempo_factor", 1.0)),
                safety_mode=bool(entry.get("safety_mode", True)),
                aggression_level=float(entry.get("aggression_level", 0.5)),
                volatility=float(entry.get("volatility", 0.5)),
                description=(entry.get("description") or "").strip(),
            )
            db.add(item)
            created += 1

        else:
            # UPDATE EXISTING (idempotent)
            existing.base_over_bias = float(entry.get("base_over_bias", existing.base_over_bias))
            existing.base_under_bias = float(entry.get("base_under_bias", existing.base_under_bias))
            existing.tempo_factor = float(entry.get("tempo_factor", existing.tempo_factor))
            existing.safety_mode = bool(entry.get("safety_mode", existing.safety_mode))
            existing.aggression_level = float(entry.get("aggression_level", existing.aggression_level))
            existing.volatility = float(entry.get("volatility", existing.volatility))
            existing.description = (entry.get("description") or existing.description or "").strip()
            updated += 1

    db.commit()
    print(f"[memory_loader] League configs loaded → created={created}, updated={updated}")


# ---------------------------------------------------------------------------
# TEAM + ALIAS SEEDER
# ---------------------------------------------------------------------------

def load_teams(db: Session):
    """
    Load teams from seed/teams.json.
    Fields: display_name, league_code, country?, aliases[]
    """
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
            # CREATE NEW TEAM
            team = Team(
                team_key=team_key,
                display_name=display_name,
                league_code=league_code,
                country=(entry.get("country") or "").strip()
            )

            # Aliases
            for alias in entry.get("aliases", []):
                alias_key = normalize_team(alias)
                if alias_key and alias_key != team_key:
                    team.aliases.append(TeamAlias(alias_key=alias_key))

            db.add(team)
            created += 1

        else:
            # UPDATE EXISTING TEAM
            existing.display_name = display_name
            existing.league_code = league_code

            if entry.get("country") is not None:
                existing.country = (entry.get("country") or "").strip()

            # REPLACE ALIASES (seed is authoritative)
            existing.aliases.clear()

            for alias in entry.get("aliases", []):
                alias_key = normalize_team(alias)
                if alias_key and alias_key != team_key:
                    existing.aliases.append(TeamAlias(alias_key=alias_key))

            updated += 1

    db.commit()
    print(f"[memory_loader] Teams loaded → created={created}, updated={updated}")
