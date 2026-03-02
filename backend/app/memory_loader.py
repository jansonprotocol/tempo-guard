import os
import json
from sqlalchemy.orm import Session

from app.models.league_config import LeagueConfig
from app.models.team import Team, TeamAlias
from app.util.text_norm import normalize_team


def _read_json(seed_path: str):
    if not os.path.exists(seed_path):
        print(f"[memory_loader] Seed file not found: {seed_path}")
        return None
    try:
        with open(seed_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[memory_loader] Failed to read JSON {seed_path}: {e}")
        return None


def load_league_configs(db: Session):
    """
    Upsert league configs from app/seed/league_configs.json
    Fields:
      league_code, base_over_bias, base_under_bias, tempo_factor,
      safety_mode, aggression_level, volatility, description
    """
    seed_path = os.path.join("backend", "app", "seed", "league_configs.json")

    if not data:
        print("[memory_loader] No league configs to load.")
        return

    created, updated = 0, 0
    for entry in data:
        code = entry.get("league_code")
        if not code:
            continue

        item = (
            db.query(LeagueConfig)
            .filter(LeagueConfig.league_code == code)
            .first()
        )

        if item is None:
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
            # Update existing (idempotent upsert)
            item.base_over_bias = float(entry.get("base_over_bias", item.base_over_bias))
            item.base_under_bias = float(entry.get("base_under_bias", item.base_under_bias))
            item.tempo_factor = float(entry.get("tempo_factor", item.tempo_factor))
            item.safety_mode = bool(entry.get("safety_mode", item.safety_mode))
            item.aggression_level = float(entry.get("aggression_level", item.aggression_level))
            item.volatility = float(entry.get("volatility", item.volatility))
            item.description = (entry.get("description") or item.description or "").strip()
            updated += 1

    db.commit()
    print(f"[memory_loader] League configs loaded. created={created}, updated={updated}")


def load_teams(db: Session):
    """
    Upsert teams + aliases from app/seed/teams.json
    Fields:
      display_name, league_code, country?, aliases?[]
    Notes:
      - team_key is a normalized version of display_name (lowercased, no accents/punct)
      - Replaces aliases each load to stay in sync with the seed file
    """
    seed_path = os.path.join("backend", "seed", "teams.json")
    data = _read_json(seed_path)
    if not data:
        print("[memory_loader] No teams to load.")
        return

    created, updated = 0, 0
    for entry in data:
        display_name = (entry.get("display_name") or "").strip()
        league_code = (entry.get("league_code") or "").strip()
        if not display_name or not league_code:
            continue

        team_key = normalize_team(display_name)
        team = db.query(Team).filter(Team.team_key == team_key).first()

        if team is None:
            team = Team(
                team_key=team_key,
                display_name=display_name,
                league_code=league_code,
                country=(entry.get("country") or "").strip(),
            )
            # set aliases
            for al in entry.get("aliases", []):
                ak = normalize_team(al)
                if ak and ak != team_key:
                    team.aliases.append(TeamAlias(alias_key=ak))
            db.add(team)
            created += 1
        else:
            # Update primary fields
            team.display_name = display_name
            team.league_code = league_code
            if entry.get("country") is not None:
                team.country = (entry.get("country") or "").strip()

            # Replace aliases to keep seed authoritative
            team.aliases.clear()
            for al in entry.get("aliases", []):
                ak = normalize_team(al)
                if ak and ak != team_key:
                    team.aliases.append(TeamAlias(alias_key=ak))
            updated += 1

    db.commit()
    print(f"[memory_loader] Teams loaded. created={created}, updated={updated}")
