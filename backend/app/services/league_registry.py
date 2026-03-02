# app/services/league_registry.py
from __future__ import annotations
import os, json
from typing import Dict, Optional

_BASE_DIR = os.path.dirname(os.path.abspath(__file__))         # .../app/services
_SEED_DIR = os.path.join(_BASE_DIR, "..", "seed")               # .../app/seed
_JSON_PATH = os.path.join(_SEED_DIR, "league_id_map.json")

def _read_json(path: str) -> Dict[str, Dict[str, int]]:
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f) or {}
            # normalize nested ints
            for code, providers in data.items():
                for provider, val in list(providers.items()):
                    if val is not None:
                        providers[provider] = int(val)
            return data
    except Exception as e:
        print(f"[league_registry] WARN: cannot read {path}: {e}")
        return {}

_LEAGUE_ID_MAP: Dict[str, Dict[str, int]] = _read_json(_JSON_PATH)

def _apply_env_overrides(mapping: Dict[str, Dict[str, int]]) -> None:
    # Allows: LEAGUE_ID_MAP__UCL__api_football=2   (example)
    prefix = "LEAGUE_ID_MAP__"
    for key, value in os.environ.items():
        if not key.startswith(prefix):
            continue
        try:
            _, code, provider = key.split("__", 2)
            mapping.setdefault(code, {})
            mapping[code][provider] = int(value)
        except Exception:
            pass

_apply_env_overrides(_LEAGUE_ID_MAP)

def get_provider_league_id(league_code: str, provider: str) -> Optional[int]:
    row = _LEAGUE_ID_MAP.get(league_code)
    if not row:
        return None
    return row.get(provider)
