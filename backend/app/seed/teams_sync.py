# backend/app/seed/teams_sync.py
"""
Teams seed file auto-sync utility.

Keeps app/seed/teams.json up to date as scrapers discover new teams,
aliases, or league changes (promotions/relegations).

Usage (from scrapers):
    from app.seed.teams_sync import sync_league_teams

    # After scraping a league — writes any new DB teams back to teams.json
    sync_league_teams(db, "FRA-L2")

Rules:
  - Only adds teams that are in the DB but missing from teams.json
  - Never removes entries (manual curation required for relegated teams)
  - Skips generic-suffix names ("fc", "sc", "mans", "star" etc.)
  - Writes aliases discovered by resolve_and_learn back to the file
  - Thread-safe via a file lock (scraper runs several leagues in sequence)
  - Idempotent: safe to call multiple times for the same league
"""
from __future__ import annotations

import json
import os
import threading
from typing import Optional

from sqlalchemy.orm import Session

# ── Path resolution ────────────────────────────────────────────────────────
# This file lives at app/seed/teams_sync.py.
# teams.json lives in the same directory.
_SEED_DIR  = os.path.dirname(os.path.abspath(__file__))
_TEAMS_FILE = os.path.join(_SEED_DIR, "teams.json")

# ── Generic suffixes that should never appear as standalone team keys ──────
_GENERIC_SUFFIXES = {
    "fc", "sc", "ac", "cf", "rc", "fk", "bk", "sk", "if", "gk",
    "afc", "bfc", "cfc", "dfc", "efc", "rfc", "sfc", "ufc",
    "utd", "united", "city", "town", "rovers", "wanderers",
    "star", "mans", "boys",
}

# ── File lock — only one scraper thread writes at a time ──────────────────
_write_lock = threading.Lock()

# ── Minimum display_name length to be considered valid ────────────────────
_MIN_NAME_LEN = 3


def _is_valid_name(name: str) -> bool:
    """Return True if name is a real team name, not a stripped fragment."""
    if not name or len(name.strip()) < _MIN_NAME_LEN:
        return False
    return name.strip().lower() not in _GENERIC_SUFFIXES


def _read_teams_file() -> list[dict]:
    """Read teams.json, returning empty list if file doesn't exist."""
    if not os.path.exists(_TEAMS_FILE):
        return []
    try:
        with open(_TEAMS_FILE, encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        print(f"[teams_sync] Warning: could not read teams.json: {e}")
        return []


def _write_teams_file(data: list[dict]) -> bool:
    """Write teams.json atomically."""
    try:
        tmp = _TEAMS_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        os.replace(tmp, _TEAMS_FILE)
        return True
    except Exception as e:
        print(f"[teams_sync] Warning: could not write teams.json: {e}")
        return False


def sync_league_teams(
    db: Session,
    league_code: str,
    country: Optional[str] = None,
    verbose: bool = True,
) -> dict:
    """
    Sync all teams for `league_code` from the DB into teams.json.

    - Adds teams present in DB but missing from teams.json.
    - Updates aliases for existing entries (adds newly discovered aliases).
    - Does NOT remove entries — relegated/promoted teams need manual curation
      or a call to update_team_league().

    Returns a summary dict with 'added', 'alias_updated', 'skipped' counts.
    """
    from app.models.team import Team, TeamAlias

    summary = {"league_code": league_code, "added": 0, "alias_updated": 0, "skipped": 0}

    # Load all teams for this league from DB
    db_teams = (
        db.query(Team)
        .filter(Team.league_code == league_code)
        .all()
    )
    if not db_teams:
        return summary

    with _write_lock:
        existing = _read_teams_file()

        # Build a lookup: normalised display_name → index in existing list
        existing_index: dict[str, int] = {}
        for i, entry in enumerate(existing):
            key = entry.get("display_name", "").strip().lower()
            if key:
                existing_index[key] = i

        changed = False

        for team in db_teams:
            display = team.display_name or team.team_key
            if not _is_valid_name(display):
                summary["skipped"] += 1
                continue

            # Collect current aliases from DB
            db_aliases = [
                a.alias_key for a in team.aliases
                if a.alias_key and a.alias_key != team.team_key
                and _is_valid_name(a.alias_key)
            ]

            lookup_key = display.strip().lower()

            if lookup_key not in existing_index:
                # New team — add it
                entry = {
                    "display_name": display,
                    "league_code":  league_code,
                    "country":      country or _infer_country(league_code),
                    "aliases":      db_aliases,
                }
                existing.append(entry)
                existing_index[lookup_key] = len(existing) - 1
                summary["added"] += 1
                changed = True
                if verbose:
                    print(f"  [teams_sync] Added: {display} ({league_code})"
                          + (f" aliases={db_aliases}" if db_aliases else ""))
            else:
                # Existing entry — merge in any new aliases
                idx = existing_index[lookup_key]
                current_aliases = set(existing[idx].get("aliases", []))
                new_aliases = set(db_aliases) - current_aliases
                if new_aliases:
                    existing[idx]["aliases"] = sorted(current_aliases | new_aliases)
                    summary["alias_updated"] += 1
                    changed = True
                    if verbose:
                        print(f"  [teams_sync] Aliases updated: {display} "
                              f"added={sorted(new_aliases)}")
                # Keep league_code current — but log a warning so promotions/
                # relegations are visible in scrape output and can be reviewed.
                file_lc = existing[idx].get("league_code")
                if file_lc != league_code:
                    print(
                        f"  [teams_sync] ⚠ League mismatch: {display} — "
                        f"teams.json says {file_lc!r} but DB has {league_code!r}. "
                        f"Update teams.json manually if this is a promotion/relegation, "
                        f"or it may be a name collision across leagues."
                    )
                    # Do NOT auto-update league here — file is source of truth.
                    # Use update_team_league() to make an intentional change.

        if changed:
            if _write_teams_file(existing):
                total = summary["added"] + summary["alias_updated"]
                if verbose and total:
                    print(f"  [teams_sync] {league_code}: "
                          f"{summary['added']} added, "
                          f"{summary['alias_updated']} alias updates "
                          f"→ teams.json updated")
            else:
                summary["error"] = "write failed"

    return summary


def update_team_league(
    display_name: str,
    new_league_code: str,
    verbose: bool = True,
) -> bool:
    """
    Update a team's league_code in teams.json.
    Use this when a team is promoted or relegated.

    Example:
        update_team_league("Ipswich Town", "ENG-CH")
    """
    with _write_lock:
        existing = _read_teams_file()
        lookup = display_name.strip().lower()
        for entry in existing:
            if entry.get("display_name", "").strip().lower() == lookup:
                old = entry.get("league_code")
                entry["league_code"] = new_league_code
                if _write_teams_file(existing):
                    if verbose:
                        print(f"  [teams_sync] {display_name}: {old} → {new_league_code}")
                    return True
                return False
        if verbose:
            print(f"  [teams_sync] Not found in teams.json: {display_name}")
        return False


def sync_all_leagues(db: Session, verbose: bool = True) -> dict:
    """
    Sync all leagues that have team records in the DB.
    Call this after a full scrape run to catch everything.
    """
    from app.models.team import Team
    leagues = [r[0] for r in db.query(Team.league_code).distinct().all()]
    total = {"added": 0, "alias_updated": 0, "skipped": 0}
    for lc in sorted(leagues):
        result = sync_league_teams(db, lc, verbose=verbose)
        total["added"]         += result.get("added", 0)
        total["alias_updated"] += result.get("alias_updated", 0)
        total["skipped"]       += result.get("skipped", 0)
    if verbose:
        print(f"  [teams_sync] Full sync done: "
              f"{total['added']} added, "
              f"{total['alias_updated']} alias updates across {len(leagues)} leagues")
    return total


# ── Country inference fallback ─────────────────────────────────────────────
_LEAGUE_COUNTRY: dict[str, str] = {
    "ENG-PL": "England", "ENG-CH": "England",
    "ESP-LL": "Spain",   "ESP-LL2": "Spain",
    "FRA-L1": "France",  "FRA-L2": "France",
    "GER-BUN": "Germany","GER-B2": "Germany",
    "ITA-SA": "Italy",   "ITA-SB": "Italy",
    "NED-ERE": "Netherlands",
    "TUR-SL": "Turkey",
    "SAU-SPL": "Saudi Arabia",
    "DEN-SL": "Denmark",
    "BEL-PL": "Belgium",
    "MEX-LMX": "Mexico",
    "BRA-SA": "Brazil",  "BRA-SB": "Brazil",
    "MLS": "USA",
    "NOR-EL": "Norway",  "SWE-AL": "Sweden",
    "CHN-CSL": "China",  "JPN-J1": "Japan",
    "COL-PA": "Colombia","POL-EK": "Poland",
    "AUT-BL": "Austria", "SUI-SL": "Switzerland",
    "CHI-LP": "Chile",   "PER-L1": "Peru",
    "POR-LP": "Portugal",
    "UCL": "Europe", "UEL": "Europe", "UECL": "Europe",
}


def _infer_country(league_code: str) -> str:
    return _LEAGUE_COUNTRY.get(league_code, "")
