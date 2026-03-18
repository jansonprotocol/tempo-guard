"""
backend/scripts/scrape_players.py

ATHENA v2.0 — Player-level stats scraper.

Fetches league-level stats pages from FBref (not per-team — much faster).
One page per stat category per league gives ALL players in that league.

5 stat categories per league:
  - stats    → goals, assists, xG, xAG, progressive carries/passes, minutes
  - gca      → shot-creating actions
  - passing  → completion %
  - defense  → tackles, interceptions, blocks, clearances, aerials
  - keepers  → save %, clean sheet %, PSxG-GA

For 35 leagues × 5 categories = 175 fetches at 15s each ≈ 45 minutes.

10-match interval logic:
  PlayerSeasonStats rows only get their stat columns rewritten when the
  player's matches_played has increased by ≥10 since last_match_count.
  Player records and matches_played are ALWAYS updated (lightweight).
  Stats columns are only rewritten on interval — saves DB churn, reduces
  noise from single-match variance.

Also writes SquadSnapshot per team after processing each league.

Usage:
    cd backend
    venv312\\Scripts\\activate
    python -m scripts.scrape_players                    # all leagues
    python -m scripts.scrape_players --league ENG-PL    # single league
    python -m scripts.scrape_players --force            # ignore 10-match interval and cache

NOTE: Chrome now opens once per league (for all 5 fetches) instead of once per page.
      Run AFTER scrape_fbref.py (needs league snapshots for context).
      Run AFTER discover_team_ids.py (needs fbref_team_id in teams.json).
"""
from __future__ import annotations

import io
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, date, timedelta
from difflib import get_close_matches
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import pandas as pd
import requests
from seleniumbase import Driver
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.database.db import SessionLocal
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.models.team import Team
from app.services.data_providers.fbref_urls import (
    extract_comp_info,
    league_stats_url,
    STAT_CATEGORIES,
)
from app.core.constants import SEASON_MAP, SCHEDULE_URLS  # <-- imported from shared constants
from app.util.team_resolver import resolve_and_learn

# ── Config ───────────────────────────────────────────────────────────────────
SLEEP_BETWEEN_PAGES   = 15   # seconds between fetches (FBref rate limit)
SLEEP_BETWEEN_LEAGUES = 8    # extra wait between leagues
MATCH_INTERVAL        = 10   # only rewrite stats when MP delta >= this
HEADLESS = False
SCRAPER_API_KEY: str | None = os.environ.get("SCRAPER_API_KEY")

# Cache settings
CACHE_TABLE = "stats_fetch_cache"
CACHE_TTL_HOURS = 24

# ── HTML fetch helpers (shared with other scrapers) ──────────────────────────

def _get_html(url: str, label: str, driver=None) -> str | None:
    """Fetch HTML. If API key is set, use ScraperAPI.
       If a Selenium driver is provided, reuse it; otherwise create a new one."""
    if SCRAPER_API_KEY:
        return _fetch_api(url, label)
    if driver:
        return _fetch_with_driver(driver, url, label)
    return _fetch_selenium(url, label)


def _fetch_api(url: str, label: str) -> str | None:
    print(f"  [ScraperAPI] {label}")
    try:
        resp = requests.get(
            "http://api.scraperapi.com",
            params={"api_key": SCRAPER_API_KEY, "url": url, "render": "true", "premium": "true"},
            timeout=90,
        )
        if resp.status_code != 200:
            print(f"    HTTP {resp.status_code}")
            return None
        print(f"    {len(resp.text)} bytes")
        return resp.text
    except Exception as e:
        print(f"    Error: {e}")
        return None


def _fetch_selenium(url: str, label: str) -> str | None:
    """Fallback: open a new browser, fetch, then quit."""
    driver = None
    try:
        driver = Driver(uc=True, headless2=HEADLESS)
        driver.uc_open_with_reconnect(url, 4)
        if not HEADLESS:
            driver.uc_gui_click_captcha()
        time.sleep(3)
        html = driver.get_page_source()
        print(f"    {len(html)} bytes")
        return html
    except Exception as e:
        print(f"    Browser error ({label}): {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


def _fetch_with_driver(driver, url: str, label: str) -> str | None:
    """Use an existing Selenium driver to fetch a page (reuse browser)."""
    try:
        driver.get(url)
        time.sleep(3)  # Allow page to load
        html = driver.page_source
        print(f"    {len(html)} bytes")
        return html
    except Exception as e:
        print(f"    Browser error ({label}): {e}")
        return None


# ── Cache helpers ────────────────────────────────────────────────────────────

def _should_fetch_league(db, league_code: str, force: bool = False) -> bool:
    """Check if we should fetch stats for this league based on cache."""
    if force:
        return True
    try:
        row = db.execute(
            text(f"SELECT last_fetched FROM {CACHE_TABLE} WHERE league_code = :code"),
            {"code": league_code}
        ).fetchone()
        if row:
            last = row[0]
            if datetime.utcnow() - last < timedelta(hours=CACHE_TTL_HOURS):
                print(f"  ⏱️  Skipping {league_code} – last fetched {last.strftime('%Y-%m-%d %H:%M')}")
                return False
    except Exception as e:
        # Table might not exist yet – fall back to fetch
        print(f"  Cache check warning: {e}")
    return True


def _update_fetch_cache(db, league_code: str):
    """Update the cache with current timestamp."""
    try:
        db.execute(
            text(f"""
                INSERT INTO {CACHE_TABLE} (league_code, last_fetched)
                VALUES (:code, :now)
                ON CONFLICT (league_code) DO UPDATE SET last_fetched = :now
            """),
            {"code": league_code, "now": datetime.utcnow()}
        )
        db.commit()
    except Exception as e:
        db.rollback()
        print(f"  Cache update warning: {e}")


# ── Position normalisation ───────────────────────────────────────────────────

def _norm_position(pos_str: str | None) -> str:
    if pos_str is None or pd.isna(pos_str) or isinstance(pos_str, (int, float)):
        return "MID"  # Default for missing/numeric positions
    primary = str(pos_str).split(",")[0].strip().upper()
    return {
        "GK": "GK", "DF": "DEF", "MF": "MID", "FW": "FWD",
    }.get(primary, "MID")


# ── Player ID extraction from HTML ──────────────────────────────────────────

def _extract_player_ids(html: str) -> dict[str, str]:
    """
    Extract player name → fbref_id mapping from stats page HTML.

    FBref player links: <a href="/en/players/e06683e8/Mohamed-Salah">
    Returns: {"Mohamed Salah": "e06683e8", ...}
    """
    pattern = re.compile(
        r'href="/en/players/([a-f0-9]{8})/[^"]*"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )
    players: dict[str, str] = {}
    for m in pattern.finditer(html):
        fbref_id = m.group(1)
        name = str(m.group(2)).strip() 
        if name and fbref_id and name not in players:
            players[name] = fbref_id
    return players


# ── Table parsing ────────────────────────────────────────────────────────────

def _flatten_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Flatten MultiIndex columns the same way scrape_fbref.py does."""
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [
            " ".join(str(v) for v in col if str(v) != "nan" and v != "").strip()
            for col in df.columns
        ]
    return df


def _find_col(cols: list[str], *patterns: str) -> str | None:
    """Find first column whose name contains any of the patterns (case-insensitive)."""
    # Ensure all column names are strings
    cols = [str(c) if not isinstance(c, str) else c for c in cols]
    cols_lower = [(c, c.lower()) for c in cols]
    for pattern in patterns:
        p = pattern.lower()
        for orig, low in cols_lower:
            if p in low:
                return orig
    return None


def _find_col_exact(cols: list[str], *patterns: str) -> str | None:
    """Find column where the last word matches the pattern exactly."""
    # Ensure all column names are strings
    cols = [str(c) if not isinstance(c, str) else c for c in cols]
    cols_parts = [(c, c.lower().split()) for c in cols]
    for pattern in patterns:
        p = pattern.lower()
        for orig, parts in cols_parts:
            if parts and parts[-1] == p:
                return orig
    return None


def _safe_float(val) -> float:
    """Convert a value to float, returning 0.0 for non-numeric."""
    try:
        v = float(val)
        return v if pd.notna(v) else 0.0
    except (ValueError, TypeError):
        return 0.0


def _parse_stats_table(html: str, category: str) -> pd.DataFrame | None:
    """
    Parse the main stats table from a league-level FBref stats page.

    Returns a DataFrame with flattened columns, or None on failure.
    Skips any summary/total rows (Player == "Squad Total" etc).
    """
    try:
        tables = pd.read_html(io.StringIO(html))
    except Exception as e:
        print(f"    Could not parse HTML tables ({category}): {e}")
        return None

    if not tables:
        return None

    # Find the largest table (the main stats table)
    # Filter to tables that have a "Player" column
    best = None
    for t in tables:
        t = _flatten_columns(t)
        if _find_col(list(t.columns), "player") and (best is None or len(t) > len(best)):
            best = t

    if best is None:
        print(f"    No table with 'Player' column found ({category})")
        return None

    best = _flatten_columns(best)

    # Ensure column names are strings
    best.columns = best.columns.astype(str)

    # Remove header repeat rows and summary rows
    player_col = _find_col(list(best.columns), "player")
    if player_col:
        best = best[~best[player_col].astype(str).str.contains(
            r"^(Player|Squad Total|Opponent Total)$", na=False, case=False
        )]

    return best


# ── Column mapping per stat category ────────────────────────────────────────

def _extract_standard(df: pd.DataFrame) -> dict[str, pd.Series]:
    """
    Extract columns from the Standard Stats table.
    Returns a dict of series keyed by our internal field names.
    """
    cols = list(df.columns)
    result = {}

    # Identity columns
    result["player_name"]  = df[_find_col(cols, "player")]  if _find_col(cols, "player")  else None
    result["squad"]        = df[_find_col(cols, "squad")]    if _find_col(cols, "squad")   else None
    result["pos"]          = df[_find_col(cols, "pos")]      if _find_col(cols, "pos")     else None

    # Playing time
    mp_col  = _find_col_exact(cols, "mp")  or _find_col(cols, "playing time mp", "matches played")
    min_col = _find_col_exact(cols, "min") or _find_col(cols, "playing time min")

    result["mp"]  = df[mp_col].apply(_safe_float)  if mp_col  else pd.Series(0, index=df.index)
    result["min"] = df[min_col].apply(_safe_float)  if min_col else pd.Series(0, index=df.index)

    # Per 90 stats (prefer "Per 90" columns, fall back to totals ÷ 90s)
    ninety_col = _find_col(cols, "90s", "playing time 90s")

    # Attack per-90
    for field, p90_pat, total_pat in [
        ("goals_per90",   "per 90 minutes gls",   "performance gls"),
        ("assists_per90", "per 90 minutes ast",   "performance ast"),
        ("xg_per90",      "per 90 minutes xg",    "expected xg"),
        ("xa_per90",      "per 90 minutes xag",   "expected xag"),
    ]:
        p90_col = _find_col(cols, p90_pat)
        if p90_col:
            result[field] = df[p90_col].apply(_safe_float)
        elif ninety_col:
            total_col = _find_col(cols, total_pat)
            if total_col:
                nineties = df[ninety_col].apply(_safe_float).replace(0, float("nan"))
                result[field] = (df[total_col].apply(_safe_float) / nineties).fillna(0.0)
            else:
                result[field] = pd.Series(0.0, index=df.index)
        else:
            result[field] = pd.Series(0.0, index=df.index)

    # Progression (totals — we'll compute per90 ourselves)
    for field, pat in [
        ("progressive_passes_per90",  "prgp"),
        ("progressive_carries_per90", "prgc"),
    ]:
        col = _find_col(cols, f"progression {pat}", pat)
        if col and ninety_col:
            nineties = df[ninety_col].apply(_safe_float).replace(0, float("nan"))
            result[field] = (df[col].apply(_safe_float) / nineties).fillna(0.0)
        elif col:
            # Fallback: store as-is (total, not per90) — better than 0
            result[field] = df[col].apply(_safe_float)
        else:
            result[field] = pd.Series(0.0, index=df.index)

    return result


def _extract_gca(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Extract SCA per 90 from the GCA table."""
    cols = list(df.columns)
    result = {}
    result["player_name"] = df[_find_col(cols, "player")] if _find_col(cols, "player") else None

    sca90_col = _find_col(cols, "sca sca90", "sca90")
    if sca90_col:
        result["sca_per90"] = df[sca90_col].apply(_safe_float)
    else:
        # Fallback: SCA total / 90s
        sca_col = _find_col(cols, "sca sca", " sca")
        ninety_col = _find_col(cols, "90s")
        if sca_col and ninety_col:
            nineties = df[ninety_col].apply(_safe_float).replace(0, float("nan"))
            result["sca_per90"] = (df[sca_col].apply(_safe_float) / nineties).fillna(0.0)
        else:
            result["sca_per90"] = pd.Series(0.0, index=df.index) if not df.empty else pd.Series(dtype=float)

    return result


def _extract_passing(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Extract pass completion % from the Passing table."""
    cols = list(df.columns)
    result = {}
    result["player_name"] = df[_find_col(cols, "player")] if _find_col(cols, "player") else None

    cmp_pct_col = _find_col(cols, "cmp%", "total cmp%")
    result["pass_completion_pct"] = df[cmp_pct_col].apply(_safe_float) if cmp_pct_col else pd.Series(0.0, index=df.index)

    return result


def _extract_defense(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Extract defensive stats from the Defense table."""
    cols = list(df.columns)
    result = {}
    result["player_name"] = df[_find_col(cols, "player")] if _find_col(cols, "player") else None

    ninety_col = _find_col(cols, "90s")
    nineties = df[ninety_col].apply(_safe_float).replace(0, float("nan")) if ninety_col else None

    for field, pat in [
        ("tackles_won_per90",   "tklw"),
        ("interceptions_per90", " int"),
        ("blocks_per90",        "blocks blocks"),
        ("clearances_per90",    "clr"),
    ]:
        col = _find_col(cols, pat)
        if col and nineties is not None:
            result[field] = (df[col].apply(_safe_float) / nineties).fillna(0.0)
        elif col:
            result[field] = df[col].apply(_safe_float)
        else:
            result[field] = pd.Series(0.0, index=df.index)

    # Aerial duel win %
    aerial_col = _find_col(cols, "won%", "aerial duels won%")
    result["aerials_won_pct"] = df[aerial_col].apply(_safe_float) if aerial_col else pd.Series(0.0, index=df.index)

    return result


def _extract_keepers(df: pd.DataFrame) -> dict[str, pd.Series]:
    """Extract GK stats from the Keepers table."""
    cols = list(df.columns)
    result = {}
    result["player_name"] = df[_find_col(cols, "player")] if _find_col(cols, "player") else None

    for field, pat in [
        ("save_pct",       "save%"),
        ("cs_pct",         "cs%"),
        ("psxg_minus_ga",  "psxg+/-"),
    ]:
        col = _find_col(cols, pat)
        result[field] = df[col].apply(_safe_float) if col else pd.Series(0.0, index=df.index)

    return result


# ── DB operations ────────────────────────────────────────────────────────────

def _get_or_create_player(db, fbref_id: str, name: str, team: str, league_code: str, position: str) -> Player:
    # Resolve team name WITHOUT league restriction to find canonical team
    from app.services.resolve_team import resolve_team_name
    resolved_team = resolve_team_name(db, team)  # no league_code = search all leagues

    # Get the actual team record to find its league
    team_record = db.query(Team).filter_by(team_key=resolved_team).first()
    if team_record:
        actual_league = team_record.league_code
    else:
        # Team not found – fall back to scraped league (should not happen if teams are seeded)
        actual_league = league_code
        print(f"Warning: Team '{resolved_team}' not found for player {name}")

    player = db.query(Player).filter_by(fbref_id=fbref_id).first()
    if player:
        if player.current_team != resolved_team or player.league_code != actual_league:
            player.current_team = resolved_team
            player.league_code = actual_league
        player.position = position
        player.last_scraped = datetime.utcnow()
        return player
    else:
        player = Player(
            fbref_id=fbref_id,
            name=name,
            current_team=resolved_team,
            league_code=actual_league,
            position=position,
            last_scraped=datetime.utcnow(),
        )
        db.add(player)
        db.flush()
        return player


def _write_squad_snapshot(db, league_code: str, team_players: dict[str, list[int]]):
    """Write a SquadSnapshot per team with today's player IDs."""
    today = date.today()
    for team, player_ids in team_players.items():
        existing = (
            db.query(SquadSnapshot)
            .filter_by(team=team, league_code=league_code, snapshot_date=today)
            .first()
        )
        ids_json = json.dumps(player_ids)
        if existing:
            existing.player_ids = ids_json
            existing.created_at = datetime.utcnow()
        else:
            db.add(SquadSnapshot(
                team=team,
                league_code=league_code,
                snapshot_date=today,
                player_ids=ids_json,
                created_at=datetime.utcnow(),
            ))


# ── Main per-league processing ───────────────────────────────────────────────

def scrape_league_players(league_code: str, schedule_url: str, force: bool = False):
    """Scrape all player stats for one league."""
    print(f"\n{'='*60}")
    print(f"[players] {league_code}")

    # ── Cache check ─────────────────────────────────────────────────
    db_check = SessionLocal()
    try:
        if not _should_fetch_league(db_check, league_code, force):
            return
    finally:
        db_check.close()

    comp_info = extract_comp_info(schedule_url)
    if not comp_info:
        print(f"  Could not parse comp info from URL")
        return

    comp_id, slug = comp_info
    season = SEASON_MAP.get(league_code, "2025-2026")

    # ── Create a shared Selenium driver if not using API ─────────────
    shared_driver = None
    if not SCRAPER_API_KEY:
        from seleniumbase import Driver
        shared_driver = Driver(uc=True, headless2=HEADLESS)
        # Prime the driver with a blank page (optional)
        shared_driver.uc_open_with_reconnect("about:blank", 1)

    # ── Fetch all 5 stat category pages ──────────────────────────────
    raw_pages: dict[str, str] = {}
    try:
        for cat in STAT_CATEGORIES:
            url = league_stats_url(comp_id, slug, cat)
            print(f"\n  Fetching {cat}: {url}")
            html = _get_html(url, f"{league_code}/{cat}", driver=shared_driver)

            if html and "Just a moment" not in html and len(html) > 5000:
                raw_pages[cat] = html
            else:
                print(f"    ⚠ Failed or blocked for {cat}")

            if cat != STAT_CATEGORIES[-1]:
                time.sleep(SLEEP_BETWEEN_PAGES)
    finally:
        # Close the shared driver after all fetches
        if shared_driver:
            try:
                shared_driver.quit()
            except Exception:
                pass

    if "stats" not in raw_pages:
        print(f"  Cannot proceed without standard stats page — skipping {league_code}")
        return

    # ── Extract player IDs from the standard stats page HTML ─────────
    player_id_map = _extract_player_ids(raw_pages["stats"])
    print(f"\n  Extracted {len(player_id_map)} player IDs from HTML links")

    # ── Parse tables ─────────────────────────────────────────────────
    parsed: dict[str, dict] = {}  # category → extracted dict

    for cat, html in raw_pages.items():
        df = _parse_stats_table(html, cat)
        if df is None:
            continue

        extractor = {
            "stats":   _extract_standard,
            "gca":     _extract_gca,
            "passing": _extract_passing,
            "defense": _extract_defense,
            "keepers": _extract_keepers,
        }.get(cat)

        if extractor:
            parsed[cat] = extractor(df)
            player_col = parsed[cat].get("player_name")
            n = len(player_col) if player_col is not None else 0
            print(f"  Parsed {cat}: {n} rows")

    if "stats" not in parsed or parsed["stats"].get("player_name") is None:
        print(f"  No standard stats parsed — skipping {league_code}")
        return

    # ── Build merged player data ─────────────────────────────────────
    std = parsed["stats"]
    n_players = len(std["player_name"])
    print(f"\n  Processing {n_players} players...")

    # Build lookup dicts for supplementary categories (by player name)
    supplements: dict[str, dict[str, dict[str, float]]] = {}
    for cat in ["gca", "passing", "defense", "keepers"]:
        if cat in parsed and parsed[cat].get("player_name") is not None:
            names = parsed[cat]["player_name"]
            supplements[cat] = {}
            for i in range(len(names)):
                name = str(names.iloc[i]).strip()
                supplements[cat][name] = {
                    k: v.iloc[i] if hasattr(v, "iloc") else 0.0
                    for k, v in parsed[cat].items()
                    if k != "player_name" and hasattr(v, "iloc")
                }

    # ── Write to DB ──────────────────────────────────────────────────
    db = SessionLocal()
    try:
        # Load existing season stats for this league (for quick lookup)
        existing_stats_map = {}
        existing_stats_rows = db.query(PlayerSeasonStats).filter_by(
            league_code=league_code,
            season=season
        ).all()
        for stat in existing_stats_rows:
            existing_stats_map[stat.player_id] = stat

        stats_written = 0
        stats_skipped = 0
        players_created = 0
        team_players: dict[str, list[int]] = {}  # team → [player_id, ...]

        for i in range(n_players):
            name = str(std["player_name"].iloc[i]).strip()
            squad = str(std["squad"].iloc[i]).strip() if std.get("squad") is not None else ""
            pos_raw = str(std["pos"].iloc[i]) if std.get("pos") is not None else ""
            mp = int(std["mp"].iloc[i]) if std.get("mp") is not None else 0
            minutes = int(std["min"].iloc[i]) if std.get("min") is not None else 0

            if not name or name == "nan" or mp == 0:
                continue

            # Resolve fbref_id
            fbref_id = player_id_map.get(name)
            if not fbref_id:
                continue  # can't link without ID

            position = _norm_position(pos_raw)

            # Get or create Player record
            player = _get_or_create_player(db, fbref_id, name, squad, league_code, position)
            if not player.id:
                db.flush()

            # Track squad composition
            team_players.setdefault(squad, []).append(player.id)

            # Collect all stats for this player
            all_stats: dict[str, float] = {}
            for field in [
                "goals_per90", "assists_per90", "xg_per90", "xa_per90",
                "progressive_passes_per90", "progressive_carries_per90",
            ]:
                if field in std and hasattr(std[field], "iloc"):
                    all_stats[field] = float(std[field].iloc[i])

            # Merge supplementary stats
            for cat, lookup in supplements.items():
                if name in lookup:
                    all_stats.update(lookup[name])

            # Check existing stats
            existing = existing_stats_map.get(player.id)
            mp_delta = mp - (existing.last_match_count if existing else 0)

            if existing and mp_delta < MATCH_INTERVAL and not force:
                # Skip writing stats, but still update appearance counts
                existing.matches_played = mp
                existing.minutes = minutes
                existing.last_updated = datetime.utcnow()
                stats_skipped += 1
            else:
                if existing:
                    # Update existing record
                    existing.matches_played = mp
                    existing.minutes = minutes
                    existing.last_match_count = mp
                    for field, value in all_stats.items():
                        if hasattr(existing, field):
                            setattr(existing, field, round(value, 3))
                    existing.last_updated = datetime.utcnow()
                else:
                    # Create new record
                    new_stat = PlayerSeasonStats(
                        player_id=player.id,
                        season=season,
                        league_code=league_code,
                        matches_played=mp,
                        minutes=minutes,
                        last_match_count=mp,
                        last_updated=datetime.utcnow(),
                    )
                    for field, value in all_stats.items():
                        if hasattr(new_stat, field):
                            setattr(new_stat, field, round(value, 3))
                    db.add(new_stat)
                    existing_stats_map[player.id] = new_stat  # add to map for consistency
                stats_written += 1

        # Write squad snapshots
        _write_squad_snapshot(db, league_code, team_players)

        db.commit()
        print(f"\n  [players] {league_code} done:")
        print(f"    Players processed: {stats_written + stats_skipped}")
        print(f"    Stats written:     {stats_written}")
        print(f"    Stats skipped:     {stats_skipped} (below {MATCH_INTERVAL}-match interval)")
        print(f"    Teams with squads: {len(team_players)}")

        # Update fetch cache after successful processing
        _update_fetch_cache(db, league_code)

    except Exception as e:
        db.rollback()
        print(f"  DB error: {e}")
        raise
    finally:
        db.close()


# ── Entry point ──────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Scrape FBref player stats")
    parser.add_argument("--league", type=str, default=None, help="Single league")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--api", type=str, default=None, metavar="KEY", help="ScraperAPI key")
    parser.add_argument("--force", action="store_true", help="Ignore 10-match interval and cache")
    args = parser.parse_args()

    if args.headless:
        HEADLESS = True
    if args.api:
        SCRAPER_API_KEY = args.api

    if args.league:
        if args.league not in SCHEDULE_URLS:
            print(f"[players] Unknown league: {args.league}")
            print(f"  Available: {sorted(SCHEDULE_URLS.keys())}")
            sys.exit(1)
        scrape_league_players(args.league, SCHEDULE_URLS[args.league], args.force)
    else:
        print("[players] Starting player stats scrape for all leagues")
        print(f"[players] {len(SCHEDULE_URLS)} leagues, 5 pages each")
        print(f"[players] Estimated time: ~{len(SCHEDULE_URLS) * 5 * SLEEP_BETWEEN_PAGES // 60} minutes\n")

        codes = list(SCHEDULE_URLS.keys())
        for i, (code, url) in enumerate(SCHEDULE_URLS.items()):
            scrape_league_players(code, url, args.force)
            if i < len(codes) - 1:
                print(f"\n  Waiting {SLEEP_BETWEEN_LEAGUES}s before next league...")
                time.sleep(SLEEP_BETWEEN_LEAGUES)

    print("\n[players] Done.")
