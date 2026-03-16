# backend/app/services/form_delta.py
"""
ATHENA v2.0 — Form Delta: Over/Under Performance Rating.
Uses batch resolution for lightning-fast team name unification.
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import Optional, List, Dict, Set

import pandas as pd
from sqlalchemy.orm import Session

from app.database.models_fbref import FBrefSnapshot
from app.models.team_config import TeamConfig
from app.models.league_config import LeagueConfig
from app.util.team_resolver import batch_resolve_team_names


# Season boundary: matches before this date are "previous season"
# Aug–May leagues: previous season ended ~June 2025
# Calendar-year leagues: previous season ended ~Dec 2025
_AUG_MAY_CUTOFF = "2025-07-01"
_CALENDAR_CUTOFF = "2025-12-15"

_CALENDAR_YEAR_PREFIXES = {"BRA", "MLS", "NOR", "SWE", "CHN", "JPN", "COL"}


def _is_calendar_league(league_code: str) -> bool:
    prefix = league_code.split("-")[0] if "-" in league_code else league_code
    return prefix in _CALENDAR_YEAR_PREFIXES


def _season_cutoff(league_code: str) -> str:
    return _CALENDAR_CUTOFF if _is_calendar_league(league_code) else _AUG_MAY_CUTOFF


def _compute_standings(db: Session, df: pd.DataFrame, home_col: str, away_col: str) -> List[dict]:
    """
    Compute league standings from match results using BATCH RESOLUTION
    to unify team names with maximum performance.
    """
    # Step 1: Collect ALL unique team names from the dataframe in one pass
    all_raw_names: Set[str] = set()
    for _, row in df.iterrows():
        all_raw_names.add(str(row[home_col]).strip())
        all_raw_names.add(str(row[away_col]).strip())
    
    # Step 2: Batch resolve ALL names with a single set of database queries
    # This is the performance magic - 2 queries total, regardless of match count
    resolved_names = batch_resolve_team_names(db, list(all_raw_names))
    
    # Step 3: Process all matches with pre-resolved names (no per-row DB queries!)
    teams: Dict[str, dict] = {}

    for _, row in df.iterrows():
        # Get raw names
        ht_raw = str(row[home_col]).strip()
        at_raw = str(row[away_col]).strip()
        
        # Use pre-resolved canonical names (fast dictionary lookup)
        ht = resolved_names.get(ht_raw, ht_raw)
        at = resolved_names.get(at_raw, at_raw)
        
        hg = int(row["hg"])
        ag = int(row["ag"])

        # Initialize teams if not exists
        for team in [ht, at]:
            if team not in teams:
                teams[team] = {
                    "team": team,
                    "raw_names": set(),  # Track variants for debugging
                    "p": 0, "w": 0, "d": 0, "l": 0,
                    "gf": 0, "ga": 0, "pts": 0
                }
            # Store raw name for reference (useful for debugging)
            if ht == team:
                teams[team]["raw_names"].add(ht_raw)
            if at == team:
                teams[team]["raw_names"].add(at_raw)

        # Home team
        teams[ht]["p"] += 1
        teams[ht]["gf"] += hg
        teams[ht]["ga"] += ag
        if hg > ag:
            teams[ht]["w"] += 1
            teams[ht]["pts"] += 3
        elif hg == ag:
            teams[ht]["d"] += 1
            teams[ht]["pts"] += 1
        else:
            teams[ht]["l"] += 1

        # Away team
        teams[at]["p"] += 1
        teams[at]["gf"] += ag
        teams[at]["ga"] += hg
        if ag > hg:
            teams[at]["w"] += 1
            teams[at]["pts"] += 3
        elif ag == hg:
            teams[at]["d"] += 1
            teams[at]["pts"] += 1
        else:
            teams[at]["l"] += 1

    # Convert to list and sort
    standings = []
    for team_data in teams.values():
        # Convert raw_names set to list for JSON serialization
        team_data["raw_names"] = list(team_data["raw_names"])
        standings.append(team_data)

    # Sort: points desc → GD desc → GF desc
    standings.sort(
        key=lambda t: (t["pts"], t["gf"] - t["ga"], t["gf"]),
        reverse=True,
    )

    # Add positions
    for i, t in enumerate(standings):
        t["pos"] = i + 1
        t["gd"] = t["gf"] - t["ga"]

    return standings


def _load_and_split_snapshot(
    db: Session, league_code: str
) -> tuple[Optional[pd.DataFrame], Optional[pd.DataFrame], Optional[str], Optional[str]]:
    """
    Load FBref snapshot and split into previous season + current season.
    Returns (prev_df, curr_df, home_col, away_col) or (None, None, None, None).
    """
    row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not row:
        return None, None, None, None

    try:
        df = pd.read_parquet(io.BytesIO(row.data))
    except Exception:
        return None, None, None, None

    # Parse scores
    score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
    if score_col and "hg" not in [str(c).lower() for c in df.columns]:
        from app.services.data_providers.fbref_base import _parse_score_column
        df = _parse_score_column(df, score_col)

    if "hg" not in df.columns or "ag" not in df.columns:
        return None, None, None, None

    # Resolve columns
    col_map = {str(c).lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")

    if not all([date_col, home_col, away_col]):
        return None, None, None, None

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, "hg", "ag"])

    # Split by season boundary
    cutoff = pd.Timestamp(_season_cutoff(league_code))
    prev_df = df[df[date_col] < cutoff].copy()
    curr_df = df[df[date_col] >= cutoff].copy()

    return prev_df, curr_df, home_col, away_col


def compute_form_delta(db: Session, league_code: str) -> dict:
    """
    Compute Form Delta for all teams in a league using BATCH RESOLUTION.
    Now lightning fast even for leagues with thousands of matches.
    """
    prev_df, curr_df, home_col, away_col = _load_and_split_snapshot(db, league_code)

    if curr_df is None or curr_df.empty:
        return {"league_code": league_code, "error": "No current season data", "teams": []}

    # Current standings - now with batch resolution (super fast!)
    current_standings = _compute_standings(db, curr_df, home_col, away_col)

    # Previous season standings (for expected position)
    if prev_df is not None and not prev_df.empty and len(prev_df) >= 30:
        prev_standings = _compute_standings(db, prev_df, home_col, away_col)
        prev_pos_map = {t["team"]: t["pos"] for t in prev_standings}
    else:
        prev_pos_map = {}

    # Load team power data
    team_configs = {
        tc.team: tc
        for tc in db.query(TeamConfig).filter_by(league_code=league_code).all()
    }

    # Load league info
    league_cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    display_name = league_cfg.description if league_cfg else league_code

    # Build expected position:
    # Primary: previous season final position
    # Fallback: rank by squad_power (if no prev season data for this team)
    power_ranked = sorted(
        [(t, tc.squad_power or 50.0) for t, tc in team_configs.items()],
        key=lambda x: x[1], reverse=True,
    )
    power_pos_map = {t: i + 1 for i, (t, _) in enumerate(power_ranked)}

    n_teams = len(current_standings)

    # Tier boundaries for zone comparison
    # Top tier: top 25%, mid tier: middle 50%, bottom tier: bottom 25%
    top_cutoff = max(1, n_teams // 4)
    bottom_cutoff = n_teams - top_cutoff

    # Compute tier averages per zone
    zone_keys = ["atk_power", "mid_power", "def_power", "gk_power"]
    zone_labels = ["atk", "mid", "def", "gk"]

    def tier_label(expected_pos: int) -> str:
        if expected_pos <= top_cutoff:
            return "top"
        elif expected_pos > bottom_cutoff:
            return "bottom"
        return "mid"

    # Collect power values per tier
    tier_powers: dict[str, dict[str, list[float]]] = {
        "top": {z: [] for z in zone_labels},
        "mid": {z: [] for z in zone_labels},
        "bottom": {z: [] for z in zone_labels},
    }

    # First pass: assign expected positions
    team_expected: dict[str, int] = {}
    for t in current_standings:
        team_name = t["team"]
        # Priority: previous season position > squad power rank
        if team_name in prev_pos_map:
            exp = prev_pos_map[team_name]
        elif team_name in power_pos_map:
            exp = power_pos_map[team_name]
        else:
            exp = t["pos"]  # fallback to current position
        # Clamp to league size
        team_expected[team_name] = min(exp, n_teams)

    # Collect tier power values
    for team_name, exp_pos in team_expected.items():
        tc = team_configs.get(team_name)
        if not tc:
            continue
        tier = tier_label(exp_pos)
        for zk, zl in zip(zone_keys, zone_labels):
            val = getattr(tc, zk, None)
            if val is not None:
                tier_powers[tier][zl].append(float(val))

    # Compute tier averages
    tier_avgs: dict[str, dict[str, float]] = {}
    for tier in ["top", "mid", "bottom"]:
        tier_avgs[tier] = {}
        for zl in zone_labels:
            vals = tier_powers[tier][zl]
            tier_avgs[tier][zl] = round(sum(vals) / len(vals), 1) if vals else 50.0

    # Second pass: build results
    results = []
    for standing in current_standings:
        team_name = standing["team"]
        actual_pos = standing["pos"]
        expected_pos = team_expected.get(team_name, actual_pos)
        delta = expected_pos - actual_pos  # positive = overperforming

        if delta >= 3:
            status = "overperforming"
        elif delta <= -3:
            status = "underperforming"
        elif delta > 0:
            status = "slightly_over"
        elif delta < 0:
            status = "slightly_under"
        else:
            status = "on_track"

        # Zone analysis
        tc = team_configs.get(team_name)
        tier = tier_label(expected_pos)
        zones = {}
        worst_gap = 0.0
        primary_weakness = None

        for zk, zl in zip(zone_keys, zone_labels):
            team_val = float(getattr(tc, zk, None) or 50.0) if tc else 50.0
            tier_avg = tier_avgs[tier][zl]
            gap = round(team_val - tier_avg, 1)

            if gap >= 3:
                verdict = "above_tier"
            elif gap <= -3:
                verdict = "below_tier"
            else:
                verdict = "at_tier"

            zones[zl] = {
                "power": round(team_val, 1),
                "tier_avg": tier_avg,
                "gap": gap,
                "verdict": verdict,
            }

            # Track worst zone
            if gap < worst_gap:
                worst_gap = gap
                primary_weakness = zl.upper()

        results.append({
            "team": team_name,
            "actual_pos": actual_pos,
            "expected_pos": expected_pos,
            "form_delta": delta,
            "status": status,
            "current_pts": standing["pts"],
            "current_played": standing["p"],
            "current_gd": standing["gd"],
            "prev_season_pos": prev_pos_map.get(team_name),
            "zones": zones,
            "primary_weakness": primary_weakness if delta < 0 else None,
            "primary_strength": primary_weakness if delta > 0 else None,
        })

    # Sort by form_delta descending (most overperforming first)
    results.sort(key=lambda t: t["form_delta"], reverse=True)

    return {
        "league_code": league_code,
        "display_name": display_name,
        "total_teams": n_teams,
        "tier_boundaries": {
            "top": f"pos 1–{top_cutoff}",
            "mid": f"pos {top_cutoff + 1}–{bottom_cutoff}",
            "bottom": f"pos {bottom_cutoff + 1}–{n_teams}",
        },
        "teams": results,
    }
