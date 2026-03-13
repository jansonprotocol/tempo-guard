# backend/app/services/form_delta.py
"""
ATHENA v2.0 — Form Delta: Over/Under Performance Rating.

Compares each team's CURRENT league position against their HISTORICAL
natural position (derived from previous season final standings).

Example:
  Ajax historically finishes top 3 (expected_pos ≈ 2)
  This season they sit 5th (actual_pos = 5)
  form_delta = expected_pos - actual_pos = 2 - 5 = -3  → UNDERPERFORMING

  NEC historically finishes mid-table (expected_pos ≈ 9)
  This season they sit 3rd (actual_pos = 3)
  form_delta = 9 - 3 = +6  → OVERPERFORMING

The zonal breakdown then identifies WHERE the performance gap originates:
  - Compare team's ATK/MID/DEF/GK power vs the average power of teams
    at their expected tier (top-3, mid-table, bottom-5).
  - If Ajax's DEF power is 47 but top-3 average DEF is 56, the defense
    is dragging them down.

Data sources:
  - FBref snapshots (current + previous season match data) → standings
  - TeamConfig (squad_power, zonal power) → power context
  - No new tables needed — everything is derived at query time
"""
from __future__ import annotations

import io
from datetime import datetime
from typing import Optional

import pandas as pd
from sqlalchemy.orm import Session

from app.database.models_fbref import FBrefSnapshot
from app.models.team_config import TeamConfig
from app.models.league_config import LeagueConfig


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


def _compute_standings(df: pd.DataFrame, home_col: str, away_col: str) -> list[dict]:
    """
    Compute league standings from match results.
    Returns list of dicts sorted by points desc, GD desc, GF desc.
    """
    teams: dict[str, dict] = {}

    for _, row in df.iterrows():
        ht = str(row[home_col]).strip()
        at = str(row[away_col]).strip()
        hg = int(row["hg"])
        ag = int(row["ag"])

        for team in [ht, at]:
            if team not in teams:
                teams[team] = {"team": team, "p": 0, "w": 0, "d": 0, "l": 0, "gf": 0, "ga": 0, "pts": 0}

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

    # Sort: points desc → GD desc → GF desc
    standings = sorted(
        teams.values(),
        key=lambda t: (t["pts"], t["gf"] - t["ga"], t["gf"]),
        reverse=True,
    )

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
    Compute Form Delta for all teams in a league.

    Returns:
      {
        "league_code": "NED-ERE",
        "display_name": "Eredivisie (Netherlands)",
        "teams": [
          {
            "team": "Ajax",
            "actual_pos": 5,
            "expected_pos": 2,
            "form_delta": -3,
            "status": "underperforming",
            "current_pts": 42,
            "current_played": 25,
            "prev_season_pos": 2,
            "zones": {
              "atk": {"power": 58.2, "tier_avg": 62.1, "gap": -3.9, "verdict": "below_tier"},
              "mid": {"power": 53.1, "tier_avg": 55.0, "gap": -1.9, "verdict": "at_tier"},
              "def": {"power": 47.0, "tier_avg": 56.3, "gap": -9.3, "verdict": "below_tier"},
              "gk":  {"power": 52.0, "tier_avg": 54.0, "gap": -2.0, "verdict": "at_tier"},
            },
            "primary_weakness": "DEF",
          },
          ...
        ]
      }
    """
    prev_df, curr_df, home_col, away_col = _load_and_split_snapshot(db, league_code)

    if curr_df is None or curr_df.empty:
        return {"league_code": league_code, "error": "No current season data", "teams": []}

    # Current standings
    current_standings = _compute_standings(curr_df, home_col, away_col)

    # Previous season standings (for expected position)
    if prev_df is not None and not prev_df.empty and len(prev_df) >= 30:
        prev_standings = _compute_standings(prev_df, home_col, away_col)
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
