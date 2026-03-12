# backend/app/services/player_index.py
"""
ATHENA v2.0 — Player Power Index Engine.

Converts raw per-90 stats into a 0–100 power index per player,
then aggregates into zonal team scores (ATK/MID/DEF/GK).

Algorithm:
  1. Group all PlayerSeasonStats rows by position within a league+season
  2. For each stat column, compute z-score within the position group:
       z = (player_value - group_mean) / group_stdev
  3. Apply role-specific weights to each z-score
  4. Sum weighted z-scores into a composite
  5. Convert to 0–100 scale:
       power = clip(50 + composite * 15, 0, 100)

     This centres average players at 50, with:
       ±1σ → 35–65
       ±2σ → 20–80
       ±3σ → 5–95 (elite / terrible)

  6. Squad aggregation picks the top players by minutes at each position
     and computes weighted zonal averages.

The scale is LEAGUE-RELATIVE — a "75" in ENG-PL means something different
than a "75" in NOR-EL.  Cross-league normalisation (Phase 3) uses
LeagueConfig.strength_coefficient to translate between leagues.

Usage:
    from app.services.player_index import compute_league_power
    results = compute_league_power("ENG-PL", "2025-2026")
    # Writes power_index to PlayerSeasonStats
    # Writes squad/zonal power to TeamConfig + SquadSnapshot
"""
from __future__ import annotations

import json
from datetime import datetime, date
from typing import Optional

import numpy as np
from sqlalchemy.orm import Session

from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.models.team_config import TeamConfig


# ── Role-specific stat weights ───────────────────────────────────────────────
# Each position uses a different weighting of available stats.
# Weights per position must sum to 1.0.
# Stats not listed for a position contribute 0 to the composite.

ROLE_WEIGHTS: dict[str, dict[str, float]] = {
    "FWD": {
        "goals_per90":     0.30,
        "assists_per90":   0.20,
        "xg_per90":        0.25,
        "xa_per90":        0.15,
        "sca_per90":       0.10,
    },
    "MID": {
        "goals_per90":                0.10,
        "assists_per90":              0.15,
        "xg_per90":                   0.10,
        "xa_per90":                   0.20,
        "sca_per90":                  0.15,
        "progressive_passes_per90":   0.15,
        "progressive_carries_per90":  0.10,
        "pass_completion_pct":        0.05,
    },
    "DEF": {
        "tackles_won_per90":    0.25,
        "interceptions_per90":  0.25,
        "blocks_per90":         0.15,
        "clearances_per90":     0.15,
        "aerials_won_pct":      0.10,
        # Defenders who contribute going forward get a small bonus
        "progressive_passes_per90":  0.05,
        "progressive_carries_per90": 0.05,
    },
    "GK": {
        "save_pct":        0.40,
        "cs_pct":          0.30,
        "psxg_minus_ga":   0.30,
    },
}

# ── Squad aggregation config ─────────────────────────────────────────────────
# How many players per zone (by minutes played) to include in the average
ZONE_SLOTS = {
    "FWD": 3,
    "MID": 4,
    "DEF": 4,
    "GK":  1,
}

# Zone weights for overall squad_power
ZONE_WEIGHTS = {
    "FWD": 0.30,
    "MID": 0.25,
    "DEF": 0.30,
    "GK":  0.15,
}

# Minimum minutes to be considered for power index (filters youth/cameos)
MIN_MINUTES = 180  # ~2 full matches


def _get_stat_value(row: PlayerSeasonStats, stat: str) -> float:
    """Safely get a stat value from a PlayerSeasonStats row."""
    val = getattr(row, stat, None)
    if val is None:
        return 0.0
    try:
        return float(val)
    except (ValueError, TypeError):
        return 0.0


def compute_player_power(
    stats_rows: list[PlayerSeasonStats],
    position: str,
) -> dict[int, float]:
    """
    Compute power index for a group of players at the same position.

    Args:
        stats_rows: all PlayerSeasonStats rows for this position group
        position: GK/DEF/MID/FWD

    Returns:
        dict mapping PlayerSeasonStats.id → power index (0–100)
    """
    weights = ROLE_WEIGHTS.get(position, ROLE_WEIGHTS["MID"])

    if len(stats_rows) < 3:
        # Not enough players for meaningful z-scores — assign neutral 50
        return {r.id: 50.0 for r in stats_rows}

    stat_names = list(weights.keys())

    # Build matrix: rows = players, cols = stats
    matrix = np.zeros((len(stats_rows), len(stat_names)))
    for i, row in enumerate(stats_rows):
        for j, stat in enumerate(stat_names):
            matrix[i, j] = _get_stat_value(row, stat)

    # Z-score normalise each column
    means  = np.nanmean(matrix, axis=0)
    stdevs = np.nanstd(matrix, axis=0)

    # Avoid division by zero — if stdev is 0, all players are identical
    stdevs[stdevs < 1e-9] = 1.0

    z_matrix = (matrix - means) / stdevs

    # Weighted composite per player
    weight_vec = np.array([weights.get(s, 0.0) for s in stat_names])
    composites = z_matrix @ weight_vec

    # Convert to 0–100 scale
    # 50 = league average for this position
    # Each σ = 15 points
    results = {}
    for i, row in enumerate(stats_rows):
        power = float(np.clip(50.0 + composites[i] * 15.0, 0.0, 100.0))
        results[row.id] = round(power, 1)

    return results


def compute_league_power(
    db: Session,
    league_code: str,
    season: str,
    write_snapshots: bool = True,
) -> dict:
    """
    Compute power indices for ALL players in a league+season,
    then aggregate into zonal team scores.

    Steps:
      1. Load all PlayerSeasonStats for this league+season
      2. Group by position → compute z-score power per group
      3. Write power_index back to PlayerSeasonStats rows
      4. For each team: pick top players per zone, compute zonal averages
      5. Write squad_power + zonal scores to TeamConfig
      6. Optionally write SquadSnapshot

    Returns summary dict for logging/API response.
    """
    # ── Load all stats rows with their Player records ────────────────
    rows = (
        db.query(PlayerSeasonStats)
        .filter_by(league_code=league_code, season=season)
        .all()
    )

    if not rows:
        print(f"[player_index] No stats found for {league_code} {season}")
        return {"league_code": league_code, "players": 0}

    # Load player records for position info
    player_ids = list(set(r.player_id for r in rows))
    players = {
        p.id: p
        for p in db.query(Player).filter(Player.id.in_(player_ids)).all()
    }

    # Filter to players with meaningful minutes
    qualified = [r for r in rows if r.minutes and r.minutes >= MIN_MINUTES]
    print(f"[player_index] {league_code} {season}: {len(rows)} total, {len(qualified)} qualified (≥{MIN_MINUTES} min)")

    # ── Group by position and compute power indices ──────────────────
    position_groups: dict[str, list[PlayerSeasonStats]] = {}
    for row in qualified:
        player = players.get(row.player_id)
        pos = player.position if player else "MID"
        position_groups.setdefault(pos, []).append(row)

    all_power: dict[int, float] = {}  # stats_row_id → power

    for pos, group in position_groups.items():
        power_map = compute_player_power(group, pos)
        all_power.update(power_map)
        print(f"  {pos}: {len(group)} players, "
              f"power range {min(power_map.values()):.1f}–{max(power_map.values()):.1f}")

    # Write power_index back to DB
    for row in qualified:
        if row.id in all_power:
            row.power_index = all_power[row.id]

    # ── Squad aggregation ────────────────────────────────────────────
    # Group qualified players by team
    team_groups: dict[str, dict[str, list[tuple[PlayerSeasonStats, Player]]]] = {}

    for row in qualified:
        player = players.get(row.player_id)
        if not player or not player.current_team:
            continue
        team = player.current_team
        pos = player.position or "MID"
        team_groups.setdefault(team, {}).setdefault(pos, []).append((row, player))

    team_results = {}

    for team, positions in team_groups.items():
        zonal_scores = {}

        for zone, n_slots in ZONE_SLOTS.items():
            zone_players = positions.get(zone, [])
            # Sort by minutes (most-used first) and take top N
            zone_players.sort(key=lambda x: x[0].minutes or 0, reverse=True)
            top = zone_players[:n_slots]

            if top:
                powers = [all_power.get(r.id, 50.0) for r, _ in top]
                zonal_scores[zone] = round(float(np.mean(powers)), 1)
            else:
                zonal_scores[zone] = None

        # Compute overall squad_power as weighted blend
        squad_power = 0.0
        weight_sum = 0.0
        for zone, weight in ZONE_WEIGHTS.items():
            if zonal_scores.get(zone) is not None:
                squad_power += zonal_scores[zone] * weight
                weight_sum += weight

        squad_power = round(squad_power / weight_sum, 1) if weight_sum > 0 else None

        team_results[team] = {
            "squad_power": squad_power,
            "atk_power": zonal_scores.get("FWD"),
            "mid_power": zonal_scores.get("MID"),
            "def_power": zonal_scores.get("DEF"),
            "gk_power":  zonal_scores.get("GK"),
        }

        # ── Write to TeamConfig ──────────────────────────────────────
        tc = (
            db.query(TeamConfig)
            .filter_by(league_code=league_code, team=team)
            .first()
        )
        if tc:
            tc.squad_power = squad_power
            tc.atk_power   = zonal_scores.get("FWD")
            tc.mid_power   = zonal_scores.get("MID")
            tc.def_power   = zonal_scores.get("DEF")
            tc.gk_power    = zonal_scores.get("GK")

        # ── Write SquadSnapshot zonal scores ─────────────────────────
        if write_snapshots:
            today = date.today()
            snap = (
                db.query(SquadSnapshot)
                .filter_by(team=team, league_code=league_code, snapshot_date=today)
                .first()
            )
            if snap:
                snap.squad_power = squad_power
                snap.atk_power   = zonal_scores.get("FWD")
                snap.mid_power   = zonal_scores.get("MID")
                snap.def_power   = zonal_scores.get("DEF")
                snap.gk_power    = zonal_scores.get("GK")

    db.commit()

    print(f"\n  [player_index] {league_code}: {len(team_results)} teams updated")

    return {
        "league_code": league_code,
        "season": season,
        "players_indexed": len(all_power),
        "teams_updated": len(team_results),
        "teams": team_results,
    }


def compute_all_leagues(db: Session, season_map: dict[str, str]) -> list[dict]:
    """
    Run power index computation for all leagues.

    season_map: {"ENG-PL": "2025-2026", "BRA-SA": "2026", ...}
    Returns list of per-league result dicts.
    """
    results = []
    for league_code, season in season_map.items():
        try:
            result = compute_league_power(db, league_code, season)
            results.append(result)
        except Exception as e:
            print(f"[player_index] Error for {league_code}: {e}")
            results.append({"league_code": league_code, "error": str(e)})
    return results
