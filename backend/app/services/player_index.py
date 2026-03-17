# backend/app/services/player_index.py
from __future__ import annotations  # <-- MUST BE FIRST

import json
from datetime import datetime, date
from typing import Optional

import numpy as np
from sqlalchemy.orm import Session

from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.models.team_config import TeamConfig
from app.services.resolve_team import resolve_team_name  


ROLE_WEIGHTS: dict[str, dict[str, float]] = {
    "FWD": {
        "goals_per90": 0.30, "assists_per90": 0.20, "xg_per90": 0.25,
        "xa_per90": 0.15, "sca_per90": 0.10,
    },
    "MID": {
        "goals_per90": 0.10, "assists_per90": 0.15, "xg_per90": 0.10,
        "xa_per90": 0.20, "sca_per90": 0.15,
        "progressive_passes_per90": 0.15, "progressive_carries_per90": 0.10,
        "pass_completion_pct": 0.05,
    },
    "DEF": {
        "tackles_won_per90": 0.25, "interceptions_per90": 0.25,
        "blocks_per90": 0.15, "clearances_per90": 0.15, "aerials_won_pct": 0.10,
        "progressive_passes_per90": 0.05, "progressive_carries_per90": 0.05,
    },
    "GK": {
        "save_pct": 0.40, "cs_pct": 0.30, "psxg_minus_ga": 0.30,
    },
}

ZONE_SLOTS = {"FWD": 3, "MID": 4, "DEF": 4, "GK": 1}
ZONE_WEIGHTS = {"FWD": 0.30, "MID": 0.25, "DEF": 0.30, "GK": 0.15}
MIN_MINUTES = 180


def _get_stat_value(row: PlayerSeasonStats, stat: str) -> float:
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
    weights = ROLE_WEIGHTS.get(position, ROLE_WEIGHTS["MID"])

    if len(stats_rows) < 3:
        return {r.id: 50.0 for r in stats_rows}

    stat_names = list(weights.keys())
    matrix = np.zeros((len(stats_rows), len(stat_names)))
    for i, row in enumerate(stats_rows):
        for j, stat in enumerate(stat_names):
            matrix[i, j] = _get_stat_value(row, stat)

    means  = np.nanmean(matrix, axis=0)
    stdevs = np.nanstd(matrix, axis=0)
    stdevs[stdevs < 1e-9] = 1.0
    z_matrix = (matrix - means) / stdevs

    weight_vec = np.array([weights.get(s, 0.0) for s in stat_names])
    composites = z_matrix @ weight_vec

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
    rows = (
        db.query(PlayerSeasonStats)
        .filter_by(league_code=league_code, season=season)
        .all()
    )

    if not rows:
        print(f"[player_index] No stats found for {league_code} {season}")
        return {"league_code": league_code, "players": 0}

    player_ids = list(set(r.player_id for r in rows))
    players = {
        p.id: p
        for p in db.query(Player).filter(Player.id.in_(player_ids)).all()
    }

    qualified = [r for r in rows if r.minutes and r.minutes >= MIN_MINUTES]
    print(f"[player_index] {league_code} {season}: {len(rows)} total, {len(qualified)} qualified (≥{MIN_MINUTES} min)")

    # ── Group by position and compute power indices ──────────────────
    position_groups: dict[str, list[PlayerSeasonStats]] = {}
    for row in qualified:
        player = players.get(row.player_id)
        pos = player.position if player else "MID"
        position_groups.setdefault(pos, []).append(row)

    all_power: dict[int, float] = {}

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
    team_groups: dict[str, dict[str, list[tuple[PlayerSeasonStats, Player]]]] = {}

    for row in qualified:
        player = players.get(row.player_id)
        if not player or not player.current_team:
            continue
        # Resolve the raw team name to its canonical key
        resolved_team = resolve_team_name(db, player.current_team, league_code)
        pos = player.position or "MID"
        team_groups.setdefault(resolved_team, {}).setdefault(pos, []).append((row, player))

    team_results = {}

    for team, positions in team_groups.items():
        zonal_scores = {}

        for zone, n_slots in ZONE_SLOTS.items():
            zone_players = positions.get(zone, [])
            zone_players.sort(key=lambda x: x[0].minutes or 0, reverse=True)
            top = zone_players[:n_slots]

            if top:
                powers = [all_power.get(r.id, 50.0) for r, _ in top]
                zonal_scores[zone] = round(float(np.mean(powers)), 1)
            else:
                zonal_scores[zone] = None

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

        # Write to TeamConfig (use resolved team name)
        tc = db.query(TeamConfig).filter_by(league_code=league_code, team=team).first()
        if tc:
            tc.squad_power = squad_power
            tc.atk_power   = zonal_scores.get("FWD")
            tc.mid_power   = zonal_scores.get("MID")
            tc.def_power   = zonal_scores.get("DEF")
            tc.gk_power    = zonal_scores.get("GK")
        else:
            # Create if not exists
            tc = TeamConfig(
                league_code=league_code,
                team=team,
                squad_power=squad_power,
                atk_power=zonal_scores.get("FWD"),
                mid_power=zonal_scores.get("MID"),
                def_power=zonal_scores.get("DEF"),
                gk_power=zonal_scores.get("GK"),
            )
            db.add(tc)

        # Write SquadSnapshot zonal scores (use resolved team name)
        if write_snapshots:
            today = date.today()
            snap = db.query(SquadSnapshot).filter_by(
                team=team, league_code=league_code, snapshot_date=today
            ).first()
            if snap:
                snap.squad_power = squad_power
                snap.atk_power   = zonal_scores.get("FWD")
                snap.mid_power   = zonal_scores.get("MID")
                snap.def_power   = zonal_scores.get("DEF")
                snap.gk_power    = zonal_scores.get("GK")
            else:
                snap = SquadSnapshot(
                    team=team,
                    league_code=league_code,
                    snapshot_date=today,
                    squad_power=squad_power,
                    atk_power=zonal_scores.get("FWD"),
                    mid_power=zonal_scores.get("MID"),
                    def_power=zonal_scores.get("DEF"),
                    gk_power=zonal_scores.get("GK"),
                    player_ids="[]"  # empty list, can be filled later
                )
                db.add(snap)

    # ── Performance ratings (player vs team average) ─────────────────
    _apply_performance_ratings(db, league_code, qualified, players, team_results)

    db.commit()
    print(f"\n  [player_index] {league_code}: {len(team_results)} teams updated")

    return {
        "league_code": league_code,
        "season": season,
        "players_indexed": len(all_power),
        "teams_updated": len(team_results),
        "teams": team_results,
    }

def _apply_performance_ratings(
    db: Session,
    league_code: str,
    qualified: list[PlayerSeasonStats],
    players: dict[int, Player],
    team_results: dict[str, dict],
):
    """
    Calculate how much each player over/underperforms relative to their
    team's squad_power. Written to PlayerSeasonStats.performance_delta.

    Positive delta = player is stronger than team average (carrying the team)
    Negative delta = player is weaker than team average (dragging the team down)

    Uses the Player → current_team join to find which team each player belongs to,
    then compares their power_index against that team's squad_power.
    """
    for row in qualified:
        if row.power_index is None:
            continue

        player = players.get(row.player_id)
        if not player or not player.current_team:
            continue

        team_data = team_results.get(player.current_team)
        if not team_data or team_data.get("squad_power") is None:
            continue

        row.performance_delta = round(row.power_index - team_data["squad_power"], 2)


def compute_all_leagues(db: Session, season_map: dict[str, str]) -> list[dict]:
    results = []
    for league_code, season in season_map.items():
        try:
            result = compute_league_power(db, league_code, season)
            results.append(result)
        except Exception as e:
            print(f"[player_index] Error for {league_code}: {e}")
            results.append({"league_code": league_code, "error": str(e)})
    return results
