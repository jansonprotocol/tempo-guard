"""
backend/app/services/player_power_reconstruct.py

Reconstruct player and team power as of a specific date using match-level stats.
"""

from datetime import date
from typing import Dict, List, Optional
from sqlalchemy import func
from sqlalchemy.orm import Session
import numpy as np

from app.models.models_players import Player, PlayerMatchStats, PlayerSeasonStats
from app.models.team_config import TeamConfig
from app.services.player_index import compute_player_power, ZONE_SLOTS, ZONE_WEIGHTS, MIN_MINUTES


def reconstruct_player_power_as_of(
    db: Session,
    player_id: int,
    as_of_date: date,
    position: str
) -> Optional[float]:
    """
    Reconstruct a player's power index as it would have been on a given date,
    using only match stats from before that date.
    """
    match_stats = db.query(PlayerMatchStats).filter(
        PlayerMatchStats.player_id == player_id,
        PlayerMatchStats.match_date < as_of_date
    ).all()

    if not match_stats:
        return None

    total_minutes = sum(ms.minutes for ms in match_stats)
    if total_minutes < MIN_MINUTES:
        return None

    per90 = total_minutes / 90
    aggregated = {
        "goals_per90": sum(ms.goals for ms in match_stats) / per90,
        "assists_per90": sum(ms.assists for ms in match_stats) / per90,
        "xg_per90": sum(ms.xg for ms in match_stats) / per90,
        "xa_per90": sum(ms.xa for ms in match_stats) / per90,
        "shots_per90": sum(ms.shots for ms in match_stats) / per90,
        "shots_on_target_per90": sum(ms.shots_on_target for ms in match_stats) / per90,
        "pass_completion_pct": (
            sum(ms.passes_completed for ms in match_stats) /
            max(1, sum(ms.passes_attempted for ms in match_stats)) * 100
        ),
        "tackles_per90": sum(ms.tackles for ms in match_stats) / per90,
        "interceptions_per90": sum(ms.interceptions for ms in match_stats) / per90,
        "blocks_per90": sum(ms.blocks for ms in match_stats) / per90,
        "saves_per90": sum(ms.saves for ms in match_stats) / per90,
    }

    power = _calculate_power_from_aggregates(aggregated, position)
    return round(power, 1)


def _calculate_power_from_aggregates(stats: Dict, position: str) -> float:
    """
    Calculate power index from aggregated stats.
    Mirrors the compute_player_power logic in player_index.py.
    """
    from app.services.player_index import ROLE_WEIGHTS
    weights = ROLE_WEIGHTS.get(position, ROLE_WEIGHTS["MID"])

    composite = 0.0
    total_weight = 0.0

    for stat_name, weight in weights.items():
        if stat_name in stats and stats[stat_name] is not None:
            composite += stats[stat_name] * weight
            total_weight += weight

    if total_weight == 0:
        return 50.0

    avg_composite = composite / total_weight
    power = 50.0 + (avg_composite * 10)
    return max(0.0, min(100.0, power))


def reconstruct_team_power_as_of(
    db: Session,
    team_key: str,
    league_code: str,
    as_of_date: date
) -> Dict[str, Optional[float]]:
    """
    Reconstruct a team's zonal power scores as of a given date.
    """
    players = db.query(Player).filter(
        Player.current_team == team_key,
        Player.league_code == league_code
    ).all()

    if not players:
        return {
            "squad_power": None,
            "atk_power": None,
            "mid_power": None,
            "def_power": None,
            "gk_power": None,
        }

    player_powers = []
    for player in players:
        power = reconstruct_player_power_as_of(db, player.id, as_of_date, player.position)
        if power:
            player_powers.append({
                "player_id": player.id,
                "position": player.position,
                "power": power,
                "minutes": _get_player_minutes_up_to(db, player.id, as_of_date),
            })

    # Group by position and take top N (same logic as player_index.py)
    position_groups: Dict[str, List] = {}
    for p in player_powers:
        pos = p["position"]
        position_groups.setdefault(pos, []).append(p)

    zonal_scores = {}
    for zone, n_slots in ZONE_SLOTS.items():
        group = position_groups.get(zone, [])
        group.sort(key=lambda x: x.get("minutes", 0), reverse=True)
        top = group[:n_slots]

        if top:
            powers = [p["power"] for p in top]
            zonal_scores[zone] = round(float(np.mean(powers)), 1)
        else:
            zonal_scores[zone] = None

    # Weighted squad power
    squad_power = 0.0
    weight_sum = 0.0
    for zone, weight in ZONE_WEIGHTS.items():
        if zonal_scores.get(zone) is not None:
            squad_power += zonal_scores[zone] * weight
            weight_sum += weight

    squad_power_final = round(squad_power / weight_sum, 1) if weight_sum > 0 else None

    return {
        "squad_power": squad_power_final,
        "atk_power": zonal_scores.get("FWD"),
        "mid_power": zonal_scores.get("MID"),
        "def_power": zonal_scores.get("DEF"),
        "gk_power": zonal_scores.get("GK"),
    }


def _get_player_minutes_up_to(db: Session, player_id: int, as_of_date: date) -> int:
    """Get total minutes played by a player up to (not including) a given date."""
    result = (
        db.query(func.sum(PlayerMatchStats.minutes))
        .filter(
            PlayerMatchStats.player_id == player_id,
            PlayerMatchStats.match_date < as_of_date,
        )
        .scalar()
    )
    return result or 0
