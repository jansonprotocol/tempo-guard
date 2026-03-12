# backend/app/services/squad_availability.py
"""
ATHENA v2.0 — Squad Availability & Bench Depth Module.

Analyses the gap between a team's expected XI and their bench to detect
vulnerability. When key players are missing (inferred from minutes drop-off),
the engine can auto-adjust deg_nudge to reflect structural degradation.

Three outputs per team:
  1. expected_xi_power:  avg power of top 11 by minutes (positionally constrained)
  2. bench_power:        avg power of next 5–7 by minutes
  3. depth_vulnerability: per-zone flag when XI-to-bench gap > threshold

Integration:
  - depth_vulnerability feeds into deg_nudge via _auto_deg_from_depth()
  - Called from predict.py before the pipeline runs
  - Returns 0.0 if player data is unavailable (graceful fallback)

NOTE: This module does NOT know about real-time injuries or lineups.
It uses minutes-based heuristics to infer the expected XI. For day-of
adjustments, use the manual override endpoint (future session).
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from app.models.models_players import Player, PlayerSeasonStats
from app.models.team_config import TeamConfig


# ── Config ───────────────────────────────────────────────────────────────────

# Minimum positional slots for expected XI (1 GK, 4 DEF, 3 MID, 3 FWD = 11)
XI_SLOTS = {"GK": 1, "DEF": 4, "MID": 3, "FWD": 3}

# Bench size to analyse
BENCH_SIZE = 5

# Power gap threshold per zone — if the XI-to-bench drop exceeds this,
# flag the zone as vulnerable
VULNERABILITY_THRESHOLD = 12.0  # points on 0–100 scale

# Maximum auto-generated deg_nudge from depth vulnerability
# This is deliberately small — it's a supplement, not a replacement
# for calibration-derived deg_nudge values.
AUTO_DEG_MAX = 0.06

# Minimum minutes for a player to be considered in XI/bench selection
MIN_MINUTES_XI = 270  # ~3 full matches


def get_squad_depth(
    db: Session,
    team: str,
    league_code: str,
    season: str,
) -> dict | None:
    """
    Analyse squad depth for a team.

    Returns dict with:
      - xi_power: float (avg power of expected XI)
      - bench_power: float (avg power of bench)
      - zone_gaps: dict[str, float] (XI minus bench power per zone)
      - vulnerable_zones: list[str] (zones where gap > threshold)

    Returns None if insufficient player data.
    """
    # Load all players for this team+league+season
    players = (
        db.query(Player)
        .filter_by(current_team=team, league_code=league_code)
        .all()
    )

    if not players:
        return None

    player_ids = [p.id for p in players]
    player_map = {p.id: p for p in players}

    stats = (
        db.query(PlayerSeasonStats)
        .filter(
            PlayerSeasonStats.player_id.in_(player_ids),
            PlayerSeasonStats.season == season,
            PlayerSeasonStats.league_code == league_code,
        )
        .all()
    )

    # Filter to players with enough minutes and a computed power index
    qualified = [
        s for s in stats
        if s.minutes and s.minutes >= MIN_MINUTES_XI
        and s.power_index is not None
    ]

    if len(qualified) < 11:
        return None  # not enough data

    # ── Build expected XI (positionally constrained) ─────────────────
    # Sort all qualified players by minutes desc within each position
    by_position: dict[str, list[PlayerSeasonStats]] = {}
    for s in qualified:
        player = player_map.get(s.player_id)
        pos = player.position if player else "MID"
        by_position.setdefault(pos, []).append(s)

    for pos in by_position:
        by_position[pos].sort(key=lambda s: s.minutes or 0, reverse=True)

    # Pick top N per position for XI
    xi: list[PlayerSeasonStats] = []
    for pos, n_slots in XI_SLOTS.items():
        available = by_position.get(pos, [])
        xi.extend(available[:n_slots])

    # ── Build bench (next players by minutes, not in XI) ─────────────
    xi_ids = {s.id for s in xi}
    remaining = [s for s in qualified if s.id not in xi_ids]
    remaining.sort(key=lambda s: s.minutes or 0, reverse=True)
    bench = remaining[:BENCH_SIZE]

    if not xi or not bench:
        return None

    # ── Compute powers ───────────────────────────────────────────────
    xi_power = sum(s.power_index for s in xi) / len(xi)
    bench_power = sum(s.power_index for s in bench) / len(bench)

    # ── Per-zone gap analysis ────────────────────────────────────────
    zone_gaps = {}
    vulnerable_zones = []

    for pos, n_slots in XI_SLOTS.items():
        xi_zone = [s for s in xi if player_map.get(s.player_id) and player_map[s.player_id].position == pos]
        bench_zone = [s for s in bench if player_map.get(s.player_id) and player_map[s.player_id].position == pos]

        if xi_zone and bench_zone:
            xi_avg = sum(s.power_index for s in xi_zone) / len(xi_zone)
            bench_avg = sum(s.power_index for s in bench_zone) / len(bench_zone)
            gap = xi_avg - bench_avg
        elif xi_zone:
            xi_avg = sum(s.power_index for s in xi_zone) / len(xi_zone)
            # No bench players at this position — maximum vulnerability
            gap = xi_avg - 35.0  # assume very weak replacement
        else:
            gap = 0.0

        zone_gaps[pos] = round(gap, 1)
        if gap > VULNERABILITY_THRESHOLD:
            vulnerable_zones.append(pos)

    return {
        "xi_power": round(xi_power, 1),
        "bench_power": round(bench_power, 1),
        "overall_gap": round(xi_power - bench_power, 1),
        "zone_gaps": zone_gaps,
        "vulnerable_zones": vulnerable_zones,
        "xi_count": len(xi),
        "bench_count": len(bench),
    }


def auto_deg_from_depth(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
    season: str,
) -> float:
    """
    Compute an automatic deg_nudge adjustment based on squad depth vulnerability.

    If either team has vulnerable zones (large XI-to-bench gaps), this suggests
    structural fragility — injuries or rotation will cause a bigger performance
    drop than the macro model expects.

    Returns: additive adjustment to deg_pressure (0.0 to +AUTO_DEG_MAX).
    Returns 0.0 if player data is unavailable or no vulnerability detected.

    Called from predict.py as an optional enhancement to the DEG module.
    """
    home_depth = get_squad_depth(db, home_team, league_code, season)
    away_depth = get_squad_depth(db, away_team, league_code, season)

    total_vulnerable = 0
    max_gap = 0.0

    for depth in [home_depth, away_depth]:
        if depth:
            total_vulnerable += len(depth["vulnerable_zones"])
            for gap in depth["zone_gaps"].values():
                max_gap = max(max_gap, gap)

    if total_vulnerable == 0:
        return 0.0

    # Scale: 1 vulnerable zone = small nudge, 4+ = near maximum
    # The gap magnitude also matters — a 20-point gap is much worse than 13
    zone_factor = min(total_vulnerable / 6.0, 1.0)
    gap_factor = min((max_gap - VULNERABILITY_THRESHOLD) / 20.0, 1.0) if max_gap > VULNERABILITY_THRESHOLD else 0.0

    nudge = AUTO_DEG_MAX * (zone_factor * 0.6 + gap_factor * 0.4)
    nudge = round(min(nudge, AUTO_DEG_MAX), 4)

    if nudge > 0.005:
        home_zones = home_depth["vulnerable_zones"] if home_depth else []
        away_zones = away_depth["vulnerable_zones"] if away_depth else []
        print(
            f"[squad_avail] Depth vulnerability: "
            f"{home_team} zones={home_zones}, {away_team} zones={away_zones} "
            f"→ auto_deg={nudge:+.4f}"
        )

    return nudge
