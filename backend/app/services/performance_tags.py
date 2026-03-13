# backend/app/services/performance_tags.py
"""
ATHENA v2.0 — Performance Tags for Match Predictions.

Generates human-readable contextual tags for any matchup based on:
  1. Form Delta (over/underperforming vs expected league position)
  2. Zonal strengths/weaknesses (DEF leaking, strong ATK, etc.)
  3. Matchup narrative (Underperformer vs Overperformer)

Two output levels:
  - matchup_tag:  "Underperformer vs On Track" (one line, for card headers)
  - detail_tags:  ["Home: Defense Leaking", "Away: Strong Attack", ...]
                  (expanded detail, per-team zone insights)

Called from:
  - routes_predict.py (main matchup endpoint)
  - routes_batch.py (GET /predictions for the list)
  - routes_player_power.py (standalone endpoint)

Returns empty lists gracefully when player data is unavailable.
"""
from __future__ import annotations

from typing import Optional

from sqlalchemy.orm import Session

from app.models.team_config import TeamConfig


# ── Tag thresholds ───────────────────────────────────────────────────────────

# Zone power vs league average thresholds
ZONE_STRONG_THRESHOLD = 5.0    # power above league avg = "strong" tag
ZONE_WEAK_THRESHOLD   = -5.0   # power below league avg = "leaking/weak" tag
ZONE_ELITE_THRESHOLD  = 10.0   # well above = "elite" tag
ZONE_POOR_THRESHOLD   = -10.0  # well below = "collapsing" tag

# Form delta thresholds
OVER_THRESHOLD  = 3    # positions above expected = overperforming
UNDER_THRESHOLD = -3   # positions below expected = underperforming


def _team_status_label(form_delta: int | None) -> str:
    """Human-readable label for a team's form delta."""
    if form_delta is None:
        return "No Data"
    if form_delta >= 5:
        return "Surging"
    if form_delta >= OVER_THRESHOLD:
        return "Overperformer"
    if form_delta >= 1:
        return "Slightly Over"
    if form_delta == 0:
        return "On Track"
    if form_delta >= UNDER_THRESHOLD:
        return "Slightly Under"
    if form_delta >= -5:
        return "Underperformer"
    return "In Crisis"


def _zone_tag(zone_name: str, power: float, league_avg: float) -> Optional[str]:
    """
    Generate a zone-specific tag based on how the team's zone power
    compares to the league average for that zone.
    """
    gap = power - league_avg

    if zone_name == "ATK":
        if gap >= ZONE_ELITE_THRESHOLD:
            return "Elite Attack"
        if gap >= ZONE_STRONG_THRESHOLD:
            return "Strong Attack"
        if gap <= ZONE_POOR_THRESHOLD:
            return "Attack Broken"
        if gap <= ZONE_WEAK_THRESHOLD:
            return "Blunt Attack"

    elif zone_name == "MID":
        if gap >= ZONE_ELITE_THRESHOLD:
            return "Midfield Dominant"
        if gap >= ZONE_STRONG_THRESHOLD:
            return "Strong Midfield"
        if gap <= ZONE_POOR_THRESHOLD:
            return "Midfield Collapse"
        if gap <= ZONE_WEAK_THRESHOLD:
            return "Midfield Imbalanced"

    elif zone_name == "DEF":
        if gap >= ZONE_ELITE_THRESHOLD:
            return "Fortress Defense"
        if gap >= ZONE_STRONG_THRESHOLD:
            return "Solid Defense"
        if gap <= ZONE_POOR_THRESHOLD:
            return "Defense Collapsing"
        if gap <= ZONE_WEAK_THRESHOLD:
            return "Defense Leaking"

    elif zone_name == "GK":
        if gap >= ZONE_ELITE_THRESHOLD:
            return "Elite Keeper"
        if gap >= ZONE_STRONG_THRESHOLD:
            return "Strong in Goal"
        if gap <= ZONE_POOR_THRESHOLD:
            return "GK Liability"
        if gap <= ZONE_WEAK_THRESHOLD:
            return "Weak in Goal"

    return None


def get_team_tags(
    tc: TeamConfig | None,
    league_avgs: dict[str, float],
    form_delta: int | None = None,
) -> dict:
    """
    Generate performance tags for a single team.

    Args:
        tc: TeamConfig with squad/zonal power (or None)
        league_avgs: {"atk": avg, "mid": avg, "def": avg, "gk": avg}
        form_delta: expected_pos - actual_pos (or None if unknown)

    Returns:
        {
            "status": "Underperformer",
            "form_delta": -3,
            "zone_tags": ["Defense Leaking", "Blunt Attack"],
            "primary_issue": "DEF" | None,
        }
    """
    result = {
        "status": _team_status_label(form_delta),
        "form_delta": form_delta,
        "zone_tags": [],
        "primary_issue": None,
    }

    if not tc:
        return result

    worst_gap = 0.0
    worst_zone = None

    for zone_key, zone_name in [("atk_power", "ATK"), ("mid_power", "MID"),
                                 ("def_power", "DEF"), ("gk_power", "GK")]:
        power = getattr(tc, zone_key, None)
        if power is None:
            continue

        avg_key = zone_name.lower()
        league_avg = league_avgs.get(avg_key, 50.0)
        tag = _zone_tag(zone_name, float(power), league_avg)

        if tag:
            result["zone_tags"].append(tag)

        gap = float(power) - league_avg
        if gap < worst_gap:
            worst_gap = gap
            worst_zone = zone_name

    # Primary issue: worst zone if it's below threshold
    if worst_gap <= ZONE_WEAK_THRESHOLD:
        result["primary_issue"] = worst_zone

    return result


def _compute_league_zone_avgs(db: Session, league_code: str) -> dict[str, float]:
    """Compute average zonal power across all teams in a league."""
    teams = (
        db.query(TeamConfig)
        .filter(
            TeamConfig.league_code == league_code,
            TeamConfig.squad_power.isnot(None),
        )
        .all()
    )

    if not teams:
        return {"atk": 50.0, "mid": 50.0, "def": 50.0, "gk": 50.0}

    avgs = {}
    for zone_key, avg_key in [("atk_power", "atk"), ("mid_power", "mid"),
                               ("def_power", "def"), ("gk_power", "gk")]:
        vals = [float(getattr(t, zone_key)) for t in teams if getattr(t, zone_key) is not None]
        avgs[avg_key] = round(sum(vals) / len(vals), 1) if vals else 50.0

    return avgs


def generate_match_tags(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
    form_deltas: dict[str, int] | None = None,
) -> dict:
    """
    Generate performance tags for a full matchup.

    Args:
        form_deltas: optional pre-computed {team_name: delta} dict.
                     If None, form delta info is skipped (tags still work
                     from zonal power data alone).

    Returns:
        {
            "matchup_tag": "Underperformer vs Overperformer",
            "home": { "status": "...", "zone_tags": [...], ... },
            "away": { "status": "...", "zone_tags": [...], ... },
        }
    """
    # Load team configs
    home_cfg = (
        db.query(TeamConfig)
        .filter_by(league_code=league_code, team=home_team)
        .first()
    )
    away_cfg = (
        db.query(TeamConfig)
        .filter_by(league_code=league_code, team=away_team)
        .first()
    )

    # Compute league zone averages
    league_avgs = _compute_league_zone_avgs(db, league_code)

    # Form delta lookup
    home_delta = form_deltas.get(home_team) if form_deltas else None
    away_delta = form_deltas.get(away_team) if form_deltas else None

    home_tags = get_team_tags(home_cfg, league_avgs, home_delta)
    away_tags = get_team_tags(away_cfg, league_avgs, away_delta)

    # Build matchup headline
    matchup_tag = f"{home_tags['status']} vs {away_tags['status']}"

    return {
        "matchup_tag": matchup_tag,
        "home": home_tags,
        "away": away_tags,
    }


def generate_match_tags_with_delta(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
) -> dict:
    """
    Full version that also computes form delta from standings.
    Slower than generate_match_tags() but self-contained.
    """
    # Try to compute form deltas from snapshot
    form_deltas = {}
    try:
        from app.services.form_delta import compute_form_delta
        delta_data = compute_form_delta(db, league_code)
        if delta_data and delta_data.get("teams"):
            form_deltas = {
                t["team"]: t["form_delta"]
                for t in delta_data["teams"]
            }
    except Exception:
        pass  # Form delta unavailable — tags still work from zonal power

    return generate_match_tags(db, league_code, home_team, away_team, form_deltas)
