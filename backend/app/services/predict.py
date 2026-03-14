# backend/app/services/predict.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.engine.types import MatchRequest, Prediction
from app.engine.pipeline import evaluate_athena
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig
from app.services.squad_availability import auto_deg_from_depth


# ── v2.0: Player power blend weight ─────────────────────────────────────────
# Controls how much player-derived squad power influences support_delta.
# 0.0 = player power has no effect (v1.x behaviour)
# 0.3 = 30% influence (recommended starting point)
# 1.0 = player power fully replaces macro team_nudge (not recommended)
#
# This is deliberately conservative. The macro signals (goals, SOT, tempo)
# have been battle-tested through calibration. Player power is new and
# unvalidated. Starting at 30% lets it influence predictions without
# dominating until calibration confirms its accuracy.
PLAYER_POWER_BLEND = 0.30

# Maximum effect player power delta can have on support_delta.
# Prevents a single massive squad imbalance from overwhelming the model.
PLAYER_POWER_MAX_EFFECT = 0.08

# International league codes — used for cross-league normalisation
INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}

# Calendar-year leagues use "2026", Aug–May leagues use "2025-2026"
_CALENDAR_YEAR_PREFIXES = {"BRA", "MLS", "NOR", "SWE", "CHN", "JPN", "COL"}


def _current_season(league_code: str) -> str:
    """
    Derive the current season label from a league code.
    Calendar-year leagues (BRA-SA, MLS, NOR-EL, etc.) → "2026"
    Aug–May leagues (ENG-PL, ESP-LL, etc.) → "2025-2026"
    """
    prefix = league_code.split("-")[0] if "-" in league_code else league_code
    if prefix in _CALENDAR_YEAR_PREFIXES:
        return "2026"
    return "2025-2026"


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _get_league_config(db: Session, league_code: str) -> LeagueConfig | None:
    return (
        db.query(LeagueConfig)
        .filter(LeagueConfig.league_code == league_code)
        .first()
    )


def _get_league_bias(db: Session, league_code: str) -> tuple[float, float, float]:
    """
    Look up league-level bias configuration from DB.
    Returns (over_bias, under_bias, tempo_factor).

    These values are applied as ADDITIVE adjustments inside evaluate_athena:
      - over_bias - under_bias  → shifts support_delta
      - tempo_factor            → scales raw tempo signal (1.0 = neutral)

    Falls back to conservative neutral defaults when league not found.
    """
    cfg = _get_league_config(db, league_code)
    if not cfg:
        return 0.05, 0.05, 0.50  # neutral defaults

    return (
        float(cfg.base_over_bias  or 0.5),
        float(cfg.base_under_bias or 0.5),
        float(cfg.tempo_factor    or 0.50),
    )


def _get_team_configs(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
) -> tuple[TeamConfig | None, TeamConfig | None]:
    """Return (home_cfg, away_cfg) — either may be None."""
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
    return home_cfg, away_cfg


def _get_team_nudge(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
) -> float:
    """
    Look up per-team calibration nudges for both teams in the matchup.
    Returns combined nudge = (home_nudge + away_nudge) / 2.

    Each team's nudge reflects ATHENA's historical miss pattern for that team:
      positive → team matchups tend to produce more goals than signals suggest
      negative → team matchups tend to produce fewer goals than signals suggest

    Both home and away nudges are averaged because both teams contribute
    equally to the total goals context. A leaky home defense and a clinical
    away attack should both push the combined nudge positive.

    Returns 0.0 if no team configs exist yet (neutral — no effect).
    """
    home_cfg, away_cfg = _get_team_configs(db, league_code, home_team, away_team)
    home_nudge = float(home_cfg.over_nudge or 0.0) if home_cfg else 0.0
    away_nudge = float(away_cfg.over_nudge or 0.0) if away_cfg else 0.0
    return (home_nudge + away_nudge) / 2.0


def _get_player_power_nudge(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
) -> float:
    """
    v2.0 — Compute a support_delta adjustment from player-derived squad power.

    Reads squad_power from TeamConfig for both teams and computes:
      power_delta = (home_squad_power - away_squad_power) / 100

    This delta is then scaled by PLAYER_POWER_BLEND and clipped to
    ±PLAYER_POWER_MAX_EFFECT.

    For international competitions (UCL/UEL/UECL), the raw squad_power
    values are normalised by each team's home league strength_coefficient
    before computing the delta. This ensures a "75" in the Eredivisie
    is correctly treated as weaker than a "75" in the Premier League.

    Returns 0.0 in any of these cases:
      - Either team has no squad_power yet (player data not scraped)
      - PLAYER_POWER_BLEND is 0.0
      - Both teams have identical squad power
    """
    if PLAYER_POWER_BLEND <= 0.0:
        return 0.0

    home_cfg, away_cfg = _get_team_configs(db, league_code, home_team, away_team)

    home_power = float(home_cfg.squad_power) if home_cfg and home_cfg.squad_power is not None else None
    away_power = float(away_cfg.squad_power) if away_cfg and away_cfg.squad_power is not None else None

    # Both teams need power scores — graceful fallback to 0.0 otherwise
    if home_power is None or away_power is None:
        return 0.0

    # ── Cross-league normalisation for international competitions ─────
    # If this is a UCL/UEL/UECL match, the two teams may come from
    # different leagues with different quality baselines.
    # Multiply each team's power by their home league's strength_coefficient.
    if league_code in INTL_LEAGUE_CODES:
        home_league = home_cfg.league_code if home_cfg else None
        away_league = away_cfg.league_code if away_cfg else None

        if home_league:
            home_league_cfg = _get_league_config(db, home_league)
            if home_league_cfg and home_league_cfg.strength_coefficient:
                home_power *= float(home_league_cfg.strength_coefficient)

        if away_league:
            away_league_cfg = _get_league_config(db, away_league)
            if away_league_cfg and away_league_cfg.strength_coefficient:
                away_power *= float(away_league_cfg.strength_coefficient)

    # power_delta: positive = home stronger, negative = away stronger
    # Normalise to a small range (squad powers are 0–100)
    power_delta = (home_power - away_power) / 100.0

    # Apply blend weight and clip
    nudge = power_delta * PLAYER_POWER_BLEND
    nudge = _clip(nudge, -PLAYER_POWER_MAX_EFFECT, PLAYER_POWER_MAX_EFFECT)

    if abs(nudge) > 0.005:
        print(
            f"[predict] Player power: {home_team}={home_power:.1f} vs "
            f"{away_team}={away_power:.1f} → delta={power_delta:.3f} "
            f"→ nudge={nudge:+.4f} (blend={PLAYER_POWER_BLEND})"
        )

    return round(nudge, 4)


def _apply_module_adjustments(
    req: MatchRequest,
    db: Session,
) -> MatchRequest:
    """
    Apply league-level sensitivity multipliers and team-level module nudges
    to the raw DEG/DET/EPS features in the MatchRequest before the pipeline runs.

    This is the layer that allows:
      - League-wide amplification of DEG/DET/EPS signals based on calibration history
        (e.g. Italian Serie A gets higher DEG sensitivity — structured decline is real there)
      - Per-team DET nudge that pushes volatile teams toward bilateral chaos escalation
        (e.g. Man City + Liverpool both carry +0.12 det_nudge → bilateral fires when they meet)
      - Per-team DEG nudge that captures team-specific structural decline signals
        (e.g. newly promoted sides get +0.06 deg_nudge even when recent form looks ok)

    All adjustments are additive/multiplicative on the raw feature values.
    Safe fallbacks ensure old predictions without DEG/DET fields behave identically.

    Returns a new MatchRequest with adjusted values.
    """
    cfg = _get_league_config(db, req.league_code)
    home_cfg, away_cfg = _get_team_configs(
        db, req.league_code, req.home_team, req.away_team
    )

    # ── League sensitivity multipliers ───────────────────────────────
    deg_sens = float(cfg.deg_sensitivity or 1.0) if cfg else 1.0
    det_sens = float(cfg.det_sensitivity or 1.0) if cfg else 1.0
    eps_sens = float(cfg.eps_sensitivity or 1.0) if cfg else 1.0

    # ── Team module nudges ────────────────────────────────────────────
    home_det_n = float(home_cfg.det_nudge or 0.0) if home_cfg else 0.0
    away_det_n = float(away_cfg.det_nudge or 0.0) if away_cfg else 0.0
    home_deg_n = float(home_cfg.deg_nudge or 0.0) if home_cfg else 0.0
    away_deg_n = float(away_cfg.deg_nudge or 0.0) if away_cfg else 0.0

    avg_det_nudge = (home_det_n + away_det_n) / 2.0
    avg_deg_nudge = (home_deg_n + away_deg_n) / 2.0

    # ── Raw feature values with safe defaults ─────────────────────────
    raw_deg      = req.deg_pressure  if req.deg_pressure  is not None else 0.0
    raw_det      = req.det_boost     if req.det_boost      is not None else 0.30
    raw_home_det = req.home_det      if req.home_det       is not None else 0.30
    raw_away_det = req.away_det      if req.away_det       is not None else 0.30
    raw_eps      = req.eps_stability if req.eps_stability  is not None else 0.65

    # ── Apply DEG adjustments ─────────────────────────────────────────
    adj_deg = _clip(raw_deg * deg_sens + avg_deg_nudge, 0.0, 1.0)

    # ── Apply DET adjustments ─────────────────────────────────────────
    adj_home_det = _clip(raw_home_det * det_sens + home_det_n, 0.0, 1.0)
    adj_away_det = _clip(raw_away_det * det_sens + away_det_n, 0.0, 1.0)
    adj_det      = _clip(raw_det * det_sens + avg_det_nudge,   0.0, 1.0)

    # ── Apply EPS adjustments ─────────────────────────────────────────
    raw_instability = 1.0 - raw_eps
    adj_instability = _clip(raw_instability * eps_sens, 0.0, 0.90)
    adj_eps = 1.0 - adj_instability

    # Log only when adjustments are non-trivial
    if abs(adj_deg - raw_deg) > 0.01 or abs(adj_det - raw_det) > 0.01:
        print(
            f"[predict] Module adjustments for {req.home_team} vs {req.away_team}: "
            f"deg {raw_deg:.3f}→{adj_deg:.3f} (×{deg_sens} +{avg_deg_nudge:.3f}) | "
            f"det {raw_det:.3f}→{adj_det:.3f} (×{det_sens} +{avg_det_nudge:.3f}) | "
            f"home_det {raw_home_det:.3f}→{adj_home_det:.3f} | "
            f"away_det {raw_away_det:.3f}→{adj_away_det:.3f} | "
            f"eps {raw_eps:.3f}→{adj_eps:.3f} (×{eps_sens})"
        )

    return req.model_copy(update={
        "deg_pressure":  round(adj_deg,      3),
        "det_boost":     round(adj_det,      3),
        "home_det":      round(adj_home_det, 3),
        "away_det":      round(adj_away_det, 3),
        "eps_stability": round(adj_eps,      3),
    })


def predict_match(db: Session, req: MatchRequest) -> Prediction:
    """
    Main entry point for generating ATHENA predictions.

    Pipeline:
      0. v2.0: Resolve team names through Team/Alias tables
      1. Load league calibration biases + sensitivities
      2. Load team over/under nudges (support_delta shift)
      3. v2.0: Compute player power nudge (squad strength delta)
      4. v2.0: Compute squad depth vulnerability → auto DEG adjustment
      5. Apply DEG/DET/EPS sensitivity and team module nudges to MatchRequest
      6. Run evaluate_athena with combined nudges
    """
    # v2.0: Resolve team names so all downstream lookups (TeamConfig, squad power,
    # performance tags) use the canonical name from the Team table.
    # This ensures "FC Fredericia" finds the same TeamConfig as "Fredericia".
    try:
        from app.services.resolve_team import resolve_team_name
        home_resolved = resolve_team_name(db, req.home_team, req.league_code)
        away_resolved = resolve_team_name(db, req.away_team, req.league_code)
        if home_resolved != req.home_team or away_resolved != req.away_team:
            req = req.model_copy(update={
                "home_team": home_resolved,
                "away_team": away_resolved,
            })
    except Exception:
        pass  # resolver not available — proceed with original names

    over_bias, under_bias, tempo_factor = _get_league_bias(db, req.league_code)
    team_nudge = _get_team_nudge(
        db, req.league_code, req.home_team, req.away_team
    )

    # v2.0: Player power contribution to support_delta.
    # This is ADDITIVE on top of the calibration-derived team_nudge.
    # If player data doesn't exist for either team, returns 0.0 (no effect).
    player_nudge = _get_player_power_nudge(
        db, req.league_code, req.home_team, req.away_team
    )
    combined_nudge = team_nudge + player_nudge

    # v2.0: Squad depth vulnerability → auto DEG boost.
    # If either team has large XI-to-bench power gaps in any zone,
    # this adds a small positive nudge to deg_pressure (max +0.06).
    # Returns 0.0 if player data is unavailable or no vulnerability.
    season = _current_season(req.league_code)
    depth_deg = auto_deg_from_depth(
        db, req.league_code, req.home_team, req.away_team, season
    )
    if depth_deg > 0:
        current_deg = req.deg_pressure if req.deg_pressure is not None else 0.0
        req = req.model_copy(update={
            "deg_pressure": round(current_deg + depth_deg, 3),
        })

    # Apply league sensitivities + team module nudges to DEG/DET/EPS features.
    adjusted_req = _apply_module_adjustments(req, db)

    return evaluate_athena(adjusted_req, over_bias, under_bias, tempo_factor, combined_nudge)
