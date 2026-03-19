# backend/app/services/predict.py
from __future__ import annotations

from datetime import date
from sqlalchemy.orm import Session

from app.engine.types import MatchRequest, Prediction
from app.engine.pipeline import evaluate_athena
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig
from app.services.squad_availability import auto_deg_from_depth
from app.services.form_delta_history import get_historical_form_delta


# ── v2.0: Player power blend weight ─────────────────────────────────────────
PLAYER_POWER_BLEND = 0.30
PLAYER_POWER_MAX_EFFECT = 0.08

# International league codes — used for cross-league normalisation
INTL_LEAGUE_CODES = {"UCL", "UEL", "UECL", "EC", "WC"}

# Calendar-year leagues use "2026", Aug–May leagues use "2025-2026"
_CALENDAR_YEAR_PREFIXES = {"BRA", "MLS", "NOR", "SWE", "CHN", "JPN", "COL"}


def _current_season(league_code: str) -> str:
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
    Look up per-team calibration nudges from TeamConfig.
    Returns the average of home_nudge and away_nudge.
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
    if PLAYER_POWER_BLEND <= 0.0:
        return 0.0

    home_cfg, away_cfg = _get_team_configs(db, league_code, home_team, away_team)
    home_power = float(home_cfg.squad_power) if home_cfg and home_cfg.squad_power is not None else None
    away_power = float(away_cfg.squad_power) if away_cfg and away_cfg.squad_power is not None else None

    if home_power is None or away_power is None:
        return 0.0

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

    power_delta = (home_power - away_power) / 100.0
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
    cfg = _get_league_config(db, req.league_code)
    home_cfg, away_cfg = _get_team_configs(
        db, req.league_code, req.home_team, req.away_team
    )

    deg_sens = float(cfg.deg_sensitivity or 1.0) if cfg else 1.0
    det_sens = float(cfg.det_sensitivity or 1.0) if cfg else 1.0
    eps_sens = float(cfg.eps_sensitivity or 1.0) if cfg else 1.0

    home_det_n = float(home_cfg.det_nudge or 0.0) if home_cfg else 0.0
    away_det_n = float(away_cfg.det_nudge or 0.0) if away_cfg else 0.0
    home_deg_n = float(home_cfg.deg_nudge or 0.0) if home_cfg else 0.0
    away_deg_n = float(away_cfg.deg_nudge or 0.0) if away_cfg else 0.0

    avg_det_nudge = (home_det_n + away_det_n) / 2.0
    avg_deg_nudge = (home_deg_n + away_deg_n) / 2.0

    raw_deg      = req.deg_pressure  if req.deg_pressure  is not None else 0.0
    raw_det      = req.det_boost     if req.det_boost      is not None else 0.30
    raw_home_det = req.home_det      if req.home_det       is not None else 0.30
    raw_away_det = req.away_det      if req.away_det       is not None else 0.30
    raw_eps      = req.eps_stability if req.eps_stability  is not None else 0.65

    adj_deg = _clip(raw_deg * deg_sens + avg_deg_nudge, 0.0, 1.0)

    adj_home_det = _clip(raw_home_det * det_sens + home_det_n, 0.0, 1.0)
    adj_away_det = _clip(raw_away_det * det_sens + away_det_n, 0.0, 1.0)
    adj_det      = _clip(raw_det * det_sens + avg_det_nudge,   0.0, 1.0)

    raw_instability = 1.0 - raw_eps
    adj_instability = _clip(raw_instability * eps_sens, 0.0, 0.90)
    adj_eps = 1.0 - adj_instability

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


# ── Form delta nudge helper (league‑level sensitivity) ─────────────────────
def _get_form_delta_nudge(
    db: Session,
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date | None,
) -> float:
    """
    Compute a support_delta adjustment based on the difference in form delta
    between the two teams, scaled by the league's form_delta_sensitivity.
    """
    cfg = _get_league_config(db, league_code)
    if not cfg or not cfg.form_delta_sensitivity:
        return 0.0

    target_date = match_date if match_date is not None else date.today()
    home_delta = get_historical_form_delta(db, home_team, league_code, target_date)
    away_delta = get_historical_form_delta(db, away_team, league_code, target_date)

    if home_delta is None or away_delta is None:
        return 0.0

    delta_diff = home_delta - away_delta
    nudge = delta_diff * cfg.form_delta_sensitivity
    nudge = _clip(nudge, -0.05, 0.05)
    return round(nudge, 4)


def predict_match(db: Session, req: MatchRequest) -> Prediction:
    """
    Main entry point for generating ATHENA predictions.

    Pipeline:
      0. Resolve team names to canonical keys.
      1. Load league biases and sensitivities.
      2. Compute team nudge (base over_nudge from TeamConfig).
      3. Compute player power nudge.
      4. Compute form delta nudge (league‑level).
      5. Apply squad depth adjustment (auto DEG).
      6. Apply DEG/DET/EPS module adjustments.
      7. Run evaluate_athena with combined nudges.
    """
    # Resolve team names
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

    # Team nudge (base over_nudge only)
    team_nudge = _get_team_nudge(
        db, req.league_code, req.home_team, req.away_team
    )

    # Player power nudge
    player_nudge = _get_player_power_nudge(
        db, req.league_code, req.home_team, req.away_team
    )

    # Form delta nudge (league‑level)
    form_nudge = _get_form_delta_nudge(
        db, req.league_code, req.home_team, req.away_team, req.match_date
    )

    combined_nudge = team_nudge + player_nudge + form_nudge

    # Squad depth vulnerability → auto DEG boost
    season = _current_season(req.league_code)
    depth_deg = auto_deg_from_depth(
        db, req.league_code, req.home_team, req.away_team, season
    )
    if depth_deg > 0:
        current_deg = req.deg_pressure if req.deg_pressure is not None else 0.0
        req = req.model_copy(update={
            "deg_pressure": round(current_deg + depth_deg, 3),
        })

    # Apply league sensitivities and team module nudges
    adjusted_req = _apply_module_adjustments(req, db)

    return evaluate_athena(adjusted_req, over_bias, under_bias, tempo_factor, combined_nudge)
