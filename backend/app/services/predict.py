# backend/app/services/predict.py
from __future__ import annotations

from sqlalchemy.orm import Session

from app.engine.types import MatchRequest, Prediction
from app.engine.pipeline import evaluate_athena
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig


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
    # deg_sensitivity amplifies the structural decline signal league-wide.
    # avg_deg_nudge adds team-specific decline correction.
    # Result is clipped to [0, 1].
    adj_deg = _clip(raw_deg * deg_sens + avg_deg_nudge, 0.0, 1.0)

    # ── Apply DET adjustments ─────────────────────────────────────────
    # det_sensitivity amplifies league-wide volatility.
    # Team nudges applied individually to home_det/away_det for bilateral check,
    # and averaged into det_boost for the overall DET module signal.
    adj_home_det = _clip(raw_home_det * det_sens + home_det_n, 0.0, 1.0)
    adj_away_det = _clip(raw_away_det * det_sens + away_det_n, 0.0, 1.0)
    adj_det      = _clip(raw_det * det_sens + avg_det_nudge,   0.0, 1.0)

    # ── Apply EPS adjustments ─────────────────────────────────────────
    # EPS stability: 1.0 = perfectly stable, 0.0 = chaotic.
    # To amplify instability: increase the deviation from stability.
    # eps_sensitivity > 1.0 makes the ceiling taper more aggressive.
    # eps_sensitivity < 1.0 softens the taper.
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

    # Return a new MatchRequest with adjusted feature values.
    # All other fields (core features, identity) are unchanged.
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
      1. Load league calibration biases + sensitivities
      2. Load team over/under nudges (support_delta shift)
      3. Apply DEG/DET/EPS sensitivity and team module nudges to MatchRequest
      4. Run evaluate_athena with adjusted features
    """
    over_bias, under_bias, tempo_factor = _get_league_bias(db, req.league_code)
    team_nudge = _get_team_nudge(
        db, req.league_code, req.home_team, req.away_team
    )

    # Apply league sensitivities + team module nudges to DEG/DET/EPS features.
    # This is where Man City + Liverpool can override the league's Under guard —
    # their det_nudges push home_det and away_det above BILATERAL_MIN_DET,
    # the escalator fires, and the corridor ceiling expands into Over territory.
    adjusted_req = _apply_module_adjustments(req, db)

    return evaluate_athena(adjusted_req, over_bias, under_bias, tempo_factor, team_nudge)
