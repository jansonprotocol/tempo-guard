# backend/app/services/predict.py
"""
ATHENA prediction pipeline.

v2.2 performance fix: LeagueConfig and both TeamConfigs are now loaded
ONCE at the top of predict_match() and passed through to every helper.
Previously each helper called _get_team_configs() / _get_league_config()
independently, resulting in 11 DB queries per prediction (3 unique).
Now it costs exactly 3 queries regardless of how many nudge layers fire.
"""
from __future__ import annotations

from datetime import date
from typing import Optional
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


# ── Config loaders — called ONCE per prediction ───────────────────────────────

def _load_league_config(db: Session, league_code: str) -> LeagueConfig | None:
    return (
        db.query(LeagueConfig)
        .filter(LeagueConfig.league_code == league_code)
        .first()
    )


def _load_team_configs(
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


# ── Nudge helpers — all accept pre-loaded configs, zero additional DB queries ─

def _league_bias(cfg: LeagueConfig | None) -> tuple[float, float, float]:
    if not cfg:
        return 0.5, 0.5, 0.50
    return (
        float(cfg.base_over_bias  or 0.5),
        float(cfg.base_under_bias or 0.5),
        float(cfg.tempo_factor    or 0.50),
    )


def _team_base_nudge(
    home_cfg: TeamConfig | None,
    away_cfg: TeamConfig | None,
) -> float:
    """Average of home/away over_nudge from TeamConfig."""
    home_nudge = float(home_cfg.over_nudge or 0.0) if home_cfg else 0.0
    away_nudge = float(away_cfg.over_nudge or 0.0) if away_cfg else 0.0
    return (home_nudge + away_nudge) / 2.0


def _team_form_nudge(
    db: Session,
    home_cfg: TeamConfig | None,
    away_cfg: TeamConfig | None,
    home_team: str,
    away_team: str,
    league_code: str,
    match_date: date | None,
) -> float:
    """
    v2.1: per-team form-based nudges (good/neutral/poor buckets).
    Short-circuits to 0.0 if neither team has form nudges configured.
    """
    target_date = match_date if match_date is not None else date.today()

    def _nudge_for(cfg: TeamConfig | None, team_name: str) -> float:
        if cfg is None:
            return 0.0
        good    = float(cfg.good_form_nudge    or 0.0)
        neutral = float(cfg.neutral_form_nudge or 0.0)
        poor    = float(cfg.poor_form_nudge    or 0.0)
        if good == 0.0 and neutral == 0.0 and poor == 0.0:
            return 0.0

        form_delta = get_historical_form_delta(db, team_name, league_code, target_date)
        if form_delta is None:
            return neutral

        good_thr = int(cfg.form_good_threshold if cfg.form_good_threshold is not None else 3)
        poor_thr = int(cfg.form_poor_threshold if cfg.form_poor_threshold is not None else -3)

        if form_delta >= good_thr:
            selected, bucket = good, "good"
        elif form_delta <= poor_thr:
            selected, bucket = poor, "poor"
        else:
            selected, bucket = neutral, "neutral"

        if abs(selected) > 0.001:
            print(
                f"[predict] Form nudge ({bucket}): {team_name} "
                f"form_delta={form_delta} → nudge={selected:+.4f}"
            )
        return selected

    home_nudge = _nudge_for(home_cfg, home_team)
    away_nudge = _nudge_for(away_cfg, away_team)
    return round((home_nudge + away_nudge) / 2.0, 4)


def _player_power_nudge(
    db: Session,
    home_cfg: TeamConfig | None,
    away_cfg: TeamConfig | None,
    league_code: str,
    home_team: str = "",
    away_team: str = "",
) -> float:
    """Squad power delta nudge. May query per-league configs for intl matches."""
    if PLAYER_POWER_BLEND <= 0.0:
        return 0.0

    # Use session-attached squad power cache if available (populated by
    # batch-predict before the fixture loop to avoid per-fixture DB queries).
    _cache = getattr(db, "_squad_power_cache", {}).get(league_code, {})
    if _cache and home_team and away_team:
        home_power = _cache.get(home_team)
        away_power = _cache.get(away_team)
    else:
        home_power = float(home_cfg.squad_power) if home_cfg and home_cfg.squad_power is not None else None
        away_power = float(away_cfg.squad_power) if away_cfg and away_cfg.squad_power is not None else None

    if home_power is None or away_power is None:
        return 0.0

    if league_code in INTL_LEAGUE_CODES:
        home_league = home_cfg.league_code if home_cfg else None
        away_league = away_cfg.league_code if away_cfg else None

        if home_league:
            hl_cfg = _load_league_config(db, home_league)
            if hl_cfg and hl_cfg.strength_coefficient:
                home_power *= float(hl_cfg.strength_coefficient)

        if away_league:
            al_cfg = _load_league_config(db, away_league)
            if al_cfg and al_cfg.strength_coefficient:
                away_power *= float(al_cfg.strength_coefficient)

    power_delta = (home_power - away_power) / 100.0
    nudge = _clip(power_delta * PLAYER_POWER_BLEND, -PLAYER_POWER_MAX_EFFECT, PLAYER_POWER_MAX_EFFECT)

    if abs(nudge) > 0.005:
        print(
            f"[predict] Player power: home={home_power:.1f} vs "
            f"away={away_power:.1f} → delta={power_delta:.3f} "
            f"→ nudge={nudge:+.4f} (blend={PLAYER_POWER_BLEND})"
        )

    return round(nudge, 4)


def _league_form_delta_nudge(
    db: Session,
    cfg: LeagueConfig | None,
    home_team: str,
    away_team: str,
    league_code: str,
    match_date: date | None,
) -> float:
    """League-level form delta sensitivity nudge."""
    if not cfg or not cfg.form_delta_sensitivity:
        return 0.0

    target_date = match_date if match_date is not None else date.today()
    home_delta = get_historical_form_delta(db, home_team, league_code, target_date)
    away_delta = get_historical_form_delta(db, away_team, league_code, target_date)

    if home_delta is None or away_delta is None:
        return 0.0

    nudge = (home_delta - away_delta) * cfg.form_delta_sensitivity
    return round(_clip(nudge, -0.05, 0.05), 4)


def _apply_module_adjustments(
    req: MatchRequest,
    cfg: LeagueConfig | None,
    home_cfg: TeamConfig | None,
    away_cfg: TeamConfig | None,
) -> MatchRequest:
    """Apply DEG/DET/EPS league sensitivities and per-team module nudges."""
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

    adj_deg      = _clip(raw_deg * deg_sens + avg_deg_nudge, 0.0, 1.0)
    adj_home_det = _clip(raw_home_det * det_sens + home_det_n, 0.0, 1.0)
    adj_away_det = _clip(raw_away_det * det_sens + away_det_n, 0.0, 1.0)
    adj_det      = _clip(raw_det * det_sens + avg_det_nudge,   0.0, 1.0)
    adj_eps      = 1.0 - _clip(1.0 - raw_eps, 0.0, 0.90) if eps_sens == 1.0 else \
                   1.0 - _clip((1.0 - raw_eps) * eps_sens, 0.0, 0.90)

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


# ── Main entry point ──────────────────────────────────────────────────────────

def predict_match(db: Session, req: MatchRequest) -> Prediction:
    """
    Main entry point for generating ATHENA predictions.

    Pipeline:
      0. Resolve team names to canonical keys.
      1. Load LeagueConfig + both TeamConfigs (3 DB queries, ONCE).
      2. Extract league biases.
      3. Compute team base nudge (over_nudge).
      4. Compute player power nudge.
      5. Compute league-level form delta nudge.
      6. Compute per-team form nudges (v2.1).
      7. Apply squad depth adjustment (auto DEG).
      8. Apply DEG/DET/EPS module adjustments.
      9. Run evaluate_athena with combined nudges.
    """
    # ── 0. Resolve team names ─────────────────────────────────────────
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
        pass

    # ── 1. Load configs ONCE ──────────────────────────────────────────
    cfg = _load_league_config(db, req.league_code)
    home_cfg, away_cfg = _load_team_configs(
        db, req.league_code, req.home_team, req.away_team
    )

    # ── 2. League biases ──────────────────────────────────────────────
    over_bias, under_bias, tempo_factor = _league_bias(cfg)

    # ── 3–6. Nudge stack ──────────────────────────────────────────────
    base_nudge  = _team_base_nudge(home_cfg, away_cfg)
    power_nudge = _player_power_nudge(db, home_cfg, away_cfg, req.league_code, req.home_team, req.away_team)
    form_nudge  = _league_form_delta_nudge(
        db, cfg, req.home_team, req.away_team, req.league_code, req.match_date
    )
    team_form_nudge = _team_form_nudge(
        db, home_cfg, away_cfg,
        req.home_team, req.away_team, req.league_code, req.match_date
    )

    combined_nudge = base_nudge + power_nudge + form_nudge + team_form_nudge

    # ── 7. Squad depth → auto DEG boost ──────────────────────────────
    season = _current_season(req.league_code)
    depth_deg = auto_deg_from_depth(
        db, req.league_code, req.home_team, req.away_team, season
    )
    if depth_deg > 0:
        current_deg = req.deg_pressure if req.deg_pressure is not None else 0.0
        req = req.model_copy(update={
            "deg_pressure": round(current_deg + depth_deg, 3),
        })

    # ── 8. Module adjustments ─────────────────────────────────────────
    adjusted_req = _apply_module_adjustments(req, cfg, home_cfg, away_cfg)

    # ── 9. Evaluate ───────────────────────────────────────────────────
    return evaluate_athena(adjusted_req, over_bias, under_bias, tempo_factor, combined_nudge)
