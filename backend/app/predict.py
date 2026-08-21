"""
Prediction orchestration — features in, Prediction out.

This is the local, file-backed replacement for the old database-driven
`services/predict.py`. The engine itself (`evaluate_athena`) is untouched; what
changed is where its tuning inputs come from:

    league biases / sensitivities   config/leagues.json   (was league_configs)
    per-team nudges                 config/leagues.json   (was team_configs)
    match features                  data/*.parquet        (was fbref_snapshots)

Two entry points mirror the two ways a match is predicted:

    predict_fixture()  a match whose features you already computed
    predict_match()    resolve features from the store, then predict
"""
from __future__ import annotations

from datetime import date
from typing import Optional

from app.data import config, features
from app.data.config import LeagueConfig
from app.engine.pipeline import evaluate_athena
from app.engine.rationale import humanize
from app.engine.types import MatchRequest, ModuleFlags, Prediction


def _clip(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _team_nudge(cfg: LeagueConfig, home_team: str, away_team: str) -> float:
    """Average of the configured per-team over-nudges (0.0 when unset)."""
    nudges = cfg.team_nudges or {}
    return (float(nudges.get(home_team, 0.0)) + float(nudges.get(away_team, 0.0))) / 2.0


def _apply_sensitivities(req: MatchRequest, cfg: LeagueConfig) -> MatchRequest:
    """Scale the DEG/DET/EPS module inputs by their per-league sensitivities."""
    deg = req.deg_pressure if req.deg_pressure is not None else 0.0
    det = req.det_boost if req.det_boost is not None else 0.30
    h_det = req.home_det if req.home_det is not None else 0.30
    a_det = req.away_det if req.away_det is not None else 0.30
    eps = req.eps_stability if req.eps_stability is not None else 0.65

    eps_sens = float(cfg.eps_sensitivity or 1.0)
    adj_eps = 1.0 - _clip((1.0 - eps) * eps_sens, 0.0, 0.90)

    return req.model_copy(update={
        "deg_pressure":  round(_clip(deg * float(cfg.deg_sensitivity or 1.0), 0.0, 1.0), 3),
        "det_boost":     round(_clip(det * float(cfg.det_sensitivity or 1.0), 0.0, 1.0), 3),
        "home_det":      round(_clip(h_det * float(cfg.det_sensitivity or 1.0), 0.0, 1.0), 3),
        "away_det":      round(_clip(a_det * float(cfg.det_sensitivity or 1.0), 0.0, 1.0), 3),
        "eps_stability": round(adj_eps, 3),
    })


def predict_fixture(
    req: MatchRequest,
    cfg: Optional[LeagueConfig] = None,
    module_flags: Optional[ModuleFlags] = None,
) -> Prediction:
    """
    Run the engine for a request whose features are already populated.

    `module_flags` takes precedence when given (ablation and dial search pass it
    explicitly); otherwise the league's `module_overrides` are layered over the
    measured defaults.
    """
    cfg = cfg or config.get(req.league_code)
    adjusted = _apply_sensitivities(req, cfg)

    if module_flags is None and cfg.module_overrides:
        module_flags = ModuleFlags(**cfg.module_overrides)

    prediction = evaluate_athena(
        adjusted,
        league_bias_over=float(cfg.base_over_bias),
        league_bias_under=float(cfg.base_under_bias),
        tempo_factor=float(cfg.tempo_factor),
        team_nudge=_team_nudge(cfg, req.home_team, req.away_team),
        confidence_scale=float(cfg.confidence_scale),
        confidence_floor=float(cfg.confidence_floor),
        module_flags=module_flags,
        norm_mean=float(cfg.mu_mean),
        norm_std=float(cfg.mu_std),
        max_under_line=cfg.max_under_line,
        min_over_line=cfg.min_over_line,
        min_win_prob=cfg.min_win_prob,
        use_possession=cfg.use_possession,
    )
    prediction.rationale = humanize(prediction)
    return prediction


def build_request(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    min_matches: int = features.MIN_MATCHES,
) -> Optional[MatchRequest]:
    """
    Resolve as-of features for a fixture and package them into a MatchRequest.
    Returns None when there is too little history to predict from.
    """
    m = features.asof_features(league_code, home_team, away_team,
                               match_date, min_matches=min_matches)
    if not m:
        return None

    return MatchRequest(
        league_code=league_code,
        home_team=home_team,
        away_team=away_team,
        match_date=match_date,
        sot_proj_total=m.get("sot_proj_total"),
        support_idx_over_delta=m.get("support_idx_over_delta"),
        p_two_plus=m.get("p_two_plus"),
        p_home_tt05=m.get("p_home_tt05"),
        p_away_tt05=m.get("p_away_tt05"),
        tempo_index=m.get("tempo_index"),
        mu_total=m.get("mu_total"),
        league_mu=m.get("league_mu"),
        deg_pressure=m.get("deg_pressure"),
        det_boost=m.get("det_boost"),
        home_det=m.get("home_det"),
        away_det=m.get("away_det"),
        eps_stability=m.get("eps_stability"),
    )


def predict_match(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: date,
    cfg: Optional[LeagueConfig] = None,
    min_matches: int = features.MIN_MATCHES,
) -> Optional[Prediction]:
    """
    Full path: resolve features from the store, then predict.
    Returns None when the fixture has too little history.
    """
    req = build_request(league_code, home_team, away_team, match_date, min_matches)
    if req is None:
        return None
    pred = predict_fixture(req, cfg)

    # Descriptive only, and deliberately attached here rather than inside
    # predict_fixture: the replay path calls that thousands of times and must
    # not pay for tags nothing downstream reads.
    try:
        from app.data import tags as _tags
        h, a = _tags.for_fixture(league_code, req.home_team, req.away_team, match_date)
        pred.home_tags = h.labels()
        pred.away_tags = a.labels()
    except Exception:
        pass          # a tag failure must never cost a tip

    return pred
