# backend/app/api/routes_player_power.py
"""
ATHENA v2.0 — Player Power Calibration Tuning Endpoints.
...
"""
from __future__ import annotations
import sys
import os
from pathlib import Path

# Compute the absolute path to the scripts folder
# Current file: /app/app/api/routes_player_power.py
# Go up three levels to /app, then into backend/scripts
scripts_path = Path(__file__).resolve().parent.parent.parent / "backend" / "scripts"
if not scripts_path.exists():
    raise RuntimeError(
        f"Scripts folder not found at {scripts_path}. "
        "Make sure the 'scripts' directory exists and is deployed."
    )
sys.path.insert(0, str(scripts_path))

# Now import scrape_players and get its constants
import scrape_players
SEASON_MAP = scrape_players.SEASON_MAP
SCHEDULE_URLS = scrape_players.SCHEDULE_URLS  # Not used in this file but kept for completeness

import io
from typing import Optional

import pandas as pd
from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot
from app.engine.pipeline import evaluate_athena
from app.engine.types import MatchRequest
from app.models.league_config import LeagueConfig
from app.models.team_config import TeamConfig
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot
from app.services.data_providers.fbref_base import asof_features, _parse_score_column
from app.services.player_power_backtest import (
    get_historical_player_nudge,
    has_any_snapshots,
)
from app.util.asian_lines import evaluate_market, hit_weight

router = APIRouter()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/player-power/status")
def player_power_status(
    league_code: Optional[str] = Query(None, description="Filter to a single league"),
    db: Session = Depends(get_db),
):
    """
    Report player data coverage.
    Shows how many players, teams with squad_power, and snapshots exist.
    """
    if league_code:
        leagues = [league_code]
    else:
        # All leagues that have snapshots
        snaps = db.query(FBrefSnapshot.league_code).distinct().all()
        leagues = sorted([s[0] for s in snaps])

    results = []
    for lc in leagues:
        # Player count
        player_count = (
            db.query(Player)
            .filter_by(league_code=lc)
            .count()
        )

        # Stats count
        stats_count = (
            db.query(PlayerSeasonStats)
            .filter_by(league_code=lc)
            .count()
        )

        # Stats with power index computed
        indexed_count = (
            db.query(PlayerSeasonStats)
            .filter(
                PlayerSeasonStats.league_code == lc,
                PlayerSeasonStats.power_index.isnot(None),
            )
            .count()
        )

        # Teams with squad_power
        teams_with_power = (
            db.query(TeamConfig)
            .filter(
                TeamConfig.league_code == lc,
                TeamConfig.squad_power.isnot(None),
            )
            .count()
        )

        # Total teams
        total_teams = (
            db.query(TeamConfig)
            .filter_by(league_code=lc)
            .count()
        )

        # Snapshot count
        snapshot_count = (
            db.query(SquadSnapshot)
            .filter_by(league_code=lc)
            .count()
        )

        results.append({
            "league_code": lc,
            "players": player_count,
            "season_stats": stats_count,
            "indexed": indexed_count,
            "teams_with_power": f"{teams_with_power}/{total_teams}",
            "snapshots": snapshot_count,
            "ready": teams_with_power > 0 and snapshot_count > 0,
        })

    total_ready = sum(1 for r in results if r["ready"])
    return {
        "leagues": results,
        "total_leagues": len(results),
        "leagues_ready": total_ready,
    }


@router.get("/player-power/evaluate")
def evaluate_player_power(
    league_code: str = Query(..., description="League to evaluate"),
    limit: int = Query(80, ge=10, le=300, description="Max matches to test"),
    blend_weight: float = Query(0.30, ge=0.0, le=1.0, description="Player power blend weight"),
    min_matches_before: int = Query(3, ge=2, le=20),
    db: Session = Depends(get_db),
):
    """
    A/B comparison: run the calibration sim loop TWICE for a league.

    Pass A: team_nudge = 0.0 (v1 macro-only)
    Pass B: team_nudge = player_power_nudge (v2 with squad power)

    Reports:
      - Hit rate for each pass
      - Which matches flipped (miss→hit or hit→miss)
      - Net improvement
      - Suggested blend weight
    """
    # ── Load and prepare snapshot ────────────────────────────────────
    row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not row:
        return {"error": f"No snapshot for {league_code}"}

    df = pd.read_parquet(io.BytesIO(row.data))

    score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
    if score_col and "hg" not in df.columns:
        df = _parse_score_column(df, score_col)
    if "hg" not in df.columns or "ag" not in df.columns:
        return {"error": "No parseable score column"}

    if isinstance(df.columns[0], tuple):
        df.columns = [
            " ".join(str(p) for p in col if not str(p).startswith("Unnamed")).strip() or str(col[-1])
            for col in df.columns
        ]

    col_map  = {str(c).lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")
    if not all([date_col, home_col, away_col]):
        return {"error": "Missing Date/Home/Away columns"}

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, "hg", "ag"])
    df = df.drop_duplicates(subset=[date_col, home_col, away_col])
    df = df.sort_values(date_col, ascending=False).head(limit)

    # ── League config ────────────────────────────────────────────────
    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    over_bias  = float(cfg.base_over_bias  or 0.5) if cfg else 0.5
    under_bias = float(cfg.base_under_bias or 0.5) if cfg else 0.5
    tempo_fac  = float(cfg.tempo_factor    or 0.50) if cfg else 0.50

    # ── Check if player power data exists ────────────────────────────
    has_snapshots = has_any_snapshots(db, league_code)
    if not has_snapshots:
        return {
            "error": f"No squad snapshots with power scores for {league_code}. "
                     f"Run scrape_players + player_index first.",
            "league_code": league_code,
        }

    # ── Run A/B sim ──────────────────────────────────────────────────
    results_a = []  # without player power
    results_b = []  # with player power
    skipped = 0
    match_details = []

    for _, match_row in df.iterrows():
        match_date = match_row[date_col].date()
        home_team  = str(match_row[home_col])
        away_team  = str(match_row[away_col])
        hg = int(match_row["hg"])
        ag = int(match_row["ag"])

        try:
            metrics = asof_features(
                league_code, home_team, away_team, match_date,
                min_matches=min_matches_before,
            )
        except Exception:
            skipped += 1
            continue

        if not metrics:
            skipped += 1
            continue

        try:
            req = MatchRequest(
                league_code=league_code,
                home_team=home_team,
                away_team=away_team,
                match_date=match_date,
                sot_proj_total=metrics.get("sot_proj_total"),
                support_idx_over_delta=metrics.get("support_idx_over_delta"),
                p_two_plus=metrics.get("p_two_plus"),
                p_home_tt05=metrics.get("p_home_tt05"),
                p_away_tt05=metrics.get("p_away_tt05"),
                tempo_index=metrics.get("tempo_index"),
                deg_pressure=metrics.get("deg_pressure"),
                det_boost=metrics.get("det_boost"),
                home_det=metrics.get("home_det"),
                away_det=metrics.get("away_det"),
                eps_stability=metrics.get("eps_stability"),
            )

            # Pass A: no player power
            pred_a = evaluate_athena(req, over_bias, under_bias, tempo_fac, team_nudge=0.0)
            market_a = pred_a.translated_play.market
            result_a = evaluate_market(market_a, hg, ag)
            hw_a = hit_weight(result_a)

            # Pass B: with player power
            player_nudge = get_historical_player_nudge(
                db, league_code, home_team, away_team, match_date,
                blend_weight=blend_weight,
            )
            pred_b = evaluate_athena(req, over_bias, under_bias, tempo_fac, team_nudge=player_nudge)
            market_b = pred_b.translated_play.market
            result_b = evaluate_market(market_b, hg, ag)
            hw_b = hit_weight(result_b)

        except Exception:
            skipped += 1
            continue

        if hw_a < 0 or hw_b < 0:
            skipped += 1
            continue

        results_a.append(hw_a)
        results_b.append(hw_b)

        # Track flips
        a_hit = hw_a >= 0.5
        b_hit = hw_b >= 0.5
        if a_hit != b_hit or market_a != market_b:
            match_details.append({
                "date": match_date.strftime("%d/%m/%Y"),
                "match": f"{home_team} vs {away_team}",
                "actual": f"{hg}-{ag}",
                "v1_market": market_a,
                "v2_market": market_b,
                "v1_hit": a_hit,
                "v2_hit": b_hit,
                "player_nudge": player_nudge,
                "flip": "miss→hit" if not a_hit and b_hit else
                        "hit→miss" if a_hit and not b_hit else
                        "market_changed",
            })

    # ── Compute results ──────────────────────────────────────────────
    n = len(results_a)
    if n == 0:
        return {"error": "No matches could be evaluated", "skipped": skipped}

    hits_a = sum(1 for h in results_a if h >= 0.5)
    hits_b = sum(1 for h in results_b if h >= 0.5)
    rate_a = round(hits_a / n * 100, 1)
    rate_b = round(hits_b / n * 100, 1)
    delta  = round(rate_b - rate_a, 1)

    flips_positive = sum(1 for d in match_details if d["flip"] == "miss→hit")
    flips_negative = sum(1 for d in match_details if d["flip"] == "hit→miss")

    # ── Suggest optimal blend weight ─────────────────────────────────
    suggestion = "keep_current"
    if delta > 1.0 and flips_positive > flips_negative:
        suggestion = f"increase_blend (current {blend_weight}, try {min(1.0, blend_weight + 0.10):.2f})"
    elif delta < -1.0 and flips_negative > flips_positive:
        suggestion = f"decrease_blend (current {blend_weight}, try {max(0.0, blend_weight - 0.10):.2f})"
    elif abs(delta) <= 1.0:
        suggestion = f"blend_weight={blend_weight} is near-optimal for {league_code}"

    return {
        "league_code": league_code,
        "matches_evaluated": n,
        "skipped": skipped,
        "blend_weight_tested": blend_weight,
        "v1_macro_only": {
            "hits": hits_a,
            "misses": n - hits_a,
            "hit_rate": rate_a,
        },
        "v2_with_player_power": {
            "hits": hits_b,
            "misses": n - hits_b,
            "hit_rate": rate_b,
        },
        "delta_pp": delta,
        "flips": {
            "miss_to_hit": flips_positive,
            "hit_to_miss": flips_negative,
            "net_improvement": flips_positive - flips_negative,
        },
        "suggestion": suggestion,
        "match_details": match_details[:30],  # cap to prevent huge responses
    }


@router.get("/player-power/sweep")
def sweep_blend_weights(
    league_code: str = Query(..., description="League to sweep"),
    limit: int = Query(80, ge=10, le=200),
    min_matches_before: int = Query(3, ge=2, le=20),
    db: Session = Depends(get_db),
):
    """
    Test multiple blend weights (0.0 to 0.50 in 0.05 steps) and find the
    weight that maximises hit rate for this league.

    Returns a ranked list of blend weights with their hit rates.
    Useful for finding the optimal PLAYER_POWER_BLEND per league.
    """
    # ── Load and prepare snapshot ────────────────────────────────────
    row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not row:
        return {"error": f"No snapshot for {league_code}"}

    if not has_any_snapshots(db, league_code):
        return {"error": f"No squad snapshots for {league_code}"}

    df = pd.read_parquet(io.BytesIO(row.data))
    score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
    if score_col and "hg" not in df.columns:
        df = _parse_score_column(df, score_col)
    if "hg" not in df.columns or "ag" not in df.columns:
        return {"error": "No parseable score column"}

    if isinstance(df.columns[0], tuple):
        df.columns = [
            " ".join(str(p) for p in col if not str(p).startswith("Unnamed")).strip() or str(col[-1])
            for col in df.columns
        ]

    col_map  = {str(c).lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")
    if not all([date_col, home_col, away_col]):
        return {"error": "Missing columns"}

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")
    df = df.dropna(subset=[date_col, "hg", "ag"])
    df = df.drop_duplicates(subset=[date_col, home_col, away_col])
    df = df.sort_values(date_col, ascending=False).head(limit)

    cfg = db.query(LeagueConfig).filter_by(league_code=league_code).first()
    over_bias  = float(cfg.base_over_bias  or 0.5) if cfg else 0.5
    under_bias = float(cfg.base_under_bias or 0.5) if cfg else 0.5
    tempo_fac  = float(cfg.tempo_factor    or 0.50) if cfg else 0.50

    # ── Pre-compute features for all matches ─────────────────────────
    match_data = []
    for _, match_row in df.iterrows():
        match_date = match_row[date_col].date()
        home_team  = str(match_row[home_col])
        away_team  = str(match_row[away_col])
        hg = int(match_row["hg"])
        ag = int(match_row["ag"])

        try:
            metrics = asof_features(
                league_code, home_team, away_team, match_date,
                min_matches=min_matches_before,
            )
        except Exception:
            continue
        if not metrics:
            continue

        try:
            req = MatchRequest(
                league_code=league_code,
                home_team=home_team,
                away_team=away_team,
                match_date=match_date,
                sot_proj_total=metrics.get("sot_proj_total"),
                support_idx_over_delta=metrics.get("support_idx_over_delta"),
                p_two_plus=metrics.get("p_two_plus"),
                p_home_tt05=metrics.get("p_home_tt05"),
                p_away_tt05=metrics.get("p_away_tt05"),
                tempo_index=metrics.get("tempo_index"),
                deg_pressure=metrics.get("deg_pressure"),
                det_boost=metrics.get("det_boost"),
                home_det=metrics.get("home_det"),
                away_det=metrics.get("away_det"),
                eps_stability=metrics.get("eps_stability"),
            )
            match_data.append((req, hg, ag, match_date, home_team, away_team))
        except Exception:
            continue

    if not match_data:
        return {"error": "No matches could be prepared"}

    # ── Sweep blend weights ──────────────────────────────────────────
    weights_to_test = [round(w * 0.05, 2) for w in range(0, 11)]  # 0.0 to 0.50
    sweep_results = []

    for blend_w in weights_to_test:
        hits = 0
        total = 0

        for req, hg, ag, match_date, home_team, away_team in match_data:
            if blend_w > 0:
                nudge = get_historical_player_nudge(
                    db, league_code, home_team, away_team, match_date,
                    blend_weight=blend_w,
                )
            else:
                nudge = 0.0

            try:
                pred = evaluate_athena(req, over_bias, under_bias, tempo_fac, team_nudge=nudge)
                market = pred.translated_play.market
                result = evaluate_market(market, hg, ag)
                hw = hit_weight(result)
                if hw < 0:
                    continue
                total += 1
                if hw >= 0.5:
                    hits += 1
            except Exception:
                continue

        if total > 0:
            sweep_results.append({
                "blend_weight": blend_w,
                "hits": hits,
                "total": total,
                "hit_rate": round(hits / total * 100, 1),
            })

    # Sort by hit rate descending
    sweep_results.sort(key=lambda x: x["hit_rate"], reverse=True)

    best = sweep_results[0] if sweep_results else None
    baseline = next((r for r in sweep_results if r["blend_weight"] == 0.0), None)

    return {
        "league_code": league_code,
        "matches_tested": len(match_data),
        "baseline_hit_rate": baseline["hit_rate"] if baseline else None,
        "best_blend_weight": best["blend_weight"] if best else None,
        "best_hit_rate": best["hit_rate"] if best else None,
        "improvement_pp": round(best["hit_rate"] - baseline["hit_rate"], 1) if best and baseline else None,
        "sweep": sweep_results,
    }


@router.get("/player-power/squads")
def squad_power_overview(
    db: Session = Depends(get_db),
):
    """
    Returns all teams with zonal power scores, grouped by league.
    Used by the Squad Intel frontend page.
    """
    # Get all teams that have squad_power computed
    teams = (
        db.query(TeamConfig)
        .filter(TeamConfig.squad_power.isnot(None))
        .order_by(TeamConfig.league_code, TeamConfig.team)
        .all()
    )

    # Get league display names
    league_cfgs = {
        lc.league_code: {
            "description": lc.description or lc.league_code,
            "strength_coefficient": float(lc.strength_coefficient or 1.0),
        }
        for lc in db.query(LeagueConfig).all()
    }

    # Group by league
    leagues: dict = {}
    for t in teams:
        lc = t.league_code
        if lc not in leagues:
            lc_info = league_cfgs.get(lc, {})
            leagues[lc] = {
                "league_code": lc,
                "display_name": lc_info.get("description", lc),
                "strength_coefficient": lc_info.get("strength_coefficient", 1.0),
                "teams": [],
            }

        coeff = leagues[lc]["strength_coefficient"]

        raw_squad = round(float(t.squad_power), 1) if t.squad_power else None
        raw_atk   = round(float(t.atk_power), 1)   if t.atk_power else None
        raw_mid   = round(float(t.mid_power), 1)    if t.mid_power else None
        raw_def   = round(float(t.def_power), 1)    if t.def_power else None
        raw_gk    = round(float(t.gk_power), 1)     if t.gk_power else None

        leagues[lc]["teams"].append({
            "team": t.team,
            # Raw league-relative scores
            "squad_power": raw_squad,
            "atk_power": raw_atk,
            "mid_power": raw_mid,
            "def_power": raw_def,
            "gk_power": raw_gk,
            # Global scores (coefficient-adjusted) — comparable across leagues
            "global_squad": round(raw_squad * coeff, 1) if raw_squad else None,
            "global_atk":   round(raw_atk * coeff, 1)   if raw_atk else None,
            "global_mid":   round(raw_mid * coeff, 1)    if raw_mid else None,
            "global_def":   round(raw_def * coeff, 1)    if raw_def else None,
            "global_gk":    round(raw_gk * coeff, 1)     if raw_gk else None,
        })

    # Sort teams within each league by squad_power descending
    for lc_data in leagues.values():
        lc_data["teams"].sort(
            key=lambda t: t["squad_power"] or 0, reverse=True
        )

    return {
        "leagues": list(leagues.values()),
        "total_leagues": len(leagues),
        "total_teams": len(teams),
    }


@router.get("/player-power/form-delta")
def form_delta_endpoint(
    league_code: str = Query(..., description="League to analyse"),
    db: Session = Depends(get_db),
):
    """
    Compute Form Delta for a league: actual vs expected league position
    based on previous season standings, with zonal breakdown of why.
    """
    from app.services.form_delta import compute_form_delta
    return compute_form_delta(db, league_code)


@router.get("/player-power/form-delta/all")
def form_delta_all(
    db: Session = Depends(get_db),
):
    """
    Compute Form Delta for ALL leagues with snapshot data.
    Returns a summary per league (top overperformers and underperformers).
    """
    from app.services.form_delta import compute_form_delta

    snapshots = db.query(FBrefSnapshot.league_code).distinct().all()
    league_codes = sorted([s[0] for s in snapshots])

    # Skip international competitions — no league standings
    skip = {"UCL", "UEL", "UECL", "EC", "WC"}
    league_codes = [lc for lc in league_codes if lc not in skip]

    results = []
    for lc in league_codes:
        try:
            data = compute_form_delta(db, lc)
            if data.get("error") or not data.get("teams"):
                continue

            teams = data["teams"]
            top_over = [t for t in teams if t["form_delta"] >= 3][:3]
            top_under = [t for t in teams if t["form_delta"] <= -3][:3]

            results.append({
                "league_code": lc,
                "display_name": data["display_name"],
                "total_teams": data["total_teams"],
                "overperformers": [
                    {"team": t["team"], "delta": t["form_delta"],
                     "actual": t["actual_pos"], "expected": t["expected_pos"]}
                    for t in top_over
                ],
                "underperformers": [
                    {"team": t["team"], "delta": t["form_delta"],
                     "actual": t["actual_pos"], "expected": t["expected_pos"],
                     "weakness": t.get("primary_weakness")}
                    for t in top_under
                ],
            })
        except Exception as e:
            print(f"[form_delta] Error for {lc}: {e}")

    return {"leagues": results, "total_leagues": len(results)}


@router.post("/player-power/reindex")
def reindex_player_power(
    league_code: str = Query(None, description="Single league code, or omit for all leagues"),
    season: str = Query(None, description="Season label (e.g. '2025-2026' or '2026'). Auto-detected if omitted."),
    db: Session = Depends(get_db)
):
    """
    Manually trigger player power index recomputation for one or all leagues.
    Updates TeamConfig (squad_power, atk_power, etc.) and player power_index.
    """
    from app.services.player_index import compute_league_power

    if league_code:
        leagues = [league_code]
    else:
        snapshots = db.query(FBrefSnapshot.league_code).distinct().all()
        leagues = [s[0] for s in snapshots]

    results = []
    for lc in leagues:
        try:
            # Determine season if not provided
            if not season:
                season = SEASON_MAP.get(lc, "2025-2026")
            result = compute_league_power(db, lc, season)
            results.append({
                "league_code": lc,
                "players_indexed": result.get("players_indexed", 0),
                "teams_updated": result.get("teams_updated", 0)
            })
        except Exception as e:
            results.append({
                "league_code": lc,
                "error": str(e)
            })

    return {"reindex_results": results}


@router.get("/player-power/match-tags")
def match_performance_tags(
    league_code: str = Query(...),
    home_team: str = Query(...),
    away_team: str = Query(...),
    db: Session = Depends(get_db),
):
    """
    Generate performance tags for a matchup.
    Returns matchup headline + per-team zone tags.
    """
    from app.services.performance_tags import generate_match_tags_with_delta
    return generate_match_tags_with_delta(db, league_code, home_team, away_team)
