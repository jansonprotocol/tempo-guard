# backend/app/services/form_delta_history.py
"""Compute point‑in‑time form delta for a team."""
import io
from datetime import date
from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session
from app.database.models_fbref import FBrefSnapshot
from app.models.team_config import TeamConfig
from app.models.league_config import LeagueConfig
from app.services.form_delta import _season_cutoff, _compute_standings
from app.services.data_providers.fbref_base import _parse_score_column, _resolve_columns

def get_historical_form_delta(
    db: Session,
    team: str,
    league_code: str,
    match_date: date
) -> Optional[int]:
    """
    Return form delta (expected_pos - actual_pos) for a team as of match_date.
    """
    # Use in-memory snapshot if warmed by feature_cache (avoids DB read + parquet parse)
    from app.services.data_providers.fbref_base import _SNAPSHOT_OVERRIDE
    if league_code in _SNAPSHOT_OVERRIDE:
        df = _SNAPSHOT_OVERRIDE[league_code].copy()
    else:
        snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
        if not snap:
            return None
        try:
            df = pd.read_parquet(io.BytesIO(snap.data))
        except Exception:
            return None

    # Parse scores
    score_col = next((c for c in df.columns if str(c).lower() in ("score", "scores")), None)
    if score_col and "hg" not in [str(c).lower() for c in df.columns]:
        df = _parse_score_column(df, score_col)

    if "hg" not in df.columns or "ag" not in df.columns:
        return None

    # Get column names
    col_map = {str(c).lower(): c for c in df.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home") or col_map.get("home_team")
    away_col = col_map.get("away") or col_map.get("away_team")

    if not all([date_col, home_col, away_col]):
        return None

    df[date_col] = pd.to_datetime(df[date_col], errors="coerce")

    # Filter matches up to match_date
    df = df[df[date_col] <= pd.Timestamp(match_date)]
    if df.empty:
        return None

    # Compute standings as of that date – using the same function as form_delta.py
    standings = _compute_standings(db, df, home_col, away_col)

    # Find team's actual position
    actual_pos = None
    for entry in standings:
        # _compute_standings returns dict with key "team_key" (from form_delta.py)
        if entry.get("team_key") == team:  # FIXED: use team_key instead of team
            actual_pos = entry["pos"]
            break
    if actual_pos is None:
        return None

    # Expected position: previous season final or squad power rank
    # Determine season cutoff
    cutoff = pd.Timestamp(_season_cutoff(league_code))

    # Separate previous and current season matches
    prev_df = df[df[date_col] < cutoff]
    curr_df = df[df[date_col] >= cutoff]

    expected_pos = None

    # Try previous season standings first
    if not prev_df.empty and len(prev_df) >= 30:
        prev_standings = _compute_standings(db, prev_df, home_col, away_col)
        for entry in prev_standings:
            if entry.get("team_key") == team:  # FIXED: use team_key
                expected_pos = entry["pos"]
                break

    # Fallback to squad power ranking if no previous season data
    if expected_pos is None:
        # Get all teams in league with squad power
        teams = db.query(TeamConfig).filter_by(league_code=league_code).all()
        if teams:
            # Sort by squad power descending
            sorted_teams = sorted(teams, key=lambda t: t.squad_power or 0, reverse=True)
            for i, t in enumerate(sorted_teams):
                if t.team == team:
                    expected_pos = i + 1
                    break

    if expected_pos is None:
        return None

    return expected_pos - actual_pos
