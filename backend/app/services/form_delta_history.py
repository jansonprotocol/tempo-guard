# backend/app/services/form_delta_history.py
"""Compute point‑in‑time form delta for a team."""
from datetime import date
from typing import Optional
import pandas as pd
from sqlalchemy.orm import Session
from app.database.models_fbref import FBrefSnapshot
from app.models.team_config import TeamConfig
from app.models.league_config import LeagueConfig
from app.services.form_delta import _compute_standings, _season_cutoff

def get_historical_form_delta(
    db: Session,
    team: str,
    league_code: str,
    match_date: date
) -> Optional[int]:
    """
    Return form delta (expected_pos - actual_pos) for a team as of match_date.
    """
    # Load snapshot
    snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not snap:
        return None
    df = pd.read_parquet(io.BytesIO(snap.data))
    # Parse scores and columns as in form_delta.py
    # ... (copy necessary helper code)
    # Filter matches up to match_date
    df = df[df['date'] <= pd.Timestamp(match_date)]
    if df.empty:
        return None
    # Compute standings
    standings = _compute_standings(db, df, home_col, away_col)  # need to resolve columns
    # Find team
    for entry in standings:
        if entry['team'] == team:
            actual_pos = entry['pos']
            break
    else:
        return None
    # Expected position: previous season final or squad power rank
    # Reuse logic from form_delta.compute_form_delta
    # ...
    return expected_pos - actual_pos
