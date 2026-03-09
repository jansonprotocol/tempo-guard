"""
backend/scripts/add_round_type_migration.py

Adds the `round_type` column to the fbref_fixtures table.

Run once, locally and on Render:
    cd backend
    python -m scripts.add_round_type_migration

Safe to re-run — skips if column already exists.
"""
from __future__ import annotations

import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from sqlalchemy import text
from app.database.db import engine


def run() -> None:
    with engine.connect() as conn:
        # Check if column already exists
        result = conn.execute(text(
            "SELECT column_name FROM information_schema.columns "
            "WHERE table_name='fbref_fixtures' AND column_name='round_type'"
        ))
        if result.fetchone():
            print("[migration] round_type already exists — skipping.")
            return

        conn.execute(text(
            "ALTER TABLE fbref_fixtures ADD COLUMN round_type VARCHAR(64) NULL"
        ))
        conn.commit()
        print("[migration] Added round_type column to fbref_fixtures.")


if __name__ == "__main__":
    run()
