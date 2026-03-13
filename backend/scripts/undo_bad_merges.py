"""
backend/scripts/undo_bad_merges.py

Reverses incorrect team name merges made by fix_duplicates.py.
Edit the undo_fix() calls at the bottom before running.

Usage:
    cd backend
    python -m scripts.undo_bad_merges
"""
import sys
from pathlib import Path
from sqlalchemy import text

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from dotenv import load_dotenv
load_dotenv()

from app.database.db import SessionLocal


def undo_fix(wrong_name, correct_name, league):
    db = SessionLocal()
    print(f"Reversing: '{wrong_name}' back to '{correct_name}' [{league}]")
    try:
        p = db.execute(text(
            "UPDATE players SET current_team = :correct "
            "WHERE current_team = :wrong AND league_code = :league"
        ), {"correct": correct_name, "wrong": wrong_name, "league": league})

        fh = db.execute(text(
            "UPDATE fbref_fixtures SET home_team = :correct "
            "WHERE home_team = :wrong AND league_code = :league"
        ), {"correct": correct_name, "wrong": wrong_name, "league": league})

        fa = db.execute(text(
            "UPDATE fbref_fixtures SET away_team = :correct "
            "WHERE away_team = :wrong AND league_code = :league"
        ), {"correct": correct_name, "wrong": wrong_name, "league": league})

        tc = db.execute(text(
            "UPDATE team_configs SET team = :correct "
            "WHERE team = :wrong AND league_code = :league"
        ), {"correct": correct_name, "wrong": wrong_name, "league": league})

        ss = db.execute(text(
            "UPDATE squad_snapshots SET team = :correct "
            "WHERE team = :wrong AND league_code = :league"
        ), {"correct": correct_name, "wrong": wrong_name, "league": league})

        db.commit()
        print(f"  Recovered {p.rowcount} players, "
              f"{fh.rowcount + fa.rowcount} fixtures, "
              f"{tc.rowcount} team_configs, {ss.rowcount} snapshots")
    except Exception as e:
        db.rollback()
        print(f"  Error: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    # Add the corrections you need here:
    # undo_fix("wrong_name", "correct_name", "LEAGUE-CODE")
    #
    # Examples from your previous run:
    # undo_fix("Sheffield Weds", "Sheffield United", "ENG-CH")
    pass
