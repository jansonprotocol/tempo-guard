import sys
import os
from pathlib import Path
from sqlalchemy import text

# Path setup
path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal

def undo_fix(wrong_name, correct_name, league):
    db = SessionLocal()
    print(f"Correction: Moving {wrong_name} back to {correct_name} ({league})")
    try:
        # 1. Fix Players
        p = db.execute(text("UPDATE players SET current_team = :correct WHERE current_team = :wrong AND league_code = :league"),
                   {"correct": correct_name, "wrong": wrong_name, "league": league})
        
        # 2. Fix Fixtures
        fh = db.execute(text("UPDATE fbref_fixtures SET home_team = :correct WHERE home_team = :wrong AND league_code = :league"),
                   {"correct": correct_name, "wrong": wrong_name, "league": league})
        fa = db.execute(text("UPDATE fbref_fixtures SET away_team = :correct WHERE away_team = :wrong AND league_code = :league"),
                   {"correct": correct_name, "wrong": wrong_name, "league": league})
        
        db.commit()
        print(f"✅ Recovered {p.rowcount} players and {fh.rowcount + fa.rowcount} fixtures.")
    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    # Add the ones that got messed up here
    undo_fix("Sheffield Weds", "Sheffield United", "ENG-CH")
    undo_fix("Deportivo Cali", "Cúcuta Deportivo", "COL-B")
    undo_fix("Manchester Utd", "Manchester City", "ENG-PR")
    undo_fix("U Concepción", "Dep. Concepción", "CHI-2")
