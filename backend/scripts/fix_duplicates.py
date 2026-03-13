import sys
import os
from pathlib import Path
from thefuzz import fuzz # You might need to pip install thefuzz

# Path setup
path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from sqlalchemy import text

def smart_sweep(threshold=80):
    db = SessionLocal()
    print(f"🕵️ Starting Smart Sweep (Similarity Threshold: {threshold}%)")
    
    try:
        # 1. Get all unique team names from the players table
        result = db.execute(text("SELECT DISTINCT current_team, league_code FROM players")).fetchall()
        
        # Organize by league
        leagues = {}
        for team, league in result:
            if league not in leagues: leagues[league] = []
            leagues[league].append(team)

        for league, teams in leagues.items():
            # Compare every team with every other team in the same league
            for i, name_a in enumerate(teams):
                for name_b in teams[i+1:]:
                    score = fuzz.token_set_ratio(name_a, name_b)
                    
                    if score >= threshold:
                        # Logic: Usually the shorter name is our 'Master' name
                        master = name_a if len(name_a) < len(name_b) else name_b
                        variant = name_b if master == name_a else name_a
                        
                        print(f"🔗 Match Found ({score}%): '{variant}' -> '{master}'")
                        
                        # Apply the fix (using our previous SQL logic)
                        db.execute(text("UPDATE players SET current_team = :m WHERE current_team = :v AND league_code = :l"),
                                   {"m": master, "v": variant, "l": league})
                        
                        db.execute(text("UPDATE fbref_fixtures SET home_team = :m WHERE home_team = :v AND league_code = :l"),
                                   {"m": master, "v": variant, "l": league})
                        
                        db.execute(text("UPDATE fbref_fixtures SET away_team = :m WHERE away_team = :v AND league_code = :l"),
                                   {"m": master, "v": variant, "l": league})

        db.commit()
        print("\n✅ Sweep Complete. Database is synchronized.")
        
    except Exception as e:
        db.rollback()
        print(f"❌ Sweep Failed: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    smart_sweep()
