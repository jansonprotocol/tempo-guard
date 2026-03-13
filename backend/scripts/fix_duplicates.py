import sys
import os
from pathlib import Path
from thefuzz import fuzz
from sqlalchemy import text

# Path setup to find the 'app' folder
path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal

# 🛑 SAFETY: Words that, if mismatched, should NEVER be merged automatically
FORBIDDEN_PAIRS = [
    ("City", "United"), ("City", "Utd"),
    ("United", "Wednesday"), ("Utd", "Weds"),
    ("Real", "Atletico"), ("Inter", "Milan"),
    ("Benfica", "Sporting")
]

def is_safe(name_a, name_b):
    """Returns False if the names are known rivals or distinct entities."""
    a, b = name_a.lower(), name_b.lower()
    for word1, word2 in FORBIDDEN_PAIRS:
        w1, w2 = word1.lower(), word2.lower()
        if (w1 in a and w2 in b) or (w1 in b and w2 in a):
            return False
    return True

def run_smart_clean():
    db = SessionLocal()
    print("🔍 Scanning database for potential team duplicates...")
    
    try:
        # 1. Get unique teams
        rows = db.execute(text("SELECT DISTINCT current_team, league_code FROM players")).fetchall()
        
        leagues = {}
        for team, league in rows:
            if not team: continue
            leagues.setdefault(league, []).append(team)

        proposals = []

        # 2. Compare within leagues
        for league, teams in leagues.items():
            for i, name_a in enumerate(teams):
                for name_b in teams[i+1:]:
                    # Increase threshold to 88% for higher accuracy
                    score = fuzz.token_set_ratio(name_a, name_b)
                    
                    if score >= 88 and is_safe(name_a, name_b):
                        # Shorter name is usually the master
                        master = name_a if len(name_a) < len(name_b) else name_b
                        variant = name_b if master == name_a else name_a
                        proposals.append((variant, master, league, score))

        if not proposals:
            print("✅ No duplicates found.")
            return

        # 3. Review phase
        print(f"\n💡 Found {len(proposals)} potential matches:")
        print("-" * 50)
        valid_proposals = []
        for v, m, l, s in proposals:
            print(f"[{l}] {v}  --->  {m} ({s}% match)")
        print("-" * 50)

        confirm = input("\n⚠️ Proceed with these changes? (type 'yes' to commit): ")

        if confirm.lower() == 'yes':
            for variant, master, league, _ in proposals:
                # Update Players
                db.execute(text("UPDATE players SET current_team = :m WHERE current_team = :v AND league_code = :l"),
                           {"m": master, "v": variant, "l": league})
                # Update Fixtures
                db.execute(text("UPDATE fbref_fixtures SET home_team = :m WHERE home_team = :v AND league_code = :l"),
                           {"m": master, "v": variant, "l": league})
                db.execute(text("UPDATE fbref_fixtures SET away_team = :m WHERE away_team = :v AND league_code = :l"),
                           {"m": master, "v": variant, "l": league})
                # Update Snapshots
                db.execute(text("UPDATE fbref_snapshots SET data = REPLACE(data::text, :v, :m)::bytea WHERE league_code = :l"),
                           {"m": master, "v": variant, "l": league})
            
            db.commit()
            print("\n✨ Database cleaned successfully!")
        else:
            print("\n❌ Operation cancelled. No changes made.")

    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    run_smart_clean()
