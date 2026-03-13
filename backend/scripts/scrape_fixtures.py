"""
backend/scripts/fix_duplicates.py

Scans the database for team name duplicates within each league and
proposes merges. Interactive — asks for confirmation before writing.

Usage:
    cd backend
    python -m scripts.fix_duplicates
"""
import sys
from pathlib import Path
from rapidfuzz import fuzz
from sqlalchemy import text

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from dotenv import load_dotenv
load_dotenv()

from app.database.db import SessionLocal

# Words that, if mismatched, should NEVER be merged automatically
FORBIDDEN_PAIRS = [
    ("City", "United"), ("City", "Utd"),
    ("United", "Wednesday"), ("Utd", "Weds"),
    ("Real", "Atletico"), ("Inter", "Milan"),
    ("Benfica", "Sporting"), ("Feyenoord", "Feyen"),
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
    print("Scanning database for potential team duplicates...")

    try:
        # Get unique teams from players table
        rows = db.execute(text(
            "SELECT DISTINCT current_team, league_code FROM players WHERE current_team IS NOT NULL"
        )).fetchall()

        leagues = {}
        for team, league in rows:
            if not team:
                continue
            leagues.setdefault(league, []).append(team)

        proposals = []

        # Compare within leagues
        for league, teams in leagues.items():
            for i, name_a in enumerate(teams):
                for name_b in teams[i + 1:]:
                    score = fuzz.token_set_ratio(name_a, name_b)

                    if score >= 88 and is_safe(name_a, name_b):
                        # Shorter name is usually the master
                        master = name_a if len(name_a) <= len(name_b) else name_b
                        variant = name_b if master == name_a else name_a
                        proposals.append((variant, master, league, score))

        if not proposals:
            print("No duplicates found.")
            return

        print(f"\nFound {len(proposals)} potential matches:")
        print("-" * 60)
        for v, m, l, s in proposals:
            print(f"  [{l}] '{v}' ---> '{m}' ({s}% match)")
        print("-" * 60)

        confirm = input("\nProceed with these changes? (type 'yes' to commit): ")

        if confirm.lower() == 'yes':
            for variant, master, league, _ in proposals:
                # Update Players
                db.execute(text(
                    "UPDATE players SET current_team = :m "
                    "WHERE current_team = :v AND league_code = :l"
                ), {"m": master, "v": variant, "l": league})

                # Update Fixtures
                db.execute(text(
                    "UPDATE fbref_fixtures SET home_team = :m "
                    "WHERE home_team = :v AND league_code = :l"
                ), {"m": master, "v": variant, "l": league})
                db.execute(text(
                    "UPDATE fbref_fixtures SET away_team = :m "
                    "WHERE away_team = :v AND league_code = :l"
                ), {"m": master, "v": variant, "l": league})

                # Update TeamConfig
                db.execute(text(
                    "UPDATE team_configs SET team = :m "
                    "WHERE team = :v AND league_code = :l"
                ), {"m": master, "v": variant, "l": league})

                # Update SquadSnapshots
                db.execute(text(
                    "UPDATE squad_snapshots SET team = :m "
                    "WHERE team = :v AND league_code = :l"
                ), {"m": master, "v": variant, "l": league})

                print(f"  Merged '{variant}' -> '{master}' [{league}]")

            db.commit()
            print("\nDatabase cleaned successfully!")
        else:
            print("\nCancelled. No changes made.")

    except Exception as e:
        db.rollback()
        print(f"Error: {e}")
    finally:
        db.close()


if __name__ == "__main__":
    run_smart_clean()
