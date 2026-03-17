"""
backend/scripts/fix_player_leagues.py

Reassign players to the correct league based on their team's canonical entry.
Uses the alias system to resolve raw team names to canonical team keys.
Run this once to fix cross‑league contamination.
"""

import sys
from pathlib import Path
from dotenv import load_dotenv

# Load environment variables
env_path = Path(__file__).parent.parent / ".env"
load_dotenv(dotenv_path=env_path)

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.models.team import Team
from app.models.models_players import Player
from app.services.resolve_team import resolve_team_name

def main():
    db = SessionLocal()
    try:
        # Get all players with a current_team
        players = db.query(Player).filter(Player.current_team.isnot(None)).all()
        print(f"Found {len(players)} players with a team.")

        updated = 0
        skipped_no_team = 0
        skipped_no_resolution = 0

        for player in players:
            if not player.current_team:
                skipped_no_team += 1
                continue

            # Resolve the raw team name to its canonical key
            resolved_key = resolve_team_name(db, player.current_team, player.league_code)

            # Find the team with that key (should be unique)
            team = db.query(Team).filter(Team.team_key == resolved_key).first()
            if not team:
                # No team record for this resolved key – skip (may need to be created)
                skipped_no_resolution += 1
                continue

            # Update player's league if different
            if player.league_code != team.league_code:
                old = player.league_code
                player.league_code = team.league_code
                updated += 1
                print(f"Player {player.name} (ID {player.id}): {old} -> {team.league_code}")

        db.commit()
        print(f"\nDone. Updated {updated} players, skipped {skipped_no_team} with no team, "
              f"{skipped_no_resolution} with unresolved team key.")

    finally:
        db.close()

if __name__ == "__main__":
    main()
