"""
backend/scripts/fix_player_leagues.py

Reassign players to the correct league based on their team's canonical entry.
Run this once to fix cross‑league contamination.
"""

import sys
from pathlib import Path

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.models.team import Team
from app.models.models_players import Player
from app.util.text_norm import normalize_team

def main():
    db = SessionLocal()
    try:
        # Get all players with a current_team
        players = db.query(Player).filter(Player.current_team.isnot(None)).all()
        print(f"Found {len(players)} players with a team.")

        # Build a mapping from normalized team key to its canonical league
        # (if multiple teams share the same key, we'll log them)
        team_map = {}
        teams = db.query(Team).all()
        for t in teams:
            norm = normalize_team(t.team_key)  # team_key is already normalized, but double‑check
            team_map.setdefault(norm, []).append(t)

        updated = 0
        skipped_multiple = 0
        skipped_none = 0

        for player in players:
            if not player.current_team:
                continue
            norm = normalize_team(player.current_team)
            candidates = team_map.get(norm)
            if not candidates:
                # No team found with that key – skip (player may be from an un‑seeded league)
                skipped_none += 1
                continue
            if len(candidates) > 1:
                # Multiple teams share the same key – ambiguous, log and skip
                print(f"Ambiguous team key '{norm}' for player {player.name} (ID {player.id}): "
                      f"candidates: {[(t.team_key, t.league_code) for t in candidates]}")
                skipped_multiple += 1
                continue
            correct_team = candidates[0]
            if player.league_code != correct_team.league_code:
                old_league = player.league_code
                player.league_code = correct_team.league_code
                updated += 1
                print(f"Player {player.name} (ID {player.id}): {old_league} -> {correct_team.league_code}")

        db.commit()
        print(f"\nDone. Updated {updated} players, skipped {skipped_none} with no team, "
              f"{skipped_multiple} with ambiguous team key.")

    finally:
        db.close()

if __name__ == "__main__":
    main()
