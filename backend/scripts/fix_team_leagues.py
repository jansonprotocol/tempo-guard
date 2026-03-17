"""
backend/scripts/fix_team_leagues.py

Automatically correct team league assignments based on player data.
For each team, finds the most common league among its players and updates the teams table.
"""

import sys
from pathlib Path
from collections import Counter

path_root = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(path_root))

from app.database.db import SessionLocal
from app.models.team import Team
from app.models.models_players import Player
from app.util.text_norm import normalize_team

def main():
    db = SessionLocal()
    try:
        # Get all distinct team names from players
        team_names = db.query(Player.current_team).distinct().all()
        team_names = [t[0] for t in team_names if t[0] and t[0].strip()]

        updated = 0
        ambiguous = 0
        no_players = 0

        for raw_name in team_names:
            norm = normalize_team(raw_name)

            # Find all players with this normalized team name
            players = db.query(Player).filter(
                Player.current_team == raw_name
            ).all()

            if not players:
                continue

            # Count league occurrences
            league_counts = Counter(p.league_code for p in players if p.league_code)
            if not league_counts:
                continue

            # Get the most common league
            most_common = league_counts.most_common(1)[0]
            top_league, top_count = most_common

            # Check if there's a tie for first place
            top_league_counts = [cnt for league, cnt in league_counts.items() if cnt == top_count]
            if len(top_league_counts) > 1:
                print(f"Ambiguous league for team '{raw_name}' (normalized '{norm}'): "
                      f"multiple leagues have {top_count} players each: {league_counts}")
                ambiguous += 1
                continue

            # Find the team record(s) with this normalized key
            teams = db.query(Team).filter(
                Team.team_key == norm
            ).all()

            if not teams:
                # No team record for this normalized name – we could create one, but for now skip
                print(f"No team record found for '{raw_name}' (norm '{norm}') – skipping")
                continue

            if len(teams) > 1:
                # Multiple teams share the same key – ambiguous, log and skip
                print(f"Multiple teams with key '{norm}': {[(t.team_key, t.league_code) for t in teams]} – skipping")
                ambiguous += 1
                continue

            team = teams[0]
            if team.league_code != top_league:
                old = team.league_code
                team.league_code = top_league
                updated += 1
                print(f"Updated team '{team.display_name}' ({norm}): {old} -> {top_league} (based on {top_count} players)")

        db.commit()
        print(f"\nDone. Updated {updated} teams, skipped {ambiguous} ambiguous cases.")

    finally:
        db.close()

if __name__ == "__main__":
    main()
