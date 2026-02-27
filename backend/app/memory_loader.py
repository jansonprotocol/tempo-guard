import json
import os
from sqlalchemy.orm import Session
from app.models.league_config import LeagueConfig

def load_league_configs(db: Session):
    """Load league configs from JSON seed file into DB if missing."""

    # Path to seed file
    seed_path = os.path.join("app", "seed", "league_configs.json")

    if not os.path.exists(seed_path):
        print("No seed file found.")
        return

    # Read JSON file
    with open(seed_path, "r") as f:
        data = json.load(f)

    # Insert entries if not already in DB
    for entry in data:
        exists = (
            db.query(LeagueConfig)
            .filter(LeagueConfig.league_code == entry["league_code"])
            .first()
        )

        if exists:
            continue

        print(f"Seeding league config: {entry['league_code']}")

        config = LeagueConfig(
            league_code=entry["league_code"],
            base_over_bias=entry["base_over_bias"],
            base_under_bias=entry["base_under_bias"],
            tempo_factor=entry["tempo_factor"],
            safety_mode=entry["safety_mode"],
            aggression_level=entry["aggression_level"],
            volatility=entry["volatility"],
            description=entry["description"],
        ) 

        db.add(config)

    db.commit()
    print("League config seeding complete.")
