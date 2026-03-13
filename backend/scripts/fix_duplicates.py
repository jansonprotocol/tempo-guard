import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from app.database.db import SessionLocal
from app.models.team_config import TeamConfig
from app.models.models_players import PlayerSeasonStats

def merge(master: str, variant: str, league: str):
    db = SessionLocal()
    # 1. Re-point all player stats to the master name
    db.query(PlayerSeasonStats).filter_by(team=variant, league_code=league).update({"team": master})
    
    # 2. Merge hit rate data
    v_node = db.query(TeamConfig).filter_by(team=variant, league_code=league).first()
    m_node = db.query(TeamConfig).filter_by(team=master, league_code=league).first()
    
    if v_node and m_node:
        m_node.over_matches += v_node.over_matches
        m_node.under_matches += v_node.under_matches
        db.delete(v_node)
        
    db.commit()
    print(f"Consolidated {variant} into {master}")

if __name__ == "__main__":
    merge("Fredericia", "FC Fredericia", "DEN-SL")
