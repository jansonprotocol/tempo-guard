from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

def merge_teams(master_name, variant_name, league):
    db = SessionLocal()
    try:
        # Update the snapshots where the 'wrong' name was used
        updated = db.query(FBrefSnapshot).filter(
            FBrefSnapshot.league_code == league,
            FBrefSnapshot.home_team == variant_name
        ).update({"home_team": master_name})

        updated += db.query(FBrefSnapshot).filter(
            FBrefSnapshot.league_code == league,
            FBrefSnapshot.away_team == variant_name
        ).update({"away_team": master_name})

        db.commit()
        if updated > 0:
            print(f"✅ Success: Merged '{variant_name}' into '{master_name}' ({updated} rows updated)")
        else:
            print(f"ℹ️ No rows found for '{variant_name}' in {league}")
            
    except Exception as e:
        db.rollback()
        print(f"❌ Error: {e}")
    finally:
        db.close()

if __name__ == "__main__":
    # Add any teams here that are showing up twice in your dropdowns
    merge_teams("Fredericia", "FC Fredericia", "DEN-SL")
    merge_teams("Ajax", "Ajax Amsterdam", "NED-ERE")
