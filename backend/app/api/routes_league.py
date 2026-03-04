from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.database.db import get_db
from app.models.league_config import LeagueConfig

router = APIRouter()

# League code → human-readable name map (used in dropdown)
LEAGUE_NAMES = {
    "ENG-PL":  "Premier League",
    "ESP-LL":  "La Liga",
    "FRA-L1":  "Ligue 1",
    "GER-BUN": "Bundesliga",
    "ITA-SA":  "Serie A",
    "NED-ERE": "Eredivisie",
    "TUR-SL":  "Süper Lig",
    "BRA-SA":  "Série A (Brazil)",
    "MLS":     "MLS",
    "SAU-SPL": "Saudi Pro League",
    "DEN-SL":  "Superliga",
    "ESP-LL2": "Segunda División",
    "BEL-PL":  "Pro League",
    "NOR-EL":  "Eliteserien",
    "SWE-AL":  "Allsvenskan",
    "MEX-LMX": "Liga MX",
    "UCL":     "Champions League",
    "UEL":     "Europa League",
    "UECL":    "Conference League",
    "EC":      "European Championship",
    "WC":      "World Cup",
}

# League code → flag emoji
LEAGUE_FLAGS = {
    "ENG-PL":  "🏴󠁧󠁢󠁥󠁮󠁧󠁿",
    "ESP-LL":  "🇪🇸",
    "FRA-L1":  "🇫🇷",
    "GER-BUN": "🇩🇪",
    "ITA-SA":  "🇮🇹",
    "NED-ERE": "🇳🇱",
    "TUR-SL":  "🇹🇷",
    "BRA-SA":  "🇧🇷",
    "MLS":     "🇺🇸",
    "SAU-SPL": "🇸🇦",
    "DEN-SL":  "🇩🇰",
    "ESP-LL2": "🇪🇸",
    "BEL-PL":  "🇧🇪",
    "NOR-EL":  "🇳🇴",
    "SWE-AL":  "🇸🇪",
    "MEX-LMX": "🇲🇽",
    "UCL":     "🏆",
    "UEL":     "🏆",
    "UECL":    "🏆",
    "EC":      "🌍",
    "WC":      "🌍",
}

# -------------------------------------------------------------------
# GET /api/league-configs
# -------------------------------------------------------------------
@router.get("/league-configs")
def get_league_configs(db: Session = Depends(get_db)):
    rows = db.query(LeagueConfig).all()
    return [
        {
            "league_code":    r.league_code,
            "over_bias":      r.base_over_bias,
            "under_bias":     r.base_under_bias,
            "tempo_factor":   r.tempo_factor,
            "safety_mode":    r.safety_mode,
            "aggression_level": r.aggression_level,
            "volatility":     r.volatility,
            "description":    r.description,
        }
        for r in rows
    ]


# -------------------------------------------------------------------
# GET /api/league-list  (frontend dropdown)
# -------------------------------------------------------------------
@router.get("/league-list")
def league_list(db: Session = Depends(get_db)):
    rows = db.query(LeagueConfig).order_by(LeagueConfig.league_code).all()
    return [
        {
            "code": r.league_code,
            "name": LEAGUE_NAMES.get(r.league_code, r.league_code),
            "flag": LEAGUE_FLAGS.get(r.league_code, "🌍"),
        }
        for r in rows
    ]


# -------------------------------------------------------------------
# POST /api/league-upsert
# -------------------------------------------------------------------
class UpsertLeaguePayload(BaseModel):
    league_code:      str
    base_over_bias:   float = 0.0
    base_under_bias:  float = 0.0
    tempo_factor:     float = 1.0
    safety_mode:      bool  = True
    aggression_level: float = 0.5
    volatility:       float = 0.5
    description:      str   = ""


@router.post("/league-upsert")
def league_upsert(payload: UpsertLeaguePayload, db: Session = Depends(get_db)):
    item = (
        db.query(LeagueConfig)
        .filter(LeagueConfig.league_code == payload.league_code)
        .first()
    )
    if item is None:
        item = LeagueConfig(league_code=payload.league_code)

    item.base_over_bias   = payload.base_over_bias
    item.base_under_bias  = payload.base_under_bias
    item.tempo_factor     = payload.tempo_factor
    item.safety_mode      = payload.safety_mode
    item.aggression_level = payload.aggression_level
    item.volatility       = payload.volatility
    item.description      = payload.description

    db.add(item)
    db.commit()
    db.refresh(item)
    return {"message": "upsert_ok", "league_code": item.league_code}
