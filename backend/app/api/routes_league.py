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
    "CHN-CSL": "Chinese Super League",
    "JPN-J1":  "J1 League",
    "COL-PA":  "Primera A",
    "CUB-PD":  "Primera División",
    "ITA-SB":  "Serie B (Italy)",
    "FRA-L2":  "Ligue 2",
    "GER-B2":  "2. Bundesliga",
    "POL-EK":  "Ekstraklasa",
    "BRA-SB":  "Série B (Brazil)",
    "AUT-BL":  "Austria Bundesliga",
    "SUI-SL":  "Switzerland Super League",
    "CHI-LP":  "Chile Liga de Primera",
    "PER-L1":  "Peru Liga 1",
    "POR-LP":  "Portugal Liga Portugal",
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
    "CHN-CSL": "🇨🇳",
    "JPN-J1":  "🇯🇵",
    "COL-PA":  "🇨🇴",
    "CUB-PD":  "🇨🇺",
    "ITA-SB":  "🇮🇹",
    "FRA-L2":  "🇫🇷",
    "GER-B2":  "🇩🇪",
    "POL-EK":  "🇵🇱",
    "BRA-SB":  "🇧🇷",
    "AUT-BL":  "🇦🇹",
    "SUI-SL":  "🇨🇭",
    "CHI-LP":  "🇨🇱",
    "PER-L1":  "🇵🇪",
    "POR-LP":  "🇵🇹",
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
# Always driven by the canonical LEAGUE_NAMES dict — never the DB.
# Stale/old DB rows with wrong codes are ignored entirely.
# -------------------------------------------------------------------
@router.get("/league-list")
def league_list():
    return [
        {
            "code": code,
            "name": LEAGUE_NAMES[code],
            "flag": LEAGUE_FLAGS.get(code, "🌍"),
        }
        for code in LEAGUE_NAMES
    ]


# -------------------------------------------------------------------
# POST /api/league-cleanup  (one-time: remove stale league_code rows)
# -------------------------------------------------------------------
@router.post("/league-cleanup")
def league_cleanup(db: Session = Depends(get_db)):
    valid_codes = set(LEAGUE_NAMES.keys())
    all_rows    = db.query(LeagueConfig).all()
    removed     = []
    for row in all_rows:
        if row.league_code not in valid_codes:
            db.delete(row)
            removed.append(row.league_code)
    db.commit()
    return {"removed": removed, "kept": list(valid_codes)}


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



# -------------------------------------------------------------------
# POST /api/league-reset-biases
# Resets all league configs to neutral calibration baseline.
# Run this once after changing the bias scale, then recalibrate.
# -------------------------------------------------------------------

# Neutral baseline — matches the new scale in routes_calibration.py
NEUTRAL_OVER_BIAS   = 0.05
NEUTRAL_UNDER_BIAS  = 0.05
NEUTRAL_TEMPO       = 0.50  # 0.5 = neutral (multiplier of 1.0)


@router.post("/league-reset-biases")
def league_reset_biases(
    league_code: str = None,  # if None, resets ALL leagues
    db: Session = Depends(get_db),
):
    """
    Resets league bias values to neutral baseline:
      base_over_bias  = 0.05
      base_under_bias = 0.05
      tempo_factor    = 0.50

    Pass ?league_code=ENG-PL to reset a single league.
    Call without params to reset all leagues at once.
    """
    q = db.query(LeagueConfig)
    if league_code:
        q = q.filter(LeagueConfig.league_code == league_code)

    rows = q.all()
    if not rows:
        return {
            "message": "No leagues found to reset.",
            "league_code": league_code,
        }

    reset = []
    for row in rows:
        before = {
            "base_over_bias":  row.base_over_bias,
            "base_under_bias": row.base_under_bias,
            "tempo_factor":    row.tempo_factor,
        }
        row.base_over_bias  = NEUTRAL_OVER_BIAS
        row.base_under_bias = NEUTRAL_UNDER_BIAS
        row.tempo_factor    = NEUTRAL_TEMPO
        reset.append({
            "league_code": row.league_code,
            "before": before,
            "after": {
                "base_over_bias":  NEUTRAL_OVER_BIAS,
                "base_under_bias": NEUTRAL_UNDER_BIAS,
                "tempo_factor":    NEUTRAL_TEMPO,
            }
        })

    db.commit()
    return {
        "message": f"Reset {len(reset)} league(s) to neutral baseline.",
        "neutral_baseline": {
            "base_over_bias":  NEUTRAL_OVER_BIAS,
            "base_under_bias": NEUTRAL_UNDER_BIAS,
            "tempo_factor":    NEUTRAL_TEMPO,
        },
        "reset": reset,
    }

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
