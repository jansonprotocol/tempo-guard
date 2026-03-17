# backend/app/core/constants.py
# Shared constants used by scrapers and API

SEASON_MAP = {
    # Aug–May
    "ENG-PL": "2025-2026", "ENG-CH": "2025-2026",
    "ESP-LL": "2025-2026", "ESP-LL2": "2025-2026",
    "FRA-L1": "2025-2026", "FRA-L2": "2025-2026",
    "GER-BUN": "2025-2026", "GER-B2": "2025-2026",
    "ITA-SA": "2025-2026", "ITA-SB": "2025-2026",
    "NED-ERE": "2025-2026",
    "TUR-SL": "2025-2026",
    "SAU-SPL": "2025-2026",
    "DEN-SL": "2025-2026",
    "BEL-PL": "2025-2026",
    "MEX-LMX": "2025-2026",
    "POL-EK": "2025-2026",
    "AUT-BL": "2025-2026",
    "SUI-SL": "2025-2026",
    "CHI-LP": "2025-2026",
    "PER-L1": "2025-2026",
    "POR-LP": "2025-2026",
    # Calendar year
    "BRA-SA": "2026", "BRA-SB": "2026",
    "MLS": "2026",
    "NOR-EL": "2026", "SWE-AL": "2026",
    "CHN-CSL": "2026", "JPN-J1": "2026",
    "COL-PA": "2026",
}

SCHEDULE_URLS = {
    "ENG-PL":  "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    # ... (copy the full dict from your existing file)
}
