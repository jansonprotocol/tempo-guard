# backend/app/services/weather_service.py
"""
ATHENA Weather Service — per-fixture weather conditions via Open-Meteo.

WHY THIS EXISTS
===============
Total goals markets are meaningfully affected by weather:
  - Heavy rain:    ball control degrades, tempo drops → suppresses goals
  - Strong wind:   long-ball game disrupted, set pieces chaotic → suppresses scoring
  - Extreme heat:  player fatigue increases, especially second half → affects tempo
  - Cold (<5°C):   pitch firmness, keeper errors → slightly volatile

Open-Meteo is FREE, requires no API key, has no rate-limit issues at our
volume, and provides hourly forecasts up to 16 days ahead — perfect for
the 5-day batch-predict window.

API: https://api.open-meteo.com/v1/forecast

WEATHER TAG SCHEMA
==================
Each fixture gets a `weather_tag` string and a `weather_impact` float:

  weather_tag:     human-readable condition string
                   e.g. "Heavy Rain", "Strong Wind", "Clear", "Cold"

  weather_impact:  additive adjustment to deg_pressure
                   Range: -0.05 (ideal conditions) to +0.12 (severe weather)
                   Applied to deg_pressure before evaluate_athena runs.
                   e.g. heavy rain → +0.08 (more degradation expected)

  weather_details: raw values dict for transparency / debugging

INTEGRATION
===========
Called once per fixture in batch-predict, before predict_match():

    from app.services.weather_service import get_match_weather

    weather = get_match_weather(lat, lon, match_date, match_time_utc)
    if weather and weather["weather_impact"] != 0.0:
        metrics["deg_pressure"] = min(1.0, (metrics.get("deg_pressure") or 0.0)
                                       + weather["weather_impact"])

Stadium coordinates are resolved from a static lookup (STADIUM_COORDS).
Add missing stadiums there as you expand league coverage.

USAGE
=====
    from app.services.weather_service import (
        get_match_weather,
        get_stadium_coords,
        weather_impact_for_fixtures,
    )

    # Single fixture
    w = get_match_weather(51.555, -0.108, date(2026, 3, 22), hour_utc=15)

    # Batch — resolves coords from stadium lookup
    impacts = weather_impact_for_fixtures(fixtures_list)
"""
from __future__ import annotations

import time
from datetime import date, datetime
from typing import Optional

import requests

# ── Stadium coordinates lookup ────────────────────────────────────────────────
# lat, lon for the home ground of each team_key.
# Add entries here as you scrape new leagues / teams.
# Source: Wikipedia / Google Maps. Accuracy to 3 decimal places is sufficient.
STADIUM_COORDS: dict[str, tuple[float, float]] = {
    # England — Premier League
    "arsenal":                  (51.555, -0.108),
    "aston-villa":              (52.509, -1.885),
    "bournemouth":              (50.735, -1.838),
    "brentford":                (51.487, -0.289),
    "brighton":                 (50.862,  0.083),
    "chelsea":                  (51.481, -0.191),
    "crystal-palace":           (51.398, -0.086),
    "everton":                  (53.439, -2.966),
    "fulham":                   (51.475, -0.221),
    "ipswich-town":             (52.055,  1.145),
    "leicester-city":           (52.620, -1.142),
    "liverpool":                (53.431, -2.961),
    "manchester-city":          (53.483, -2.200),
    "manchester-united":        (53.463, -2.292),
    "newcastle-united":         (54.975, -1.622),
    "nottingham-forest":        (52.940, -1.133),
    "southampton":              (50.906, -1.391),
    "tottenham-hotspur":        (51.604, -0.066),
    "west-ham-united":          (51.538,  0.017),
    "wolverhampton-wanderers":  (52.590, -2.130),
    # England — Championship (sample)
    "leeds-united":             (53.777, -1.572),
    "sunderland":               (54.914, -1.388),
    "middlesbrough":            (54.578, -1.218),
    # Spain — La Liga
    "real-madrid":              (40.453, -3.688),
    "barcelona":                (41.381,  2.123),
    "atletico-madrid":          (40.436, -3.600),
    "athletic-bilbao":          (43.264, -2.950),
    "real-sociedad":            (43.301, -1.973),
    "real-betis":               (37.357, -5.982),
    "sevilla":                  (37.384, -5.971),
    "villarreal":               (39.944, -0.104),
    "valencia":                 (39.475, -0.358),
    # Germany — Bundesliga
    "bayer-leverkusen":         (51.038,  7.002),
    "borussia-dortmund":        (51.493,  7.452),
    "bayern-munich":            (48.219,  11.625),
    "rb-leipzig":               (51.346,  12.349),
    "vfb-stuttgart":            (48.792,  9.232),
    "eintracht-frankfurt":      (50.069,  8.645),
    "borussia-monchengladbach": (51.174,  6.386),
    "werder-bremen":            (53.067,  8.837),
    # Italy — Serie A
    "juventus":                 (45.110,  7.641),
    "inter-milan":              (45.478,  9.124),
    "ac-milan":                 (45.478,  9.124),
    "napoli":                   (40.828,  14.193),
    "roma":                     (41.934,  12.455),
    "lazio":                    (41.934,  12.455),
    "fiorentina":               (43.780,  11.283),
    "atalanta":                 (45.709,  9.680),
    # France — Ligue 1
    "paris-saint-germain":      (48.841,  2.253),
    "marseille":                (43.270,  5.396),
    "lyon":                     (45.765,  4.982),
    "monaco":                   (43.727,  7.415),
    "lille":                    (50.612,  3.131),
    "nice":                     (43.705,  7.192),
    # Netherlands — Eredivisie
    "ajax":                     (52.314,  4.942),
    "psv-eindhoven":            (51.442,  5.467),
    "feyenoord":                (51.894,  4.523),
    "az-alkmaar":               (52.628,  4.745),
    "fc-utrecht":               (52.079,  5.118),
    "sc-heerenveen":            (52.990,  5.931),
    "fc-twente":                (52.240,  6.846),
    "nec-nijmegen":             (51.840,  5.865),
    "fc-groningen":             (53.213,  6.596),
    "sparta-rotterdam":         (51.920,  4.466),
    "go-ahead-eagles":          (52.285,  6.124),
    "vitesse":                  (51.963,  5.923),
}

# ── Weather condition thresholds ──────────────────────────────────────────────

# Precipitation in mm/h
RAIN_HEAVY  = 4.0   # heavy rain
RAIN_MOD    = 1.5   # moderate rain

# Wind speed in km/h (at 10m height)
WIND_STRONG = 45.0  # strong wind
WIND_BREEZY = 25.0  # breezy

# Temperature in °C
TEMP_COLD   = 5.0   # cold conditions
TEMP_HOT    = 32.0  # heat stress

# deg_pressure adjustments
_IMPACT = {
    "heavy_rain":   +0.08,
    "moderate_rain":+0.04,
    "strong_wind":  +0.06,
    "breezy":       +0.02,
    "cold":         +0.03,
    "hot":          +0.03,
    "clear":        -0.02,  # ideal conditions: slight downward nudge on deg
}

# ── HTTP helper ───────────────────────────────────────────────────────────────

_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "ATHENA-TempoGuard/2.2"

_REQUEST_TIMEOUT = 8   # seconds
_MAX_RETRIES     = 2


def _fetch_open_meteo(lat: float, lon: float, match_date: date) -> Optional[dict]:
    """
    Fetch hourly weather forecast from Open-Meteo for a given location and date.
    Returns the raw JSON response or None on failure.
    """
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude":   lat,
        "longitude":  lon,
        "hourly":     "temperature_2m,precipitation,windspeed_10m",
        "timezone":   "UTC",
        "start_date": match_date.isoformat(),
        "end_date":   match_date.isoformat(),
        "forecast_days": 1,
    }
    for attempt in range(_MAX_RETRIES):
        try:
            resp = _SESSION.get(url, params=params, timeout=_REQUEST_TIMEOUT)
            if resp.status_code == 200:
                return resp.json()
        except requests.RequestException:
            if attempt < _MAX_RETRIES - 1:
                time.sleep(1)
    return None


# ── Core function ─────────────────────────────────────────────────────────────

def get_match_weather(
    lat: float,
    lon: float,
    match_date: date,
    hour_utc: int = 15,
) -> Optional[dict]:
    """
    Fetch weather conditions for a single fixture.

    Args:
        lat, lon:    Stadium coordinates.
        match_date:  Date of the match.
        hour_utc:    Kick-off hour in UTC (default 15 = 3pm UTC).
                     Used to index into the hourly forecast arrays.

    Returns:
        Dict with:
          weather_tag     (str):   e.g. "Heavy Rain", "Strong Wind", "Clear"
          weather_impact  (float): additive deg_pressure adjustment
          weather_details (dict):  raw values for transparency
        Or None if the API call fails.
    """
    data = _fetch_open_meteo(lat, lon, match_date)
    if not data or "hourly" not in data:
        return None

    hourly = data["hourly"]
    times  = hourly.get("time", [])

    # Find the index for the requested hour
    idx = None
    for i, t in enumerate(times):
        if f"T{hour_utc:02d}:00" in t:
            idx = i
            break
    if idx is None:
        idx = min(15, len(times) - 1)  # fallback: early afternoon

    def _safe(key: str) -> float:
        vals = hourly.get(key, [])
        return float(vals[idx]) if idx < len(vals) and vals[idx] is not None else 0.0

    temp_c   = _safe("temperature_2m")
    precip   = _safe("precipitation")
    wind_kmh = _safe("windspeed_10m")

    # Determine primary tag and impact
    tags:    list[str]  = []
    impacts: list[float] = []

    if precip >= RAIN_HEAVY:
        tags.append("Heavy Rain")
        impacts.append(_IMPACT["heavy_rain"])
    elif precip >= RAIN_MOD:
        tags.append("Rain")
        impacts.append(_IMPACT["moderate_rain"])

    if wind_kmh >= WIND_STRONG:
        tags.append("Strong Wind")
        impacts.append(_IMPACT["strong_wind"])
    elif wind_kmh >= WIND_BREEZY:
        tags.append("Breezy")
        impacts.append(_IMPACT["breezy"])

    if temp_c <= TEMP_COLD and not tags:
        tags.append("Cold")
        impacts.append(_IMPACT["cold"])
    elif temp_c >= TEMP_HOT and not tags:
        tags.append("Hot")
        impacts.append(_IMPACT["hot"])

    if not tags:
        tags.append("Clear")
        impacts.append(_IMPACT["clear"])

    weather_tag    = " / ".join(tags)
    weather_impact = round(sum(impacts), 3)

    return {
        "weather_tag":    weather_tag,
        "weather_impact": weather_impact,
        "weather_details": {
            "temp_c":    round(temp_c, 1),
            "precip_mm": round(precip, 2),
            "wind_kmh":  round(wind_kmh, 1),
            "hour_utc":  hour_utc,
        },
    }


def get_stadium_coords(team_key: str) -> Optional[tuple[float, float]]:
    """
    Look up stadium coordinates for a team key.
    Returns (lat, lon) or None if not found.
    """
    return STADIUM_COORDS.get(team_key.lower())


def match_hour_utc(match_time_str: Optional[str], default: int = 15) -> int:
    """
    Parse a match time string ("15:00", "20:45", "3:00 PM") into a UTC hour int.
    Returns the default if the string can't be parsed.
    FBref times are already in local time — for now we treat them as approximately
    UTC (acceptable for weather purposes, which only need ±2h precision).
    """
    if not match_time_str:
        return default
    for fmt in ("%H:%M", "%I:%M %p", "%H:%M:%S"):
        try:
            return datetime.strptime(match_time_str.strip(), fmt).hour
        except ValueError:
            continue
    return default


def weather_impact_for_fixtures(
    fixtures: list[dict],
    sleep_between: float = 0.3,
) -> dict[str, Optional[dict]]:
    """
    Batch-resolve weather for a list of fixture dicts.

    Each fixture dict must have:
        home_team   (str): team_key for the home team
        match_date  (date or str)
        match_time  (str, optional): kick-off time for hour resolution

    Returns:
        Dict keyed by "{home_team}|{match_date}" → weather result dict (or None).

    Uses stadium coords lookup. Fixtures with unknown home teams are skipped
    silently (returns None for those keys) — weather is opportunistic, not required.
    """
    results: dict[str, Optional[dict]] = {}

    for fix in fixtures:
        home   = fix.get("home_team", "")
        mdate  = fix.get("match_date")
        mtime  = fix.get("match_time")

        if isinstance(mdate, str):
            try:
                mdate = date.fromisoformat(mdate)
            except ValueError:
                continue
        if not mdate:
            continue

        key = f"{home}|{mdate}"
        coords = get_stadium_coords(home)

        if not coords:
            results[key] = None
            continue

        hour = match_hour_utc(mtime)
        weather = get_match_weather(coords[0], coords[1], mdate, hour_utc=hour)
        results[key] = weather

        if sleep_between > 0:
            time.sleep(sleep_between)

    return results
