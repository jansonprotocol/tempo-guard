# backend/app/services/data_providers/api_football.py
from __future__ import annotations
import os, time, math
from typing import Optional, Dict, Any, Tuple
from datetime import date as _date

import httpx
from app.services.league_registry import get_provider_league_id

# -----------------------------
# Configuration (env-driven)
# -----------------------------
API_BASE = os.getenv("API_FOOTBALL_BASE", "https://api-football-v1.p.rapidapi.com/v3")
API_HOST = os.getenv("API_FOOTBALL_HOST", "api-football-v1.p.rapidapi.com")
API_KEY  = os.getenv("API_FOOTBALL_KEY", "")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
}

# -----------------------------
# Utilities
# -----------------------------
def _client() -> httpx.Client:
    timeout = httpx.Timeout(12.0, connect=6.0)
    limits  = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    return httpx.Client(timeout=timeout, limits=limits, headers=HEADERS)

def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    """
    GET wrapper with basic retry on rate limits/server hiccups.
    Raises RuntimeError for fatals; caller should catch and degrade.
    """
    if not API_KEY:
        raise RuntimeError("API_FOOTBALL_KEY is missing in environment variables.")
    with _client() as client:
        for attempt in range(3):
            resp = client.get(url, params=params)
            if resp.status_code == 200:
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"API error {resp.status_code}: {resp.text[:200]}")
    raise RuntimeError("API error: max retries exceeded.")

def season_from_date(d: _date) -> int:
    """EU season switch around July."""
    return d.year if d.month >= 7 else d.year - 1

def _norm(s: str) -> str:
    return (s or "").strip().lower()

# -----------------------------
# League resolution
# -----------------------------
def resolve_league_id_by_code(league_code: str) -> Optional[int]:
    """
    Primary path: resolve via our canonical→provider mapping (JSON registry).
    Returns None if unmapped, so AUTO fallback can kick in.
    """
    return get_provider_league_id(league_code, provider="api_football")

def resolve_league_id_by_search(keyword: str, season: int) -> Optional[int]:
    """
    Optional dynamic search path if you prefer not to maintain a map entry yet.
    """
    data = _get(f"{API_BASE}/leagues", {"search": keyword, "season": season})
    items = data.get("response") or []
    if not items:
        return None
    return items[0]["league"]["id"]

# -----------------------------
# Fixture & team discovery
# -----------------------------
def find_fixture_by_teams_date(league_id: Optional[int], d: _date, home_name: str, away_name: str
                               ) -> Tuple[Optional[Dict], Optional[int], Optional[int], Optional[int]]:
    """
    Finds a single fixture on the given date by fuzzy team names.
    If league_id is provided, search is constrained to that competition; if None,
    we try per-team searches and intersect.
    Returns: (fixture, league_id, home_id, away_id)
    """
    # Helper to search fixtures for one team on that date
    def _team_id_candidates(name: str) -> list[Dict]:
        res = _get(f"{API_BASE}/teams", {"search": name})
        return res.get("response") or []

    def _fixtures_for_team(team_id: int) -> list[Dict]:
        params = {"team": team_id, "date": d.isoformat()}
        if league_id:
            params["league"] = league_id
            params["season"] = season_from_date(d)
        res = _get(f"{API_BASE}/fixtures", params)
        return res.get("response") or []

    hn, an = _norm(home_name), _norm(away_name)

    # If we have a league id, try direct listing for that league/date
    if league_id:
        season = season_from_date(d)
        payload = _get(f"{API_BASE}/fixtures", {
            "league": league_id, "season": season, "date": d.isoformat()
        })
        for fx in payload.get("response") or []:
            h = _norm(fx["teams"]["home"]["name"])
            a = _norm(fx["teams"]["away"]["name"])
            if ((hn in h or h in hn) and (an in a or a in an)) or ((hn in a or a in hn) and (an in h or h in an)):
                return fx, league_id, fx["teams"]["home"]["id"], fx["teams"]["away"]["id"]
        return None, None, None, None

    # Otherwise AUTO: resolve teams, fetch each team’s fixtures for that date, intersect by opponent
    homes = _team_id_candidates(home_name)
    aways = _team_id_candidates(away_name)
    if not homes or not aways:
        return None, None, None, None

    # Try first best match for each side; you can enhance this later with alias logic
    home_id = homes[0]["team"]["id"]
    away_id = aways[0]["team"]["id"]

    home_fx = _fixtures_for_team(home_id)
    for fx in home_fx:
        try:
            opp_id = fx["teams"]["away"]["id"] if fx["teams"]["home"]["id"] == home_id else fx["teams"]["home"]["id"]
            if opp_id == away_id:
                lid = fx["league"]["id"]
                return fx, lid, fx["teams"]["home"]["id"], fx["teams"]["away"]["id"]
        except Exception:
            continue

    # if not found with first guess, fall back to scanning more candidates (simple loop)
    for hc in homes[:4]:
        hid = hc["team"]["id"]
        for ac in aways[:4]:
            aid = ac["team"]["id"]
            for fx in _fixtures_for_team(hid):
                try:
                    opp = fx["teams"]["away"]["id"] if fx["teams"]["home"]["id"] == hid else fx["teams"]["home"]["id"]
                    if opp == aid:
                        lid = fx["league"]["id"]
                        return fx, lid, fx["teams"]["home"]["id"], fx["teams"]["away"]["id"]
                except Exception:
                    continue

    return None, None, None, None

# -----------------------------
# Team statistics to ATHENA metrics
# -----------------------------
def _poisson_p0(mu: float) -> float:
    mu = max(0.001, float(mu))
    return math.exp(-mu)

def derive_probabilities(stats_home: Dict, stats_away: Dict) -> Dict[str, float]:
    """
    Derive minimal inputs ATHENA needs from team stats.
    - p_two_plus  : probability of 2+ total goals (Poisson from combined means)
    - p_home_tt05 : P(home scores >= 1)
    - p_away_tt05 : P(away scores >= 1)
    - tempo_index : quick proxy from average goals & shots
    - sot_proj_total: conservative projection
    - support_idx_over_delta: slight tilt towards Over when warranted
    """
    # Goals per game (avg) context:
    g_home = float(((stats_home.get("goals") or {}).get("for") or {}).get("average", {}).get("home") or 1.1)
    g_away = float(((stats_away.get("goals") or {}).get("for") or {}).get("average", {}).get("away") or 0.9)

    mu_total = max(0.2, g_home + g_away)
    p0 = math.exp(-mu_total)
    p1 = mu_total * math.exp(-mu_total)
    p_two_plus = max(0.05, min(0.98, 1.0 - (p0 + p1)))

    p_home_tt05 = 1.0 - _poisson_p0(g_home)
    p_away_tt05 = 1.0 - _poisson_p0(g_away)

    tempo_index = max(0.2, min(0.9, (mu_total / 3.0)))
    sot_proj_total = max(6.0, min(16.0, mu_total * 3.2))

    support_idx_over_delta = max(-0.10, min(0.15,
        (mu_total - 2.4) * 0.06 + ((p_home_tt05 + p_away_tt05) - 1.2) * 0.05
    ))

    return {
        "p_two_plus": round(p_two_plus, 3),
        "p_home_tt05": round(p_home_tt05, 3),
        "p_away_tt05": round(p_away_tt05, 3),
        "tempo_index": round(tempo_index, 3),
        "sot_proj_total": round(sot_proj_total, 2),
        "support_idx_over_delta": round(support_idx_over_delta, 3),
    }

def team_statistics(team_id: int, league_id: int, d: _date) -> Optional[Dict[str, Any]]:
    season = season_from_date(d)
    data = _get(f"{API_BASE}/teams/statistics", {
        "team": team_id, "league": league_id, "season": season
    })
    return data.get("response")

# -----------------------------
# Public entry: Enrich a request for ATHENA
# -----------------------------
def enrich_from_live_data(league_code: str, home: str, away: str, match_date: _date,
                          league_name_for_search: Optional[str] = None) -> Dict[str, float]:
    """
    Returns a dict with keys:
      p_two_plus, p_home_tt05, p_away_tt05, tempo_index, sot_proj_total, support_idx_over_delta.
    Raises only for fatal config; otherwise returns {} so engine uses safe defaults.
    """
    try:
        season = season_from_date(match_date)

        # 1) Try mapped league id first
        league_id = resolve_league_id_by_code(league_code)

        # 2) If unmapped and a search hint is provided, try dynamic lookup by league name
        if league_id is None and league_name_for_search:
            league_id = resolve_league_id_by_search(league_name_for_search, season)

        # 3) Find a concrete fixture (returns its real league if AUTO path is used)
        fixture, lid, home_id, away_id = find_fixture_by_teams_date(league_id, match_date, home, away)
        if not fixture or not lid or not home_id or not away_id:
            return {}  # degrade safely

        # 4) Pull team stats for that league/season
        st_home = team_statistics(home_id, lid, match_date)
        st_away = team_statistics(away_id, lid, match_date)
        if not st_home or not st_away:
            return {}

        # 5) Derive minimal ATHENA inputs
        return derive_probabilities(st_home, st_away)

    except Exception as e:
        print(f"[api_football] enrich error: {e}")
        return {}
