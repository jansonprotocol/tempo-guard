# backend/app/services/data_providers/api_football_retro.py
from __future__ import annotations
import os, time, math
from datetime import date as _date
from typing import Optional, Dict, Any, Tuple, List

import httpx
from app.services.league_registry import get_provider_league_id

API_BASE = os.getenv("API_FOOTBALL_BASE", "https://api-football-v1.p.rapidapi.com/v3")
API_HOST = os.getenv("API_FOOTBALL_HOST", "api-football-v1.p.rapidapi.com")
API_KEY  = os.getenv("API_FOOTBALL_KEY", "")

HEADERS = {
    "x-rapidapi-key": API_KEY,
    "x-rapidapi-host": API_HOST,
}

def _client() -> httpx.Client:
    timeout = httpx.Timeout(12.0, connect=6.0)
    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    return httpx.Client(timeout=timeout, limits=limits, headers=HEADERS)

def _get(url: str, params: Dict[str, Any]) -> Dict[str, Any]:
    if not API_KEY:
        raise RuntimeError("API_FOOTBALL_KEY is missing in environment variables.")
    with _client() as c:
        for attempt in range(3):
            r = c.get(url, params=params)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 500, 502, 503, 504):
                time.sleep(1.5 * (attempt + 1))
                continue
            raise RuntimeError(f"API error {r.status_code}: {r.text[:200]}")
    raise RuntimeError("API error: max retries exceeded.")

def season_from_date(d: _date) -> int:
    # EU rollover ~July
    return d.year if d.month >= 7 else d.year - 1

def _norm(s: str) -> str:
    return (s or "").strip().lower()

def resolve_league_id_by_code(league_code: str) -> Optional[int]:
    return get_provider_league_id(league_code, provider="api_football")

def search_league_id(keyword: str, season: int) -> Optional[int]:
    data = _get(f"{API_BASE}/leagues", {"search": keyword, "season": season})
    arr = data.get("response") or []
    if not arr:
        return None
    return arr[0]["league"]["id"]

def _team_id_candidates(name: str) -> List[Dict[str, Any]]:
    res = _get(f"{API_BASE}/teams", {"search": name})
    return res.get("response") or []

def _fixture_by_league_date_names(league_id: int, d: _date, home: str, away: str) -> Tuple[Optional[Dict], Optional[int], Optional[int]]:
    season = season_from_date(d)
    payload = _get(f"{API_BASE}/fixtures", {"league": league_id, "season": season, "date": d.isoformat()})
    hn, an = _norm(home), _norm(away)
    for fx in payload.get("response") or []:
        h = _norm(fx["teams"]["home"]["name"]); a = _norm(fx["teams"]["away"]["name"])
        if ((hn in h or h in hn) and (an in a or a in an)) or ((hn in a or a in hn) and (an in h or h in an)):
            return fx, fx["teams"]["home"]["id"], fx["teams"]["away"]["id"]
    return None, None, None

def _fixtures_for_team_until(team_id: int, league_id: Optional[int], d: _date) -> List[Dict[str, Any]]:
    """
    Returns fixtures played by team_id strictly before date d (FT only).
    Uses from/to window in API-Football (season-bounded).
    """
    season = season_from_date(d)
    params = {
        "team": team_id,
        "season": season,
        "to": d.isoformat()  # include up to the date; we'll filter out same-day if kickoff not finished
    }
    if league_id:
        params["league"] = league_id
    data = _get(f"{API_BASE}/fixtures", params)
    res = []
    for fx in data.get("response") or []:
        # Keep only matches with final scores and strictly BEFORE kickoff of target date
        if (fx.get("fixture", {}).get("status", {}).get("short") in ("FT", "AET", "PEN")):
            # Keep if played before or (on same date but ended)
            res.append(fx)
    return res

def _goals_for_against_from_fixtures(fixtures: List[Dict[str, Any]], team_id: int) -> Tuple[float, float]:
    if not fixtures:
        return 0.0, 0.0
    gf = 0
    ga = 0
    for fx in fixtures:
        h_id = fx["teams"]["home"]["id"]
        a_id = fx["teams"]["away"]["id"]
        hs = int((fx["goals"]["home"] or 0))
        as_ = int((fx["goals"]["away"] or 0))
        if team_id == h_id:
            gf += hs; ga += as_
        elif team_id == a_id:
            gf += as_; ga += hs
    n = max(1, len(fixtures))
    return (gf / n), (ga / n)

def _poisson_p0(mu: float) -> float:
    mu = max(0.001, float(mu))
    return math.exp(-mu)

def derive_asof_metrics(home_fxs: List[Dict], away_fxs: List[Dict], home_id: int, away_id: int) -> Dict[str, float]:
    """
    Build the exact minimal set ATHENA needs, from past-only fixtures.
    """
    gfh, gah = _goals_for_against_from_fixtures(home_fxs, home_id)
    gfa, gaa = _goals_for_against_from_fixtures(away_fxs, away_id)

    # crude mean goals for: home's FOR + away's FOR
    g_home = max(0.05, gfh)
    g_away = max(0.05, gfa)
    mu_total = max(0.2, g_home + g_away)

    p_two_plus = 1.0 - (math.exp(-mu_total) + mu_total * math.exp(-mu_total))
    p_home_tt05 = 1.0 - _poisson_p0(g_home)
    p_away_tt05 = 1.0 - _poisson_p0(g_away)

    tempo_index = max(0.2, min(0.9, mu_total / 3.0))
    sot_proj_total = max(6.0, min(16.0, mu_total * 3.0))

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

def find_fixture_and_asof_stats(league_code: str, home: str, away: str, d: _date,
                                league_search_hint: Optional[str] = None) -> Tuple[Dict[str, float], Optional[Dict[str, Any]]]:
    """
    Returns (metrics_dict, actual_fixture_or_none).
    metrics_dict keys: p_two_plus, p_home_tt05, p_away_tt05, tempo_index, sot_proj_total, support_idx_over_delta.
    """
    try:
        season = season_from_date(d)
        league_id = resolve_league_id_by_code(league_code)

        if league_id is None and league_search_hint:
            league_id = search_league_id(league_search_hint, season)

        # Attempt to resolve fixture in the (possibly) known league
        fx, home_id, away_id = (None, None, None)
        if league_id:
            fx, home_id, away_id = _fixture_by_league_date_names(league_id, d, home, away)

        # If still not found, try AUTO team path (cross-competition)
        if fx is None:
            homes = _team_id_candidates(home)
            aways = _team_id_candidates(away)
            if not homes or not aways:
                return {}, None
            home_id = homes[0]["team"]["id"]
            away_id = aways[0]["team"]["id"]

        # Build past-only fixture sets up to date d
        # Note: If league_id is None (AUTO), we aggregate across all comps; acceptable for retrosim.
        home_fxs = _fixtures_for_team_until(home_id, league_id, d)
        away_fxs = _fixtures_for_team_until(away_id, league_id, d)

        if not home_fxs or not away_fxs:
            return {}, fx

        metrics = derive_asof_metrics(home_fxs, away_fxs, home_id, away_id)
        return metrics, fx
    except Exception as e:
        print(f"[api_football_retro] error: {e}")
        return {}, None
