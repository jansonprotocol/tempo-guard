# backend/app/services/data_providers/fbref_urls.py
"""
FBref URL utilities for ATHENA v2.0.

Derives league stats page URLs from the existing schedule URLs in
scrape_fbref.py and scrape_fixtures.py — no hardcoded URL duplication.

FBref URL anatomy:
  Schedule:  /en/comps/{comp_id}/schedule/{Slug}-Scores-and-Fixtures
  Stats:     /en/comps/{comp_id}/stats/{Slug}-Stats
  Shooting:  /en/comps/{comp_id}/shooting/{Slug}-Stats
  Passing:   /en/comps/{comp_id}/passing/{Slug}-Stats
  Defense:   /en/comps/{comp_id}/defense/{Slug}-Stats
  GCA:       /en/comps/{comp_id}/gca/{Slug}-Stats
  Keepers:   /en/comps/{comp_id}/keepers/{Slug}-Stats
  Squad:     /en/squads/{team_id}/{season}/{Slug}-Stats
"""
from __future__ import annotations

import re
from typing import Optional, Tuple

FBREF_BASE = "https://fbref.com"

# Stat categories needed for the player intelligence layer.
# Each maps to a sub-path under /en/comps/{comp_id}/{category}/
STAT_CATEGORIES = ["stats", "gca", "passing", "defense", "keepers", "shooting"]
# "shooting" adds xG, xGOT per player — strongest predictor of true scoring ability
# vs shots-on-target which is a weaker proxy


def extract_comp_info(schedule_url: str) -> Optional[Tuple[str, str]]:
    """
    Extract (comp_id, league_slug) from a schedule URL.

    Handles both current and historical season URL formats:
      .../comps/9/schedule/Premier-League-Scores-and-Fixtures
      .../comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures
    """
    m = re.search(
        r"/comps/(\d+)/(?:\d{4}(?:-\d{4})?/)?schedule/"
        r"(?:\d{4}(?:-\d{4})?-)?(.+)-Scores-and-Fixtures",
        schedule_url,
    )
    if not m:
        return None
    return m.group(1), m.group(2)


def league_stats_url(
    comp_id: str,
    slug: str,
    category: str = "stats",
    season: Optional[str] = None,
) -> str:
    """
    Build a league-level stats page URL.
    When season is provided (e.g. "2025-2026"), builds a year-specific URL
    which bypasses Cloudflare bot detection on some leagues (ENG-PL, ENG-CH).
    Year-specific format: /en/comps/{id}/{season}/{season}-{Slug}-Stats
    """
    if season:
        # Year-specific format includes category in path:
        # /en/comps/{id}/{season}/{category}/{season}-{Slug}-Stats
        return f"{FBREF_BASE}/en/comps/{comp_id}/{season}/{category}/{season}-{slug}-Stats"
    return f"{FBREF_BASE}/en/comps/{comp_id}/{category}/{slug}-Stats"


def squad_page_url(team_fbref_id: str, season: str, team_slug: str) -> str:
    """
    Build a per-team squad page URL (for future use / manual lookup).

    season: "2025-2026" or "2026" depending on league calendar.
    team_slug: URL-friendly team name, e.g. "Arsenal", "Bayern-Munich"
    """
    return f"{FBREF_BASE}/en/squads/{team_fbref_id}/{season}/{team_slug}-Stats"


def build_league_stat_urls(schedule_url: str) -> dict[str, str]:
    """
    From a single schedule URL, derive all 5 stat category page URLs.

    Returns dict: { "stats": url, "gca": url, "passing": url, ... }
    Returns empty dict if the schedule URL can't be parsed.
    """
    info = extract_comp_info(schedule_url)
    if not info:
        return {}

    comp_id, slug = info
    return {cat: league_stats_url(comp_id, slug, cat) for cat in STAT_CATEGORIES}
