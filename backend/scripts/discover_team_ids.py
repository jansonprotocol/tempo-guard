"""
backend/scripts/discover_team_ids.py

Auto-discovers FBref team IDs by scraping league fixture pages.

FBref fixture pages contain links to squad pages in the Home/Away columns:
  <a href="/en/squads/18bb7c10/2025-2026/Arsenal">Arsenal</a>
This script extracts those hex IDs and matches them to teams.json entries.

Usage:
    cd backend
    venv312\\Scripts\\activate
    python -m scripts.discover_team_ids                 # all leagues
    python -m scripts.discover_team_ids --league ENG-PL # single league
    python -m scripts.discover_team_ids --dry-run       # preview only

Output:
    Updates backend/app/data/teams.json with fbref_team_id per team.
    Teams that couldn't be matched are printed for manual review.

NOTE: Uses same Selenium/ScraperAPI infrastructure as scrape_fixtures.py.
      Chrome opens once per league. ~6 minutes for all leagues.
"""
from __future__ import annotations

import json
import os
import re
import sys
import time
import unicodedata
from difflib import get_close_matches
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import requests
from seleniumbase import Driver

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

SLEEP_BETWEEN = 4
HEADLESS = False
SCRAPER_API_KEY: str | None = os.environ.get("SCRAPER_API_KEY")

# ── URL sources ──────────────────────────────────────────────────────────────
# Uses the SAME fixture URL map as scrape_fixtures.py.
# Only domestic leagues — intl comps (UCL/UEL/UECL) don't have team squads.
LEAGUE_MAP = {
    "ENG-PL":  "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ENG-CH":  "https://fbref.com/en/comps/10/schedule/Championship-Scores-and-Fixtures",
    "ESP-LL":  "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "FRA-L1":  "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
    "GER-BUN": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "ITA-SA":  "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "NED-ERE": "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
    "TUR-SL":  "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
    "BRA-SA":  "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
    "MLS":     "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
    "SAU-SPL": "https://fbref.com/en/comps/70/schedule/Saudi-Pro-League-Scores-and-Fixtures",
    "DEN-SL":  "https://fbref.com/en/comps/50/schedule/Danish-Superliga-Scores-and-Fixtures",
    "ESP-LL2": "https://fbref.com/en/comps/17/schedule/Segunda-Division-Scores-and-Fixtures",
    "BEL-PL":  "https://fbref.com/en/comps/37/schedule/Belgian-Pro-League-Scores-and-Fixtures",
    "NOR-EL":  "https://fbref.com/en/comps/28/schedule/Eliteserien-Scores-and-Fixtures",
    "SWE-AL":  "https://fbref.com/en/comps/29/schedule/Allsvenskan-Scores-and-Fixtures",
    "MEX-LMX": "https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures",
    "CHN-CSL": "https://fbref.com/en/comps/62/schedule/Chinese-Super-League-Scores-and-Fixtures",
    "JPN-J1":  "https://fbref.com/en/comps/25/schedule/J1-League-Scores-and-Fixtures",
    "COL-PA":  "https://fbref.com/en/comps/41/schedule/Primera-A-Scores-and-Fixtures",
    "BRA-SB":  "https://fbref.com/en/comps/38/schedule/Serie-B-Scores-and-Fixtures",
    "ITA-SB":  "https://fbref.com/en/comps/18/schedule/Serie-B-Scores-and-Fixtures",
    "FRA-L2":  "https://fbref.com/en/comps/60/schedule/Ligue-2-Scores-and-Fixtures",
    "GER-B2":  "https://fbref.com/en/comps/33/schedule/2-Bundesliga-Scores-and-Fixtures",
    "POL-EK":  "https://fbref.com/en/comps/36/schedule/Ekstraklasa-Scores-and-Fixtures",
    "AUT-BL":  "https://fbref.com/en/comps/56/schedule/Austrian-Football-Bundesliga-Scores-and-Fixtures",
    "SUI-SL":  "https://fbref.com/en/comps/57/schedule/Swiss-Super-League-Scores-and-Fixtures",
    "CHI-LP":  "https://fbref.com/en/comps/35/schedule/Primera-Division-Scores-and-Fixtures",
    "PER-L1":  "https://fbref.com/en/comps/44/schedule/Liga-1-Scores-and-Fixtures",
    "POR-LP":  "https://fbref.com/en/comps/32/schedule/Primeira-Liga-Scores-and-Fixtures",
}


# ── HTML fetch (same as scrape_fixtures.py) ──────────────────────────────────

def _fetch_page(url: str, label: str) -> str | None:
    if SCRAPER_API_KEY:
        return _fetch_via_scraperapi(url, label)
    return _fetch_via_selenium(url, label)


def _fetch_via_scraperapi(url: str, label: str) -> str | None:
    print(f"  [ScraperAPI] {label}")
    try:
        resp = requests.get(
            "http://api.scraperapi.com",
            params={"api_key": SCRAPER_API_KEY, "url": url, "render": "true", "premium": "true"},
            timeout=60,
        )
        if resp.status_code != 200:
            print(f"  ScraperAPI error: HTTP {resp.status_code}")
            return None
        print(f"  Page loaded ({len(resp.text)} bytes)")
        return resp.text
    except Exception as e:
        print(f"  ScraperAPI error: {e}")
        return None


def _fetch_via_selenium(url: str, label: str) -> str | None:
    driver = None
    try:
        driver = Driver(uc=True, headless2=HEADLESS)
        driver.uc_open_with_reconnect(url, 4)
        if not HEADLESS:
            driver.uc_gui_click_captcha()
        time.sleep(3)
        html = driver.get_page_source()
        print(f"  Page loaded ({len(html)} bytes)")
        return html
    except Exception as e:
        print(f"  Browser error ({label}): {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass


# ── Team ID extraction ───────────────────────────────────────────────────────

def _strip_accents(s: str) -> str:
    return "".join(
        c for c in unicodedata.normalize("NFD", s)
        if unicodedata.category(c) != "Mn"
    )


def _norm(s: str) -> str:
    return _strip_accents(s.strip().lower())


def extract_team_ids(html: str) -> dict[str, str]:
    """
    Parse FBref fixture page HTML and extract team IDs from squad links.

    FBref links: <a href="/en/squads/18bb7c10/2025-2026/Arsenal">Arsenal</a>
    Returns: {"Arsenal": "18bb7c10", "Liverpool": "822bd0ba", ...}
    """
    # Pattern matches: /en/squads/{8-char hex}/... with the link text as team name
    pattern = re.compile(
        r'href="/en/squads/([a-f0-9]{8})/[^"]*"[^>]*>([^<]+)</a>',
        re.IGNORECASE,
    )

    teams: dict[str, str] = {}
    for match in pattern.finditer(html):
        fbref_id = match.group(1)
        name = match.group(2).strip()
        if name and fbref_id and name not in teams:
            teams[name] = fbref_id

    return teams


def match_to_json_entry(
    fbref_name: str,
    fbref_id: str,
    json_teams: list[dict],
    league_code: str,
) -> dict | None:
    """
    Match a discovered FBref team name to an existing teams.json entry.
    Uses exact match → accent-stripped match → fuzzy match (cutoff 0.80).
    """
    # Filter to teams in this league (or unassigned)
    candidates = [
        t for t in json_teams
        if t["league_code"] == league_code or t.get("league_code") == ""
    ]

    # Build name sets for matching
    for t in candidates:
        all_names = [t["display_name"]] + t.get("aliases", [])
        for name in all_names:
            if _norm(name) == _norm(fbref_name):
                return t

    # Fuzzy fallback
    all_candidate_names = {}
    for t in candidates:
        for name in [t["display_name"]] + t.get("aliases", []):
            all_candidate_names[_norm(name)] = t

    close = get_close_matches(_norm(fbref_name), list(all_candidate_names.keys()), n=1, cutoff=0.80)
    if close:
        return all_candidate_names[close[0]]

    return None


# ── Main ─────────────────────────────────────────────────────────────────────

def discover_league(
    league_code: str,
    url: str,
    json_teams: list[dict],
    dry_run: bool = False,
) -> tuple[int, int]:
    """
    Discover team IDs for one league.
    Returns (matched_count, unmatched_count).
    """
    print(f"\n{'='*60}")
    print(f"[discover] {league_code}")

    html = _fetch_page(url, league_code)
    if not html:
        print(f"  Failed to fetch page")
        return 0, 0

    if "Just a moment" in html or len(html) < 5000:
        print(f"  Cloudflare blocked — try again or use --api")
        return 0, 0

    discovered = extract_team_ids(html)
    print(f"  Found {len(discovered)} team IDs on page")

    matched = 0
    unmatched = 0

    for fbref_name, fbref_id in sorted(discovered.items()):
        entry = match_to_json_entry(fbref_name, fbref_id, json_teams, league_code)
        if entry:
            old_id = entry.get("fbref_team_id")
            if old_id and old_id != fbref_id:
                print(f"  ⚠ {entry['display_name']}: ID changed {old_id} → {fbref_id}")
            if not dry_run:
                entry["fbref_team_id"] = fbref_id
            print(f"  ✓ {fbref_name} → {entry['display_name']} ({fbref_id})")
            matched += 1
        else:
            print(f"  ✗ {fbref_name} ({fbref_id}) — no match in teams.json")
            unmatched += 1

    return matched, unmatched


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Discover FBref team IDs")
    parser.add_argument("--league", type=str, default=None, help="Single league only")
    parser.add_argument("--headless", action="store_true", help="Headless Chrome")
    parser.add_argument("--api", type=str, default=None, metavar="KEY", help="ScraperAPI key")
    parser.add_argument("--dry-run", action="store_true", help="Preview matches only, don't write")
    args = parser.parse_args()

    if args.headless:
        HEADLESS = True
    if args.api:
        SCRAPER_API_KEY = args.api

    # Load teams.json
    teams_path = Path(__file__).resolve().parents[1] / "app" / "data" / "teams.json"
    if not teams_path.exists():
        print(f"[discover] teams.json not found at {teams_path}")
        sys.exit(1)

    with open(teams_path) as f:
        teams_data = json.load(f)

    # Ensure every entry has the fbref_team_id field
    for t in teams_data:
        t.setdefault("fbref_team_id", None)

    total_matched = 0
    total_unmatched = 0

    if args.league:
        if args.league not in LEAGUE_MAP:
            print(f"[discover] Unknown league: {args.league}")
            print(f"  Available: {list(LEAGUE_MAP.keys())}")
            sys.exit(1)
        m, u = discover_league(args.league, LEAGUE_MAP[args.league], teams_data, args.dry_run)
        total_matched += m
        total_unmatched += u
    else:
        codes = list(LEAGUE_MAP.keys())
        for i, (code, url) in enumerate(LEAGUE_MAP.items()):
            m, u = discover_league(code, url, teams_data, args.dry_run)
            total_matched += m
            total_unmatched += u
            if i < len(codes) - 1:
                print(f"\n  Waiting {SLEEP_BETWEEN}s...")
                time.sleep(SLEEP_BETWEEN)

    print(f"\n{'='*60}")
    print(f"[discover] Done: {total_matched} matched, {total_unmatched} unmatched")

    # Report teams still missing an ID
    missing = [t for t in teams_data if not t.get("fbref_team_id") and t["league_code"] != "NATIONAL"]
    if missing:
        print(f"\n[discover] {len(missing)} teams still without fbref_team_id:")
        for t in missing:
            print(f"  {t['league_code']:12s} {t['display_name']}")

    # Write updated teams.json
    if not args.dry_run:
        with open(teams_path, "w") as f:
            json.dump(teams_data, f, indent=2, ensure_ascii=False)
        print(f"\n[discover] Updated {teams_path}")
    else:
        print(f"\n[discover] Dry run — no files written")
