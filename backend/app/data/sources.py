"""
League registry — maps ATHENA league codes to openfootball source files.

Each entry names the upstream GitHub repo and the path template for a season's
match file. `{season}` is substituted with the season key in that repo's own
convention (openfootball is not consistent between repos):

    england/deutschland/espana/italy : "2025-26/1-premierleague.txt"
    europe (france, netherlands, ...): "france/2025-26_fr1.txt"
    champions-league                 : "2025-26/cl.txt"

Season keys use the "2025-26" hyphenated form throughout ATHENA; per-repo
formatting differences are handled by the path template alone.

`full_path` points at the richer "-full" variant where one exists — those files
carry starting XIs, substitutions, cards, referees and attendance. They are not
used by the results pipeline but are what a future lineup/player layer reads.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

OPENFOOTBALL_ORG = "https://github.com/openfootball"


@dataclass(frozen=True)
class LeagueSource:
    code: str            # ATHENA league code, e.g. "ENG-PL"
    name: str            # human-readable name
    repo: str            # openfootball repo name
    path: str            # season file template, "{season}" placeholder
    full_path: Optional[str] = None   # lineup-bearing variant, when published
    calendar_year: bool = False       # True for calendar-year seasons (BRA, MLS)
    international: bool = False       # cup/international competition

    def season_path(self, season: str) -> str:
        return self.path.format(season=season)

    def season_full_path(self, season: str) -> Optional[str]:
        return self.full_path.format(season=season) if self.full_path else None

    @property
    def clone_url(self) -> str:
        return f"{OPENFOOTBALL_ORG}/{self.repo}"


# ── Registry ──────────────────────────────────────────────────────────────────
# Ordered roughly by usefulness for over/under work: the top-5 domestic leagues
# have the deepest data (including lineups), then secondary domestic, then cups.
LEAGUES: dict[str, LeagueSource] = {
    # ── Top-5 domestic (results + lineups) ────────────────────────────────
    "ENG-PL": LeagueSource(
        "ENG-PL", "English Premier League", "england",
        "{season}/1-premierleague.txt", "{season}/1-premierleague-full.txt",
    ),
    "GER-BL": LeagueSource(
        "GER-BL", "German Bundesliga", "deutschland",
        "{season}/1-bundesliga.txt", "{season}/1-bundesliga-full.txt",
    ),
    "ESP-LL": LeagueSource(
        "ESP-LL", "Spanish La Liga", "espana",
        "{season}/1-liga.txt", "{season}/1-liga-full.txt",
    ),
    "ITA-SA": LeagueSource(
        "ITA-SA", "Italian Serie A", "italy",
        "{season}/1-seriea.txt", "{season}/1-seriea-full.txt",
    ),
    "FRA-L1": LeagueSource(
        "FRA-L1", "French Ligue 1", "europe",
        "france/{season}_fr1.txt", "france/{season}_fr1-full.txt",
    ),
    # ── Secondary domestic ────────────────────────────────────────────────
    "NED-ED": LeagueSource(
        "NED-ED", "Dutch Eredivisie", "europe", "netherlands/{season}_nl1.txt",
    ),
    "POR-PL": LeagueSource(
        "POR-PL", "Portuguese Primeira Liga", "europe", "portugal/{season}_pt1.txt",
    ),
    "ENG-CH": LeagueSource(
        "ENG-CH", "English Championship", "england", "{season}/2-championship.txt",
    ),
    "GER-B2": LeagueSource(
        "GER-B2", "German 2. Bundesliga", "deutschland", "{season}/2-bundesliga2.txt",
    ),
    "ESP-L2": LeagueSource(
        "ESP-L2", "Spanish Segunda", "espana", "{season}/2-liga2.txt",
    ),
    "ITA-SB": LeagueSource(
        "ITA-SB", "Italian Serie B", "italy", "{season}/2-serieb.txt",
    ),
    "FRA-L2": LeagueSource(
        "FRA-L2", "French Ligue 2", "europe", "france/{season}_fr2.txt",
    ),
    # ── Cups / international ──────────────────────────────────────────────
    "UCL": LeagueSource(
        "UCL", "UEFA Champions League", "champions-league", "{season}/cl.txt",
        international=True,
    ),
}


def get(code: str) -> LeagueSource:
    try:
        return LEAGUES[code]
    except KeyError:
        raise KeyError(
            f"Unknown league code {code!r}. Known: {', '.join(sorted(LEAGUES))}"
        ) from None


def repos() -> set[str]:
    """Distinct upstream repos needed to cover the registry."""
    return {src.repo for src in LEAGUES.values()}


def codes() -> list[str]:
    return list(LEAGUES.keys())
