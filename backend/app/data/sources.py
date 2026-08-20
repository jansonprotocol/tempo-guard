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
    repo: str            # openfootball repo name ("" for other providers)
    path: str            # season file template, "{season}" placeholder
    full_path: Optional[str] = None   # lineup-bearing variant, when published
    calendar_year: bool = False       # True for calendar-year seasons (BRA, MLS)
    international: bool = False       # cup/international competition

    # ── Provider ──────────────────────────────────────────────────────
    # "openfootball"  git repos: deep history, broad coverage, but the
    #                 auto-update runs weekly and lags on live seasons.
    # "footballdata"  football-data.co.uk: same-day results and measured
    #                 shot counts. Only football columns are ingested —
    #                 see app.data.footballdata for why odds are excluded.
    provider: str = "openfootball"
    fd_div: Optional[str] = None       # main-league division code, e.g. "E0"
    fd_country: Optional[str] = None   # extra-league country code, e.g. "CHN"
    fd_league: Optional[str] = None    # competition within an extra file, since
                                       # some countries publish two in one file

    def season_path(self, season: str) -> str:
        return self.path.format(season=season)

    def default_seasons(self) -> list[str]:
        """
        Season keys to try for this league, newest last.

        Season naming is not universal. Leagues played across a European winter
        are keyed "2025-26"; leagues played inside a single calendar year —
        Brazil, MLS, Japan, the Nordics — are keyed "2025". Getting this wrong
        simply finds no file, so each league states which convention it uses.
        """
        if self.calendar_year:
            return ["2024", "2025", "2026"]
        return ["2024-25", "2025-26", "2026-27"]

    def all_seasons(self, since: int = 2000, until: int = 2026) -> list[str]:
        """
        Every season key this league could have, oldest first.

        openfootball carries deep history — 27 seasons of England, 17 of
        Germany, 9 of Brazil. That depth matters: resolving a 2% edge needs
        thousands of matches per league, far more than one season provides.
        Keys that have no file upstream are skipped by the loader.
        """
        if self.calendar_year:
            return [str(y) for y in range(since, until + 1)]
        return [f"{y}-{str(y + 1)[2:]}" for y in range(since, until + 1)]

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
        fd_div="E0"
    ),
    "GER-BL": LeagueSource(
        "GER-BL", "German Bundesliga", "deutschland",
        "{season}/1-bundesliga.txt", "{season}/1-bundesliga-full.txt",
        fd_div="D1"
    ),
    "ESP-LL": LeagueSource(
        "ESP-LL", "Spanish La Liga", "espana",
        "{season}/1-liga.txt", "{season}/1-liga-full.txt",
        fd_div="SP1"
    ),
    "ITA-SA": LeagueSource(
        "ITA-SA", "Italian Serie A", "italy",
        "{season}/1-seriea.txt", "{season}/1-seriea-full.txt",
        fd_div="I1"
    ),
    "FRA-L1": LeagueSource(
        "FRA-L1", "French Ligue 1", "europe",
        "france/{season}_fr1.txt", "france/{season}_fr1-full.txt",
        fd_div="F1"
    ),
    # ── Secondary domestic ────────────────────────────────────────────────
    "NED-ED": LeagueSource(
        "NED-ED", "Dutch Eredivisie", "europe", "netherlands/{season}_nl1.txt",
        fd_div="N1"
    ),
    "POR-PL": LeagueSource(
        "POR-PL", "Portuguese Primeira Liga", "europe", "portugal/{season}_pt1.txt",
        fd_div="P1"
    ),
    "ENG-CH": LeagueSource(
        "ENG-CH", "English Championship", "england", "{season}/2-championship.txt",
        fd_div="E1"
    ),
    "GER-B2": LeagueSource(
        "GER-B2", "German 2. Bundesliga", "deutschland", "{season}/2-bundesliga2.txt",
        fd_div="D2"
    ),
    "ESP-L2": LeagueSource(
        "ESP-L2", "Spanish Segunda", "espana", "{season}/2-liga2.txt",
        fd_div="SP2"
    ),
    "ITA-SB": LeagueSource(
        "ITA-SB", "Italian Serie B", "italy", "{season}/2-serieb.txt",
        fd_div="I2"
    ),
    "FRA-L2": LeagueSource(
        "FRA-L2", "French Ligue 2", "europe", "france/{season}_fr2.txt",
        fd_div="F2"
    ),
    # ── Rest of Europe ────────────────────────────────────────────────────
    "SCO-PL": LeagueSource(
        "SCO-PL", "Scottish Premiership", "europe", "scotland/{season}_sc1.txt",
        fd_div="SC0"
    ),
    "TUR-SL": LeagueSource(
        "TUR-SL", "Turkish Süper Lig", "europe", "turkey/{season}_tr1.txt",
        fd_div="T1"
    ),
    "GRE-SL": LeagueSource(
        "GRE-SL", "Greek Super League", "europe", "greece/{season}_gr1.txt",
        fd_div="G1"
    ),
    "BEL-PL": LeagueSource(
        "BEL-PL", "Belgian Pro League", "europe", "belgium/{season}_be1.txt",
        fd_div="B1"
    ),
    "AUT-BL": LeagueSource(
        "AUT-BL", "Austrian Bundesliga", "europe", "austria/{season}_at1.txt",
        fd_country="AUT", fd_league="Bundesliga"
    ),
    "SUI-SL": LeagueSource(
        "SUI-SL", "Swiss Super League", "europe", "switzerland/{season}_ch1.txt",
        fd_country="SWZ", fd_league="Super League"
    ),
    "DEN-SL": LeagueSource(
        "DEN-SL", "Danish Superliga", "europe", "denmark/{season}_dk1.txt",
        fd_country="DNK", fd_league="Superliga"
    ),
    "POL-EK": LeagueSource(
        "POL-EK", "Polish Ekstraklasa", "europe", "poland/{season}_pl1.txt",
        fd_country="POL", fd_league="Ekstraklasa"
    ),
    "CZE-FL": LeagueSource(
        "CZE-FL", "Czech First League", "europe", "czech-republic/{season}_cz1.txt",
    ),
    "RUS-PL": LeagueSource(
        "RUS-PL", "Russian Premier League", "europe", "russia/{season}_ru1.txt",
        fd_country="RUS", fd_league="Premier League"
    ),
    "UKR-PL": LeagueSource(
        "UKR-PL", "Ukrainian Premier League", "europe", "ukraine/{season}_ua1.txt",
    ),
    "CRO-1L": LeagueSource(
        "CRO-1L", "Croatian HNL", "europe", "croatia/{season}_hr1.txt",
    ),
    "ROU-L1": LeagueSource(
        "ROU-L1", "Romanian Liga I", "europe", "romania/{season}_ro1.txt",
        fd_country="ROU", fd_league="Superliga"
    ),
    # Nordic leagues run inside a calendar year.
    "NOR-EL": LeagueSource(
        "NOR-EL", "Norwegian Eliteserien", "europe", "norway/{season}_no1.txt",
        calendar_year=True,
        fd_country="NOR", fd_league="Eliteserien"
    ),
    "SWE-AL": LeagueSource(
        "SWE-AL", "Swedish Allsvenskan", "europe", "sweden/{season}_se1.txt",
        calendar_year=True,
        fd_country="SWE", fd_league="Allsvenskan"
    ),

    # ── South America (calendar-year seasons) ─────────────────────────────
    "BRA-SA": LeagueSource(
        "BRA-SA", "Brazilian Série A", "south-america", "brazil/{season}_br1.txt",
        calendar_year=True,
        fd_country="BRA", fd_league="Serie A"
    ),
    "BRA-SB": LeagueSource(
        "BRA-SB", "Brazilian Série B", "south-america", "brazil/{season}_br2.txt",
        calendar_year=True,
    ),
    "ARG-PD": LeagueSource(
        "ARG-PD", "Argentine Primera División", "south-america",
        "argentina/{season}_ar1.txt", calendar_year=True,
        fd_country="ARG", fd_league="Liga Profesional"
    ),
    "COL-PA": LeagueSource(
        "COL-PA", "Colombian Primera A", "south-america",
        "colombia/{season}_co1.txt", calendar_year=True,
    ),
    "ECU-S1": LeagueSource(
        "ECU-S1", "Ecuadorian Serie A", "south-america",
        "ecuador/{season}_ec1.txt", calendar_year=True,
    ),
    "PAR-D1": LeagueSource(
        "PAR-D1", "Paraguayan División Profesional", "south-america",
        "paraguay/{season}_py1.txt", calendar_year=True,
    ),

    # ── North America ─────────────────────────────────────────────────────
    "MLS": LeagueSource(
        "MLS", "Major League Soccer", "world",
        "north-america/major-league-soccer/{season}_mls.txt", calendar_year=True,
        fd_country="USA", fd_league="MLS"
    ),
    "MEX-LMX": LeagueSource(
        "MEX-LMX", "Liga MX", "world", "north-america/mexico/{season}_mx1.txt",
        fd_country="MEX", fd_league="Liga MX"
    ),

    # ── Asia (calendar-year seasons) ──────────────────────────────────────
    "JPN-J1": LeagueSource(
        "JPN-J1", "Japanese J1 League", "world", "asia/japan/{season}_jp1.txt",
        calendar_year=True,
        fd_country="JPN", fd_league="J1 League"
    ),
    # Sourced from football-data.co.uk rather than openfootball: the latter's
    # Asia coverage stops at 2025, so the current season was entirely missing.
    "CHN-SL": LeagueSource(
        "CHN-SL", "Chinese Super League", "", "",
        calendar_year=True, provider="footballdata", fd_country="CHN",
    ),

    # ── Africa ────────────────────────────────────────────────────────────
    "EGY-PL": LeagueSource(
        "EGY-PL", "Egyptian Premier League", "world", "africa/egypt/{season}_eg1.txt",
    ),
    "MAR-BP": LeagueSource(
        "MAR-BP", "Moroccan Botola Pro", "world", "africa/morocco/{season}_ma1.txt",
    ),
    "ALG-L1": LeagueSource(
        "ALG-L1", "Algerian Ligue 1", "world", "africa/algeria/{season}_dz1.txt",
    ),
    "NGA-PL": LeagueSource(
        "NGA-PL", "Nigerian Professional League", "world",
        "africa/nigeria/{season}_ng1.txt",
    ),
    "RSA-PL": LeagueSource(
        "RSA-PL", "South African Premiership", "world",
        "africa/south-africa/{season}_za1.txt",
    ),

    # ── Cups / international ──────────────────────────────────────────────
    # All three UEFA club competitions live in the champions-league repo, one
    # file per competition per season: cl/el/conf for the main draws and
    # clq/elq/confq for qualifying. Coverage differs — the Champions League goes
    # back to 2011-12, the Europa League to 2020-21, the Conference League to
    # 2021-22 (it did not exist before). Seasons a repo has not published are
    # skipped by the loader rather than erroring.
    "UCL": LeagueSource(
        "UCL", "UEFA Champions League", "champions-league", "{season}/cl.txt",
        international=True,
    ),
    "UEL": LeagueSource(
        "UEL", "UEFA Europa League", "champions-league", "{season}/el.txt",
        international=True,
    ),
    "UECL": LeagueSource(
        "UECL", "UEFA Conference League", "champions-league", "{season}/conf.txt",
        international=True,
    ),
    # Qualifying rounds. Separate competitions in their own right, and worth
    # loading: they are real matches between the same clubs, adding several
    # hundred fixtures a season to what is otherwise a thin sample.
    "UCL-Q": LeagueSource(
        "UCL-Q", "UEFA Champions League qualifying", "champions-league",
        "{season}/clq.txt", international=True,
    ),
    "UEL-Q": LeagueSource(
        "UEL-Q", "UEFA Europa League qualifying", "champions-league",
        "{season}/elq.txt", international=True,
    ),
    "UECL-Q": LeagueSource(
        "UECL-Q", "UEFA Conference League qualifying", "champions-league",
        "{season}/confq.txt", international=True,
    ),
    "COPA-L": LeagueSource(
        "COPA-L", "Copa Libertadores", "south-america",
        "copa-libertadores/{season}_copal.txt",
        calendar_year=True, international=True,
    ),
    # ── football-data.co.uk only ──────────────────────────────────────────
    # Competitions openfootball does not publish. These have no git mirror, so
    # they are fetched live and depend on network access at load time.
    "ENG-L1": LeagueSource(
        "ENG-L1", "English League One", "", "",
        provider="footballdata", fd_div="E2",
    ),
    "ENG-L2": LeagueSource(
        "ENG-L2", "English League Two", "", "",
        provider="footballdata", fd_div="E3",
    ),
    "ENG-NL": LeagueSource(
        "ENG-NL", "English National League", "", "",
        provider="footballdata", fd_div="EC",
    ),
    "SCO-CH": LeagueSource(
        "SCO-CH", "Scottish Championship", "", "",
        provider="footballdata", fd_div="SC1",
    ),
    "SCO-L1": LeagueSource(
        "SCO-L1", "Scottish League One", "", "",
        provider="footballdata", fd_div="SC2",
    ),
    "SCO-L2": LeagueSource(
        "SCO-L2", "Scottish League Two", "", "",
        provider="footballdata", fd_div="SC3",
    ),
    "FIN-VL": LeagueSource(
        "FIN-VL", "Finnish Veikkausliiga", "", "",
        calendar_year=True, provider="footballdata",
        fd_country="FIN", fd_league="Veikkausliiga",
    ),
    "IRL-PD": LeagueSource(
        "IRL-PD", "Irish Premier Division", "", "",
        calendar_year=True, provider="footballdata",
        fd_country="IRL", fd_league="Premier Division",
    ),
    "ARG-CLP": LeagueSource(
        "ARG-CLP", "Copa de la Liga Profesional", "", "",
        calendar_year=True, provider="footballdata",
        fd_country="ARG", fd_league="Copa De La Liga Profesional",
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
    """Distinct upstream git repos needed to cover the registry."""
    return {src.repo for src in LEAGUES.values() if src.repo}


def codes() -> list[str]:
    return list(LEAGUES.keys())
