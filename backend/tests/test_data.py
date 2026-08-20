"""
Data-layer tests — parser, team matching and the store contract.

The parser tests use inline fixtures rather than repository files, so they run
without `athena data load` having been executed.
"""
from datetime import date

import pandas as pd
import pytest

from app.data.features import _canonical, _match_team
from app.data.openfootball import parse_text

# ── Fixtures ──────────────────────────────────────────────────────────────────

RESULTS_MID = """\
= England | Premier League 2025/26

# Matches  3

▪ Regular Season - 1
Fri Aug 15 2025
  19:00   Liverpool  4-2 (1-0)  Bournemouth
                  (Hugo EKITIKE 37', Cody GAKPO 49')
Sat Aug 16
  12:30   Aston Villa  0-0 (0-0)  Newcastle United
  15:00   Brighton & Hove Albion  1-1 (0-0)  Fulham
"""

RESULTS_V = """\
= UEFA Champions League 2025/26

▪ League, Matchday 1
  Tue Sep 16 2025
    18:45  Athletic Club (ESP)     v Arsenal FC (ENG)         0-2 (0-0)
           PAE Olympiakos SFP (GRE) v Paphos FC (CYP)          0-0
    21:00  Juventus FC (ITA)       v Galatasaray SK (TUR)     3-2 a.e.t. (3-0, 1-0)
           Paris Saint-Germain FC (FRA) v Arsenal FC (ENG)    4-3 pen. 1-1 a.e.t. (1-1, 0-1)
"""

FIXTURES = """\
= English Premier League 2026/27

▪ Matchday 1
  Fri Aug 21 2026
    20:00  Arsenal FC              v Coventry City FC
  Sat Aug 22
    12:30  Hull City AFC           v Manchester United FC
    15:00  Ipswich Town FC         v Sunderland AFC
           Nottingham Forest FC    v Leeds United FC
"""

ANNOTATED = """\
= Scotland | Premiership 2025/26

▪ Matchday 1
  Sat Aug 16 2025
    15:00  Rangers FC              v St. Johnstone FC         [cancelled]
           Tottenham Hotspur (ENG) v Stade Rennais (FRA)      0-3    [awarded]
"""


# ── Score-in-middle layout ────────────────────────────────────────────────────

def test_parses_middle_score_layout():
    df = parse_text(RESULTS_MID)
    assert len(df) == 3
    first = df.iloc[0]
    assert first["home"] == "Liverpool"
    assert first["away"] == "Bournemouth"
    assert (first["hg"], first["ag"]) == (4, 2)
    assert (first["hthg"], first["htag"]) == (1, 0)
    assert first["status"] == "result"


def test_goalscorer_lines_are_ignored():
    """Scorer continuation lines must not be mistaken for matches."""
    assert len(parse_text(RESULTS_MID)) == 3


def test_date_year_is_inherited():
    df = parse_text(RESULTS_MID)
    assert df.iloc[0]["date"] == pd.Timestamp("2025-08-15")
    # "Sat Aug 16" carries no year — it must inherit 2025, not default to today.
    assert df.iloc[1]["date"] == pd.Timestamp("2025-08-16")


def test_team_names_with_ampersand():
    df = parse_text(RESULTS_MID)
    assert "Brighton & Hove Albion" in set(df["home"])


# ── "v" layout, cups, extra time ──────────────────────────────────────────────

def test_parses_v_layout_and_strips_country_codes():
    df = parse_text(RESULTS_V)
    assert df.iloc[0]["home"] == "Athletic Club"
    assert df.iloc[0]["away"] == "Arsenal FC"
    assert (df.iloc[0]["hg"], df.iloc[0]["ag"]) == (0, 2)


def test_score_without_halftime():
    df = parse_text(RESULTS_V)
    row = df[df["home"] == "PAE Olympiakos SFP"].iloc[0]
    assert (row["hg"], row["ag"]) == (0, 0)
    assert pd.isna(row["hthg"])


def test_extra_time_uses_ninety_minute_score():
    """
    Over/under settles on regulation time. "3-2 a.e.t. (3-0, 1-0)" must record
    3-0, not 3-2 — otherwise every knockout tie inflates the goal totals that
    calibration learns from.
    """
    df = parse_text(RESULTS_V)
    row = df[df["home"] == "Juventus FC"].iloc[0]
    assert (row["hg"], row["ag"]) == (3, 0)
    assert (row["hthg"], row["htag"]) == (1, 0)


def test_penalties_use_ninety_minute_score():
    df = parse_text(RESULTS_V)
    row = df[df["home"] == "Paris Saint-Germain FC"].iloc[0]
    assert (row["hg"], row["ag"]) == (1, 1)
    assert (row["hthg"], row["htag"]) == (0, 1)


# ── Fixtures ──────────────────────────────────────────────────────────────────

def test_unplayed_matches_are_fixtures():
    df = parse_text(FIXTURES)
    assert len(df) == 4
    assert set(df["status"]) == {"fixture"}
    assert df["hg"].isna().all()


def test_fixture_time_inheritance_does_not_drop_matches():
    """A fixture listed under an earlier kick-off time still parses."""
    df = parse_text(FIXTURES)
    assert "Nottingham Forest FC" in set(df["home"])


# ── Annotations ───────────────────────────────────────────────────────────────

def test_cancelled_match_is_flagged_not_treated_as_result():
    df = parse_text(ANNOTATED)
    row = df[df["home"] == "Rangers FC"].iloc[0]
    assert row["status"] == "cancelled"
    assert pd.isna(row["hg"])
    # The annotation must not leak into the team name.
    assert row["away"] == "St. Johnstone FC"


def test_awarded_match_is_flagged():
    """Forfeits carry a scoreline but are not football results."""
    df = parse_text(ANNOTATED)
    row = df[df["home"] == "Tottenham Hotspur"].iloc[0]
    assert row["status"] == "awarded"
    assert (row["hg"], row["ag"]) == (0, 3)


def test_empty_input_yields_empty_frame():
    assert parse_text("").empty


# ── Team-name canonicalisation and matching ───────────────────────────────────

@pytest.mark.parametrize("raw,expected", [
    ("AFC Bournemouth", "bournemouth"),
    ("Arsenal FC", "arsenal"),
    ("Brighton & Hove Albion FC", "brighton hove albion"),
    ("Atlético Madrid", "atletico madrid"),
])
def test_canonical_strips_decoration(raw, expected):
    assert _canonical(raw) == expected


def test_canonical_never_empties_a_name():
    """A club whose whole name is a 'club token' must keep its identity."""
    assert _canonical("PSV") != ""


PL_TEAMS = [
    "Arsenal", "Aston Villa", "Bournemouth", "Brentford", "Brighton & Hove Albion",
    "Burnley", "Chelsea FC", "Crystal Palace", "Everton", "Fulham",
    "Leeds United", "Liverpool", "Manchester City", "Manchester United",
    "Newcastle United", "Nottingham Forest", "Sunderland", "Tottenham Hotspur",
]


@pytest.mark.parametrize("probe,expected", [
    ("Arsenal FC", "Arsenal"),
    ("Leeds United FC", "Leeds United"),
    ("AFC Bournemouth", "Bournemouth"),
    ("Brighton & Hove Albion FC", "Brighton & Hove Albion"),
    ("Manchester United FC", "Manchester United"),
    ("Chelsea", "Chelsea FC"),
])
def test_match_team_resolves_suffix_drift(probe, expected):
    assert _match_team(probe, PL_TEAMS) == expected


@pytest.mark.parametrize("probe", ["Coventry City FC", "Hull City AFC", "Wrexham AFC"])
def test_match_team_refuses_wrong_club(probe):
    """
    Regression guard. A permissive fuzzy scorer once resolved "Coventry City FC"
    to "Chelsea FC", which would have produced a confident tip built on another
    club's form. Absent teams must return None so the fixture is skipped.
    """
    assert _match_team(probe, PL_TEAMS) is None


def test_match_team_handles_empty_candidates():
    assert _match_team("Arsenal", []) is None


# ── Numeric date headers (used by some non-European datasets) ─────────────────

NUMERIC_DATES = """\
= Nigeria Professional League 2024/2025

▪ Matchday 1
  08.09.
    16:00  Abia Warriors FC           v Remo Stars FC              0-2 (0-0)
  15.09.2024
    16:00  Bendel FC                  v Rivers United FC           0-0 (0-0)
"""


def test_parses_numeric_date_headers():
    """
    Some datasets head their fixtures with "08.09." rather than "Sun Sep 08".
    Nigeria's whole archive uses it; before this was handled the league parsed
    to zero matches and vanished from the registry silently.
    """
    df = parse_text(NUMERIC_DATES)
    assert len(df) == 2
    assert df.iloc[0]["date"] == pd.Timestamp("2024-09-08")   # year inherited
    assert df.iloc[1]["date"] == pd.Timestamp("2024-09-15")   # year explicit


def test_numeric_date_rejects_impossible_month():
    """A score-like line must not be mistaken for a date."""
    df = parse_text("= X 2025/26\n  45.99.\n    12:00  A v B  1-0\n")
    assert df.empty


# ── Season conventions ────────────────────────────────────────────────────────

def test_calendar_year_leagues_use_year_only_seasons():
    """
    Brazil, MLS, Japan and the Nordics play inside one calendar year and are
    keyed "2025"; European winter leagues are keyed "2025-26". Getting this
    wrong just finds no file, so the league silently disappears.
    """
    from app.data import sources

    assert sources.get("BRA-SA").calendar_year is True
    assert all("-" not in s for s in sources.get("BRA-SA").default_seasons())

    assert sources.get("ENG-PL").calendar_year is False
    assert all("-" in s for s in sources.get("ENG-PL").default_seasons())


def test_all_seasons_spans_history():
    from app.data import sources

    eng = sources.get("ENG-PL").all_seasons(since=2000, until=2026)
    assert eng[0] == "2000-01" and eng[-1] == "2026-27"
    assert len(eng) == 27

    bra = sources.get("BRA-SA").all_seasons(since=2000, until=2026)
    assert bra[0] == "2000" and bra[-1] == "2026"


def test_registry_covers_multiple_continents():
    """Guard against the registry quietly regressing to Europe-only."""
    from app.data import sources

    for code in ["BRA-SA", "MLS", "JPN-J1", "EGY-PL", "ENG-PL"]:
        assert code in sources.LEAGUES, code
    assert len(sources.LEAGUES) >= 35


def test_uefa_competitions_are_registered():
    """
    All three UEFA club competitions plus their qualifying rounds share the
    champions-league repo, one file per competition per season. Coverage
    differs by competition — the Conference League only exists from 2021-22 —
    so the loader must tolerate missing seasons rather than erroring.
    """
    from app.data import sources

    for code, filename in [
        ("UCL", "cl"), ("UEL", "el"), ("UECL", "conf"),
        ("UCL-Q", "clq"), ("UEL-Q", "elq"), ("UECL-Q", "confq"),
    ]:
        src = sources.get(code)
        assert src.repo == "champions-league", code
        assert src.season_path("2024-25") == f"2024-25/{filename}.txt"
        assert src.international is True, code


# ── football-data.co.uk provider ──────────────────────────────────────────────

def test_odds_columns_are_rejected():
    """
    The provider ingests a strict allowlist of football columns. This guard is
    the backstop: if a bookmaker column ever reaches a stored frame, loading
    must fail loudly rather than quietly letting market data into the engine.
    """
    import pandas as pd
    from app.data import footballdata as fd

    clean = pd.DataFrame({"date": [], "home": [], "away": [], "hg": [], "ast": []})
    fd.assert_no_odds(clean)          # must not raise

    for bad_col in ["B365H", "PSCH", "AvgC>2.5", "MaxH", "WHD"]:
        with pytest.raises(ValueError):
            fd.assert_no_odds(pd.DataFrame({"date": [], bad_col: []}))


def test_footballdata_season_key():
    from app.data import footballdata as fd
    assert fd.season_key(2025) == "2526"
    assert fd.season_key(1999) == "9900"


def test_provider_is_recorded_on_sources():
    """CHN-SL moved to football-data because openfootball's Asia data stops at 2025."""
    from app.data import sources
    assert sources.get("CHN-SL").provider == "footballdata"
    assert sources.get("CHN-SL").fd_country == "CHN"
    assert sources.get("ENG-PL").provider == "openfootball"


def test_domestic_fallback_follows_the_registry():
    """
    Cup fixtures resolve club form from domestic leagues. That list was once
    hardcoded to twelve leagues and silently starved the cups as coverage grew:
    67 of 113 Europa League clubs were unresolvable, including Olympiakos and
    Fenerbahce, whose leagues were loaded but simply absent from the list.
    Deriving it from the registry keeps the two in step.
    """
    from app.data import features, sources

    fallback = features._domestic_fallback()
    assert len(fallback) > 40, "fallback should span the registry, not a fixed subset"

    # Domestic leagues added after the original list must be present.
    for code in ["GRE-SL", "TUR-SL", "CZE-FL", "BEL-PL", "NOR-EL", "CRO-1L"]:
        assert code in fallback, code

    # Cup competitions must not be searched for domestic form.
    for code in ["UCL", "UEL", "UECL"]:
        assert code not in fallback, code
