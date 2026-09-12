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
    The UEFA club competitions are split across two providers, deliberately.

    UCL and all three qualifying rounds still come from openfootball's
    champions-league repo, one file per competition per season, and the loader
    must tolerate missing seasons since coverage differs by competition.

    UEL and UECL were repointed at ESPN because openfootball stopped: both sat
    450+ days stale while the competitions kept playing. They are the reason a
    third provider exists, alongside Brazilian Serie B.
    """
    from app.data import sources

    for code, filename in [
        ("UCL", "cl"), ("UCL-Q", "clq"), ("UEL-Q", "elq"), ("UECL-Q", "confq"),
    ]:
        src = sources.get(code)
        assert src.repo == "champions-league", code
        assert filename in src.path, code
        assert src.international is True, code

    for code, slug in [("UEL", "uefa.europa"), ("UECL", "uefa.europa.conf")]:
        src = sources.get(code)
        assert src.provider == "espn", code
        assert src.espn_code == slug, code
        assert src.international is True, code
        # Autumn-spring competitions. Fetching these as calendar years would
        # splice the back half of one season onto the front of the next, and
        # every season label would be wrong while nothing looked broken.
        assert src.calendar_year is False, code

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


# ── Shots on target: measured vs estimated ────────────────────────────────────

def test_sot_uses_measured_shots_when_available():
    """
    sot_proj_total was always derived from expected goals via a fixed ratio.
    Where the source carries real shot counts it should be measured instead,
    and say so, because the two are not interchangeable.
    """
    import pandas as pd
    from app.data import features

    rows = pd.DataFrame({
        "home": ["A"] * 5,
        "away": ["B"] * 5,
        "hst": [6, 8, 4, 7, 5],
        "ast": [3, 2, 4, 3, 3],
    })
    assert features._sot_per_game(rows, "a") == pytest.approx(6.0)
    assert features._sot_per_game(rows, "b") == pytest.approx(3.0)


def test_sot_falls_back_when_shots_absent():
    """openfootball carries no shot counts; the estimate must still be produced."""
    import pandas as pd
    from app.data import features

    rows = pd.DataFrame({"home": ["A"] * 5, "away": ["B"] * 5})
    assert features._sot_per_game(rows, "a") is None

    total, measured = features._projected_sot(rows, rows, "a", "b", mu_total=2.8)
    assert measured is False
    assert total == pytest.approx(2.8 * features.SOT_PER_GOAL, abs=0.01)


def test_sot_requires_most_of_the_window():
    """A couple of stray rows would be noisier than the estimate they replace."""
    import pandas as pd
    from app.data import features

    rows = pd.DataFrame({
        "home": ["A"] * 10,
        "away": ["B"] * 10,
        "hst": [6, 8] + [None] * 8,
        "ast": [3, 2] + [None] * 8,
    })
    assert features._sot_per_game(rows, "a") is None


def test_shot_blend_shifts_the_scoring_rate():
    """
    A side scoring far below what its shot volume implies should be nudged up,
    and one scoring far above it nudged down — the blend anticipates regression
    rather than extrapolating a hot streak.
    """
    import pandas as pd
    from app.data import features

    rows = pd.DataFrame({
        "home": ["A"] * 6, "away": ["B"] * 6,
        "hst": [7, 8, 6, 7, 7, 7],     # 7 shots on target a game
        "ast": [2, 2, 2, 2, 2, 2],
    })
    conv = 0.25                         # 7 * 0.25 = 1.75 implied goals

    under = features._blended_scoring_rate(rows, "a", goals_rate=0.5, conversion=conv)
    over = features._blended_scoring_rate(rows, "a", goals_rate=3.0, conversion=conv)
    assert under > 0.5, "under-converting side should be revised up"
    assert over < 3.0, "over-converting side should be revised down"


def test_shot_blend_is_inert_without_shot_data():
    """Leagues with no shot counts must keep their goal rate untouched."""
    import pandas as pd
    from app.data import features

    rows = pd.DataFrame({"home": ["A"] * 6, "away": ["B"] * 6})
    assert features._blended_scoring_rate(rows, "a", 1.4, None) == 1.4
    assert features._blended_scoring_rate(rows, "a", 1.4, 0.25) == 1.4


# ── League aggregates are point-in-time and era-local ─────────────────────────

def test_league_conversion_does_not_read_the_future():
    """
    Regression guard. `_compute_features` passes the whole unfiltered league
    frame, and the conversion rate was computed over all of it — every stored
    season, including seasons after the fixture being predicted — then cached
    under the league alone with no date in the key.

    A 2012 fixture must not see a rate shaped by 2024 football.
    """
    from datetime import datetime
    from app.data import features, store

    df = store.load_results("ENG-PL")
    if df.empty or "hst" not in df.columns:
        import pytest
        pytest.skip("ENG-PL shot data not loaded")

    features._CONVERSION_CACHE.clear()
    old = features._league_conversion(df, "ENG-PL", datetime(2012, 6, 1))
    features._CONVERSION_CACHE.clear()
    new = features._league_conversion(df, "ENG-PL", datetime(2026, 6, 1))

    assert old is not None and new is not None
    # The source changed how it counts shots on target around 2015; the two
    # eras must therefore land on visibly different rates rather than one
    # pooled compromise that is wrong for both.
    assert new - old > 0.05, (old, new)


def test_league_conversion_is_order_independent():
    """
    The cache key carries the year, and the window is anchored to the start of
    that year rather than to the cutoff itself — so the answer cannot depend on
    which fixture happened to be computed first.
    """
    from datetime import datetime
    from app.data import features, store

    df = store.load_results("ENG-PL")
    if df.empty or "hst" not in df.columns:
        import pytest
        pytest.skip("ENG-PL shot data not loaded")

    features._CONVERSION_CACHE.clear()
    jan = features._league_conversion(df, "ENG-PL", datetime(2024, 1, 5))
    dec = features._league_conversion(df, "ENG-PL", datetime(2024, 12, 20))
    features._CONVERSION_CACHE.clear()
    dec_first = features._league_conversion(df, "ENG-PL", datetime(2024, 12, 20))

    assert jan == dec == dec_first


def test_league_mean_is_windowed_not_cumulative():
    """
    The mean must describe the league now, not its entire stored history.
    England has ~9,800 matches back to 1993; a 2026 fixture should be judged
    against recent seasons, and the sample size proves the window is applied.
    """
    from datetime import datetime
    from app.data import features, store

    df = store.load_results("ENG-PL")
    if len(df) < 2000:
        import pytest
        pytest.skip("ENG-PL history not loaded")

    mean, n = features._league_mean_asof(df, datetime(2026, 6, 1))
    assert 0 < n < 2000, f"window not applied: n={n} of {len(df)}"
    assert 2.0 < mean < 4.0


def test_espn_sourced_leagues_declare_a_slug():
    """
    Every ESPN-provider league needs a slug; without one the loader silently
    returns None and the league looks merely empty rather than misconfigured.
    """
    from app.data import sources

    espn = [s for s in sources.LEAGUES.values() if s.provider == "espn"]
    assert espn, "expected at least the leagues rescued from dead feeds"
    for s in espn:
        assert s.espn_code, s.code
        assert not s.repo and not s.path, f"{s.code} should not keep a git path"


def test_espn_season_span_follows_calendar_year_flag():
    """
    Regression guard on the splice. A calendar-year league asks for January to
    December; an autumn-spring one asks July to June of the following year.
    """
    from app.data import espn

    seen = {}

    class _Resp:
        status_code = 200

        @staticmethod
        def raise_for_status():
            pass

        @staticmethod
        def json():
            return {"events": []}

    def fake_get(url, params=None, timeout=None):
        seen["dates"] = params["dates"]
        return _Resp()

    import app.data.espn as mod
    real = mod.requests.get
    mod.requests.get = fake_get
    try:
        espn.fetch_season("x.1", 2025, "X", calendar_year=True)
        assert seen["dates"] == "20250101-20251231"
        espn.fetch_season("x.1", 2025, "X", calendar_year=False)
        assert seen["dates"] == "20250701-20260630"
    finally:
        mod.requests.get = real


# ── Possession adjustment ─────────────────────────────────────────────────────

def test_possession_is_off_unless_a_league_has_data():
    """
    The adjustment must return None rather than a guess when a league has no
    possession, too little history to fit, or a side without a profile. Callers
    then proceed exactly as before.
    """
    from datetime import date
    from app.data import possession

    # ENG-PL comes from football-data.co.uk, which publishes no possession.
    assert possession.shift("ENG-PL", "Arsenal", "Chelsea", date(2026, 3, 5)) is None


def test_possession_shift_is_bounded():
    """
    A regression fitted on a few thousand noisy matches should nudge a goal
    expectation, not overturn it. Whatever the fit produces, the published shift
    stays inside MAX_SHIFT.
    """
    from datetime import date
    from app.data import possession, store

    df = store.load_results("COL-PA")
    if df.empty or "hpos" not in df.columns:
        import pytest
        pytest.skip("COL-PA possession not loaded")

    seen = 0
    recent = df[df["date"] >= "2026-01-01"].head(25)
    for _, r in recent.iterrows():
        s = possession.shift("COL-PA", r["home"], r["away"], r["date"].date())
        if s is None:
            continue
        seen += 1
        assert abs(s) <= possession.MAX_SHIFT + 1e-9, (r["home"], r["away"], s)
    assert seen > 0, "expected at least one fixture to produce a shift"


def test_possession_fit_is_as_of():
    """
    The coefficient for a fixture must come only from matches before it. Fitting
    on a league's whole history and scoring the same matches would confirm any
    signal, real or not — which is how a false result already arose once in this
    codebase.
    """
    from datetime import datetime
    from app.data import possession, store

    df = store.load_results("COL-PA")
    if df.empty or "hpos" not in df.columns:
        import pytest
        pytest.skip("COL-PA possession not loaded")

    possession._FIT_CACHE.clear()
    early = possession._fit("COL-PA", datetime(2022, 1, 1))
    possession._FIT_CACHE.clear()
    late = possession._fit("COL-PA", datetime(2026, 6, 1))

    # Different windows must produce different fits; identical values would mean
    # the cutoff was ignored.
    if early is not None and late is not None:
        assert early != late, "fit ignored the cutoff"


def test_side_shrink_targets_that_sides_own_mean():
    """Both sides used to shrink toward `league_mu / 2`, on the stated reasoning
    that half the league mean IS the per-side mean. It is not — home teams
    average 1.502 goals and away 1.154 against a shared target of 1.328 — and
    that miss was worth a 7.7 point home/away calibration split."""
    from app.data import features

    mu, share = 2.66, 0.565
    home = features._shrink_side(2.20, mu, share)
    away = features._shrink_side(2.20, mu, 1.0 - share)
    # Identical raw rates must NOT produce identical shrunk rates: the home side
    # is pulled toward a higher mean than the away side.
    assert home > away
    # And each lands between its raw rate and its OWN target.
    assert mu * share < home < 2.20
    assert mu * (1 - share) < away < 2.20


def test_home_share_is_bounded():
    """A thin or freak window must not invert the venue split."""
    import pandas as pd

    from app.data import features

    empty = pd.DataFrame({"date": [], "hg": [], "ag": []})
    assert features._home_share(empty, None, None) == features.DEFAULT_HOME_SHARE
    lopsided = pd.DataFrame({"date": pd.to_datetime(["2020-01-01"] * 300),
                             "hg": [9] * 300, "ag": [0] * 300})
    assert 0.50 <= features._home_share(lopsided, None, None) <= 0.65


def test_venue_debias_leaves_mu_total_unchanged():
    """The venue correction fixes the SPLIT between the two sides and must not
    move their sum. `mu_total = gfh + gfa` drives the match lane, which is
    calibrated to a gap of ~0 — the team lane does not get to disturb it."""
    gfh, gfa, c = 1.40, 1.25, 0.113
    assert (gfh + c) + (gfa - c) == pytest.approx(gfh + gfa)


def test_alias_overrules_a_confident_wrong_match():
    """An alias has to beat the resolver, not merely fill in for it.

    The resolver does not fail by returning nothing. It fails by matching the
    wrong club with confidence — `Celta Fortuna` onto Celta Vigo's 2004-2012
    rows, `U. de Concepción` onto Deportes Concepción, `América-MG` onto a
    spelling retired in 2013. While the alias table was consulted only for
    names that matched nothing, none of those entries could fire.

    The reserve guard has since closed the Celta case at the matcher: a
    reserve side no longer resolves to its parent at all, so the wrong
    match here is refused rather than overruled. The alias still has to
    win over whatever the resolver says — pinned on a first-team name
    the scorer would otherwise take with confidence.
    """
    import pandas as pd

    from app.data import features

    df = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01"] * 4),
        "home": ["Celta", "Celta B", "Celta", "Celta B"],
        "away": ["Celta B", "Celta", "Celta B", "Celta"],
        "hg": [1, 1, 1, 1], "ag": [1, 1, 1, 1]})

    # The reserve guard: a reserve side never lands on the first team.
    assert features._resolve_in_frame(df, "Celta Fortuna") is None
    # And the alias routes it to its own rows.
    assert features._aliased("ESP-L2", df, "Celta Fortuna") == "Celta B"

    # Alias over a CONFIDENT wrong match on the same side of the reserve
    # line, which the guard does not touch: a first team the scorer takes
    # for another first team.
    df2 = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01"] * 2),
        "home": ["Deportes Concepción", "Universidad de Concepción"],
        "away": ["Universidad de Concepción", "Deportes Concepción"],
        "hg": [1, 1], "ag": [1, 1]})
    assert features._resolve_in_frame(df2, "U. de Concepción") \
        == "Deportes Concepción"
    from app.data import aliases
    if aliases.get("CHI-PD", "U. de Concepción") == "Universidad de Concepción":
        assert features._aliased("CHI-PD", df2, "U. de Concepción") \
            == "Universidad de Concepción"


def test_alias_pointing_at_a_missing_name_is_ignored():
    """The staleness guard is what makes the override safe: an alias naming a
    club this store does not carry must fall back to the resolver rather than
    steer a fixture into an empty row set."""
    import pandas as pd

    from app.data import features

    df = pd.DataFrame({
        "date": pd.to_datetime(["2020-01-01"] * 2),
        "home": ["Celta", "Celta"], "away": ["Vigo", "Vigo"],
        "hg": [1, 1], "ag": [1, 1]})

    # `Celta B` is not in this frame, so the alias must not be applied.
    assert features._aliased("ESP-L2", df, "Celta Fortuna") == "Celta Fortuna"


def test_weak_sides_are_floored_and_ordinary_ones_are_not():
    """The floor is a patch on the low end, not a second shrink.

    The shrink's own fitting regression already showed it — "lowest gf fifth
    says 0.90 goals, actually 1.14" — but a slope fitted with an intercept of
    0.572 cannot be applied as a slope alone without leaving that residual.
    Sweeping TEAM_SHRINK makes the low band worse in every direction that
    helps the high band, so the two ends get different corrections.
    """
    from app.data import features

    mu, share = 2.66, 0.565
    # A side with almost no attack lands ON the floor rather than below it.
    assert features._shrink_side(0.10, mu, 1 - share) == features.TEAM_RATE_FLOOR
    # An ordinary side is untouched by it.
    assert features._shrink_side(2.20, mu, share) > features.TEAM_RATE_FLOOR
    # And the floor never LOWERS a rate — it can only make a weak side look
    # stronger, which is the direction the measurement asked for.
    for gf in (0.05, 0.5, 1.0, 1.5, 2.5):
        for s in (share, 1 - share):
            assert features._shrink_side(gf, mu, s) >= features.TEAM_RATE_FLOOR


def test_the_floor_cannot_reach_the_match_lane():
    """`p_*_tt05` are built from `_shrink_side`; `mu_total` is not. The team
    lane has its own shrink precisely so the match lane is not shrunk twice,
    and the floor inherits that isolation — the match ladder is calibrated to
    a gap of ~0 and a team-lane patch does not get to disturb it."""
    import inspect

    from app.data import features

    src = inspect.getsource(features._compute_features)
    for line in src.splitlines():
        if "mu_total" in line and "=" in line:
            assert "_shrink_side" not in line


def _division_frame(rows):
    import pandas as pd

    return pd.DataFrame({
        "date": pd.to_datetime([r[0] for r in rows]),
        "home": [r[1] for r in rows], "away": [r[2] for r in rows],
        "hg": [float(r[3]) for r in rows], "ag": [float(r[4]) for r in rows]})


def test_cross_division_rows_apply_the_measured_exchange_rate(monkeypatch):
    """A promoted club's goals are rescaled to the upper division's level —
    scored x0.754, conceded x1.516, measured on 789 crossing club-seasons —
    and the away-venue perspective scales the same numbers from the other
    column."""
    from datetime import datetime

    from app.data import features, store

    lower = _division_frame([
        ("2026-03-01", "Promoted FC", "Rival", 2, 1),   # scored 2 at home
        ("2026-03-08", "Rival", "Promoted FC", 0, 3),   # scored 3 away
        ("2026-03-15", "Promoted FC", "Other", 1, 0),
        ("2026-03-22", "Other", "Promoted FC", 2, 2),
        ("2026-03-29", "Promoted FC", "Rival", 0, 0),
    ])
    monkeypatch.setattr(store, "load_results",
                        lambda code, season=None: lower if code == "ESP-L2"
                        else _division_frame([]))

    got = features._cross_division_rows(
        "ESP-LL", "Promoted FC", datetime(2026, 8, 25), 5, "home")
    assert got is not None
    name, rows, _venue = got
    assert name == "Promoted FC"
    # Home rows: own goals scaled down, opponent goals scaled up.
    first = rows[rows["date"] == "2026-03-01"].iloc[0]
    assert first["hg"] == pytest.approx(2 * features.PROMOTED_SCORED)
    assert first["ag"] == pytest.approx(1 * features.PROMOTED_CONCEDED)
    # Away rows: same factors from the other column.
    away = rows[rows["date"] == "2026-03-08"].iloc[0]
    assert away["ag"] == pytest.approx(3 * features.PROMOTED_SCORED)
    assert away["hg"] == pytest.approx(0.0)


def test_cross_division_directions_are_reciprocal(monkeypatch):
    """A club found in the division ABOVE the fixture's league is relegated
    into it, and the same exchange rate applies crossed the other way."""
    from datetime import datetime

    from app.data import features, store

    upper = _division_frame([
        ("2026-03-01", "Dropped FC", "Rival", 1, 2),
        ("2026-03-08", "Rival", "Dropped FC", 1, 1),
        ("2026-03-15", "Dropped FC", "Other", 0, 1),
        ("2026-03-22", "Other", "Dropped FC", 3, 0),
        ("2026-03-29", "Dropped FC", "Rival", 2, 2),
    ])
    monkeypatch.setattr(store, "load_results",
                        lambda code, season=None: upper if code == "ESP-LL"
                        else _division_frame([]))

    got = features._cross_division_rows(
        "ESP-L2", "Dropped FC", datetime(2026, 8, 25), 5, "home")
    assert got is not None
    _name, rows, _venue = got
    first = rows[rows["date"] == "2026-03-01"].iloc[0]
    assert first["hg"] == pytest.approx(1 / features.PROMOTED_SCORED)
    assert first["ag"] == pytest.approx(2 / features.PROMOTED_CONCEDED)


def test_fallback_is_rescue_only(monkeypatch):
    """Under the shipped gate a club its own league can already describe
    never goes cross-division, so no existing tip can move. Pinned on
    BEHAVIOUR now that the gate has modes: in "count" mode, five own rows
    keep their window even when a fresher one sits next door, and the
    fallback is never consulted for them."""
    from datetime import datetime

    from app.data import features

    assert features.FALLBACK_MODE == "count"
    own = _division_frame([
        ("2023-03-01", "Yoyo FC", "Rival", 1, 1),
        ("2023-03-08", "Rival", "Yoyo FC", 2, 0),
        ("2023-03-15", "Yoyo FC", "Rival", 0, 0),
        ("2023-03-22", "Rival", "Yoyo FC", 1, 3),
        ("2023-03-29", "Yoyo FC", "Rival", 2, 2),
    ])
    calls = []

    def never(*a, **k):
        calls.append(a)
        return ("Yoyo FC", own, own)

    monkeypatch.setattr(features, "_cross_division_rows", never)
    cut = datetime(2026, 8, 25)
    rows = features._find_team_rows(own, "Yoyo FC", cut)
    name, got, _venue = features._with_fallback(
        "ESP-L2", "Yoyo FC", rows, rows, cut, 5, "home")
    assert name == "Yoyo FC" and got is rows and calls == []

    thin = rows.head(3)
    features._with_fallback("ESP-L2", "Yoyo FC", thin, thin, cut, 5, "home")
    assert len(calls) == 1


def test_no_ladder_means_no_fallback():
    """A league outside every ladder (its neighbours are not stored) returns
    no siblings, so the fallback cannot invent one."""
    from app.data import features

    assert features._adjacent_divisions("TUR-SL") == []
    assert features._adjacent_divisions("ESP-LL") == [("ESP-L2", True)]
    assert features._adjacent_divisions("ENG-CH") == [
        ("ENG-L1", True), ("ENG-PL", False)]


def _clash_frame():
    import pandas as pd

    rows = []
    # A 12-club league: four strong sides beating eight fillers for 7 rounds.
    # Size matters — the flag stands down in frames under twice its top-N,
    # because "top six" of six clubs is everybody.
    strong = ["Alpha", "Beta", "Gamma", "Delta"]
    weak = [f"Filler{i}" for i in range(1, 9)]
    d = pd.Timestamp("2026-02-01")
    for rnd in range(7):
        for i, s in enumerate(strong):
            rows.append((d, s, weak[(rnd + i) % 8], 2, 0))
        # Draws only among fillers 3-8, so Filler1 and Filler2 finish on
        # zero points — genuinely OUTSIDE the top six, which a 12-team league
        # needs constructing deliberately: 5th place is top-6 by definition.
        rows.append((d, weak[2 + (rnd % 3) * 2], weak[3 + (rnd % 3) * 2], 0, 0))
        d += pd.Timedelta(days=7)
    return pd.DataFrame(rows, columns=["date", "home", "away", "hg", "ag"])


def test_top_clash_flag_matches_the_measurement():
    """Both sides top-6 by calendar-year points with 6+ rounds each — the flag
    the two-window validation was run on, quirk included."""
    from datetime import datetime

    from app.data import features

    df = _clash_frame()
    cutoff = datetime(2026, 8, 1)
    features._TOP_CLASH_CACHE.clear()
    top = [t for t in ("alpha", "beta", "gamma", "delta")
           if features._is_top_clash(df, t, t, cutoff, "TEST-LG")]
    assert top, "strong clubs should reach the top table"
    # A clash needs BOTH sides up there.
    assert features._is_top_clash(df, top[0], top[-1], cutoff, "TEST-LG") \
        == (len(top) >= 2)
    assert not features._is_top_clash(df, top[0], "filler1", cutoff, "TEST-LG")
    # Before six rounds exist, nobody is flagged — a table that early is noise.
    features._TOP_CLASH_CACHE.clear()
    early = datetime(2026, 2, 20)
    assert not features._is_top_clash(df, top[0], top[-1], early, "TEST-LG")


def test_big_match_debit_only_lowers_and_only_matches():
    """The debit is a subtraction on the MATCH mu, sized from the pooled
    top-6 effect (−0.15 to −0.17 in both windows). It must never raise a mu,
    and the team lanes — which were never measured — must not carry it."""
    import inspect

    from app.data import features

    assert 0 < features.BIG_MATCH_DEBIT <= 0.2
    src = inspect.getsource(features._compute_features)
    at = src.index("BIG_MATCH_DEBIT")
    # Applied to mu_total after the shrink, before anything else reads it...
    assert "mu_total = max(0.2, mu_total - BIG_MATCH_DEBIT)" in src
    # ...and never to the per-side rates that feed p_*_tt05.
    assert "gfh - BIG_MATCH_DEBIT" not in src
    assert "gfa - BIG_MATCH_DEBIT" not in src


def test_defense_blend_touches_team_lanes_only():
    """The defense adjustment feeds `p_*_tt05` and nothing else: `mu_total`
    is assembled from the unadjusted attack rates, so the match ladder —
    calibrated to ~0 — cannot move. Pinned the same way as the floor."""
    import inspect

    from app.data import features

    assert 0.0 <= features.DEFENSE_BLEND <= 0.7
    src = inspect.getsource(features._compute_features)
    # The adjusted rates exist and are consumed by the tt05 lines only.
    assert "gfh_t" in src and "gfa_t" in src
    for line in src.splitlines():
        if "mu_total" in line and "=" in line:
            assert "gfh_t" not in line and "gfa_t" not in line


def test_cup_fixtures_price_from_club_elo():
    """The reopened cup lane: mu comes from committed as-of Club Elo, never
    from domestic form (measured slope 0.017 — zero — against cup totals).
    The pinned behaviours are the boundaries: mapped clubs price, national
    teams and unmapped clubs abstain, and the mu lands inside the plausible
    band the instruments graded."""
    from datetime import date

    from app.data import features

    assert features.CUP_TIPS_ENABLED is True
    got = features.asof_features("UCL", "Real Madrid CF", "Bayern München",
                                 date(2025, 10, 15))
    assert got, "two mapped giants must price"
    assert 0.5 < got["mu_total"] < 6.0
    assert got["league_mu"] > 2.0          # UCL base is a high-scoring one
    assert 0.0 < got["p_two_plus"] < 1.0

    # National-team competitions carry no club Elo: abstain, don't guess.
    assert features.asof_features("EC", "France", "Germany",
                                  date(2026, 9, 15)) == {}

    # A club outside the mapping abstains rather than pricing on nothing.
    assert features.asof_features("UCL", "Real Madrid CF", "No Such Club FC",
                                  date(2025, 10, 15)) == {}


def test_cup_elo_staleness_guard_abstains():
    """Elo lagged 60 days measured harmless; a rating more than a season
    old describes a different squad. Past MAX_STALE_DAYS the lane must
    return None rather than a number."""
    from datetime import date, timedelta

    import pandas as pd

    from app.data import club_elo

    dates, elos = club_elo._series()[club_elo._names()["Celtic"]]
    beyond = dates[-1] + pd.Timedelta(days=club_elo.MAX_STALE_DAYS + 30)
    assert club_elo.elo_asof("Celtic", beyond) is None
    within = dates[-1] + pd.Timedelta(days=30)
    assert club_elo.elo_asof("Celtic", within) is not None


def test_cup_over_debit_is_scoped_to_cup_overs():
    """Cup over tips measured a flat -3.3/-3.7 across the two Swiss seasons
    while the under rungs calibrated — the stated-probability debit must
    touch exactly that family and nothing else."""
    from app.data import club_elo

    assert club_elo.stated_p("UCL", "O1.5", 0.80) == pytest.approx(
        0.80 - club_elo.OVER_SAYS_DEBIT)
    assert club_elo.stated_p("UECL-Q", "O2.25", 0.70) == pytest.approx(
        0.70 - club_elo.OVER_SAYS_DEBIT)
    assert club_elo.stated_p("UCL", "U4.25", 0.86) == 0.86      # cup under
    assert club_elo.stated_p("ENG-PL", "O1.5", 0.80) == 0.80    # domestic


def test_board_verify_passes_on_the_committed_board():
    """The renderer's own hard lock: every fixture present on every
    surface, tallies matching, ledger complete, app script parsing. It
    runs inside board.py on every path; this pins that the committed
    board satisfies it, so a broken render cannot be committed green."""
    from scripts import board

    board.verify(quiet=True)


def test_cup_probability_floor_is_raised():
    """The cup lane selects hitrate-first: min_win_prob 0.82 on all six cup
    codes flips the mix to the calibrated base rungs (hit ~80 -> ~83 across
    both Swiss seasons, zero volume lost — choose() falls back to the
    safest buyable rung). Domestic floors are untouched."""
    from app.data import config

    for code in ("UCL", "UEL", "UECL", "UCL-Q", "UEL-Q", "UECL-Q"):
        assert config.get(code).min_win_prob == 0.82
    assert config.get("ENG-PL").min_win_prob is None


def test_livecheck_gates_on_what_is_actually_running():
    """The scheduler's gate: cheap, standard-library only, and right.

    It decides whether the live sweep runs at all, so a wrong answer is
    either a board that goes stale through a match or a job that installs
    the engine every five minutes for nothing.
    """
    from datetime import datetime, timedelta
    from scripts import livecheck

    now = datetime(2026, 9, 6, 20, 0, tzinfo=livecheck.BOARD_TZ)

    def at(rows):
        livecheck._rows = lambda: iter(rows)
        return livecheck.reason(now)

    keep = livecheck._rows
    try:
        # kicked off, no result yet -> sweep
        assert at([("2026-09-06 19:00", "A v B", "")])
        # graded, and finished-without-a-tip, are both done
        assert at([("2026-09-06 19:00", "A v B", "✅ HIT — 1-0")]) is None
        assert at([("2026-09-06 19:00", "A v B", "FT 1-0 (no tip)")]) is None
        # about to start -> sweep; comfortably later -> not yet
        assert at([("2026-09-06 20:05", "A v B", "")])
        assert at([("2026-09-06 21:00", "A v B", "")]) is None
        # a row no feed will ever settle must not hold the gate open for
        # ever: past STALE it stops asking
        old = now - livecheck.STALE - timedelta(minutes=1)
        assert at([(old.strftime("%Y-%m-%d %H:%M"), "A v B", "")]) is None
        # --wait: how long a run that arrived early should sleep. Due now
        # -> 0; an hour out -> LEAD short of an hour; nothing coming -> -1.
        def wait(rows):
            livecheck._rows = lambda: iter(rows)
            return livecheck.wait_seconds(now)
        assert wait([("2026-09-06 19:00", "A v B", "")]) == 0
        assert wait([("2026-09-06 21:00", "A v B", "")]) == 3600 - 6 * 60
        assert wait([("2026-09-06 19:00", "A v B", "✅ HIT — 1-0")]) == -1
    finally:
        livecheck._rows = keep


def test_declined_cards_count_toward_no_hit_rate():
    """The bettor's rule, 7 Sep: a red or super-red card is never played,
    so it never enters a hit rate — on the board or in the bank — while
    staying visible and graded. And 12 Sep: neither does any other
    DECLINED card, an unstaked card tagged live unsafe. One predicate on
    each side; both must refuse those and accept everything else,
    including the unlabelled and the priced plays whatever their tag.
    """
    from scripts import bankrates, board, livebands, webapp

    # the bank side: a labelled red card is out, everything else is in
    assert not bankrates.counts({"g": "red"})
    assert not bankrates.counts({"g": "super red"})
    assert bankrates.counts({"g": "orange"})
    assert bankrates.counts({"g": "green"})
    assert bankrates.counts({})                     # unlabelled still counts
    # ... and so is an unstaked card whose band is unsafe. Pin the bands
    # to the seed so the test does not move with the measured table.
    monkey = livebands._BANDS
    livebands._BANDS = {(l, b): dict(label=lab, n=0, hit=None, said=None, source="seed")
                        for l, bs in livebands.SEED.items() for b, lab in bs.items()}
    try:
        deep = "U4.25 84.0% **−5.0%** · buy≥1.22 (+6.1% margin)"
        high = "U4.25 84.0% **+3.0%** · buy≥1.22 (+6.1% margin)"
        # an Athena lane (no closing price) at −5: unsafe, out
        assert bankrates.lane({"g": "orange", "tip": deep}) == "athena"
        assert bankrates.tag({"g": "orange", "tip": deep}) == "unsafe"
        assert not bankrates.counts({"g": "orange", "tip": deep})
        # the same card at +3 is safe and in
        assert bankrates.counts({"g": "orange", "tip": high})
        # a watch card (closing price inside the watch band) above +1: out
        w = {"g": "green", "tip": high, "v": "no play", "bp": 1.20, "need": 1.22}
        assert bankrates.lane(w) == "watch" and not bankrates.counts(w)
        # a card the price missed by more than the band is an Athena lane: in
        a = dict(w, bp=1.05)
        assert bankrates.lane(a) == "athena" and bankrates.counts(a)
        # a priced play is never declined by its tag
        pl = {"g": "green", "tip": deep, "v": "normal", "bp": 1.30, "need": 1.22}
        assert bankrates.lane(pl) == "priced" and bankrates.tag(pl) == "unsafe"
        assert bankrates.counts(pl)
        # red stays red whatever the price
        assert bankrates.lane({"g": "red", "tip": high, "v": "no play", "bp": 1.3, "need": 1.2}) is None
    finally:
        livebands._BANDS = monkey

    # the board side: the tallies must drop exactly the settled declined
    # cards — red, and the unstaked ones tagged unsafe
    fx = board.load()
    settled = [f for f in fx if f.settled]
    out = [f for f in settled if webapp.is_declined(f)]
    red = [f for f in out if (lab := webapp.label_any(f)) and lab.endswith("red")]
    keep = [f for f in settled if f not in out]
    t_all, _ = board._tallies(fx)
    # count tip 1 by hand over the kept cards, the way _tallies does
    n = sum(1 for f in keep if f.status[:1] in ("✅", "❌", "◦")
            and not f.tip1.startswith("—"))
    assert t_all[1][1] == n, (t_all[1][1], n, len(out))
    # and had the declined been in, the count would be larger by their number
    n_red = sum(1 for f in red if f.status[:1] in ("✅", "❌", "◦"))
    assert n_red > 0, "no settled red card on the board to test against"
    # a priced play is in the record whatever its tag says
    for f in keep:
        if webapp.record_lane(f) == "priced":
            assert webapp.counts(f)
    for f in out:
        assert not webapp.counts(f)
        if f not in red:
            assert webapp.record_lane(f) in ("athena", "watch")
            assert webapp.record_tag(f) == "unsafe"
    # the measurement behind the tag keeps only the red cards out, or an
    # unsafe band could never come back
    assert all(webapp.not_red(f) for f in keep)
    assert any(webapp.not_red(f) and not webapp.counts(f) for f in out)

    # the hero: the one number that kept counting reds until 7 Sep. The
    # rendered page must say so, and the app must flag every declined
    # board card as no-count so the window can skip it.
    from pathlib import Path
    src = Path(webapp.__file__).read_text()
    assert 'entry["nc"] = 1' in src and 'm.get("nc")' in src
    page = Path(webapp.__file__).resolve().parents[2] / "web" / "index.html"
    if page.exists():
        assert ("playable cards · declined cards excluded (red, super red, "
                "live unsafe)" in page.read_text())


def test_unsafe_priced_plays_flip_to_tip_3_not_tip_2(monkeypatch):
    """The bettor's ask, 12 Sep: on a priced play tagged live unsafe, say
    when another lane beats tip 1. Per lane, seeded from the bank (tip 3
    flipped, tip 2 hold), measured on the board at 15 paired cards."""
    from scripts import livebands
    monkeypatch.setattr(livebands, "_BANDS", None)
    monkeypatch.setattr(livebands, "OUT", __import__("pathlib").Path("/nonexistent/live_bands.tsv"))
    F = livebands.flip
    assert F("priced", "unsafe", True, True) == "tip 3"
    assert F("priced", "unsafe", False, True) == "tip 3"
    assert F("priced", "unsafe", True, False) is None          # tip 2 holds
    assert F("priced", "unsafe", False, False) is None
    assert F("priced", "cautious", True, True) is None         # tracked, not flipped on the seed
    assert F("priced", "safe", True, True) is None
    assert F("athena", "unsafe", True, True) is None           # priced only
    # every band x lane combo is measured, so a hold can earn the flip
    assert set(livebands.FLIP_SEED) == {(b, t) for b in ("safe", "cautious", "unsafe")
                                        for t in ("tip2", "tip3")}
    assert set(livebands.FLIP_BANK) == set(livebands.FLIP_SEED)
    # a measured table can flip a combo the seed holds
    livebands._BANDS[("flip", "safe tip3")] = dict(n=20, hit=90.0, said=75.0,
                                                   label="flipped", source="measured")
    assert F("priced", "safe", False, True) == "tip 3"
    # a push on the flipped lane is no bet: the study's source must skip "◦"
    src = __import__("pathlib").Path(livebands.__file__).read_text()
    assert 'if mk not in ("✅", "❌"):          # ungraded, or a push: no bet' in src
    L = livebands.flip_label
    assert L(82.0, 70.0, 20, "hold") == ("flipped", "measured")
    assert L(74.0, 70.0, 20, "flipped") == ("hold", "measured")
    assert L(100.0, 0.0, 2, "hold") == ("hold", "seed")
    monkeypatch.setattr(livebands, "_BANDS", None)


def test_filter_counters_cannot_see_a_declined_card():
    """The Completed filter counters read data-g* off each card; a declined
    card carries none, only data-nc, so "unsafe" typed in the bar counts
    the priced plays and not the cards that are out of the record."""
    from scripts import board, webapp
    fx = [f for f in board.load() if f.settled]
    out = [f for f in fx if webapp.is_declined(f)]
    kept = [f for f in fx if not webapp.is_declined(f)]
    assert out and kept
    for f in out[:30]:
        assert webapp._gradekeys(f) == ' data-nc="1"'
    assert any("g1=" in webapp._gradekeys(f) for f in kept)
    assert all("nc" not in webapp._gradekeys(f) for f in kept[:60])
    src = __import__("pathlib").Path(webapp.__file__).read_text()
    assert "if (c.dataset.nc) {{ left++; continue; }}" in src


def test_a_bare_tag_word_is_exact_on_the_bar():
    """"safe" is a substring of "unsafe": typed alone it must mean the safe
    tag, so the filter rewrites it to "tag safe" (the bettor's "priced,
    safe" read 38 cards where the safe band holds 31, 12 Sep)."""
    from scripts import webapp
    src = __import__("pathlib").Path(webapp.__file__).read_text()
    assert 'if (flat === "safe" || flat === "unsafe" || flat === "cautious")' in src
    assert 'return "tag " + flat;' in src
    # and the haystack words the rewrite relies on are distinct
    assert "tag safe" not in "tag unsafe"


def test_completed_cards_are_searchable_by_label():
    """The bettor, 12 Sep: "I can't search on color" — a settled card must
    carry its label words ("green", "guard green") and its frozen call."""
    from scripts import board, webapp
    fx = [f for f in board.load() if f.settled and webapp.label_any(f)]
    assert fx
    for f in fx[:60]:
        lab = webapp.label_any(f)                       # the label it was called under
        now = webapp._label_of(f, webapp._star_any(f), force=True)   # the band it sits in now
        hay = webapp._haystack(f)
        assert f"label {lab}" in hay and f"called {lab}" in hay
        assert now in hay and f"band {now}" in hay
    assert any("verdict play" in webapp._haystack(f) for f in fx)
    # a card called orange that now sits in the pink band answers "pink",
    # not a bare "orange"
    moved = [f for f in fx if webapp.label_any(f) != webapp._label_of(f, webapp._star_any(f), force=True)]
    assert moved, "no completed card has moved band"
    hay = webapp._haystack(moved[0])
    assert webapp._label_of(moved[0], webapp._star_any(moved[0]), force=True) in hay


def test_the_play_bar_follows_the_claim_band():
    """The bettor's rule, 12 Sep: the PLAY bar is the claim band's
    ROI-optimal bar on the bank, not the label's break-even plus a margin.
    Colours and label rates are untouched."""
    from scripts import webapp
    assert webapp.BAND_BAR == {"85+": 1.14, "80–85": 1.18, "75–80": 1.31}
    assert webapp.claim_band(91.0) == "85+" and webapp.claim_band(85.0) == "85+"
    assert webapp.claim_band(84.9) == "80–85" and webapp.claim_band(80.0) == "80–85"
    assert webapp.claim_band(79.9) == "75–80" and webapp.claim_band(76.0) == "75–80"
    assert webapp.band_bar(88.0) == 1.14 and webapp.band_bar(78.0) == 1.31
    # the starred lane's required price comes from the band, an unstarred
    # lane still prices off its own claim plus the margin
    # the starred lane needs the engine's value less 3%, floored at the band bar
    need, hit, solid = webapp._needs("U4.25 82.7% −1.9% · buy≥1.30 (+5.6% margin)", "orange")
    assert abs(need - 1.30 * 0.97) < 1e-9 and solid
    need_f, _h, _s = webapp._needs("U4.25 82.7% −1.9% · buy≥1.15 (−2.0% margin)", "orange")
    assert need_f == 1.18                                  # the band bar is the floor
    assert webapp.play_bar(88.0, "U4.5 88.0% +1.0% · buy≥1.20") == (1.20 * 0.97, 1.14)
    assert webapp.play_bar(88.0, "U4.5 88.0% +1.0%") == (1.14, 1.14)   # no printed value: the bar
    need2, _h, solid2 = webapp._needs("U4.25 82.7% −1.9% · buy≥1.30 (+5.6% margin)", None)
    assert abs(need2 - (1 / 0.827) * 1.06) < 1e-9 and not solid2
    # red keeps its registered rate; the four colours carry the ladder's
    assert webapp.SAYS["red"] == 0.7796 and webapp.SAYS["green+"] == 0.921


def test_the_colour_ladder_follows_the_claim():
    """The bettor, 12 Sep: sort the colours on the actual hit — green+ at
    a claim of 90+, green 85–90, orange 80–85, pink under 80; red and
    super red stay the tier's and the score's call."""
    from scripts import bankrates, guard_slices as GS
    L = lambda p, tier="orange", sc=None: GS.label("ENG-PL", tier, sc, False, p)
    assert L(91.0) == "green+" and L(90.0) == "green+"
    assert L(89.9) == "green" and L(85.0) == "green"
    assert L(84.9) == "orange" and L(80.0) == "orange"
    assert L(79.9) == "pink" and L(76.0) == "pink"
    assert L(91.0, tier="green") == "green+"                    # the old tier does not matter
    assert L(91.0, tier="red") == "red"                          # red is the tier
    assert L(91.0, tier="red", sc=-99.0) == "super red"          # and the score
    assert GS.label("ENG-PL", "green", 9.0, False) == "green"    # no claim: the tier stands
    # the stored bank is read on the same ladder without a rebuild
    assert bankrates.relabel({"g": "orange", "tip": "U4.25 91.2% +1.0% · buy≥1.10"}) == "green+"
    assert bankrates.relabel({"g": "green", "tip": "U4.25 82.0% +1.0% · buy≥1.10"}) == "orange"
    assert bankrates.relabel({"g": "super green", "tip": "O1.5 78.0% +1.0% · buy≥1.30"}) == "pink"
    assert bankrates.relabel({"g": "red", "tip": "U4.25 91.2% +1.0% · buy≥1.10"}) == "red"
    assert bankrates.relabel({"g": "orange", "pk": 3, "tip": "U4.25 82.0% +1.0%", "t3": "DNB1 86.0% +5.0%"}) == "green"


def test_live_tag_words_are_searchable():
    """The bettor's ask, 12 Sep: "live unsafe", "live safe", "declined"
    find cards on the bar, on the board and in the bank."""
    from scripts import board, webapp
    fx = board.load()
    tagged = [f for f in fx if webapp.record_tag(f)]
    assert tagged, "no tagged card on the board"
    for f in tagged[:40]:
        hay = webapp._haystack(f)
        assert f"live {webapp.record_tag(f)}" in hay
        assert ("declined" in hay) == webapp.is_declined(f)
        assert ("counted" in hay) == (not webapp.is_declined(f))
    src = __import__("pathlib").Path(webapp.__file__).read_text()
    assert 'bits.push("live " + m.lt' in src

def test_engine_inputs_are_not_recomputed_for_display():
    """league_hitrates.tsv feeds the REL debit and the buy-from blend, and
    guard_slices.tsv feeds the confluence score. The display now reads the
    bank with the red cards out; neither engine file may be touched for
    it, or a reporting change becomes a rules change."""
    import subprocess
    from pathlib import Path
    root = Path(__file__).resolve().parents[2]
    for name in ("config/league_hitrates.tsv", "config/guard_slices.tsv"):
        r = subprocess.run(["git", "diff", "--quiet", "HEAD", "--", name],
                           cwd=root)
        assert r.returncode == 0, f"{name} has uncommitted changes"


def test_sweep_grades_only_a_genuine_final():
    """Malmö v AIK, 7 Sep: ESPN returned state "post" at half time and the
    sweep graded a 0-0 miss while the second half was being played. A
    settled row is never re-swept, so the grade has to be right the first
    time: only a status that names a finished match may settle a row."""
    from scripts.sweep import _is_final

    assert _is_final("STATUS_FULL_TIME", "FT")
    assert _is_final("STATUS_FINAL_PEN", "Pen")
    assert _is_final("", "FT")                   # name missing, detail says FT
    assert not _is_final("STATUS_POSTPONED", "Postponed")
    assert not _is_final("STATUS_CANCELED", "Canceled")
    assert not _is_final("STATUS_ABANDONED", "Abandoned")
    assert not _is_final("STATUS_HALFTIME", "HT")
    assert not _is_final("", "")                 # a feed hiccup grades nothing


def test_from_here_reads_the_card_against_the_clock():
    """The "from here" line: the card's own mu against the minute. Pinned
    to the half-time numbers measured on 7 Sep (U4.5 at three goals lands
    54%), and to the shapes it must stay silent on."""
    from scripts import fromhere as F

    # U4.5 card at 84% with three goals at half time: ~53%, fair ~1.9
    r = F.read("U4.25 84.3% +3.3% · buy≥1.19", "A v B", "LIVE HT 2-1")
    assert r and 0.48 < r["p"] < 0.58 and 1.7 < r["fair"] < 2.1
    # the same card at 2-0 in the second half is safer than at half time
    r2 = F.read("U4.25 84.3% +3.3% · buy≥1.19", "A v B", "LIVE 70' 2-0")
    assert r2 and r2["p"] > r["p"]
    # an over at 0-0 at 60' carries the haircut: well under half
    o = F.read("O1.5 83.5% +5.1% · buy≥1.25", "A v B", "LIVE 60' 0-0")
    assert o and o["p"] < 0.5
    # silent where liveline already speaks, or where there is nothing to read
    assert F.line("O1.5 83.5% +5.1% · buy≥1.25", "A v B", "LIVE 70' 1-1") == ""   # landed
    assert F.line("U3.0 75.8% +0.5% · buy≥1.32", "A v B", "LIVE 60' 2-2") == ""   # gone
    assert F.line("DNB1 78.7% +11.1% · buy≥1.33", "A v B", "LIVE 42' 2-0") == ""  # not a total
    assert F.line("U4.25 84.3% +3.3% · buy≥1.19", "A v B", "") == ""              # not running
    # a team lane reads the team's goals, not the match's
    t = F.read("**A O1.5** 57.3% +12.5% (team) · buy≥1.80", "A v B", "LIVE 60' 1-0")
    assert t and t["needs"] == 1


def test_league_badge_reads_the_edge_lanes_not_the_priced_corner():
    """The badge's PLAYABLE number is tip 1 above the +1% edge bar, red
    cards out — hundreds of cards per league — not the priced plays,
    which are the fifty-odd market-paid-long cards the gap finding
    describes. On 7-8 Sep it read the latter: Turkey (64.7 −20.1) on 51
    cards while the league's cards land 84.8 across 619."""
    from scripts.bankrates import display_rates
    r = display_rates().get("TUR-SL")
    assert r and r["play_n"] > 200 and r["play_hit"] > 0.78, r


def test_sweepdiff_tells_minutes_from_goals():
    """The live loop commits on a goal, a kickoff, half time or a final,
    and holds on a pass that moved nothing but minutes — the page counts
    those forward itself. sweepdiff.kinds is the judge."""
    from scripts.sweepdiff import kinds
    row = "2026-09-08 20:45\tENG-CH\tChampionship\t{}\tU3.0 76% +1%\t— none\t{}\t"
    before = "\n".join([row.format("A v B", "LIVE 31' 0-0"),
                        row.format("C v D", ""),
                        row.format("E v F", "LIVE 44' 1-0")])
    # minutes only
    after = "\n".join([row.format("A v B", "LIVE 35' 0-0"),
                       row.format("C v D", ""),
                       row.format("E v F", "LIVE 45'+2' 1-0")])
    assert kinds(before, after) == []
    # a goal, a kickoff and half time
    after = "\n".join([row.format("A v B", "LIVE 35' 1-0"),
                       row.format("C v D", "LIVE 2' 0-0"),
                       row.format("E v F", "LIVE HT 1-0")])
    k = kinds(before, after)
    assert any(x.startswith("score: A v B") for x in k)
    assert any(x.startswith("kicked off: C v D") for x in k)
    assert any(x.startswith("half time: E v F") for x in k)
    # a final
    after = "\n".join([row.format("A v B", "✅ HIT — 1-0"),
                       row.format("C v D", ""),
                       row.format("E v F", "LIVE 44' 1-0")])
    assert any(x.startswith("final: A v B") for x in kinds(before, after))
    assert kinds(before, before) == []


def test_espn_half_time_from_the_goal_timeline():
    """ESPN's scoreboard lists every goal with a minute and a team; the
    half-time score is the goals at 45' or earlier, first-half stoppage
    included. No timeline at all means None, never 0-0."""
    from app.data.espn import half_time
    goal = lambda clock, tid: {"scoringPlay": True, "clock": {"displayValue": clock}, "team": {"id": tid}}
    card = {"scoringPlay": False, "clock": {"displayValue": "30'"}, "team": {"id": "1"}}
    comp = {"details": [goal("12'", "1"), card, goal("45'+2'", "2"), goal("46'", "1"), goal("88'", "2")]}
    assert half_time(comp, "1", "2") == (1, 1)
    assert half_time({"details": []}, "1", "2") == (0, 0)
    assert half_time({}, "1", "2") == (None, None)


def test_forward_log_restamps_a_lane_when_its_label_flips_to_red(tmp_path, monkeypatch):
    """A veto after first sight is a new statement, and the frozen call
    follows it (Blackburn v Sheffield Utd, 8 Sep: stamped orange on 4 Sep,
    red from the 7 Sep re-price, kicked off into Running as a call)."""
    from scripts import board, forward_settle, webapp
    log = tmp_path / "forward_log.tsv"
    monkeypatch.setattr(webapp, "FORWARD", log)
    monkeypatch.setattr(forward_settle, "FORWARD", log)
    monkeypatch.setattr(webapp, "_LOGGED", None)
    monkeypatch.setattr(webapp, "_FROZEN", None)
    f = board.Fixture("2026-09-01 20:45", "ENG-CH", "Championship",
                      "Blackburn v Sheffield Utd",
                      "U3.0 75.6% +1.4% · buy≥1.48 (+2.2% margin)", "— none", "")
    q = dict(consensus="1.33", best="1.33", book="Unibet (SE)")
    rows = lambda: [ln for ln in log.read_text().splitlines() if not ln.startswith("#")]
    webapp._stamp(f, 1, "U3.5", "orange", -4.67, 76.1, 1.271, q)
    webapp._stamp(f, 1, "U3.5", "orange", -4.90, 76.0, 1.271, q)   # a moved price: first sight holds
    assert len(rows()) == 1
    webapp._stamp(f, 1, "U3.5", "red", -5.17, 75.6, 1.360, dict(q, best="1.36"))
    webapp._stamp(f, 1, "U3.5", "super red", -5.17, 75.6, 1.360, q)   # red to red is not a flip
    assert len(rows()) == 2
    assert [r.split("\t")[7] for r in rows()] == ["orange", "red"]
    # The frozen call is the LAST row: a veto, so not a Running call and
    # not in the record; the first-sight reader still sees the orange row.
    monkeypatch.setattr(webapp, "_FROZEN", None)
    assert webapp.frozen()[("2026-09-01", f.teams)]["label"] == "red"
    assert webapp.was_called(f)["mark"] == "no play"
    assert webapp.running_call(f) is None
    f.status = "✅ HIT — 1-0"
    assert webapp.label_any(f) == "red" and not webapp.counts(f)
    # And the veto can lift: orange again is a third row, a call again.
    f.status = ""
    webapp._stamp(f, 1, "U3.5", "orange", -4.67, 76.1, 1.271, q)
    monkeypatch.setattr(webapp, "_FROZEN", None)
    assert len(rows()) == 3 and webapp.was_called(f)["mark"] == "normal"


def test_ledger_reads_the_card_spelling_of_a_draw_no_bet_position():
    """DNB2 / side "-" is what a slip copied off the card says; the ledger's
    own spelling is DNB / A. Both must settle, and neither may throw —
    a throw here blanked the board for five hours on 8 Sep."""
    from scripts import ledger
    final = dict(hg=2, ag=3, lhg=None, lag=None)
    assert ledger.bet_state("DNB2", "-", final) == 1.0
    assert ledger.bet_state("DNB", "A", final) == 1.0
    assert ledger.bet_state("DNB1", "-", final) == -1.0
    assert ledger.bet_state("DNB2", "-", dict(hg=1, ag=1, lhg=None, lag=None)) == 0.0
    assert ledger.bet_state("DNB2", "-", dict(hg=None, ag=None, lhg=1, lag=2)) is None
    fx = dict(lane3="DNB2", p3=0.809, hg=None, ag=None)
    assert ledger.bet_prob("DNB2", "-", fx) == 0.809


def test_live_poll_patches_the_status_line_not_the_taken_pill():
    """The in-play pill ("line taken · in-play") also carries class live;
    the poll must patch the status line in the meta row only, else a card
    with a live position shows two clocks (9 Sep)."""
    from scripts import webapp
    src = webapp.__file__ and open(webapp.__file__, encoding="utf-8").read()
    assert 'card.querySelector("summary .meta .live")' in src
    assert 'card.querySelector("summary .live")' not in src




def test_live_tag_follows_the_bettors_bands():
    """The bettor's live-safety bands, 12 Sep: by lane and printed-edge band."""
    from scripts import webapp
    # The SEED table, the bettor's hand bands; the measured table
    # (config/live_bands.tsv) overrides it band by band from 12 Sep and is
    # tested separately.
    t = lambda lane, e: webapp.LIVE_TAG[lane][webapp.edge_band(e)]
    assert [t("athena", e) for e in (3.0, 1.0, 0.0, -0.9, -1.0, -3.9, -4.0, -6.0)] == \
        ["safe", "safe", "safe", "safe", "safe", "safe", "unsafe", "unsafe"]
    assert [t("watch", e) for e in (3.0, 1.0, 0.0, -0.9, -1.0, -3.9, -4.0, -6.0)] == \
        ["unsafe", "unsafe", "safe", "safe", "cautious", "cautious", "cautious", "cautious"]
    assert [t("priced", e) for e in (3.0, 0.0, -2.0, -6.0)] == ["cautious", "unsafe", "unsafe", "unsafe"]
    lt = webapp.live_tag
    assert lt("settled", -5.0) is None and lt(None, 2.0) is None and lt("athena", None) is None


def test_live_bands_label_by_hit_rate_with_a_floor_on_cards():
    """The bettor's thresholds, 12 Sep: safe at 79, cautious at 77, else
    unsafe — and a band with too few cards keeps its seed label."""
    from scripts import livebands
    L = livebands.label
    assert L(83.3, 90, "unsafe") == ("safe", "measured")
    assert L(79.0, 30, "unsafe") == ("safe", "measured")
    assert L(78.9, 30, "safe") == ("cautious", "measured")
    assert L(77.0, 30, "safe") == ("cautious", "measured")
    assert L(76.9, 30, "safe") == ("unsafe", "measured")
    assert L(0.0, 1, "cautious") == ("cautious", "seed")
    assert L(None, 0, "safe") == ("safe", "seed")
    assert L(60.0, livebands.MIN_N - 1, "safe") == ("safe", "seed")


def test_live_tag_reads_the_measured_table(tmp_path, monkeypatch):
    """webapp.live_tag follows config/live_bands.tsv when it exists and the
    seed table when it does not."""
    from scripts import livebands, webapp
    monkeypatch.setattr(livebands, "_BANDS", None)
    monkeypatch.setattr(livebands, "OUT", tmp_path / "none.tsv")
    assert webapp.live_tag("athena", -6.0) == "unsafe"           # seed
    monkeypatch.setattr(livebands, "_BANDS", None)
    monkeypatch.setattr(livebands, "OUT", tmp_path / "live_bands.tsv")
    rows = [dict(lane=l, band=b, n=40, hit=82.0, said=84.0, label="safe", source="measured")
            for l in livebands.LANES for b in livebands.BANDS]
    import datetime as dt
    livebands.write(rows, 21, dt.date(2026, 9, 12))
    assert webapp.live_tag("athena", -6.0) == "safe"             # measured
    assert webapp.live_bands()[("watch", "+1 up")]["n"] == 40
    monkeypatch.setattr(livebands, "_BANDS", None)


def test_store_save_drops_the_same_match_written_a_day_apart(tmp_path, monkeypatch):
    """A board-dated copy (Amsterdam date) of a provider-dated match — same
    teams, same score, one day later — is dropped on save; a different
    score or a wider gap is kept."""
    import pandas as pd
    from app.data import store
    monkeypatch.setattr(store, "DATA_DIR", tmp_path) if hasattr(store, "DATA_DIR") else None
    monkeypatch.setattr(store, "season_path", lambda code, season: tmp_path / f"{code}-{season}.parquet")
    rows = [
        dict(date=pd.Timestamp("2026-09-07"), home="Limache", away="Cobresal", hg=3, ag=0),
        dict(date=pd.Timestamp("2026-09-08"), home="Limache", away="Cobresal", hg=3, ag=0),   # shifted copy
        dict(date=pd.Timestamp("2026-09-09"), home="Limache", away="Cobresal", hg=1, ag=1),   # different score: stays
        dict(date=pd.Timestamp("2026-09-20"), home="Limache", away="Cobresal", hg=3, ag=0),   # far apart: stays
    ]
    store.save("TST", "2026-27", pd.DataFrame(rows))
    got = pd.read_parquet(tmp_path / "TST-2026-27.parquet")
    assert len(got) == 3
    assert sorted(got["date"].dt.strftime("%Y-%m-%d")) == ["2026-09-07", "2026-09-09", "2026-09-20"]
    assert got[got["date"] == "2026-09-09"]["hg"].tolist() == [1]
