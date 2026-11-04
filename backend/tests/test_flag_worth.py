"""
Scoring a tracked flag — and not being fooled by which cards it lands on.

One flag exists purely to be scored later: the league cross-check
(31 Oct), frozen at kickoff so each row is a real prediction. The band
check was the first, and this script is what retired it on 1 Nov — its
veto scored +0.96 against the tier it fired on, so it was describing
which cards it landed on, not how they finished.

THE TRAP THIS EXISTS TO AVOID: a flag's words are not spread evenly
across tiers. A veto lands far more often on a weak card than on a
STRONG one, so the raw rate behind "veto" mostly reports WHICH CARDS
GET VETOED. Scored that way a word can look damning while adding
nothing — which is exactly what the band check's veto turned out to
be: 69.2% raw against a tier that already predicted 72.3%.
"""
import pytest

from scripts import flag_worth as fw


def _r(stratum, call, hit, live=True, date="2026-10-15"):
    return dict(date=date, fixture=f"{stratum}-{call}-{hit}",
                stratum=stratum, call=call, live=live, hit=hit)


# ── the stratification, which is the whole point ──────────────────────────

def test_a_word_that_only_lands_on_weak_cards_scores_zero():
    """The band veto's actual shape: it looks bad, and it is only
    reporting that weak cards get vetoed."""
    rs = ([_r("weak", "veto", i < 7) for i in range(10)]      # 70%
          + [_r("weak", "ok", i < 7) for i in range(10)]      # 70%
          + [_r("strong", "ok", i < 9) for i in range(10)])   # 90%
    s = fw.score(rs, "veto")
    assert s["rate"] == pytest.approx(70.0)
    assert s["predicted"] == pytest.approx(70.0)
    assert abs(s["z"]) < 1.0, "a word that matches its stratum adds nothing"


def test_a_word_that_is_worse_than_its_own_stratum_scores_negative():
    rs = ([_r("weak", "veto", i < 3) for i in range(10)]      # 30%
          + [_r("weak", "ok", i < 8) for i in range(20)]      # 40%... 8/20
          + [_r("strong", "ok", i < 18) for i in range(20)])
    s = fw.score(rs, "veto")
    assert s["rate"] < s["predicted"]
    assert s["z"] < 0


def test_the_prediction_leaves_the_word_out():
    """A word must not help set the rate it is judged against,
    otherwise one that is most of a stratum can never look unusual."""
    rs = ([_r("t", "veto", False) for _ in range(20)]
          + [_r("t", "ok", i < 14) for i in range(15)])      # others 93.3%
    s = fw.score(rs, "veto")
    assert s["predicted"] == pytest.approx(100 * 14 / 15, abs=0.1), \
        "the 20 vetoes must be judged against the 15 others alone"
    assert s["rate"] == 0.0


def test_a_unanimous_comparison_group_is_reported_not_dropped():
    """No variance means no z — but a word that vanishes from the report
    reads as a word that was fine."""
    rs = ([_r("t", "veto", False) for _ in range(20)]
          + [_r("t", "ok", True) for _ in range(15)])
    s = fw.score(rs, "veto")
    assert s is not None and s["z"] is None
    assert s["rate"] == 0.0 and s["predicted"] == pytest.approx(100.0)


def test_a_stratum_with_too_few_others_is_skipped():
    rs = [_r("t", "veto", True) for _ in range(9)] + [_r("t", "ok", False)]
    assert fw.score(rs, "veto") is None


def test_a_word_nobody_used_scores_nothing():
    rs = [_r("t", "ok", True) for _ in range(30)]
    assert fw.score(rs, "veto") is None


# ── backfilled rows ────────────────────────────────────────────────────────

def test_backfilled_rows_are_left_out_by_default(tmp_path, monkeypatch):
    """The band log's earliest rows were reconstructed as-of rather than
    read at the time, on a median of six of his own bets. They are not a
    prediction anybody could have acted on, and including them flips the
    veto from 69% to 92%."""
    log = tmp_path / "f.tsv"
    log.write_text(
        "# head\n"
        "2026-10-01\tA v B\tENG-PL\tp\t9\t50.0\t90\t85.0\t-35.0\tveto\tas-of\n"
        "2026-10-02\tC v D\tENG-PL\tp\t9\t50.0\t90\t85.0\t-35.0\tveto\t2026-10-02 10:00\n",
        encoding="utf-8")
    spec = dict(fw.LOGS["league"], path=log)
    monkeypatch.setattr(fw, "results", lambda: {("2026-10-01", "A v B"): True,
                                                ("2026-10-02", "C v D"): True})
    assert [r["date"] for r in fw.rows(spec)] == ["2026-10-02"]
    assert len(fw.rows(spec, include_backfill=True)) == 2


def test_only_settled_rows_are_scored(tmp_path, monkeypatch):
    log = tmp_path / "f.tsv"
    log.write_text(
        "# head\n"
        "2026-10-02\tC v D\tENG-PL\tp\t9\t50.0\t90\t85.0\t-35.0\tveto\t2026-10-02 10:00\n"
        "2026-12-30\tE v F\tENG-PL\tp\t9\t50.0\t90\t85.0\t-35.0\tveto\t2026-10-02 10:00\n",
        encoding="utf-8")
    spec = dict(fw.LOGS["league"], path=log)
    monkeypatch.setattr(fw, "results", lambda: {("2026-10-02", "C v D"): True})
    assert len(fw.rows(spec)) == 1


def test_a_push_counts_as_a_hit():
    """The project's convention everywhere."""
    assert "◦" in fw.HIT_MARKS and "✅" in fw.HIT_MARKS


# ── both logs are wired, and read at the right columns ────────────────────

def test_the_retired_band_check_is_not_scorable_any_more():
    """It was dumped on 1 Nov after this script showed its veto carried
    nothing; the log went with it. A stale entry here would read a file
    that no longer exists and report an empty flag as a healthy one."""
    assert "band" not in fw.LOGS
    assert not (fw.ROOT / "config" / "tier_band_log.tsv").exists()


def test_every_wired_flag_can_be_scored():
    assert set(fw.LOGS) == {"league"}
    for name, spec in fw.LOGS.items():
        assert spec["call"] < spec["ncols"] and spec["read_at"] < spec["ncols"]
        assert spec["stratum"] < spec["call"]


def test_the_columns_match_the_real_headers():
    """A column index off by one would score the wrong field and still
    print a confident answer."""
    import re
    for name, spec in fw.LOGS.items():
        if not spec["path"].exists():
            continue
        head = [ln for ln in spec["path"].read_text(encoding="utf-8")
                .splitlines() if ln.startswith("# date")]
        if not head:
            continue
        cols = [c.strip("# ") for c in head[0].split("\t")]
        assert cols[spec["call"]] == "call", (name, cols[spec["call"]])
        assert cols[spec["read_at"]] == "read_at", name
        assert len(cols) == spec["ncols"], name
        assert cols[spec["stratum"]] in ("tier", "profile"), name
