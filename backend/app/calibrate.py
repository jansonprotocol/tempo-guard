"""
Calibration — tune a league's dials until its hit rate improves.

Implements the calibration flow end to end:

    1. fetch that league's matches from before the cutoff date
    2. retro-simulate every one of them through the engine
    3. report the results and the resulting hit rate
    4. search for the dial settings that raise it
    5. re-run the simulation on the winning settings to verify the gain

Everything runs against stored parquet snapshots, so a full league replay is
fast and completely offline.

WHY THE REPLAY IS HONEST
========================
Each match is predicted using `asof_features`, which reads only matches
*strictly before* that match's date. A replay of the 2025-26 season therefore
reproduces what the engine would have said on the morning of each fixture — no
lookahead, no fitting on the outcome being scored.

The search is a grid over the dials that actually move predictions:

    bias_shift    net over/under pressure, applied as base_over_bias /
                  base_under_bias around the neutral 0.5 point
    tempo_factor  scales the raw tempo signal

Candidate settings are scored on the same replayed matches, and the winner is
re-verified on a fresh replay before anything is written to config/leagues.json.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date
from typing import Callable, Iterable, Optional

import pandas as pd

from app.data import config, features, store
from app.data.config import LeagueConfig
from app.predict import predict_fixture, build_request
from app.engine.types import MatchRequest, ModuleFlags
from app.util.asian_lines import evaluate_market, hit_weight

# Minimum matches of history before a fixture can be replayed. Lower than the
# prediction default so early-season fixtures are not all skipped.
CALIB_MIN_MATCHES = 4


# ── Result containers ─────────────────────────────────────────────────────────

@dataclass
class MatchOutcome:
    match_date: date
    home: str
    away: str
    market: str
    lean: str
    confidence: float
    total_goals: int
    hit: bool

    def as_row(self) -> dict:
        return {
            "date": self.match_date, "home": self.home, "away": self.away,
            "market": self.market, "lean": self.lean,
            "confidence": self.confidence, "goals": self.total_goals,
            "hit": self.hit,
        }


@dataclass
class ReplayResult:
    league_code: str
    outcomes: list[MatchOutcome] = field(default_factory=list)
    skipped: int = 0

    @property
    def sample(self) -> int:
        return len(self.outcomes)

    @property
    def hits(self) -> int:
        return sum(1 for o in self.outcomes if o.hit)

    @property
    def hit_rate(self) -> float:
        return self.hits / self.sample if self.sample else 0.0

    def by_market(self) -> dict[str, tuple[int, int]]:
        """market -> (hits, sample)"""
        out: dict[str, list[int]] = {}
        for o in self.outcomes:
            slot = out.setdefault(o.market, [0, 0])
            slot[0] += int(o.hit)
            slot[1] += 1
        return {k: (v[0], v[1]) for k, v in sorted(out.items())}

    def by_lean(self) -> dict[str, tuple[int, int]]:
        out: dict[str, list[int]] = {}
        for o in self.outcomes:
            slot = out.setdefault(o.lean, [0, 0])
            slot[0] += int(o.hit)
            slot[1] += 1
        return {k: (v[0], v[1]) for k, v in sorted(out.items())}

    def to_frame(self) -> pd.DataFrame:
        return pd.DataFrame([o.as_row() for o in self.outcomes])


# ── Replay ────────────────────────────────────────────────────────────────────

def _requests_for(
    league_code: str,
    before: Optional[date],
    season: Optional[str],
    min_matches: int,
    limit: Optional[int] = None,
) -> list[tuple[MatchRequest, int]]:
    """
    Build (request, total_goals) pairs for every replayable match.

    Feature resolution is the expensive part of a replay and does not depend on
    the dials being tuned, so it is done once here and reused for every
    candidate setting in the search.
    """
    df = store.load_results(league_code, season)
    if df.empty:
        return []

    if before is not None:
        df = df[df["date"].dt.date < before]

    # Trim before building features, not after. Slicing the result instead
    # meant computing features for a league's entire history and discarding
    # all but the tail — for England that is 9,880 fixtures to keep 400.
    if limit:
        df = df.tail(limit)

    pairs: list[tuple[MatchRequest, tuple[int, int]]] = []
    for _, row in df.iterrows():
        mdate = row["date"].date()
        req = build_request(
            league_code, str(row["home"]), str(row["away"]), mdate,
            min_matches=min_matches,
        )
        if req is None:
            continue
        pairs.append((req, (int(row["hg"]), int(row["ag"]))))
    return pairs


def replay(
    league_code: str,
    cfg: Optional[LeagueConfig] = None,
    before: Optional[date] = None,
    season: Optional[str] = None,
    min_matches: int = CALIB_MIN_MATCHES,
    _pairs: Optional[list[tuple[MatchRequest, tuple[int, int]]]] = None,
    module_flags: Optional["ModuleFlags"] = None,
) -> ReplayResult:
    """
    Retro-simulate every completed match for a league and grade the calls.

    `before` restricts the replay to matches before a date (defaults to all
    stored results). `_pairs` lets the search reuse precomputed features.
    """
    cfg = cfg or config.get(league_code)
    pairs = _pairs if _pairs is not None else _requests_for(
        league_code, before, season, min_matches
    )

    result = ReplayResult(league_code=league_code)

    for req, (hg, ag) in pairs:
        pred = predict_fixture(req, cfg, module_flags=module_flags)
        market = pred.translated_play.market
        w = hit_weight(evaluate_market(market, hg, ag))
        if w < 0:
            # Unrecognised market — excluded rather than scored as a loss.
            result.skipped += 1
            continue

        result.outcomes.append(MatchOutcome(
            match_date=req.match_date,
            home=req.home_team,
            away=req.away_team,
            market=market,
            lean=pred.corridor.lean,
            confidence=pred.confidence_score,
            total_goals=hg + ag,
            hit=bool(w >= 1.0),
        ))

    return result


# ── Search ────────────────────────────────────────────────────────────────────

# Net bias shift applied around neutral. The engine scales this by BIAS_SCALE
# (0.12), so ±0.5 here is the practical full range.
BIAS_SHIFTS = [round(x * 0.05, 2) for x in range(-10, 11)]     # -0.50 .. +0.50
TEMPO_FACTORS = [round(0.40 + x * 0.02, 2) for x in range(0, 16)]  # 0.40 .. 0.70


def _with_dials(cfg: LeagueConfig, bias_shift: float, tempo: float) -> LeagueConfig:
    """Clone a config with a net bias shift and tempo factor applied."""
    from copy import deepcopy
    c = deepcopy(cfg)
    c.base_over_bias = round(min(1.0, max(0.0, 0.5 + bias_shift / 2)), 3)
    c.base_under_bias = round(min(1.0, max(0.0, 0.5 - bias_shift / 2)), 3)
    c.tempo_factor = tempo
    return c


# Fraction of a league's matches used to search dials; the remainder is held
# back to verify the winner on fixtures the search never saw.
HOLDOUT_FRACTION = 0.30

# Guards against writing a dial change that is really just noise. A holdout of
# 30 matches moves ~3.3 percentage points per match, so a "+7%" gain there can
# be two lucky results. Both thresholds must be cleared before anything is
# written to config.
MIN_HOLDOUT_SAMPLE = 50
MIN_IMPROVEMENT = 0.02


@dataclass
class CalibrationReport:
    league_code: str
    baseline_hit_rate: float
    baseline_sample: int
    best_hit_rate: float
    best_sample: int
    verified_hit_rate: float
    improvement: float
    bias_shift: float
    tempo_factor: float
    applied: bool
    baseline: ReplayResult
    verified: ReplayResult
    candidates_tried: int
    # Holdout validation — the honest test of whether a gain generalises.
    train_baseline: float = 0.0
    train_best: float = 0.0
    holdout_baseline: float = 0.0
    holdout_tuned: float = 0.0
    holdout_sample: int = 0
    generalises: bool = False


def calibrate(
    league_code: str,
    before: Optional[date] = None,
    season: Optional[str] = None,
    apply: bool = False,
    min_matches: int = CALIB_MIN_MATCHES,
    progress: Optional[Callable[[str], None]] = None,
    bias_shifts: Optional[Iterable[float]] = None,
    tempo_factors: Optional[Iterable[float]] = None,
) -> CalibrationReport:
    """
    Full calibration cycle for one league.

    Set `apply=True` to write the winning dials into config/leagues.json.
    """
    say = progress or (lambda _m: None)
    cfg = config.get(league_code)

    # ── 1–3. Baseline replay ──────────────────────────────────────────
    say(f"Loading {league_code} matches and computing as-of features…")
    pairs = _requests_for(league_code, before, season, min_matches)
    if not pairs:
        raise ValueError(
            f"No replayable matches for {league_code}. "
            f"Run `athena data load` first, or widen the date range."
        )

    say(f"Replaying {len(pairs)} matches at current settings…")
    baseline = replay(league_code, cfg, _pairs=pairs)

    # ── Chronological train/holdout split ─────────────────────────────
    # Split by date, not at random: tuning on the past and testing on the
    # future is how the engine is actually used, and it prevents a dial set
    # from being validated on the very matches that selected it.
    pairs_sorted = sorted(pairs, key=lambda p: p[0].match_date)
    cut = int(len(pairs_sorted) * (1 - HOLDOUT_FRACTION))
    train_pairs, holdout_pairs = pairs_sorted[:cut], pairs_sorted[cut:]

    train_baseline = replay(league_code, cfg, _pairs=train_pairs)
    holdout_baseline = replay(league_code, cfg, _pairs=holdout_pairs)

    # ── 4. Search for better dials — on the training split only ───────
    shifts = list(bias_shifts) if bias_shifts is not None else BIAS_SHIFTS
    tempos = list(tempo_factors) if tempo_factors is not None else TEMPO_FACTORS
    say(f"Searching {len(shifts)}×{len(tempos)} dial combinations on "
        f"{len(train_pairs)} training matches…")

    best_rate = train_baseline.hit_rate
    best_shift, best_tempo = 0.0, cfg.tempo_factor
    best_sample = train_baseline.sample
    tried = 0

    for shift in shifts:
        for tempo in tempos:
            tried += 1
            trial = replay(league_code, _with_dials(cfg, shift, tempo), _pairs=train_pairs)
            # Require a real sample so a tiny high-rate subset cannot win.
            if trial.sample < max(20, train_baseline.sample * 0.5):
                continue
            if trial.hit_rate > best_rate:
                best_rate, best_shift, best_tempo = trial.hit_rate, shift, tempo
                best_sample = trial.sample

    # ── 5. Verify on the holdout the search never saw ─────────────────
    best_cfg = _with_dials(cfg, best_shift, best_tempo)
    say(f"Verifying (bias_shift={best_shift:+.2f}, tempo={best_tempo:.2f}) on "
        f"{len(holdout_pairs)} unseen matches…")
    holdout_tuned = replay(league_code, best_cfg, _pairs=holdout_pairs)
    verified = replay(league_code, best_cfg, _pairs=pairs)

    # A gain counts only if it survives on matches the search never saw, by a
    # margin large enough not to be a couple of lucky results.
    improvement = holdout_tuned.hit_rate - holdout_baseline.hit_rate
    generalises = (
        improvement >= MIN_IMPROVEMENT
        and holdout_tuned.sample >= MIN_HOLDOUT_SAMPLE
    )

    # ── Apply — only when the improvement generalises ─────────────────
    applied = False
    if apply and generalises:
        best_cfg.last_calibrated = date.today().isoformat()
        best_cfg.last_hit_rate = round(holdout_tuned.hit_rate, 4)
        best_cfg.last_sample = holdout_tuned.sample
        config.save(best_cfg)
        applied = True

    return CalibrationReport(
        league_code=league_code,
        baseline_hit_rate=baseline.hit_rate,
        baseline_sample=baseline.sample,
        best_hit_rate=best_rate,
        best_sample=best_sample,
        verified_hit_rate=verified.hit_rate,
        improvement=improvement,
        bias_shift=best_shift,
        tempo_factor=best_tempo,
        applied=applied,
        baseline=baseline,
        verified=verified,
        candidates_tried=tried,
        train_baseline=train_baseline.hit_rate,
        train_best=best_rate,
        holdout_baseline=holdout_baseline.hit_rate,
        holdout_tuned=holdout_tuned.hit_rate,
        holdout_sample=holdout_tuned.sample,
        generalises=generalises,
    )
