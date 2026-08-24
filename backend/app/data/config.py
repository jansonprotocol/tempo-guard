"""
League configuration — the git-native replacement for the `league_configs` and
`team_configs` database tables.

Calibration's whole purpose is to tune a handful of per-league dials until the
hit rate improves. Those dials now live in a single JSON file:

    config/leagues.json

Storing them as JSON rather than database rows means every calibration run
produces a reviewable diff, tuning history is in git, and the engine needs no
database to run. `LeagueConfig` is a plain dataclass with the same field names
the engine already expects, so `evaluate_athena` is untouched.
"""
from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Optional

_REPO_ROOT = Path(__file__).resolve().parents[3]
CONFIG_DIR = Path(os.environ.get("ATHENA_CONFIG_DIR", _REPO_ROOT / "config"))
LEAGUES_FILE = CONFIG_DIR / "leagues.json"


@dataclass
class LeagueConfig:
    """
    Per-league tuning dials.

    Calibration adjusts `base_over_bias` / `base_under_bias` (and optionally
    `tempo_factor`); everything else is either descriptive or a sensitivity
    multiplier applied to the DEG/DET/EPS module inputs.
    """
    league_code: str
    name: str = ""

    # ── Calibration dials (0.0–1.0, neutral at 0.5) ───────────────────
    base_over_bias:  float = 0.5
    base_under_bias: float = 0.5
    tempo_factor:    float = 0.50

    # ── Module sensitivity multipliers (1.0 = neutral) ────────────────
    deg_sensitivity: float = 1.0
    det_sensitivity: float = 1.0
    eps_sensitivity: float = 1.0

    # ── Confidence shaping ────────────────────────────────────────────
    confidence_scale: float = 1.0
    confidence_floor: float = 0.60
    min_confidence:   float = 0.0

    # ── Cross-league strength (used for cup fixtures) ─────────────────
    strength_coefficient: float = 1.0

    # ── Scoring norm, used to judge how unusual a fixture is ──────────
    # Measured from stored results by `athena lanes --recalc`. The sharp lane
    # compares a fixture's goal expectation against these, so "high scoring"
    # means high *for this league* — 2.4 goals is routine in Serie A and
    # notably quiet in the Bundesliga.
    goal_mean: float = 2.70
    goal_std:  float = 1.65

    # The sharp lane standardises a fixture's goal *expectation*, which is a
    # rolling average and far less variable than individual results. Comparing
    # it against goal_std (the spread of actual totals, ~1.65) crushed every
    # z-score into a range that never reached the trigger. These are mu's own
    # mean and spread, measured per league.
    mu_mean: float = 2.70
    mu_std:  float = 0.45

    # ── Per-team over nudges: {team_name: nudge} ──────────────────────
    # Loosest under line worth offering in this league, and tightest over.
    # None means no restriction.
    #
    # Not derivable from the goal data, which is why it is config rather than a
    # rule. Italian Serie B averages 2.51 goals and Serie A 2.55 — four
    # hundredths apart — yet the loose under rungs are reportedly unplayable in
    # one and fine in the other. An automatic cap on distance-from-mean cannot
    # separate leagues that are statistically identical, and when tried it
    # stripped U4.25 across the whole 2.5-goal cluster and cost 0.53 points of
    # edge in the leagues that did not need it.
    #
    # So this carries a judgement about market prices that the engine has no
    # access to, made by whoever actually places the bets.
    # ── Dials that actually reach the market selector ─────────────────
    # Since prob_select took over, the published market comes from
    # market_select.choose(mu, league_mu, max_under, min_over) at a probability
    # floor. tempo_factor, bias and the module flags feed the flowchart that
    # call overrides, so they no longer change a tip. These four do.
    min_win_prob: Optional[float] = None     # per-league floor; None = global 0.79
    use_possession: bool = False             # apply the fitted possession shift
    # Season stage lifts the goal expectation in a campaign's closing stretch.
    # Unlike every other feature dial this describes what the match is WORTH
    # rather than how good the sides are, so it is the one input the goal model
    # does not already carry in some form.
    #
    # ON by default, which no other feature dial here has earned. It is inert
    # for the first 92% of a season and changes nothing there; across the 9% of
    # fixtures in a closing stretch it took 81.3% to 82.7%, rescuing 47 bets and
    # breaking 30. The net stayed positive at every shift from 0.05 to 0.30 and
    # in both halves of the sample, so it is not a constant fitted to its own
    # data — see scripts/season_stage_validate.py for the full ledger and for
    # the honest weakness, which is that older seasons contributed +4 of the +17.
    use_season_stage: bool = True
    # How far the modules' net opinion may move the goal expectation, in goals
    # per unit of lean score. Zero disconnects them, which is what they have
    # effectively been since probability selection took over the market choice:
    # toggling any of burst_sentinel, det, ulr, deg or mfr changes zero markets
    # out of 998. Non-zero is the only route by which they can reach a tip.
    module_mu_scale: float = 0.0
    max_under_line: Optional[float] = None
    min_over_line: Optional[float] = None

    team_nudges: dict[str, float] = field(default_factory=dict)

    # ── Module overrides: {module_name: bool} ─────────────────────────
    # Sparse — only modules that differ from the measured defaults in
    # engine.types.ModuleFlags need an entry. These are the highest-leverage
    # calibration dials available, because a toggle changes market selection
    # outright rather than nudging a score.
    module_overrides: dict[str, bool] = field(default_factory=dict)

    # ── Calibration bookkeeping ───────────────────────────────────────
    last_calibrated: Optional[str] = None
    last_hit_rate:   Optional[float] = None
    last_sample:     Optional[int] = None

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict) -> "LeagueConfig":
        known = {f for f in cls.__dataclass_fields__}
        return cls(**{k: v for k, v in d.items() if k in known})


# ── Load / save ───────────────────────────────────────────────────────────────

_CACHE: Optional[dict[str, LeagueConfig]] = None


def load_all(refresh: bool = False) -> dict[str, LeagueConfig]:
    global _CACHE
    if _CACHE is not None and not refresh:
        return _CACHE

    if LEAGUES_FILE.exists():
        raw = json.loads(LEAGUES_FILE.read_text(encoding="utf-8"))
        _CACHE = {code: LeagueConfig.from_dict(d) for code, d in raw.items()}
    else:
        _CACHE = {}
    return _CACHE


def get(league_code: str) -> LeagueConfig:
    """Fetch a league's config, falling back to neutral defaults."""
    cfgs = load_all()
    if league_code in cfgs:
        return cfgs[league_code]
    return LeagueConfig(league_code=league_code)


def save(cfg: LeagueConfig) -> None:
    """Persist one league's config back to config/leagues.json."""
    cfgs = load_all()
    cfgs[cfg.league_code] = cfg
    save_all(cfgs)


def save_all(cfgs: dict[str, LeagueConfig]) -> Path:
    global _CACHE
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    payload = {code: c.to_dict() for code, c in sorted(cfgs.items())}
    LEAGUES_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    _CACHE = cfgs
    return LEAGUES_FILE
