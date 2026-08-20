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
