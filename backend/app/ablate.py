"""
Module ablation — measure what each engine module is actually worth.

The engine carries a stack of named modules, each of which was added because it
seemed reasonable. This asks a harder question of every one of them: if it were
switched off, would the hit rate get worse?

Method: replay a league with all modules on to get a baseline, then replay it
again with exactly one module disabled. The difference is that module's
contribution on those matches.

    contribution = hit_rate(all on) - hit_rate(one off)

    positive -> the module earns its place
    ~zero    -> the module changes nothing measurable
    negative -> the module is COSTING accuracy and should go

Features are computed once and shared across every variant, so a full ablation
over nine leagues is seconds of work rather than minutes.

As with calibration, results are reported on a chronological holdout as well as
in-sample, because a module that helps on the matches you tuned against is not
the same as a module that helps.
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

from app.calibrate import CALIB_MIN_MATCHES, ReplayResult, _requests_for, replay
from app.data import config
from app.engine.types import ModuleFlags

# Every toggleable module, in the order they run in the pipeline.
MODULES = list(ModuleFlags().model_dump().keys())


@dataclass
class ModuleEffect:
    module: str
    baseline: float
    without: float
    contribution: float      # baseline - without; positive means it helps
    changed: int             # how many predictions the module altered
    sample: int

    @property
    def verdict(self) -> str:
        if self.changed == 0:
            return "inert"
        if self.contribution > 0.005:
            return "helps"
        if self.contribution < -0.005:
            return "HURTS"
        return "neutral"


def _changed_count(base: ReplayResult, variant: ReplayResult) -> int:
    """How many fixtures got a different market when the module was disabled."""
    b = {(o.match_date, o.home, o.away): o.market for o in base.outcomes}
    return sum(
        1 for o in variant.outcomes
        if b.get((o.match_date, o.home, o.away)) not in (None, o.market)
    )


def ablate_league(
    league_code: str,
    season: Optional[str] = None,
    min_matches: int = CALIB_MIN_MATCHES,
    pairs=None,
) -> tuple[float, int, list[ModuleEffect]]:
    """
    Ablate every module for one league.
    Returns (baseline_hit_rate, sample, effects sorted worst-first).
    """
    cfg = config.get(league_code)
    pairs = pairs if pairs is not None else _requests_for(
        league_code, None, season, min_matches
    )
    if not pairs:
        return 0.0, 0, []

    base = replay(league_code, cfg, _pairs=pairs,
                  module_flags=ModuleFlags.all_on())
    if base.sample == 0:
        return 0.0, 0, []

    effects: list[ModuleEffect] = []
    for name in MODULES:
        flags = ModuleFlags.all_on()
        setattr(flags, name, False)
        variant = replay(league_code, cfg, _pairs=pairs, module_flags=flags)
        effects.append(ModuleEffect(
            module=name,
            baseline=base.hit_rate,
            without=variant.hit_rate,
            contribution=base.hit_rate - variant.hit_rate,
            changed=_changed_count(base, variant),
            sample=base.sample,
        ))

    effects.sort(key=lambda e: e.contribution)
    return base.hit_rate, base.sample, effects


def ablate_many(
    league_codes: list[str],
    min_matches: int = CALIB_MIN_MATCHES,
    progress=None,
) -> tuple[dict[str, tuple[float, int, list[ModuleEffect]]], dict[str, dict]]:
    """
    Ablate several leagues and aggregate each module's effect across all of them.

    The aggregate is what matters for a prune decision: a module that helps one
    league and hurts two others is not carrying its weight.
    """
    say = progress or (lambda _m: None)
    per_league: dict[str, tuple[float, int, list[ModuleEffect]]] = {}
    totals: dict[str, dict] = {
        m: {"weighted": 0.0, "sample": 0, "changed": 0,
            "helps": 0, "hurts": 0, "neutral": 0, "inert": 0}
        for m in MODULES
    }

    for code in league_codes:
        say(f"ablating {code}…")
        rate, sample, effects = ablate_league(code, min_matches=min_matches)
        if sample == 0:
            continue
        per_league[code] = (rate, sample, effects)
        for e in effects:
            t = totals[e.module]
            t["weighted"] += e.contribution * e.sample
            t["sample"] += e.sample
            t["changed"] += e.changed
            t[e.verdict.lower()] = t.get(e.verdict.lower(), 0) + 1

    for m, t in totals.items():
        t["mean_contribution"] = t["weighted"] / t["sample"] if t["sample"] else 0.0

    return per_league, totals
