"""
Calibration proposals — search what the engine can toggle, recommend nothing
blindly.

`calibrate --apply` writes straight to config. This does the same search but
stops short of the write: it produces a reviewable list of edits, each with the
gain it earned and the sample that earned it, for a human to accept or reject.

WHAT CAN ACTUALLY BE TOGGLED
============================
In rough order of leverage, measured rather than assumed:

  module flags     nine on/off switches. The highest-leverage dials available,
                   because flipping one changes market selection outright.
                   Ablation found burst_sentinel worth -1.91% and det -0.45%
                   as global defaults, but that is an average — a league can
                   legitimately disagree with the global verdict.

  bias_shift       net over/under pressure. Low leverage: the engine scales it
                   by 0.12, so the full range moves support_delta by +/-0.06
                   and rarely flips a market.

  tempo_factor     scales the raw tempo signal before every threshold sees it.

EVERY GAIN IS HOLDOUT-VALIDATED
===============================
A search over ~30 candidates against a few hundred matches will always find
something that looks better in-sample. Each candidate is therefore scored on a
chronological training split and re-scored on matches it never saw; only the
holdout number is reported as the gain, and candidates are accepted greedily
only while the holdout keeps improving.

This is deliberately conservative. The expected outcome for most leagues is an
empty proposal, and that is a useful answer rather than a failure.
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from datetime import date
from pathlib import Path
from typing import Any, Callable, Iterable, Optional

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
from app.data.config import CONFIG_DIR, LeagueConfig
from app.engine.types import ModuleFlags

PROPOSALS_FILE = CONFIG_DIR / "proposals.json"

# Fraction of a league's matches held back from the search entirely.
HOLDOUT_FRACTION = 0.30

# A candidate must clear both to be proposed: enough unseen matches to mean
# something, and a margin wide enough not to be two lucky results.
MIN_HOLDOUT_SAMPLE = 40
MIN_GAIN = 0.015

BIAS_SHIFTS = [round(x * 0.1, 2) for x in range(-5, 6)]
TEMPO_FACTORS = [round(0.40 + x * 0.05, 2) for x in range(0, 7)]


@dataclass
class ProposedEdit:
    field: str            # "module.burst_sentinel", "bias_shift", "tempo_factor"
    current: Any
    proposed: Any
    holdout_gain: float   # improvement on matches the search never saw
    train_gain: float     # improvement on the matches it searched
    note: str = ""


@dataclass
class LeagueProposal:
    league_code: str
    sample: int
    holdout_sample: int
    baseline: float           # current settings, full replay
    holdout_baseline: float
    holdout_proposed: float
    edits: list[ProposedEdit] = field(default_factory=list)

    @property
    def gain(self) -> float:
        return self.holdout_proposed - self.holdout_baseline

    def to_dict(self) -> dict:
        d = asdict(self)
        d["gain"] = round(self.gain, 4)
        return d


def _with_edits(cfg: LeagueConfig, edits: Iterable[ProposedEdit]) -> LeagueConfig:
    """Clone a config with a set of proposed edits applied."""
    from copy import deepcopy

    c = deepcopy(cfg)
    for e in edits:
        if e.field.startswith("module."):
            c.module_overrides = dict(c.module_overrides or {})
            c.module_overrides[e.field.split(".", 1)[1]] = e.proposed
        elif e.field == "bias_shift":
            c.base_over_bias = round(min(1.0, max(0.0, 0.5 + e.proposed / 2)), 3)
            c.base_under_bias = round(min(1.0, max(0.0, 0.5 - e.proposed / 2)), 3)
        elif e.field == "tempo_factor":
            c.tempo_factor = e.proposed
    return c


def _flags_for(cfg: LeagueConfig) -> ModuleFlags:
    return ModuleFlags(**(cfg.module_overrides or {}))


def _score(code: str, cfg: LeagueConfig, pairs) -> float:
    r = replay(code, cfg, _pairs=pairs, module_flags=_flags_for(cfg))
    return r.hit_rate if r.sample else 0.0


def propose_league(
    league_code: str,
    season: Optional[str] = None,
    min_matches: int = CALIB_MIN_MATCHES,
    limit: Optional[int] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> Optional[LeagueProposal]:
    """
    Search a league's toggles and return the edits worth making, or None when
    there is too little data to judge.

    Nothing is written. The caller decides what to do with the result.
    """
    say = progress or (lambda _m: None)
    cfg = config.get(league_code)

    pairs = _requests_for(league_code, None, season, min_matches)
    if limit:
        pairs = pairs[-limit:]
    if len(pairs) < 60:
        return None

    pairs = sorted(pairs, key=lambda p: p[0].match_date)
    cut = int(len(pairs) * (1 - HOLDOUT_FRACTION))
    train, holdout = pairs[:cut], pairs[cut:]
    if len(holdout) < MIN_HOLDOUT_SAMPLE:
        return None

    base_all = _score(league_code, cfg, pairs)
    base_train = _score(league_code, cfg, train)
    base_holdout = _score(league_code, cfg, holdout)

    # ── Candidate edits ───────────────────────────────────────────────
    current_flags = _flags_for(cfg).model_dump()
    candidates: list[ProposedEdit] = []

    for name, now in current_flags.items():
        candidates.append(ProposedEdit(
            field=f"module.{name}", current=now, proposed=not now,
            holdout_gain=0.0, train_gain=0.0,
            note="flip this module for this league only",
        ))

    cur_shift = round((cfg.base_over_bias - cfg.base_under_bias), 2)
    for s in BIAS_SHIFTS:
        if abs(s - cur_shift) < 1e-6:
            continue
        candidates.append(ProposedEdit(
            field="bias_shift", current=cur_shift, proposed=s,
            holdout_gain=0.0, train_gain=0.0, note="net over/under pressure",
        ))

    for t in TEMPO_FACTORS:
        if abs(t - cfg.tempo_factor) < 1e-6:
            continue
        candidates.append(ProposedEdit(
            field="tempo_factor", current=cfg.tempo_factor, proposed=t,
            holdout_gain=0.0, train_gain=0.0, note="scales the tempo signal",
        ))

    say(f"{league_code}: {len(pairs)} matches, testing {len(candidates)} candidates")

    # ── Greedy accumulation, holdout-validated at every step ──────────
    accepted: list[ProposedEdit] = []
    best_train = base_train
    best_holdout = base_holdout
    remaining = list(candidates)

    while remaining:
        scored = []
        for cand in remaining:
            trial = _with_edits(cfg, accepted + [cand])
            t = _score(league_code, trial, train)
            if t > best_train + 1e-9:
                scored.append((t, cand))
        if not scored:
            break

        scored.sort(key=lambda x: -x[0])
        t_best, cand = scored[0]

        trial = _with_edits(cfg, accepted + [cand])
        h = _score(league_code, trial, holdout)
        if h <= best_holdout + 1e-9:
            # Looked good on the training split, did not survive. Drop it and
            # keep going — the next candidate may generalise where this did not.
            remaining = [c for c in remaining if c is not cand]
            continue

        cand.train_gain = round(t_best - base_train, 4)
        cand.holdout_gain = round(h - best_holdout, 4)
        accepted.append(cand)
        best_train, best_holdout = t_best, h
        remaining = [c for c in remaining
                     if c is not cand and c.field != cand.field]

    total_gain = best_holdout - base_holdout
    if total_gain < MIN_GAIN:
        accepted = []
        best_holdout = base_holdout

    return LeagueProposal(
        league_code=league_code,
        sample=len(pairs),
        holdout_sample=len(holdout),
        baseline=round(base_all, 4),
        holdout_baseline=round(base_holdout, 4),
        holdout_proposed=round(best_holdout, 4),
        edits=accepted,
    )


def propose_many(
    league_codes: Iterable[str],
    season: Optional[str] = None,
    limit: Optional[int] = None,
    progress: Optional[Callable[[str], None]] = None,
) -> list[LeagueProposal]:
    out = []
    for code in league_codes:
        try:
            p = propose_league(code, season=season, limit=limit, progress=progress)
        except Exception as exc:            # a bad league must not kill the run
            (progress or (lambda _m: None))(f"{code}: skipped ({exc})")
            continue
        if p is not None:
            out.append(p)
    return out


# ── Persistence: propose, review, accept ──────────────────────────────────────

def save_proposals(proposals: list[LeagueProposal]) -> Path:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated": date.today().isoformat(),
        "leagues": {p.league_code: p.to_dict() for p in proposals},
    }
    PROPOSALS_FILE.write_text(
        json.dumps(payload, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    return PROPOSALS_FILE


def load_proposals() -> dict:
    if not PROPOSALS_FILE.exists():
        return {}
    return json.loads(PROPOSALS_FILE.read_text(encoding="utf-8"))


def accept(league_codes: Optional[Iterable[str]] = None) -> dict[str, list[str]]:
    """
    Apply saved proposals to config/leagues.json.

    Only ever called explicitly — proposing never writes. Returns
    {league_code: [descriptions of what was applied]}.
    """
    data = load_proposals()
    leagues = data.get("leagues", {})
    wanted = set(league_codes) if league_codes else set(leagues)

    cfgs = config.load_all(refresh=True)
    applied: dict[str, list[str]] = {}

    for code, p in leagues.items():
        if code not in wanted or not p.get("edits"):
            continue
        cfg = cfgs.get(code) or LeagueConfig(league_code=code)
        edits = [ProposedEdit(**e) for e in p["edits"]]
        cfgs[code] = _with_edits(cfg, edits)
        cfgs[code].last_calibrated = date.today().isoformat()
        cfgs[code].last_hit_rate = p.get("holdout_proposed")
        cfgs[code].last_sample = p.get("holdout_sample")
        applied[code] = [f"{e.field}: {e.current} -> {e.proposed}" for e in edits]

    if applied:
        config.save_all(cfgs)
    return applied
