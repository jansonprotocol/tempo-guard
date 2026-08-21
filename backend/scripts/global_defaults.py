"""
Are the per-league proposals really per-league?

The proposal search returned 11 leagues wanting a change, and the changes were
not scattered: seven independently chose tempo_factor 0.40 and four chose
bias_shift -0.50. Both are the *edge* of the search grid.

That pattern has a much simpler explanation than eleven separate league
personalities. If the global defaults (tempo_factor 0.50-0.56, bias_shift 0.0)
are simply set wrong, every league with enough signal to notice will drift
toward the same boundary, and the search — which can only express the answer as
a per-league override — will report it eleven times.

The two readings make different predictions, and this settles which is right:

    per-league   lowering the global default helps the eleven and hurts the
                 rest; pooled hit rate is flat or worse.

    global       lowering the global default helps almost everywhere; pooled
                 hit rate rises.

It also matters statistically. Eleven overrides fitted on ~120 unseen matches
each are eleven chances to be fooled; one global default measured on the pooled
holdout of every league is a single hypothesis on several thousand matches. If
the answer is global, it is both a bigger gain and a far safer one.

Grid extends past the proposal grid's edge deliberately — if 0.40 only won
because the search could not go lower, that shows up here.
"""
from __future__ import annotations

import sys
from copy import deepcopy
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.calibrate import CALIB_MIN_MATCHES, _requests_for, replay
from app.data import config
from app.engine.types import ModuleFlags

LEAGUES = [
    "ENG-PL", "GER-BL", "ESP-LL", "ITA-SA", "FRA-L1", "FRA-L2",
    "NED-ED", "POR-PL", "BEL-PL", "TUR-SL", "GRE-SL", "SCO-PL",
    "AUT-BL", "SUI-SL", "DEN-SL", "SWE-AL", "NOR-EL", "POL-EK",
    "CZE-FL", "FIN-VL", "IRL-PD", "RUS-PL", "ENG-CH", "ENG-L1",
    "ENG-L2", "SCO-CH", "BRA-SA", "BRA-SB", "ARG-PD", "ARG-CLP",
    "COL-PA", "MEX-LMX", "MLS", "JPN-J1", "CHN-SL",
]
LIMIT = 400
HOLDOUT_FRACTION = 0.30

TEMPOS = [0.25, 0.30, 0.35, 0.40, 0.45, 0.50, 0.56, 0.65]
SHIFTS = [-0.8, -0.6, -0.4, -0.2, 0.0, 0.2, 0.4]


def _scored(code, cfg, pairs):
    flags = ModuleFlags(**(cfg.module_overrides or {}))
    r = replay(code, cfg, _pairs=pairs, module_flags=flags)
    return r.hits, r.sample


def main() -> None:
    # Build features once per league; every candidate setting reuses them.
    data = []
    for code in LEAGUES:
        try:
            pairs = _requests_for(code, None, None, CALIB_MIN_MATCHES, limit=LIMIT)
        except Exception as exc:
            print(f"{code}: skipped ({exc})", flush=True)
            continue
        if len(pairs) < 60:
            continue
        pairs.sort(key=lambda p: p[0].match_date)
        cut = int(len(pairs) * (1 - HOLDOUT_FRACTION))
        data.append((code, config.get(code), pairs[:cut], pairs[cut:]))
        print(f"  built {code} ({len(pairs)})", flush=True)

    if not data:
        print("no data")
        return

    n_hold = sum(len(h) for _, _, _, h in data)
    print(f"\n{len(data)} leagues, pooled holdout {n_hold} matches\n")

    def pooled(mutate) -> tuple[float, float, int]:
        th = ts = hh = hs = 0
        for code, cfg, train, hold in data:
            c = mutate(deepcopy(cfg))
            a, b = _scored(code, c, train)
            th += a
            ts += b
            a, b = _scored(code, c, hold)
            hh += a
            hs += b
        return (th / ts if ts else 0), (hh / hs if hs else 0), hs

    print("TEMPO_FACTOR (global, applied to every league)")
    print(f"  {'value':>6}  {'train':>7}  {'holdout':>8}")
    best_t = None
    for t in TEMPOS:
        def m(c, t=t):
            c.tempo_factor = t
            return c
        tr, ho, _ = pooled(m)
        mark = "   <- current default" if abs(t - 0.56) < 1e-9 else ""
        print(f"  {t:6.2f}  {tr:7.1%}  {ho:8.1%}{mark}", flush=True)
        if best_t is None or ho > best_t[1]:
            best_t = (t, ho)

    print(f"\n  best holdout: tempo_factor {best_t[0]} at {best_t[1]:.1%}")

    print("\nBIAS_SHIFT (global, at the winning tempo_factor)")
    print(f"  {'value':>6}  {'train':>7}  {'holdout':>8}")
    best_s = None
    for s in SHIFTS:
        def m(c, s=s, t=best_t[0]):
            c.tempo_factor = t
            c.base_over_bias = round(min(1.0, max(0.0, 0.5 + s / 2)), 3)
            c.base_under_bias = round(min(1.0, max(0.0, 0.5 - s / 2)), 3)
            return c
        tr, ho, _ = pooled(m)
        mark = "   <- current default" if abs(s) < 1e-9 else ""
        print(f"  {s:6.2f}  {tr:7.1%}  {ho:8.1%}{mark}", flush=True)
        if best_s is None or ho > best_s[1]:
            best_s = (s, ho)

    print(f"\n  best holdout: bias_shift {best_s[0]} at {best_s[1]:.1%}")

    # ── Who wins and who loses under the global change ───────────────
    print("\nPER-LEAGUE EFFECT of the winning global setting (holdout)")
    print(f"  {'league':8s} {'n':>4}  {'now':>7}  {'global':>7}  {'delta':>7}")
    wins = losses = flat = 0
    for code, cfg, _train, hold in data:
        a0, b0 = _scored(code, cfg, hold)
        c = deepcopy(cfg)
        c.tempo_factor = best_t[0]
        c.base_over_bias = round(min(1.0, max(0.0, 0.5 + best_s[0] / 2)), 3)
        c.base_under_bias = round(min(1.0, max(0.0, 0.5 - best_s[0] / 2)), 3)
        a1, b1 = _scored(code, c, hold)
        if not b0:
            continue
        d = a1 / b1 - a0 / b0
        if d > 0.005:
            wins += 1
        elif d < -0.005:
            losses += 1
        else:
            flat += 1
        print(f"  {code:8s} {b0:4d}  {a0 / b0:7.1%}  {a1 / b1:7.1%}  {d:+7.1%}")

    print(f"\n  better {wins}   worse {losses}   unchanged {flat}")
    if wins > losses * 2:
        print("  -> the drift is global, not eleven league personalities")
    elif losses >= wins:
        print("  -> genuinely per-league; a global change would do harm")
    else:
        print("  -> mixed; treat the per-league proposals individually")


if __name__ == "__main__":
    main()
