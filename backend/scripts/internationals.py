"""Build the international league set — national-team football, by confederation.

    python scripts/internationals.py --write        fetch and store every code
    python scripts/internationals.py --write INT-UEFA INT-CAF
    python scripts/internationals.py                report, write nothing
    python scripts/internationals.py --since 2020   shallower history
    python scripts/internationals.py --norms        measure the four league
                                                    norms and write leagues.json
    python scripts/internationals.py --slate 14     print a futurematch slate of
                                                    the next 14 days' fixtures

WHY SIX CODES AND NOT ONE. The bettor asked (20 Sep) whether folding every
international competition into a single set with "special bars for each
continent" would buy enough coverage to price national-team football. Measured
first, built after: goals per match run 2.16 in CONMEBOL, 2.39 in CAF, 2.63 in
friendlies, 2.80 in the AFC, 2.89 in UEFA and 3.10 in CONCACAF, and U4.5 runs
92.2% against 77.4% between the two ends — fifteen points. The sd across
confederations is 0.312 goals where the sd across the board's 57 club leagues
is 0.295. Confederations differ from each other as much as leagues do, so one
pooled code would have been the Simpson's-paradox error config/ladder_rates.tsv
already had to be rescued from. Each confederation is its own league here, and
the board's per-league machinery — hit rates, guard slices, badges, the REL
debit's base rate — then works unchanged.

FRIENDLIES GET THEIR OWN CODE. They are 37% of all international football and
they are not the same game: squads assembled four days earlier, rolling
substitutions, nothing at stake. Filing them under a confederation would also
be a lie about half of them, because a Brazil–Japan friendly belongs to no
confederation. INT-FR carries them all.

WHAT IS DELIBERATELY NOT SPLIT. Inside UEFA, World Cup qualifiers run 3.15
goals against 2.54 in the Euro finals, and it is tempting to make those
separate codes too. They are not, because that gap is WHO PLAYS WHOM — a
qualifying group contains Gibraltar and San Marino and a Euro final does not —
and the engine's team ratings already carry exactly that. Splitting the code as
well would charge the same effect twice. The confederation gaps are a different
animal: CONMEBOL is ten strong sides playing each other and scoring 2.16, which
no team rating explains away.

WHAT IS LEFT OUT. The World Cup finals (fifa.world) is 64 matches every four
years and the next is 2030 — too thin to carry a code, and nothing forward
looking is lost by waiting. Those matches are not stored.

NAMES NEED NO RESOLVER. Every row here comes from ESPN and only from ESPN, so
the 216 national-team spellings are self-consistent by construction. This is
the same rule app/data/espn.py states for club leagues sourced there: a league
sourced from ESPN is sourced from ESPN only, never merged with another
provider's naming.

THE SEASON IS THE CALENDAR YEAR, which is the one convention that fits. There
is no international "2025-26": a qualifying campaign runs across two or three
calendar years and a Nations League group sits inside one.

ONE PLUMBING NOTE. The international slugs do NOT accept the
dates=YYYYMMDD-YYYYMMDD span every club fetch uses — they answer HTTP 400 and
want the bare year — so fetch_season is called with span=str(year).
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import espn, store

SINCE, UNTIL = 2012, 2026

# code -> (display name, the ESPN slugs that feed it)
SETS: dict[str, tuple[str, tuple[str, ...]]] = {
    "INT-UEFA": ("UEFA internationals",
                 ("uefa.nations", "fifa.worldq.uefa",
                  "uefa.euroq", "uefa.euro")),
    "INT-CONCACAF": ("CONCACAF internationals",
                     ("concacaf.nations.league", "fifa.worldq.concacaf",
                      "concacaf.gold")),
    "INT-CAF": ("CAF internationals",
                ("fifa.worldq.caf", "caf.nations")),
    "INT-AFC": ("AFC internationals",
                ("fifa.worldq.afc", "afc.asian.cup")),
    "INT-CONMEBOL": ("CONMEBOL internationals",
                     ("fifa.worldq.conmebol", "conmebol.america")),
    "INT-FR": ("International friendlies",
               ("fifa.friendly",)),
}


def season(code: str, year: int, slugs) -> pd.DataFrame:
    """One calendar year of one code: every slug that feeds it, concatenated
    and de-duplicated on (date, home, away).

    The de-duplication is not theoretical. A play-off can be filed under both
    a qualifying slug and a continental one, and two copies of a match would
    distort every rolling feature computed from it — the same reason
    store.save drops duplicates on the way in.
    """
    frames = []
    for slug in slugs:
        try:
            df = espn.fetch_season(slug, year, code, span=str(year))
        except Exception as exc:                       # noqa: BLE001
            print(f"  {code} {year} {slug}: failed ({exc})", file=sys.stderr)
            continue
        if len(df):
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=espn.COLUMNS)
    out = pd.concat(frames, ignore_index=True)
    out = out.drop_duplicates(subset=["date", "home", "away"], keep="first")
    # espn.fetch_season stamps its own "FT"; the store's result view filters on
    # "result", which app/data/loader.py assigns as the last step of every
    # normal ingest. This path does not go through the loader, so it says so
    # here — without it the parquet lands and load_results answers nothing.
    out["status"] = "result"
    return out.sort_values("date").reset_index(drop=True)


def build(code: str, write: bool, since: int, until: int) -> tuple[int, int]:
    """(matches, seasons written) for one code."""
    name, slugs = SETS[code]
    total, wrote = 0, 0
    for year in range(since, until + 1):
        df = season(code, year, slugs)
        if df.empty:
            continue
        total += len(df)
        wrote += 1
        if write:
            store.save(code, str(year), df)
    return total, wrote


# The four per-league norms in config/leagues.json. goal_mean/goal_std are the
# spread of actual totals and come straight off the stored results. mu_mean and
# mu_std are meant to be "mu's own mean and spread", and mu is readable per
# fixture from build_request, so the mean is measured directly.
#
# MU_STD_SCALE IS AN HONEST FUDGE AND SAYS SO. Whatever wrote the other 56
# leagues' mu_std is not in this tree any more (`athena lanes --recalc`, long
# gone), and measuring mu_total's own spread the obvious way gives a number
# about a third of what is stored — ALG-L1 0.196 against 0.581, ENG-PL 0.206
# against 0.556, NED-ED 0.281 against 0.710, BRA-SA 0.185/0.513, SWE-AL
# 0.226/0.622, COL-PA 0.147/0.394. The RATIO, though, is stable: 2.53 to 2.97,
# mean 2.73, sd 0.14 across six leagues of very different character. So the
# scale factor puts these six on the same footing as every other league by
# construction, which is what matters — the sharp lane standardises against
# this, and a std three times too tight would make it fire three times too
# eagerly here and nowhere else. It is a calibration to the existing set, not a
# measurement, and it should be replaced the day the original recalc is rebuilt.
MU_STD_SCALE = 2.73
NORM_N = 800          # most recent fixtures to measure over, as retrosim uses


def norms(code: str) -> dict | None:
    """The four scoring norms for one code, measured from the store."""
    import statistics

    from app.predict import build_request

    df = store.load_results(code)
    if df is None or df.empty:
        return None
    d = df.sort_values("date")
    totals = [int(h) + int(a) for h, a in zip(d["hg"], d["ag"])]
    mus = []
    for h, a, dt in list(zip(d["home"], d["away"], d["date"]))[-NORM_N:]:
        req = build_request(code, h, a,
                            dt.date() if hasattr(dt, "date") else dt)
        if req and req.mu_total:
            mus.append(float(req.mu_total))
    if len(totals) < 50 or len(mus) < 50:
        return None
    return {
        "goal_mean": round(statistics.mean(totals), 3),
        "goal_std": round(statistics.pstdev(totals), 3),
        "mu_mean": round(statistics.mean(mus), 3),
        "mu_std": round(statistics.pstdev(mus) * MU_STD_SCALE, 3),
        "_n": len(totals), "_mu_n": len(mus),
    }


def write_norms(codes: list[str]) -> None:
    """Add or refresh these codes in config/leagues.json.

    use_season_stage is OFF for every international code. The feature measures
    how far into its campaign a fixture falls, as played/expected matches per
    team — and a national side plays seven matches one calendar year and twelve
    the next, with a qualifying campaign running across two or three of them.
    The ratio the feature is built on does not exist here, so it is not used.
    """
    import json

    path = Path(__file__).resolve().parents[2] / "config" / "leagues.json"
    cfg = json.loads(path.read_text())
    for code in codes:
        n = norms(code)
        if n is None:
            print(f"{code:14} too little stored to measure norms — skipped")
            continue
        row = dict(cfg.get(code) or {})
        row.update({
            "league_code": code, "name": SETS[code][0],
            "base_over_bias": 0.5, "base_under_bias": 0.5,
            "tempo_factor": 0.5,
            "deg_sensitivity": 1.0, "det_sensitivity": 1.0,
            "eps_sensitivity": 1.0,
            "confidence_scale": 1.0, "confidence_floor": 0.6,
            "min_confidence": 0.0, "strength_coefficient": 1.0,
            "goal_mean": n["goal_mean"], "goal_std": n["goal_std"],
            "mu_mean": n["mu_mean"], "mu_std": n["mu_std"],
            "max_under_line": None, "min_over_line": None,
            "min_win_prob": None, "module_mu_scale": 0.0,
            "module_overrides": {}, "team_nudges": {},
            "use_possession": False, "use_season_stage": False,
            "last_calibrated": None, "last_hit_rate": None,
            "last_sample": None,
        })
        cfg[code] = row
        print(f"{code:14} goals {n['goal_mean']:.3f}±{n['goal_std']:.3f} "
              f"mu {n['mu_mean']:.3f}±{n['mu_std']:.3f} "
              f"on {n['_n']} matches ({n['_mu_n']} priced)")
    path.write_text(json.dumps(cfg, indent=2, sort_keys=True) + "\n")
    print(f"config/leagues.json now holds {len(cfg)} leagues")


def slate(days: int, codes: list[str]) -> None:
    """Print futurematch slate rows for the international fixtures still to
    be played in the next `days` days.

    The board has no international screenshots to work from and does not need
    any: unlike the club leagues, whose slates come off the bettor's Sofascore
    favourites, every national fixture is in the same feed the results come
    from. So this prints the four-column form futurematch reads and the whole
    round can be priced without anyone typing a fixture name.

    KICKOFFS ARE CONVERTED. ESPN stamps its fixtures in UTC and the board keeps
    them in the European clock the rest of it uses, so an 18:45Z Nations League
    match is written 20:45 — the same +2 the live sweep already applies.
    """
    from datetime import datetime, timedelta, timezone

    import requests

    now = datetime.now(timezone.utc)
    until = now + timedelta(days=days)
    rows = []
    for code in codes:
        name, slugs = SETS[code]
        for slug in slugs:
            try:
                evs = requests.get(
                    espn.SCOREBOARD.format(code=slug),
                    params={"dates": str(now.year), "limit": espn.LIMIT},
                    timeout=espn.TIMEOUT).json().get("events") or []
            except Exception as exc:                   # noqa: BLE001
                print(f"# {code} {slug}: failed ({exc})", file=sys.stderr)
                continue
            for ev in evs:
                c = (ev.get("competitions") or [{}])[0]
                if ((c.get("status") or {}).get("type") or {}).get("completed"):
                    continue
                try:
                    ko = datetime.strptime(ev["date"], "%Y-%m-%dT%H:%MZ")
                except (KeyError, ValueError):
                    continue
                ko = ko.replace(tzinfo=timezone.utc)
                if not now <= ko <= until:
                    continue
                sides = {x.get("homeAway"): x for x in c.get("competitors") or []}
                h, a = sides.get("home"), sides.get("away")
                if not h or not a:
                    continue
                board = (ko + timedelta(hours=2)).strftime("%Y-%m-%d %H:%M")
                rows.append((board, code, name,
                             f"{h['team']['displayName']} v "
                             f"{a['team']['displayName']}"))
    for r in sorted(set(rows)):
        print("\t".join(r))
    print(f"# {len(set(rows))} international fixtures in the next {days} days",
          file=sys.stderr)


def main() -> None:
    args = sys.argv[1:]
    write = "--write" in args
    since = int(args[args.index("--since") + 1]) if "--since" in args else SINCE
    codes = [a for a in args if a in SETS] or list(SETS)

    if "--norms" in args:
        write_norms(codes)
        return

    if "--slate" in args:
        slate(int(args[args.index("--slate") + 1]), codes)
        return

    grand = 0
    for code in codes:
        n, seasons = build(code, write, since, UNTIL)
        grand += n
        name = SETS[code][0]
        print(f"{code:14} {n:6} matches over {seasons:3} seasons   {name}")
    verb = "stored" if write else "found (dry run; pass --write)"
    print(f"{grand} international matches {verb}")


if __name__ == "__main__":
    main()
