"""What the store folds, what it refuses, and what is still split.

The fold in store._unify_names decides which spellings are one club, and it
runs invisibly inside every load_results. This prints it, so the decision can
be read rather than trusted.

    python scripts/name_unify.py                every league
    python scripts/name_unify.py --leagues ENG-PL,ESP-LL
    python scripts/name_unify.py --hidden       ONLY the residue

Three sections:

  FOLDS      variant -> primary, per league, with row counts
  REFUSED    pairs whose names match but which played on the same day, so
             they are different clubs — the guard firing, which is the thing
             worth watching after any data refresh
  STILL SPLIT  clubs whose recent rows remain invisible to the name a
             hand-typed fixture resolves to. This is the residue the
             canonical key cannot reach, and every line is a candidate for a
             typed pair in config/team_merges.json. The share it reports is
             the same measurement that motivated the fold: before it, 22.2%
             of every row stored since Aug 2024 was hidden.
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd

from app.data import store
from app.data.features import _aliased, _resolve_in_frame

RECENT = pd.Timestamp("2024-08-01")


def hidden_rows(code: str, df: pd.DataFrame) -> tuple[list, int, int]:
    """Recent rows a fixture's resolved name will never see.

    Run on the ALREADY-FOLDED frame, so a shared canonical key can no longer
    hide anything and this reports only what the key cannot reach: one
    spelling abbreviating a word the other spells out. The test is that one
    name's canonical TOKENS are a subset of the other's — `manchester united`
    inside `manchester united fc` is already folded, but `man united` against
    `manchester united` is not — plus the same day test, so a reserve side
    that really played is never reported as a hidden half of its first team.
    """
    if df.empty or "home" not in df.columns:
        return [], 0, 0
    seen = pd.concat([df[["home", "date"]].rename(columns={"home": "t"}),
                      df[["away", "date"]].rename(columns={"away": "t"})])
    counts = seen.groupby("t").size().to_dict()
    recent = seen[seen["date"] >= RECENT].groupby("t").size().to_dict()
    days = store._day_sets(df)

    from app.data.names import canonical
    toks = {str(n): set(canonical(str(n)).split()) for n in counts}
    live = [n for n in counts if recent.get(n, 0)]

    out, hid, paired = [], 0, set()
    for a in live:
        for b in counts:
            if a == b or (a, b) in paired or not toks[a] or not toks[b]:
                continue
            if not (toks[a] < toks[b] or toks[b] < toks[a]):
                continue
            if days.get(a, set()) & days.get(b, set()):
                continue
            paired.add((a, b))
            paired.add((b, a))
            probe = min((a, b), key=len)
            picked = _resolve_in_frame(df, _aliased(code, df, probe))
            miss = sum(recent.get(m, 0) for m in (a, b) if m != picked)
            if not miss:
                continue
            hid += miss
            out.append((picked, miss, sorted(
                ((m, counts[m], recent.get(m, 0)) for m in (a, b)),
                key=lambda x: -x[2])))
    return out, hid, int((seen["date"] >= RECENT).sum())


def main() -> None:
    args = sys.argv[1:]
    codes = (args[args.index("--leagues") + 1].split(",")
             if "--leagues" in args else sorted(store.available_leagues()))
    only_hidden = "--hidden" in args

    n_fold, n_ref, tot_hid, tot_rec = 0, 0, 0, 0
    for code in codes:
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        # load_results has already folded, so ask the raw frame what it did.
        raw = store.load(code)
        raw = raw[raw.get("status", "result") == "result"] \
            if "status" in raw.columns else raw
        mapping, refused = store.name_groups(code, raw)
        hid, n_hid, n_rec = hidden_rows(code, df)
        tot_hid += n_hid
        tot_rec += n_rec
        n_fold += len(mapping)
        n_ref += len(refused)
        if only_hidden and not hid:
            continue
        if not (mapping or refused or hid):
            continue
        print(f"\n{code}")
        if mapping and not only_hidden:
            for variant, primary in sorted(mapping.items()):
                print(f"    fold     {variant}  ->  {primary}")
        for a, b in refused:
            print(f"    REFUSED  {b}  /  {a}   (played the same day)")
        for picked, miss, members in hid:
            names_ = "  ".join(f"{m}({c}, {r} recent)" for m, c, r in members)
            print(f"    SPLIT    resolves to {picked} — loses {miss} recent"
                  f"   [{names_}]")

    print(f"\n{n_fold} folds, {n_ref} refused")
    if tot_rec:
        print(f"{tot_hid} of {tot_rec} rows since {RECENT.date()} still "
              f"hidden ({tot_hid/tot_rec*100:.1f}%)")


if __name__ == "__main__":
    main()
