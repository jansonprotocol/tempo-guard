"""
The cup spiderweb, stage 1: can the store itself rate leagues and clubs?

The cup mu died with a slope of 0.017 because it prices a club's rate as if
its domestic league were the world. But the store holds 8,900 cup matches,
and every one is a BRIDGE between two leagues — Spain's 3rd against England's
3rd, measured in goals. Four strands, each testable before any engine change:

  RATINGS   fit a strength number per league from cup results alone: home
            supremacy = home_adv + r_home_league − r_away_league, least
            squares over every bridge. Sanity check: the big five must come
            out on top or the fit is garbage.

  SLOPE     the question that killed the old path, re-asked with strength in
            hand: do |rating gap| and rating sum predict cup totals where the
            domestic-form mu could not? Mismatch should mean blowouts (the
            1-6s that over-disperse the tails); two giants should mean the
            cagey top-clash effect at continental scale.

  OWN FORM  the old fallback read ONLY domestic form, but many clubs carry a
            deep European record of their own inside the cup frames — rows
            the path never touched. Counted here: how many clubs could be
            priced partly on their own cup history?

  TAILS     the dispersion the calibrate run inferred, measured directly:
            var/mean of cup totals against the 1.03 the domestic store shows.

Usage:  python scripts/cup_strength.py
"""
from __future__ import annotations

import json
import sys
from collections import defaultdict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from app.data import features, store

CUPS = ("UCL", "UEL", "UECL", "UCL-Q", "UEL-Q", "UECL-Q")
CACHE = Path(__file__).resolve().parents[2] / "config" / "club_leagues.json"


def club_league_map() -> dict[str, str]:
    """Each cup club's domestic league, by deepest resolution — cached,
    because the search is the expensive thing the cup replays kept paying."""
    if CACHE.exists():
        return json.loads(CACHE.read_text())

    clubs = set()
    for code in CUPS:
        df = store.load_results(code)
        if df is not None and not df.empty:
            clubs |= set(map(str, df["home"])) | set(map(str, df["away"]))
    print(f"resolving {len(clubs)} cup clubs against the domestic store...",
          file=sys.stderr)

    domestic = [c for c in store.available_leagues() if c not in CUPS]
    frames = [(c, store.load_results(c)) for c in domestic]
    out = {}
    for i, club in enumerate(sorted(clubs)):
        best, best_rows = None, 0
        for code, df in frames:
            if df is None or df.empty:
                continue
            name = features._resolve_in_frame(df, features._aliased(code, df, club))
            if name is None:
                continue
            rows = len(features._frame_index(df)["by_team"].get(
                features._norm(name), []))
            if rows > best_rows:
                best, best_rows = code, rows
        if best and best_rows >= 30:
            out[club] = best
        if i % 100 == 0:
            print(f"  {i}/{len(clubs)}", file=sys.stderr)
    CACHE.write_text(json.dumps(out, ensure_ascii=False, indent=1))
    return out


def main() -> None:
    mapping = club_league_map()
    print(f"{len(mapping)} cup clubs mapped to a domestic league\n")

    # Bridges: every cup match with both clubs mapped.
    rows = []
    own_rows = defaultdict(int)
    for code in CUPS:
        df = store.load_results(code)
        if df is None or df.empty:
            continue
        for r in df.dropna(subset=["hg", "ag"]).itertuples():
            h, a = str(r.home), str(r.away)
            own_rows[h] += 1
            own_rows[a] += 1
            li, lj = mapping.get(h), mapping.get(a)
            if li and lj and li != lj:
                rows.append((li, lj, int(r.hg), int(r.ag)))
    print(f"{len(rows)} cross-league bridges\n")

    # RATINGS by iterative least squares on supremacy.
    leagues = sorted({r[0] for r in rows} | {r[1] for r in rows})
    rating = {lg: 0.0 for lg in leagues}
    home_adv = 0.3
    for _ in range(200):
        num = defaultdict(float)
        den = defaultdict(int)
        ha_num = ha_den = 0.0
        for li, lj, hg, ag in rows:
            s = hg - ag
            ha_num += s - (rating[li] - rating[lj])
            ha_den += 1
            resid = s - home_adv
            num[li] += resid + rating[lj]
            den[li] += 1
            num[lj] += rating[li] - resid
            den[lj] += 1
        home_adv = ha_num / ha_den
        new = {lg: num[lg] / den[lg] for lg in leagues if den[lg] >= 20}
        m = sum(new.values()) / len(new)
        rating = defaultdict(float, {lg: v - m for lg, v in new.items()})
    print(f"home advantage in cups: {home_adv:+.3f} goals")
    print("league ratings (goals of supremacy vs average opponent):")
    ranked = sorted(((v, k) for k, v in rating.items() if den[k] >= 40),
                    reverse=True)
    for v, k in ranked[:10]:
        print(f"  {k:8} {v:+6.2f}   ({den[k]} bridges)")
    print("  ...")
    for v, k in ranked[-5:]:
        print(f"  {k:8} {v:+6.2f}   ({den[k]} bridges)")

    # SLOPE: totals against |gap| and sum.
    import statistics as st
    triples = [(abs(rating[li] - rating[lj]), rating[li] + rating[lj],
                hg + ag) for li, lj, hg, ag in rows
               if den[li] >= 40 and den[lj] >= 40]
    print(f"\n{len(triples)} bridges with solid ratings")
    for label, idx in (("|rating gap|", 0), ("rating sum", 1)):
        xs = [t[idx] for t in triples]
        ys = [t[2] for t in triples]
        mx, my = st.mean(xs), st.mean(ys)
        sxx = sum((x - mx) ** 2 for x in xs)
        sxy = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        slope = sxy / sxx
        se = (sum((y - my) ** 2 for y in ys) / sxx / (len(xs) - 2)) ** 0.5
        print(f"  total goals ~ {label:14} slope {slope:+.3f}  se {se:.3f}")
    # quartiles of gap -> mean total, the shape a knob would use
    qs = sorted(triples)
    n = len(qs)
    print("  by |gap| quartile:", "  ".join(
        f"{st.mean(t[2] for t in qs[i*n//4:(i+1)*n//4]):.2f}"
        for i in range(4)))

    # OWN FORM: clubs with a real European record of their own.
    deep = sum(1 for v in own_rows.values() if v >= 20)
    mid = sum(1 for v in own_rows.values() if 10 <= v < 20)
    print(f"\nclubs with 20+ own cup rows: {deep};  10-19: {mid} "
          f"(the old path used none of them)")

    # TAILS: dispersion, directly.
    for code in ("UCL", "UEL", "UECL"):
        df = store.load_results(code)
        g = (df["hg"] + df["ag"]).dropna()
        print(f"{code}: var/mean {g.var() / g.mean():.3f}   "
              f"(domestic store: 1.034)")


if __name__ == "__main__":
    main()
