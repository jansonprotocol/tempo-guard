"""Closing prices for the bank's past matches, so a retro card can carry
the same verdict a live card does.

The live board prices a card from the odds feed at render. A past match
has no feed, but football-data.co.uk publishes closing prices for sixteen
European leagues, and those are what every registered ROI table in docs/
was settled at. This module carries the small slice the bank needs —
one row per match, eight numbers — as config/retro_odds.tsv, so the
bank can be rebuilt anywhere without the CSVs.

    python scripts/retro_odds.py --build [--src DIR]   regenerate the TSV
                                                        from football-data
                                                        CSVs (2425_E0.csv …)

Same derivations as the registered tables: a DNB price from the 1X2 with
no draw stake; an O/U rung other than the 2.5 line from the book's own
2.5 price through the engine's mu. Nothing here reaches the engine — the
prediction path never sees a price.
"""
from __future__ import annotations

import csv
import os
import re
import sys
import unicodedata
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "config" / "retro_odds.tsv"
MAIN = {"ENG-PL": "E0", "ENG-CH": "E1", "GER-BL": "D1", "GER-B2": "D2",
        "ITA-SA": "I1", "ITA-SB": "I2", "ESP-LL": "SP1", "ESP-L2": "SP2",
        "FRA-L1": "F1", "FRA-L2": "F2", "NED-ED": "N1", "BEL-PL": "B1",
        "POR-PL": "P1", "TUR-SL": "T1", "GRE-SL": "G1", "SCO-PL": "SC0"}
DIV = {v: k for k, v in MAIN.items()}
DROP = {"fc", "cf", "sc", "ac", "afc", "cd", "ud", "sd", "rcd", "rc", "as",
        "ss", "us", "calcio", "de", "fk", "bk", "if", "sk", "club", "the",
        "1907", "1913", "1908", "ii", "b"}
COLS = ("mx_o", "mx_u", "mx_h", "mx_a", "b_o", "b_u", "b_h", "b_a")
_TAB: dict | None = None


def toks(s: str) -> set:
    s = unicodedata.normalize("NFD", s).encode("ascii", "ignore").decode().lower()
    return set(w for w in re.sub(r"[^a-z0-9 ]", " ", s).split() if w not in DROP)


def _num(r, *ks):
    for k in ks:
        try:
            return float(r[k])
        except (TypeError, ValueError, KeyError):
            pass
    return None


def build(src: Path) -> int:
    rows = []
    for f in sorted(src.glob("*.csv")):
        div = f.stem.split("_")[1]
        code = DIV.get(div)
        if not code:
            continue
        for r in csv.DictReader(f.open(encoding="utf-8-sig")):
            if not r.get("Date"):
                continue
            try:
                d, m, y = r["Date"].split("/")
                y = ("20" + y) if len(y) == 2 else y
            except ValueError:
                continue
            vals = [_num(r, "MaxC>2.5", "Max>2.5"), _num(r, "MaxC<2.5", "Max<2.5"),
                    _num(r, "MaxCH", "MaxH"), _num(r, "MaxCA", "MaxA"),
                    _num(r, "B365C>2.5", "B365>2.5"), _num(r, "B365C<2.5", "B365<2.5"),
                    _num(r, "B365CH", "B365H"), _num(r, "B365CA", "B365A")]
            if all(v is None for v in vals):
                continue
            rows.append([code, f"{y}-{m}-{d}", r["HomeTeam"], r["AwayTeam"]]
                        + [f"{v:.2f}" if v else "" for v in vals])
    rows.sort()
    OUT.write_text(
        "# Closing prices for the bank's past matches, from football-data.co.uk:\n"
        "# best of the panel (mx_) and one book (b_), over 2.5 / under 2.5 /\n"
        "# home / away. Written by scripts/retro_odds.py --build; read by\n"
        "# scripts/matchbank.py to give a past card the verdict a live one gets.\n"
        "# code\tdate\thome\taway\t" + "\t".join(COLS) + "\n"
        + "\n".join("\t".join(r) for r in rows) + "\n")
    return len(rows)


def table() -> dict:
    global _TAB
    if _TAB is None:
        _TAB = {}
        if OUT.exists():
            for ln in OUT.read_text().splitlines():
                if ln.startswith("#") or not ln.strip():
                    continue
                p = ln.split("\t")
                rec = dict(home=p[2], away=p[3])
                for k, v in zip(COLS, p[4:]):
                    rec[k] = float(v) if v else None
                _TAB.setdefault((p[0], p[1]), []).append(rec)
    return _TAB


def find(code: str, day: str, home: str, away: str) -> dict | None:
    th, ta = toks(home), toks(away)
    for r in table().get((code, day), []):
        if toks(r["home"]) & th and toks(r["away"]) & ta:
            return r
    return None


def _fit_mu(O: float, U: float):
    from app.engine import market_select
    oo, uu = 1 / O, 1 / U
    orr = oo + uu
    lo, hi = 0.05, 8.0
    for _ in range(50):
        mid = (lo + hi) / 2
        if market_select.p_win("O2.5", mid) < oo / orr:
            lo = mid
        else:
            hi = mid
    return (lo + hi) / 2, orr


def price(row: dict, lane: str, book: str = "mx") -> float | None:
    """The closing price of one lane at one book, derived as registered."""
    from app.engine import market_select
    if lane in ("DNB1", "DNB2"):
        H, A = row.get(book + "_h"), row.get(book + "_a")
        if not (H and A):
            return None
        h_, a_ = 1 / H, 1 / A
        return (h_ + a_) / h_ if lane == "DNB1" else (h_ + a_) / a_
    if lane in ("1X", "X2", "12"):
        return None
    O, U = row.get(book + "_o"), row.get(book + "_u")
    if not O or not U:
        return None
    if lane == "O2.5":
        return O
    if lane == "U2.5":
        return U
    mu, orr = _fit_mu(O, U)
    try:
        p = market_select.p_win(lane, mu)
    except Exception:
        return None
    if not p or p * orr >= 0.995:
        return None
    return 1 / (p * orr)


def main() -> None:
    if "--build" in sys.argv:
        i = sys.argv.index("--src") + 1 if "--src" in sys.argv else None
        src = Path(sys.argv[i]) if i else Path(os.environ.get(
            "ATHENA_FD_DIR", "/tmp/claude-0/-home-user-tempo-guard/"
            "9c589f66-8bfc-5690-b734-7ed9161bb2cc/scratchpad/fd2"))
        n = build(src)
        print(f"{n} rows -> {OUT}")
    else:
        print(f"{sum(len(v) for v in table().values())} priced matches in {OUT}")


if __name__ == "__main__":
    main()
