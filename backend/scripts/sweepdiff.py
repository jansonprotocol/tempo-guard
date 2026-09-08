"""Did this sweep change anything a reader needs a fresh page for?

    python scripts/sweepdiff.py        exit 0 = yes, 1 = only minutes moved, 2 = nothing

The live loop used to commit every pass, because every pass moves the
minute on every running card and a changed file is a changed file. That
put a commit — and a site deploy — on the board every four minutes for
as long as anything was running, and it is the deploy that costs: the
host allows a fixed number a day, and the bettor wanted the loop faster,
not slower.

Since 8 Sep the page counts the minute forward itself between sweeps
(webapp.tickClocks), so a pass that moved nothing but minutes has
nothing to tell a reader that the page is not already telling them. This
compares the swept fixture file with the committed one after stripping
the minute from every LIVE status, and says whether anything else moved:
a goal, a kickoff, half time, a final, a hand-set score. The loop uses
the answer to choose the commit's SUBJECT — "live tick" for minutes,
"live sweep" for a goal or half time, "live sweep [deploy]" for a
kickoff, a final or a new row — and the host (web/vercel.json) builds
only the last kind, plus anything that is not a sweep at all. The page
gets goals and minutes from web/live.json, which every pass commits.
"""
from __future__ import annotations

import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
FIXTURES = ROOT / "config" / "fixtures.tsv"

# "LIVE 56' 2-1" / "LIVE 90'+4' 0-0" -> "LIVE 2-1" ; "LIVE HT 0-0" stays.
_MIN = re.compile(r"LIVE \d+'(?:\+\d+')? ")


def _norm(text: str) -> list[str]:
    return [_MIN.sub("LIVE ", ln) for ln in text.splitlines()]


def kinds(before: str, after: str) -> list[str]:
    """What changed beyond the minute, as short labels; empty if nothing."""
    a, b = _norm(before), _norm(after)
    if a == b:
        return []
    old = {ln.split("\t")[3]: ln for ln in a if ln.count("\t") >= 6}
    out = []
    for ln in b:
        if ln.count("\t") < 6:
            continue
        c = ln.split("\t")
        was = old.get(c[3])
        if was == ln:
            continue
        w = was.split("\t")[6] if was else ""
        now = c[6]
        if not was:
            out.append(f"new row: {c[3]}")
        elif not w and now.startswith("LIVE"):
            out.append(f"kicked off: {c[3]}")
        elif now.startswith("LIVE HT"):
            out.append(f"half time: {c[3]}")
        elif now[:1] in "✅❌◦" or now.startswith("FT"):
            out.append(f"final: {c[3]} {now}")
        elif now.startswith("LIVE"):
            out.append(f"score: {c[3]} {now.split(' ')[-1]}")
        else:
            out.append(f"changed: {c[3]}")
    return out or ["changed"]


def main() -> None:
    try:
        before = subprocess.run(["git", "show", "HEAD:config/fixtures.tsv"],
                                cwd=ROOT, capture_output=True, text=True,
                                check=True).stdout
    except subprocess.CalledProcessError:
        raise SystemExit(0)                 # no committed copy: treat as material
    after = FIXTURES.read_text()
    if before == after:
        print("nothing moved")
        raise SystemExit(2)
    k = kinds(before, after)
    if not k:
        print("only minutes moved")
        raise SystemExit(1)
    for x in k:
        print(x)
    raise SystemExit(0)


if __name__ == "__main__":
    main()
