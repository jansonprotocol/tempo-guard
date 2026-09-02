"""
Club-name normalisation — one definition, used by everything that compares
two club names.

This lived inside `features.py` and was reached only by the resolver. The
store now needs the same reduction to fold split spellings before a frame is
ever indexed, and a second copy of a normaliser is exactly how two surfaces
start disagreeing about what a club is called. So it moved here, unchanged,
and `features.py` imports it.

The reduction is deliberately conservative. It removes decoration that
carries no identity — legal forms, sponsor prefixes, accents, punctuation —
and nothing else. `Chelsea FC` and `Chelsea` collapse; `Malaga` and
`Malaga B` do not, because `B` is a different team.
"""
from __future__ import annotations

import re
import unicodedata
from typing import Optional

# Club-name decoration that carries no identity: legal forms, sponsor prefixes
# and the like. Stripped before comparison so "AFC Bournemouth", "Bournemouth"
# and "Bournemouth FC" collapse to the same key.
CLUB_TOKENS = {
    "fc", "afc", "cf", "sc", "ac", "bc", "sk", "fk", "sv", "vfb", "vfl", "tsg",
    "rc", "as", "ss", "ssc", "us", "aj", "ogc", "rcd", "cd", "ud", "sd", "cfc",
    "club", "calcio", "nk", "if", "bk", "de", "cp", "sl", "psv", "bv", "ssv",
    "fsv", "msv", "spvgg", "kv", "rsc", "kaa", "aa", "asd", "acf", "aca",
}

# Characters whose modification lives INSIDE the codepoint, so NFD leaves them
# alone. Without these the accent-insensitive match silently fails on whole
# leagues: `Sønderjyske` would not match `Sonderjyske`, `Widzew Łódź` not
# `Widzew Lodz`. The `å`, `é`, `ş` family DO decompose and need no entry here.
UNDECOMPOSED = str.maketrans({
    "ø": "o", "æ": "ae", "œ": "oe", "ł": "l", "đ": "d", "ð": "d",
    "þ": "th", "ß": "ss", "ħ": "h", "ŧ": "t", "ı": "i", "ĸ": "k",
})


def strip_accents(s: str) -> str:
    """
    Fold a name to plain ASCII letters for matching.

    Two passes are needed. NFD splits an accented letter into base plus
    combining mark and the mark is dropped; but a letter whose glyph carries
    the modification INSIDE the codepoint has no decomposition at all and
    survives NFD unchanged. Those are translated explicitly first.
    """
    return "".join(
        c for c in unicodedata.normalize("NFD", s.translate(UNDECOMPOSED))
        if unicodedata.category(c) != "Mn"
    )


def norm(s: Optional[str]) -> str:
    return (s or "").strip().lower()


def norm_accent(s: Optional[str]) -> str:
    return strip_accents(norm(s or ""))


# Tokens that mark a club's RESERVE side. A name is a reserve side when its
# canonical form carries one of these; "Real Sociedad B", "Jong Ajax",
# "Borussia Dortmund II", "Real Madrid Castilla", "Barcelona Atlètic".
# Celta's reserve plays as "Celta Fortuna", which carries no marker, so it
# is named outright.
RESERVE_TOKENS = {"b", "ii", "iii", "jong", "u21", "u23", "u19", "castilla",
                  "atletic", "reserves", "amateure", "2"}
RESERVE_NAMES = {"celta fortuna"}


def reserve_side(name: str) -> bool:
    """Is this a club's reserve/second team rather than the club itself?

    The fuzzy matcher scores "real sociedad b" against "real sociedad" as
    a perfect token-set match, so a reserve side with thin rows resolved to
    its parent's first team — across divisions, where the parent always
    sits one rung up with a full window. Ajax's rows were about to price
    Jong Ajax. A reserve and its parent are different clubs, and no
    matcher may join them.
    """
    c = canonical(name)
    return c in RESERVE_NAMES or bool(set(c.split()) & RESERVE_TOKENS)


def canonical(name: str) -> str:
    """
    Reduce a club name to its identifying core: lowercased, accent-free,
    punctuation-free, with generic club tokens removed.

        "AFC Bournemouth"          -> "bournemouth"
        "Brighton & Hove Albion FC"-> "brighton hove albion"
        "Atlético Madrid"          -> "atletico madrid"

    Falls back to the undecorated name if stripping would empty the string
    (e.g. a club literally named "PSV").
    """
    s = strip_accents(name.lower())
    s = re.sub(r"[^a-z0-9]+", " ", s)
    tokens = [t for t in s.split() if t not in CLUB_TOKENS]
    return " ".join(tokens) if tokens else " ".join(s.split())
