import unicodedata
import re

def normalize_team(name: str) -> str:
    if not name:
        return ""
    # Lowercase
    s = name.strip().lower()
    # Remove accents/diacritics
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    # Replace non-alphanumeric with space
    s = re.sub(r"[^a-z0-9]+", " ", s)
    # Collapse spaces
    s = re.sub(r"\s+", " ", s).strip()
    return s
