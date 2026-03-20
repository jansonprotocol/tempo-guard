# backend/app/services/feature_cache.py
"""
ATHENA Feature Cache — two-level cache for asof_features computations.

WHY THIS EXISTS
===============
During calibration, _run_calibration loops over ~100 matches per league.
Each iteration calls asof_features(league_code, home, away, date), which
internally queries the DB for the FBrefSnapshot parquet blob and re-parses
it on every call. For 30 leagues × 100 matches that is 3,000 DB reads of
the same blobs — the dominant bottleneck.

TWO CACHE LEVELS
================

Level 1 — Snapshot DataFrame (per league_code):
    Reads and parses the parquet blob ONCE per league, stores the DataFrame
    in memory, and injects it into fbref_base via _SNAPSHOT_OVERRIDE so that
    every subsequent asof_features call in the same process skips the DB read.

    Requires ONE small patch in fbref_base.py — see INTEGRATION NOTE below.
    Without the patch the DataFrame is still stored in memory here, but
    fbref_base won't use it (it will still hit the DB each call).

Level 2 — Result cache (per league/home/away/date/min_matches):
    Caches the complete metrics dict returned by asof_features.
    Zero hits during the first calibration run over unique matches, but
    provides 100% hits when the same league is calibrated again (e.g., a
    second `calibrate-all` run, or repeated `/calibrate/league` calls
    during tuning sessions). Also benefits batch-predict on repeated calls.

INTEGRATION NOTE — fbref_base.py (add near the top, after imports):
=================================================================
    # Feature cache injection — set by feature_cache.py before tight loops.
    _SNAPSHOT_OVERRIDE: dict = {}  # league_code → pre-loaded pd.DataFrame

Then at the very start of asof_features(), before querying the DB:

    if league_code in _SNAPSHOT_OVERRIDE:
        df = _SNAPSHOT_OVERRIDE[league_code].copy()
    else:
        # ... your existing DB read and pd.read_parquet() logic ...

That's it. Two lines added, zero logic changed. The copy() protects the
cached DataFrame from any in-place mutations inside asof_features.
=================================================================

USAGE
=====
    from app.services.feature_cache import (
        warm_snapshot_cache,
        cached_asof_features,
        clear_feature_cache,
        cache_stats,
    )

    # Pre-warm once before a calibration loop (eliminates all DB reads)
    warm_snapshot_cache(db, league_code)

    # Drop-in replacement for asof_features in any loop
    metrics = cached_asof_features(league_code, home, away, date, min_matches=3)

    # Clear after the run so stale data does not accumulate
    clear_feature_cache(league_code)

    # Inspect cache state
    print(cache_stats())
"""
from __future__ import annotations

import io
import threading
from typing import Any, Dict, Optional

import pandas as pd

# ── Thread safety ──────────────────────────────────────────────────────────
_lock = threading.Lock()

# ── Level 1: parsed DataFrame per league_code ─────────────────────────────
_df_cache: Dict[str, pd.DataFrame] = {}

# ── Level 2: asof_features result per call signature ──────────────────────
_result_cache: Dict[str, Any] = {}


# ── Internal helpers ───────────────────────────────────────────────────────

def _result_key(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: Any,
    min_matches: int,
) -> str:
    return f"{league_code}|{home_team}|{away_team}|{match_date}|{min_matches}"


def _inject_into_fbref_base(league_code: str, df: pd.DataFrame) -> bool:
    """
    Inject a pre-loaded DataFrame into fbref_base._SNAPSHOT_OVERRIDE.
    Returns True if the integration patch is present, False otherwise.
    """
    try:
        from app.services.data_providers import fbref_base
        if hasattr(fbref_base, "_SNAPSHOT_OVERRIDE"):
            fbref_base._SNAPSHOT_OVERRIDE[league_code] = df
            return True
        else:
            print(
                f"[feature_cache] fbref_base._SNAPSHOT_OVERRIDE not found. "
                f"Add the integration patch (see module docstring) for full "
                f"per-run speedup. Result-level cache is still active."
            )
            return False
    except Exception as e:
        print(f"[feature_cache] fbref_base injection skipped: {e}")
        return False


def _remove_from_fbref_base(league_code: Optional[str] = None) -> None:
    try:
        from app.services.data_providers import fbref_base
        if not hasattr(fbref_base, "_SNAPSHOT_OVERRIDE"):
            return
        if league_code:
            fbref_base._SNAPSHOT_OVERRIDE.pop(league_code, None)
        else:
            fbref_base._SNAPSHOT_OVERRIDE.clear()
    except Exception:
        pass


# ── Public API ─────────────────────────────────────────────────────────────

def warm_snapshot_cache(db: Any, league_code: str) -> bool:
    """
    Pre-load and cache the FBrefSnapshot DataFrame for a league.

    Also injects it into fbref_base._SNAPSHOT_OVERRIDE (if the integration
    patch is present) so that asof_features skips its internal DB read.

    Call this ONCE before starting a calibration loop over a league.

    Args:
        db:           SQLAlchemy session with access to FBrefSnapshot.
        league_code:  League to warm.

    Returns:
        True if snapshot was found and cached. False if no snapshot exists.
    """
    with _lock:
        if league_code in _df_cache:
            # Already warm — re-inject in case fbref_base was reloaded
            _inject_into_fbref_base(league_code, _df_cache[league_code])
            return True

    from app.database.models_fbref import FBrefSnapshot

    row = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
    if not row:
        print(f"[feature_cache] No snapshot found for {league_code}")
        return False

    try:
        df = pd.read_parquet(io.BytesIO(row.data))
    except Exception as e:
        print(f"[feature_cache] Could not parse snapshot for {league_code}: {e}")
        return False

    size_kb = df.memory_usage(deep=True).sum() // 1024

    with _lock:
        _df_cache[league_code] = df

    injected = _inject_into_fbref_base(league_code, df)
    print(
        f"[feature_cache] Warmed {league_code}: {len(df)} rows, {size_kb}KB"
        + (" — fbref_base injection active" if injected else " — result cache only")
    )
    return True


def cached_asof_features(
    league_code: str,
    home_team: str,
    away_team: str,
    match_date: Any,
    min_matches: int = 3,
) -> Optional[dict]:
    """
    Drop-in replacement for asof_features() with result-level caching.

    On the first call for a given signature, delegates to the real
    asof_features and caches the result.  Subsequent identical calls
    (e.g., same league re-calibrated) return immediately from cache.

    If warm_snapshot_cache() was called beforehand AND the fbref_base
    integration patch is present, the underlying asof_features call
    will also skip its DB read (Level 1 speedup).
    """
    key = _result_key(league_code, home_team, away_team, match_date, min_matches)

    with _lock:
        cached = _result_cache.get(key)

    if cached is not None:
        return cached

    from app.services.data_providers.fbref_base import asof_features
    result = asof_features(
        league_code, home_team, away_team, match_date,
        min_matches=min_matches,
    )

    with _lock:
        _result_cache[key] = result

    return result


def get_cached_df(league_code: str) -> Optional[pd.DataFrame]:
    """
    Return the cached DataFrame for a league, or None if not warmed.
    Useful if you need the raw DataFrame for other computations.
    """
    with _lock:
        df = _df_cache.get(league_code)
        return df.copy() if df is not None else None


def clear_feature_cache(league_code: Optional[str] = None) -> None:
    """
    Clear cache entries to free memory and prevent stale data.

    Args:
        league_code: Clear only this league if provided, else clear all.
    """
    with _lock:
        if league_code:
            _df_cache.pop(league_code, None)
            stale = [k for k in _result_cache if k.startswith(f"{league_code}|")]
            for k in stale:
                del _result_cache[k]
        else:
            _df_cache.clear()
            _result_cache.clear()

    _remove_from_fbref_base(league_code)

    if league_code:
        print(f"[feature_cache] Cleared cache for {league_code}")
    else:
        print("[feature_cache] Full cache cleared")


def cache_stats() -> dict:
    """Return current cache statistics — useful for debugging and monitoring."""
    with _lock:
        df_memory_kb = sum(
            df.memory_usage(deep=True).sum() // 1024
            for df in _df_cache.values()
        )
        return {
            "leagues_in_df_cache":  list(_df_cache.keys()),
            "result_cache_entries": len(_result_cache),
            "df_memory_kb":         df_memory_kb,
        }
