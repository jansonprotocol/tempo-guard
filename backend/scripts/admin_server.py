"""
backend/scripts/admin_server.py

Local admin panel for ATHENA data management.
Run on your laptop only — never on Render.

Usage:
    cd backend
    venv312\Scripts\activate
    python -m scripts.admin_server

Then open http://localhost:8001 in your browser.

NOTE: SeleniumBase opens a real Chrome window for each fetch.
      Each league = 2 fetches (current + previous season).
      Do not click anything while scraping is in progress.
"""

import io
import sys
import threading
import time
import queue
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
import uvicorn

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

SLEEP_BETWEEN_FETCHES = 4   # seconds between current/previous fetch for same league
SLEEP_BETWEEN_LEAGUES = 6   # seconds between leagues

# ── League map ─────────────────────────────────────────────────────────────────
# Each entry: (current_url, prev_url)
LEAGUE_MAP = {
    "ENG-PL": (
        "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/9/2024-2025/schedule/2024-2025-Premier-League-Scores-and-Fixtures",
    ),
    "ESP-LL": (
        "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/12/2024-2025/schedule/2024-2025-La-Liga-Scores-and-Fixtures",
    ),
    "FRA-L1": (
        "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
        "https://fbref.com/en/comps/13/2024-2025/schedule/2024-2025-Ligue-1-Scores-and-Fixtures",
    ),
    "GER-BUN": (
        "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/20/2024-2025/schedule/2024-2025-Bundesliga-Scores-and-Fixtures",
    ),
    "ITA-SA": (
        "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/11/2024-2025/schedule/2024-2025-Serie-A-Scores-and-Fixtures",
    ),
    "NED-ERE": (
        "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
        "https://fbref.com/en/comps/23/2024-2025/schedule/2024-2025-Eredivisie-Scores-and-Fixtures",
    ),
    "TUR-SL": (
        "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
        "https://fbref.com/en/comps/26/2024-2025/schedule/2024-2025-Super-Lig-Scores-and-Fixtures",
    ),
    "BRA-SA": (
        "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/24/2025/schedule/2025-Serie-A-Scores-and-Fixtures",
    ),
    "MLS": (
        "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
        "https://fbref.com/en/comps/22/2025/schedule/2025-Major-League-Soccer-Scores-and-Fixtures",
    ),
    "SAU-SPL": (
        "https://fbref.com/en/comps/70/schedule/Saudi-Pro-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/70/2024-2025/schedule/2024-2025-Saudi-Pro-League-Scores-and-Fixtures",
    ),
    "DEN-SL": (
        "https://fbref.com/en/comps/50/schedule/Danish-Superliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/50/2024-2025/schedule/2024-2025-Danish-Superliga-Scores-and-Fixtures",
    ),
    "ESP-LL2": (
        "https://fbref.com/en/comps/17/schedule/Segunda-Division-Scores-and-Fixtures",
        "https://fbref.com/en/comps/17/2024-2025/schedule/2024-2025-Segunda-Division-Scores-and-Fixtures",
    ),
    "BEL-PL": (
        "https://fbref.com/en/comps/37/schedule/Belgian-Pro-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/37/2024-2025/schedule/2024-2025-Belgian-Pro-League-Scores-and-Fixtures",
    ),
    "NOR-EL": (
        "https://fbref.com/en/comps/28/schedule/Eliteserien-Scores-and-Fixtures",
        "https://fbref.com/en/comps/28/2025/schedule/2025-Eliteserien-Scores-and-Fixtures",
    ),
    "SWE-AL": (
        "https://fbref.com/en/comps/29/schedule/Allsvenskan-Scores-and-Fixtures",
        "https://fbref.com/en/comps/29/2025/schedule/2025-Allsvenskan-Scores-and-Fixtures",
    ),
    "MEX-LMX": (
        "https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures",
        "https://fbref.com/en/comps/31/2024-2025/schedule/2024-2025-Liga-MX-Scores-and-Fixtures",
    ),
    # ── New leagues ───────────────────────────────────────────────────────────
    # Calendar-year leagues (current = 2026, prev = 2025)
    "CHN-CSL": (
        "https://fbref.com/en/comps/62/schedule/Chinese-Super-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/62/2025/schedule/2025-Chinese-Super-League-Scores-and-Fixtures",
    ),
    "JPN-J1": (
        "https://fbref.com/en/comps/25/schedule/J1-League-Scores-and-Fixtures",
        "https://fbref.com/en/comps/25/2025/schedule/2025-J1-League-Scores-and-Fixtures",
    ),
    "COL-PA": (
        "https://fbref.com/en/comps/41/schedule/Primera-A-Scores-and-Fixtures",
        "https://fbref.com/en/comps/41/2025/schedule/2025-Primera-A-Scores-and-Fixtures",
    ),
    "BRA-SB": (
        "https://fbref.com/en/comps/38/schedule/Serie-B-Scores-and-Fixtures",
        "https://fbref.com/en/comps/38/2025/schedule/2025-Serie-B-Scores-and-Fixtures",
    ),
    # Aug–May season leagues (current = 2025-2026, prev = 2024-2025)
    "ITA-SB": (
        "https://fbref.com/en/comps/18/schedule/Serie-B-Scores-and-Fixtures",
        "https://fbref.com/en/comps/18/2024-2025/schedule/2024-2025-Serie-B-Scores-and-Fixtures",
    ),
    "FRA-L2": (
        "https://fbref.com/en/comps/60/schedule/Ligue-2-Scores-and-Fixtures",
        "https://fbref.com/en/comps/60/2024-2025/schedule/2024-2025-Ligue-2-Scores-and-Fixtures",
    ),
    "GER-B2": (
        "https://fbref.com/en/comps/33/schedule/2-Bundesliga-Scores-and-Fixtures",
        "https://fbref.com/en/comps/33/2024-2025/schedule/2024-2025-2-Bundesliga-Scores-and-Fixtures",
    ),
    "POL-EK": (
        "https://fbref.com/en/comps/36/schedule/Ekstraklasa-Scores-and-Fixtures",
        "https://fbref.com/en/comps/36/2024-2025/schedule/2024-2025-Ekstraklasa-Scores-and-Fixtures",
    ),
    # Cuba (CUB-PD) not on FBref — scraping skipped
}

# ── Global state ──────────────────────────────────────────────────────────────
_is_running    = False
_log_queue: queue.Queue = queue.Queue()
_last_results: dict = {}


def _emit(msg: str):
    ts = datetime.now().strftime("%H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    _log_queue.put(line)


def _get_snapshot_meta() -> dict:
    db = SessionLocal()
    try:
        rows = db.query(FBrefSnapshot).all()
        return {
            r.league_code: {
                "fetched_at": r.fetched_at.strftime("%Y-%m-%dT%H:%M") if r.fetched_at else None,
            }
            for r in rows
        }
    finally:
        db.close()


def _fetch_url(url: str, label: str) -> pd.DataFrame | None:
    """Fetch one FBref URL via SeleniumBase. Returns DataFrame or None."""
    from seleniumbase import Driver

    _emit(f"  Opening Chrome [{label}]: {url}")
    driver = None
    try:
        driver = Driver(uc=True, headless=False)
        driver.uc_open_with_reconnect(url, 4)
        driver.uc_gui_click_captcha()
        time.sleep(3)
        html = driver.get_page_source()
        _emit(f"  Page loaded [{label}] ({len(html)} bytes)")
    except Exception as e:
        _emit(f"  Browser error [{label}]: {e}")
        return None
    finally:
        if driver:
            try:
                driver.quit()
            except Exception:
                pass

    if "Just a moment" in html or len(html) < 5000:
        _emit(f"  Cloudflare blocked [{label}].")
        return None

    try:
        tables = pd.read_html(io.StringIO(html))
        _emit(f"  Found {len(tables)} tables [{label}]")
    except Exception as e:
        _emit(f"  Could not parse tables [{label}]: {e}")
        return None

    if not tables:
        _emit(f"  No tables [{label}].")
        return None

    df = max(tables, key=len)
    df = df.dropna(how="all")

    score_col = next(
        (c for c in df.columns if str(c).lower() in ("score", "scores")), None
    )
    if score_col:
        df = df[df[score_col].astype(str).str.contains(r"\d[–\-]\d", na=False)]

    if df.empty:
        _emit(f"  No completed rows [{label}].")
        return None

    _emit(f"  [{label}] {len(df)} completed matches")
    return df


def _merge_seasons(
    current: pd.DataFrame | None,
    previous: pd.DataFrame | None,
    league_code: str,
) -> pd.DataFrame | None:
    frames = [f for f in [current, previous] if f is not None and not f.empty]
    if not frames:
        return None

    combined = pd.concat(frames, ignore_index=True)

    col_map  = {c.lower(): c for c in combined.columns}
    date_col = col_map.get("date")
    home_col = col_map.get("home")
    away_col = col_map.get("away")

    if date_col and home_col and away_col:
        before = len(combined)
        combined = combined.drop_duplicates(subset=[date_col, home_col, away_col])
        dupes = before - len(combined)
        if dupes:
            _emit(f"  Removed {dupes} duplicate rows")
        combined[date_col] = pd.to_datetime(combined[date_col], errors="coerce")
        combined = combined.sort_values(date_col, ascending=True).reset_index(drop=True)

    _emit(f"  Combined: {len(combined)} rows for {league_code}")
    return combined


def _save_snapshot(league_code: str, df: pd.DataFrame) -> str:
    try:
        parquet_bytes = df.to_parquet(index=True)
        db = SessionLocal()
        try:
            snap = db.query(FBrefSnapshot).filter_by(league_code=league_code).first()
            if snap:
                snap.data       = parquet_bytes
                snap.fetched_at = datetime.utcnow()
                action = "Updated"
            else:
                db.add(FBrefSnapshot(
                    league_code=league_code,
                    data=parquet_bytes,
                    fetched_at=datetime.utcnow(),
                ))
                action = "Created"
            db.commit()
            _emit(f"  OK — DB {action} {len(df)} rows for {league_code}")
            return "ok"
        finally:
            db.close()
    except Exception as e:
        _emit(f"  Database error: {e}")
        return "error"


def _fetch_league(league_code: str, current_url: str, prev_url: str) -> str:
    _emit(f"\n{'='*50}")
    _emit(f"League: {league_code}")

    current_df = _fetch_url(current_url, "current")

    _emit(f"  Waiting {SLEEP_BETWEEN_FETCHES}s before previous season fetch...")
    time.sleep(SLEEP_BETWEEN_FETCHES)

    prev_df = _fetch_url(prev_url, "previous")

    merged = _merge_seasons(current_df, prev_df, league_code)
    if merged is None:
        _emit(f"  No data for {league_code}.")
        return "no_data"

    return _save_snapshot(league_code, merged)


def _run_scrape(selected_leagues: list):
    global _is_running, _last_results
    _is_running   = True
    _last_results = {}

    _emit(f"Starting two-season scrape for: {selected_leagues}")
    _emit("Chrome opens TWICE per league (current + previous). Do not click anything.")

    for i, code in enumerate(selected_leagues):
        urls = LEAGUE_MAP.get(code)
        if not urls:
            _emit(f"SKIP {code} — not in league map")
            continue

        result = _fetch_league(code, urls[0], urls[1])
        _last_results[code] = result

        if i < len(selected_leagues) - 1:
            _emit(f"  Waiting {SLEEP_BETWEEN_LEAGUES}s before next league...")
            time.sleep(SLEEP_BETWEEN_LEAGUES)

    _emit("Scrape complete.")
    _is_running = False


# ── FastAPI admin server ──────────────────────────────────────────────────────
admin = FastAPI()
admin.add_middleware(CORSMiddleware, allow_origins=["*"],
                     allow_methods=["*"], allow_headers=["*"])


@admin.get("/", response_class=HTMLResponse)
def serve_panel():
    panel_path = Path(__file__).parent / "admin_panel.html"
    with open(panel_path, encoding="utf-8") as f:
        return f.read()


@admin.get("/api/status")
def status():
    return {
        "is_running": _is_running,
        "leagues":    list(LEAGUE_MAP.keys()),
        "snapshots":  _get_snapshot_meta(),
        "last_run":   _last_results,
    }


@admin.post("/api/scrape")
def start_scrape(payload: dict):
    global _is_running
    if _is_running:
        return {"ok": False, "reason": "Scrape already running"}
    selected = payload.get("leagues", list(LEAGUE_MAP.keys()))
    thread   = threading.Thread(target=_run_scrape, args=(selected,), daemon=True)
    thread.start()
    return {"ok": True}


@admin.get("/api/logs")
def stream_logs():
    def generate():
        while True:
            try:
                line = _log_queue.get(timeout=30)
                yield f"data: {line}\n\n"
            except queue.Empty:
                yield "data: \n\n"
    return StreamingResponse(generate(), media_type="text/event-stream")


if __name__ == "__main__":
    print("\n  ATHENA Admin Panel — Two-Season Mode")
    print("  Open http://localhost:8001 in your browser\n")
    uvicorn.run(admin, host="127.0.0.1", port=8001, log_level="warning")
