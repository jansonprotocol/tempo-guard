"""
backend/scripts/admin_server.py

Local admin panel for ATHENA data management.
Run on your laptop only — never on Render.

Usage:
    cd backend
    venv312\Scripts\activate
    python -m scripts.admin_server

Then open http://localhost:8001 in your browser.

NOTE: SeleniumBase opens a real Chrome window for each league scraped.
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

SLEEP_BETWEEN_LEAGUES = 4  # seconds between leagues

# ── League map ────────────────────────────────────────────────────────────────
LEAGUE_MAP = {
    # Original 8
    "ENG-PL":  "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ESP-LL":  "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "FRA-L1":  "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
    "GER-BUN": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "ITA-SA":  "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "NED-ERE": "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
    "TUR-SL":  "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
    "BRA-SA":  "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
    # New 8
    "MLS":     "https://fbref.com/en/comps/22/schedule/Major-League-Soccer-Scores-and-Fixtures",
    "SAU-SPL": "https://fbref.com/en/comps/70/schedule/Saudi-Pro-League-Scores-and-Fixtures",
    "DEN-SL":  "https://fbref.com/en/comps/50/schedule/Danish-Superliga-Scores-and-Fixtures",
    "ESP-LL2": "https://fbref.com/en/comps/17/schedule/Segunda-Division-Scores-and-Fixtures",
    "BEL-PL":  "https://fbref.com/en/comps/37/schedule/Belgian-Pro-League-Scores-and-Fixtures",
    "NOR-EL":  "https://fbref.com/en/comps/28/schedule/Eliteserien-Scores-and-Fixtures",
    "SWE-AL":  "https://fbref.com/en/comps/29/schedule/Allsvenskan-Scores-and-Fixtures",
    "MEX-LMX": "https://fbref.com/en/comps/31/schedule/Liga-MX-Scores-and-Fixtures",
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
                "seasons":    r.seasons_json,
            }
            for r in rows
        }
    finally:
        db.close()


def _fetch_league(league_code: str, url: str) -> str:
    """Fetch one league from FBref using SeleniumBase UC mode."""
    from seleniumbase import Driver

    _emit(f"  Opening Chrome for {league_code}...")
    driver = None
    try:
        driver = Driver(uc=True, headless=False)
        driver.uc_open_with_reconnect(url, 4)
        driver.uc_gui_click_captcha()

        # Wait for the page to fully load
        time.sleep(3)

        html = driver.get_page_source()
        _emit(f"  Page loaded ({len(html)} bytes)")

    except Exception as e:
        _emit(f"  Browser error: {e}")
        if driver:
            driver.quit()
        return "error"
    finally:
        if driver:
            driver.quit()

    # Check we got actual FBref content, not a block page
    if "Just a moment" in html or len(html) < 5000:
        _emit(f"  Still blocked by Cloudflare.")
        return "blocked"

    # Parse tables from the HTML
    try:
        tables = pd.read_html(io.StringIO(html))
        _emit(f"  Found {len(tables)} tables")
    except Exception as e:
        _emit(f"  Could not parse tables: {e}")
        return "error"

    if not tables:
        _emit("  No tables found.")
        return "no_data"

    # Biggest table = fixtures/results table
    df = max(tables, key=len)
    df = df.dropna(how="all")

    # Keep only completed matches
    score_col = next(
        (c for c in df.columns if str(c).lower() in ("score", "scores")), None
    )
    if score_col:
        df = df[df[score_col].astype(str).str.contains(r"\d[–-]\d", na=False)]

    if df.empty:
        _emit("  No completed match rows found.")
        return "no_data"

    _emit(f"  {len(df)} completed matches found")

    # Store in database
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
            _emit(f"  OK — {action} snapshot for {league_code}")
            return "ok"
        finally:
            db.close()
    except Exception as e:
        _emit(f"  Database error: {e}")
        return "error"


def _run_scrape(selected_leagues: list):
    global _is_running, _last_results
    _is_running = True
    _last_results = {}

    _emit(f"Starting scrape for: {selected_leagues}")
    _emit("Chrome will open and close for each league — do not click anything.")

    for i, code in enumerate(selected_leagues):
        url = LEAGUE_MAP.get(code)
        if not url:
            _emit(f"SKIP {code} — not in league map")
            continue

        _emit(f"Fetching {code}...")
        result = _fetch_league(code, url)
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
    thread = threading.Thread(target=_run_scrape, args=(selected,), daemon=True)
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
    print("\n  ATHENA Admin Panel")
    print("  Open http://localhost:8001 in your browser\n")
    uvicorn.run(admin, host="127.0.0.1", port=8001, log_level="warning")
