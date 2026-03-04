"""
backend/scripts/admin_server.py

Local admin panel for ATHENA data management.
Run on your laptop only — never on Render.

Usage:
    cd backend
    python -m scripts.admin_server

Then open http://localhost:8001 in your browser.
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

import requests
import pandas as pd
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
import uvicorn

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── Browser headers ───────────────────────────────────────────────────────────
HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Referer": "https://fbref.com/en/",
}

SLEEP_BETWEEN_LEAGUES = 5

# ── League map ────────────────────────────────────────────────────────────────
LEAGUE_MAP = {
    "ENG-PL":  "https://fbref.com/en/comps/9/schedule/Premier-League-Scores-and-Fixtures",
    "ESP-LL":  "https://fbref.com/en/comps/12/schedule/La-Liga-Scores-and-Fixtures",
    "FRA-L1":  "https://fbref.com/en/comps/13/schedule/Ligue-1-Scores-and-Fixtures",
    "GER-BUN": "https://fbref.com/en/comps/20/schedule/Bundesliga-Scores-and-Fixtures",
    "ITA-SA":  "https://fbref.com/en/comps/11/schedule/Serie-A-Scores-and-Fixtures",
    "NED-ERE": "https://fbref.com/en/comps/23/schedule/Eredivisie-Scores-and-Fixtures",
    "TUR-SL":  "https://fbref.com/en/comps/26/schedule/Super-Lig-Scores-and-Fixtures",
    "BRA-SA":  "https://fbref.com/en/comps/24/schedule/Serie-A-Scores-and-Fixtures",
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
    session = requests.Session()
    session.headers.update(HEADERS)

    try:
        session.get("https://fbref.com/en/", timeout=20)
        _emit("  Warm-up OK")
    except Exception as e:
        _emit(f"  Warm-up failed (continuing): {e}")

    time.sleep(2)

    try:
        resp = session.get(url, timeout=30)
        _emit(f"  Status: {resp.status_code}")

        if resp.status_code == 403:
            _emit("  BLOCKED (403) — FBref is rate limiting. Try again later.")
            return "blocked"
        if resp.status_code != 200:
            _emit(f"  ERROR: Unexpected status {resp.status_code}")
            return "error"

    except Exception as e:
        _emit(f"  Request failed: {e}")
        return "error"

    try:
        tables = pd.read_html(io.StringIO(resp.text))
        _emit(f"  Found {len(tables)} tables")
    except Exception as e:
        _emit(f"  Could not parse tables: {e}")
        return "error"

    if not tables:
        _emit("  No tables found.")
        return "no_data"

    df = max(tables, key=len)
    df = df.dropna(how="all")

    score_col = next(
        (c for c in df.columns if str(c).lower() in ("score", "scores")), None
    )
    if score_col:
        df = df[df[score_col].astype(str).str.contains(r"\d[–-]\d", na=False)]

    if df.empty:
        _emit("  No completed match rows found.")
        return "no_data"

    _emit(f"  {len(df)} completed matches found")

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

    for i, code in enumerate(selected_leagues):
        url = LEAGUE_MAP.get(code)
        if not url:
            _emit(f"SKIP {code} — not in league map")
            continue

        _emit(f"Fetching {code}...")
        result = _fetch_league(code, url)
        _last_results[code] = result

        if i < len(selected_leagues) - 1:
            _emit(f"  Waiting {SLEEP_BETWEEN_LEAGUES}s...")
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
