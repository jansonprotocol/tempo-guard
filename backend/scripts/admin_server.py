"""
backend/scripts/admin_server.py

A tiny local web server that serves the ATHENA admin panel.
Run this on your laptop, then open http://localhost:8001 in your browser.

Usage:
    cd backend
    python -m scripts.admin_server

This NEVER runs on Render. Local only.
"""

import io
import json
import os
import queue
import sys
import threading
import time
from datetime import date, datetime, timedelta
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

# ── Make sure app/ is importable ─────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, StreamingResponse
import uvicorn

# ── Patch requests before importing soccerdata ───────────────────────────────
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BROWSER_HEADERS = {
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

_ORIG_INIT = requests.sessions.Session.__init__
def _patched_init(self, *args, **kwargs):
    _ORIG_INIT(self, *args, **kwargs)
    self.headers.update(BROWSER_HEADERS)
    retries = Retry(total=3, backoff_factor=2.0,
                    status_forcelist=[429, 500, 502, 503, 504],
                    allowed_methods=["GET", "HEAD"], raise_on_status=False)
    adapter = HTTPAdapter(max_retries=retries)
    self.mount("http://", adapter)
    self.mount("https://", adapter)

requests.sessions.Session.__init__ = _patched_init

import soccerdata as sd
import pandas as pd

from app.database.db import SessionLocal
from app.database.models_fbref import FBrefSnapshot

# ── League map ────────────────────────────────────────────────────────────────
LEAGUE_MAP = {
    "ENG-PL":  "ENG-Premier League",
    "ESP-LL":  "ESP-La Liga",
    "FRA-L1":  "FRA-Ligue 1",
    "GER-BUN": "GER-Bundesliga",
    "ITA-SA":  "ITA-Serie A",
    "NED-ERE": "NED-Eredivisie",
    "TUR-SL":  "TUR-Super Lig",
    "BRA-SA":  "BRA-Serie A",
}

CACHE_DIR = Path("/tmp/fbref_scraper_cache")
CACHE_DIR.mkdir(parents=True, exist_ok=True)
SLEEP_BETWEEN = 5

# ── Global scrape state ───────────────────────────────────────────────────────
_scrape_lock   = threading.Lock()
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
                "fetched_at": r.fetched_at.strftime("%Y-%m-%d %H:%M") if r.fetched_at else None,
                "seasons":    r.seasons_json,
            }
            for r in rows
        }
    finally:
        db.close()


def _run_scrape(selected_leagues: list[str]):
    global _is_running, _last_results
    _is_running = True
    _last_results = {}

    today   = date.today()
    year    = today.year
    start   = year - 1 if today.month < 7 else year
    seasons = [f"{start-1}-{start}", f"{start}-{start+1}"]

    _emit(f"Starting scrape for: {selected_leagues}")
    _emit(f"Seasons: {seasons}")

    for code in selected_leagues:
        comp = LEAGUE_MAP.get(code)
        if not comp:
            _emit(f"SKIP {code} — not in league map")
            continue

        _emit(f"Fetching {code} ({comp})...")
        try:
            fb = sd.FBref(
                leagues=[comp],
                seasons=seasons,
                data_dir=CACHE_DIR,
                proxy=None,
            )
            matches = None
            for fn in ("read_matches", "read_schedule", "read_team_match_stats"):
                if hasattr(fb, fn):
                    df_try = getattr(fb, fn)()
                    if isinstance(df_try, pd.DataFrame) and not df_try.empty:
                        matches = df_try
                        _emit(f"  Got {len(matches)} rows via fb.{fn}()")
                        break

            if matches is None or matches.empty:
                _emit(f"  WARNING: No data returned for {code}")
                _last_results[code] = "no_data"
                continue

            parquet_bytes = matches.to_parquet(index=True)
            db = SessionLocal()
            try:
                snap = db.query(FBrefSnapshot).filter_by(league_code=code).first()
                if snap:
                    snap.data         = parquet_bytes
                    snap.fetched_at   = datetime.utcnow()
                    snap.seasons_json = str(seasons)
                    action = "Updated"
                else:
                    db.add(FBrefSnapshot(
                        league_code=code,
                        data=parquet_bytes,
                        fetched_at=datetime.utcnow(),
                        seasons_json=str(seasons),
                    ))
                    action = "Created"
                db.commit()
                _emit(f"  OK — {action} snapshot for {code}")
                _last_results[code] = "ok"
            finally:
                db.close()

        except Exception as e:
            _emit(f"  ERROR {code}: {e}")
            _last_results[code] = "error"

        if code != selected_leagues[-1]:
            _emit(f"  Waiting {SLEEP_BETWEEN}s before next league...")
            time.sleep(SLEEP_BETWEEN)

    _emit("Scrape complete.")
    _is_running = False


# ── FastAPI admin server ──────────────────────────────────────────────────────
admin = FastAPI()
admin.add_middleware(CORSMiddleware, allow_origins=["*"],
                     allow_methods=["*"], allow_headers=["*"])


@admin.get("/", response_class=HTMLResponse)
def serve_panel():
    with open(Path(__file__).parent / "admin_panel.html") as f:
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
    """Server-Sent Events stream so the browser gets live log lines."""
    def generate():
        while True:
            try:
                line = _log_queue.get(timeout=30)
                yield f"data: {line}\n\n"
            except queue.Empty:
                yield "data: \n\n"   # heartbeat
    return StreamingResponse(generate(), media_type="text/event-stream")


if __name__ == "__main__":
    print("\n  ATHENA Admin Panel")
    print("  Open http://localhost:8001 in your browser\n")
    uvicorn.run(admin, host="127.0.0.1", port=8001, log_level="warning")
