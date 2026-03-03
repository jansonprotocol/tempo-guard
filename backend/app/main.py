# --- FBref HTTP hardening: place at the VERY TOP of app/main.py ---
import os

# 1) Ensure the process never uses a proxy
for k in ("HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "http_proxy", "https_proxy", "all_proxy"):
    os.environ.pop(k, None)
os.environ["NO_PROXY"] = "*"

# 2) Install a small on-disk HTTP cache (1 day) to avoid re-hitting FBref
try:
    import requests_cache
    requests_cache.install_cache("/tmp/http_cache", expire_after=86400)
except Exception as _e:
    print("[boot] requests_cache unavailable:", repr(_e))

# 3) Monkey-patch requests.Session so EVERY new session (including soccerdata’s)
#    has browser-like headers, sane retries, and does a warm-up visit to FBref.
try:
    import requests
    from requests.adapters import HTTPAdapter
    from urllib3.util.retry import Retry

    _ORIG_INIT = requests.sessions.Session.__init__

    def _patched_init(self, *args, **kwargs):
        _ORIG_INIT(self, *args, **kwargs)

        # Browser-like headers (User-Agent + typical Accepts)
        self.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Referer": "https://fbref.com/en/",
        })

        # Gentle retries (handle transient 429/5xx; 403 kept minimal to avoid loops)
        retries = Retry(
            total=2,
            backoff_factor=1.2,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retries)
        self.mount("http://", adapter)
        self.mount("https://", adapter)

        # WARM-UP: visit the homepage once to establish cookies before /en/comps/
        try:
            self.get("https://fbref.com/en/", timeout=20)
            print("[boot] FBref warm-up succeeded.")
        except Exception as _e:
            print("[boot] FBref warm-up failed (will continue):", repr(_e))

    requests.sessions.Session.__init__ = _patched_init
    print("[boot] Patched requests.Session with headers, retries, warm-up.")
except Exception as _e:
    print("[boot] Could not patch requests.Session:", repr(_e))

# -------------------------------------------------------------------
# (Your existing imports follow from here)

# -----------------------------------------------------

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Routers
from app.api.routes_health import router as health_router
from app.api.routes_auth import router as auth_router
from app.api.routes_league import router as league_router
from app.api.routes_team import router as team_router
from app.api.routes_predict import router as predict_router
from app.api.routes_futurematch import router as future_router
from app.api.routes_retrosim import router as retro_router
from app.api.routes_calibration import router as calib_router

# Database & models
from app.database.base import Base
from app.database.db import engine, SessionLocal

# Memory loaders
from app.memory_loader import load_league_configs, load_teams


app = FastAPI(
    title="ATHENA: Tempo Guard",
    description="Tempo-aware predictive engine (MVP).",
    version="0.2.0",
)

# ------------------------------------------------------------------------------
# CORS
# ------------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # you can restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# STARTUP: create tables + load seeds
# ------------------------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    # Create DB schema
    Base.metadata.create_all(bind=engine)

    # seed loaders
    db = SessionLocal()
    try:
        load_league_configs(db)
        load_teams(db)
    finally:
        db.close()

# ------------------------------------------------------------------------------
# ROUTERS
# ------------------------------------------------------------------------------
app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(auth_router,   prefix="/api/auth", tags=["Auth"])
app.include_router(league_router, prefix="/api",      tags=["LeagueConfig"])
app.include_router(team_router,   prefix="/api",      tags=["Teams"])
app.include_router(predict_router, prefix="/api", tags=["Predict"])
app.include_router(future_router, prefix="/api", tags=["Futurematch"])
app.include_router(retro_router,  prefix="/api", tags=["Retrosim"])
app.include_router(calib_router,  prefix="/api", tags=["Calibration"])

# ------------------------------------------------------------------------------
# STATIC FRONTEND (served at /app)
# ------------------------------------------------------------------------------
app.mount("/app", StaticFiles(directory="app/static", html=True), name="app")
