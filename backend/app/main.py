# backend/app/main.py

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
from app.database.models_fbref import FBrefSnapshot  # registers the new table

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
    allow_origins=["*"],   # restrict to your frontend domain when ready
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# STARTUP: create tables + load seeds
# ------------------------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    Base.metadata.create_all(bind=engine)   # also creates fbref_snapshots table

    db = SessionLocal()
    try:
        load_league_configs(db)
        load_teams(db)
    finally:
        db.close()

# ------------------------------------------------------------------------------
# ROUTERS
# ------------------------------------------------------------------------------
app.include_router(health_router,  prefix="/health", tags=["Health"])
app.include_router(auth_router,    prefix="/api/auth", tags=["Auth"])
app.include_router(league_router,  prefix="/api",      tags=["LeagueConfig"])
app.include_router(team_router,    prefix="/api",      tags=["Teams"])
app.include_router(predict_router, prefix="/api",      tags=["Predict"])
app.include_router(future_router,  prefix="/api",      tags=["Futurematch"])
app.include_router(retro_router,   prefix="/api",      tags=["Retrosim"])
app.include_router(calib_router,   prefix="/api",      tags=["Calibration"])

# ------------------------------------------------------------------------------
# STATIC FRONTEND (served at /app)
# ------------------------------------------------------------------------------
app.mount("/", StaticFiles(directory="app/static", html=True), name="app")
