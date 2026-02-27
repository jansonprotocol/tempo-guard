from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles

# Routers
from app.api.routes_health import router as health_router
from app.api.routes_auth import router as auth_router
from app.api.routes_league import router as league_router
from app.api.routes_simulate import router as simulate_router
from app.api.routes_team import router as team_router

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
app.include_router(simulate_router, prefix="/api",    tags=["Simulate"])
app.include_router(team_router,   prefix="/api",      tags=["Teams"])

# ------------------------------------------------------------------------------
# STATIC FRONTEND (served at /app)
# ------------------------------------------------------------------------------
app.mount("/app", StaticFiles(directory="app/static", html=True), name="app")
