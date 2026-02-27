from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Routers
from app.api.routes_auth import router as auth_router
from app.api.routes_health import router as health_router
from app.api.routes_league import router as league_router

# Database
from app.database.base import Base
from app.database.db import engine, SessionLocal

# Memory loader
from app.memory_loader import load_league_configs


app = FastAPI(
    title="ATHENA: Tempo Guard",
    description="Tempo-aware predictive engine (MVP version).",
    version="0.1.0",
)

# ---- CORS ----
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# ---- STARTUP EVENT ----
@app.on_event("startup")
def startup_event():
    # Create tables if missing
    Base.metadata.create_all(bind=engine)

    # Load league configs from seed JSON
    db = SessionLocal()
    load_league_configs(db)
    db.close()


# ---- ROUTERS ----
app.include_router(health_router, prefix="/health", tags=["Health"])
app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
app.include_router(league_router, prefix="/api", tags=["LeagueConfig"])
