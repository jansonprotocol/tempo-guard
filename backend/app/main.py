from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

# Router imports
from app.api.routes_auth import router as auth_router
from app.api.routes_health import router as health_router
from app.api.routes_league import router as league_router

# Database imports
from app.database.base import Base
from app.database.db import engine


def create_app() -> FastAPI:
    app = FastAPI(
        title="ATHENA: Tempo Guard",
        description="Tempo-aware predictive engine powered by ATHENA V5-L.",
        version="1.0.0",
    )

    # Enable CORS for frontend
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    # Auto-create database tables
    Base.metadata.create_all(bind=engine)

    # Register routers
    app.include_router(health_router, prefix="/health", tags=["Health"])
    app.include_router(auth_router, prefix="/api/auth", tags=["Auth"])
    app.include_router(league_router, prefix="/api", tags=["LeagueConfig"])

    return app


app = create_app()
