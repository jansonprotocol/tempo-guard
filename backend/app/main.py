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
from app.api.routes_batch import router as batch_router
from app.api.routes_player_power import router as player_power_router
from app.api.routes_alias_manager import router as alias_router
# Database & models
from app.database.base import Base
from app.database.db import engine, SessionLocal
from app.database.models_fbref import FBrefSnapshot  # registers the new table
from app.models.team_config import TeamConfig         # registers team_configs table
# v2.0 — player-level models (import registers tables with Base.metadata)
from app.models.models_players import Player, PlayerSeasonStats, SquadSnapshot  # noqa: F401
# Memory loaders
from app.memory_loader import load_league_configs, load_teams

from app.admin import setup_admin

app = FastAPI(
    title="ATHENA: Tempo Guard",
    description="Tempo-aware predictive engine (MVP).",
    version="2.2.0",
)

# ------------------------------------------------------------------------------
# CORS
# ------------------------------------------------------------------------------
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------------------------------
# SAFE COLUMN MIGRATIONS
# Each entry: (table, column, sql_type, default_clause_or_None)
# Runs before create_all so the app never crashes on a missing column.
# Add new columns here whenever the model grows — never remove old entries.
# ------------------------------------------------------------------------------
_COLUMN_MIGRATIONS = [
    # prediction_log
    ("prediction_log", "variance_flag", "VARCHAR",  None),
    ("prediction_log", "match_time",    "VARCHAR",  None),
    # league_configs — DEG/DET/EPS sensitivity multipliers
    ("league_configs", "deg_sensitivity", "FLOAT",  "1.0"),
    ("league_configs", "det_sensitivity", "FLOAT",  "1.0"),
    ("league_configs", "eps_sensitivity", "FLOAT",  "1.0"),
    # NEW: form delta sensitivity multiplier
    ("league_configs", "form_delta_sensitivity", "FLOAT", "0.0"),
    # league_configs — display fields (older, kept for safety)
    ("league_configs", "display_name",   "VARCHAR", "''"),
    ("league_configs", "country_code",   "VARCHAR", "''"),
    # league_configs — v2.0 cross-league strength coefficient
    ("league_configs", "strength_coefficient", "FLOAT", "1.0"),
    # team_configs — module nudges + diagnostics
    ("team_configs",   "det_nudge",      "FLOAT",   "0.0"),
    ("team_configs",   "deg_nudge",      "FLOAT",   "0.0"),
    ("team_configs",   "avg_det",        "FLOAT",   None),
    ("team_configs",   "avg_deg",        "FLOAT",   None),
    # In _COLUMN_MIGRATIONS, add:
("team_configs", "good_form_nudge",    "FLOAT", "0.0"),
("team_configs", "poor_form_nudge",    "FLOAT", "0.0"),
("team_configs", "neutral_form_nudge", "FLOAT", "0.0"),
("team_configs", "form_good_threshold", "INTEGER", "3"),
("team_configs", "form_poor_threshold", "INTEGER", "-3"),
    # team_configs — v2.0 player-derived squad power scores
    ("team_configs",   "squad_power",    "FLOAT",   None),
    ("team_configs",   "atk_power",      "FLOAT",   None),
    ("team_configs",   "mid_power",      "FLOAT",   None),
    ("team_configs",   "def_power",      "FLOAT",   None),
    ("team_configs",   "gk_power",       "FLOAT",   None),
    # player_season_stats — v2.0 performance delta
    ("player_season_stats", "performance_delta", "FLOAT", None),
]


def _safe_migrate(db):
    """
    Idempotent column migrations — runs on every startup.
    Uses sqlalchemy inspect to check existing columns before issuing ALTER TABLE,
    so it's safe to re-run indefinitely without errors or data loss.
    """
    from sqlalchemy import text, inspect as sa_inspect

    inspector = sa_inspect(db.bind)
    existing_tables = set(inspector.get_table_names())

    for table, column, sql_type, default in _COLUMN_MIGRATIONS:
        if table not in existing_tables:
            continue  # table doesn't exist yet — create_all will handle it
        existing_cols = {c["name"] for c in inspector.get_columns(table)}
        if column in existing_cols:
            continue  # already present — nothing to do
        try:
            default_clause = f" DEFAULT {default}" if default is not None else ""
            db.execute(text(
                f"ALTER TABLE {table} ADD COLUMN {column} {sql_type}{default_clause}"
            ))
            db.commit()
            print(f"[startup] Migration: added {table}.{column} ({sql_type}{default_clause})")
        except Exception as e:
            db.rollback()
            print(f"[startup] Migration warning — {table}.{column}: {e}")

    # calibration_log table (created via raw SQL because it's not a SQLAlchemy model)
    if "calibration_log" not in existing_tables:
        try:
            db.execute(text("""
                CREATE TABLE calibration_log (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    league_code VARCHAR NOT NULL,
                    hit_rate    FLOAT   NOT NULL,
                    sample_size INTEGER,
                    applied     BOOLEAN DEFAULT 0,
                    run_at      DATETIME
                )
            """))
            db.commit()
            print("[startup] Migration: created calibration_log table")
        except Exception as e:
            db.rollback()
            print(f"[startup] Migration warning — calibration_log: {e}")

    # stats_fetch_cache table (for player stats fetch cache)
    if "stats_fetch_cache" not in existing_tables:
        try:
            db.execute(text("""
                CREATE TABLE stats_fetch_cache (
                    league_code VARCHAR PRIMARY KEY,
                    last_fetched TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            db.commit()
            print("[startup] Migration: created stats_fetch_cache table")
        except Exception as e:
            db.rollback()
            print(f"[startup] Migration warning — stats_fetch_cache: {e}")


# ------------------------------------------------------------------------------
# STARTUP: migrate → create tables → load seeds
# ------------------------------------------------------------------------------
@app.on_event("startup")
def startup_event():
    db = SessionLocal()
    try:
        # 1. Safe column migrations FIRST — before anything queries the models
        _safe_migrate(db)
        # 2. Create any fully new tables defined in SQLAlchemy models
        #    This now includes: players, player_season_stats, squad_snapshots
        Base.metadata.create_all(bind=engine)
        # 3. Seed league configs + teams from JSON
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
app.include_router(batch_router,   prefix="/api")
app.include_router(player_power_router, prefix="/api", tags=["PlayerPower"])
app.include_router(alias_router, tags=["AliasManager"])


# ------------------------------------------------------------------------------
# ADMIN DASHBOARD (must be before static mount)
# ------------------------------------------------------------------------------
setup_admin(app)

# ------------------------------------------------------------------------------
# STATIC FRONTEND (served at /app)
# ------------------------------------------------------------------------------
from fastapi.responses import RedirectResponse

@app.get("/")
def root():
    return RedirectResponse(url="/app")

app.mount("/app", StaticFiles(directory="app/static", html=True), name="app")
