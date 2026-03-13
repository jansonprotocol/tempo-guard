import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from urllib.parse import urlparse

# Get the DATABASE_URL from environment
db_url = os.getenv("DATABASE_URL")
if not db_url:
    raise RuntimeError("DATABASE_URL not set")

# Parse the URL manually to extract components
parsed = urlparse(db_url)

# Extract parts (assumes standard PostgreSQL URL format)
user = parsed.username
password = parsed.password
host = parsed.hostname
port = parsed.port or 5432
database = parsed.path.lstrip('/')
# Extract SSL mode from query params if present
query_params = dict(pair.split('=') for pair in parsed.query.split('&')) if parsed.query else {}
sslmode = query_params.get('sslmode', 'require')  # default to require

# Create engine with explicit parameters
engine = create_engine(
    "postgresql+psycopg2://",  # empty URL, we'll pass connect_args
    connect_args={
        "host": host,
        "port": port,
        "user": user,
        "password": password,
        "database": database,
        "sslmode": sslmode,
    },
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
