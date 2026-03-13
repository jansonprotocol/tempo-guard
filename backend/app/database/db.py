from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import os

print(f"DEBUG: I am looking in: {os.getcwd()}")
print(f"DEBUG: The URL I found is: {os.getenv('DATABASE_URL')}")

DATABASE_URL = os.getenv("DATABASE_URL", "")

engine = create_engine(DATABASE_URL, pool_pre_ping=True)
SessionLocal = sessionmaker(bind=engine, autocommit=False, autoflush=False)

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
