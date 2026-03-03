"""
New database model to store FBref match data as a cached parquet blob.
Add this to your existing models file, or import it from here.
"""

from datetime import datetime
from sqlalchemy import Column, String, LargeBinary, DateTime
from app.database.base import Base


class FBrefSnapshot(Base):
    """
    Stores a full league match dataframe (as parquet bytes) fetched by the
    local scraper script. Render reads from here; it never calls FBref directly.
    """
    __tablename__ = "fbref_snapshots"

    league_code  = Column(String(32), primary_key=True, index=True)
    data         = Column(LargeBinary, nullable=False)   # parquet bytes
    fetched_at   = Column(DateTime, default=datetime.utcnow)
    seasons_json = Column(String(128), nullable=True)
