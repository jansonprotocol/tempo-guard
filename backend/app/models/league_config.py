from sqlalchemy import Column, Integer, String, Float, Boolean
from app.database.base import Base

class LeagueConfig(Base):
    __tablename__ = "league_configs"

    id = Column(Integer, primary_key=True, index=True)

    league_code = Column(String, unique=True, nullable=False)

    base_over_bias = Column(Float, default=0.0)
    base_under_bias = Column(Float, default=0.0)
    tempo_factor = Column(Float, default=1.0)
    safety_mode = Column(Boolean, default=True)

    aggression_level = Column(Float, default=0.5)
    volatility = Column(Float, default=0.5)

    description = Column(String, default="")

    # NEW:
    display_name = Column(String, default="")
    country_code = Column(String, default="")
