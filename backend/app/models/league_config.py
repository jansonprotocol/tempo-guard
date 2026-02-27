from sqlalchemy import Column, Integer, String, Float, Boolean
from app.database.base import Base

class LeagueConfig(Base):
    __tablename__ = "league_configs"

    id = Column(Integer, primary_key=True, index=True)

    # League identifier (e.g. "NL-EDIV")
    league_code = Column(String, unique=True, nullable=False)

    # Simple MVP configuration fields
    base_over_bias = Column(Float, default=0.0)    # + = more over, - = more under
    base_under_bias = Column(Float, default=0.0)   # + = more under, - = less under
    tempo_factor = Column(Float, default=1.0)      # >1: high tempo, <1: low tempo
    safety_mode = Column(Boolean, default=True)    # if True = prefer U3.5/4.5

    # For future expansion
    description = Column(String, default="")
