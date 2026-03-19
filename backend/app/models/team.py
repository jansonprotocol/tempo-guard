from sqlalchemy import Column, Integer, String, ForeignKey
from sqlalchemy.orm import relationship
from app.database.base import Base

class Team(Base):
    __tablename__ = "teams"

    id = Column(Integer, primary_key=True)
    team_key = Column(String, unique=True, index=True, nullable=False)  # normalized unique key
    display_name = Column(String, nullable=False)  # what you want to show in UI
    league_code = Column(String, index=True, nullable=False)  # e.g., NL-EDIV
    country = Column(String, default="")  # optional metadata
    current_position = Column(Integer, nullable=True) 
    aliases = relationship("TeamAlias", back_populates="team", cascade="all, delete-orphan")

class TeamAlias(Base):
    __tablename__ = "team_aliases"

    id = Column(Integer, primary_key=True)
    team_id = Column(Integer, ForeignKey("teams.id"))
    alias_key = Column(String, nullable=False, unique=True)

    team = relationship("Team", back_populates="aliases")

    def __str__(self):
        return self.alias_key   # or any other descriptive field
