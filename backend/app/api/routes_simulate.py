from fastapi import APIRouter, Depends
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.database.db import get_db
from app.engine.sim_engine import run_simulation


router = APIRouter()

class SimRequest(BaseModel):
    team_a: str
    team_b: str
    date: str
    league_code: str


@router.post("/simulate")
def simulate(req: SimRequest, db: Session = Depends(get_db)):

