from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from app.database.db import get_db
from app.database.models import User
from app.auth.hashing import hash_password, verify_password
from app.auth.jwt_manager import create_access_token

router = APIRouter()

# -------------------------------------------------------
# NEW: MAKE REGISTER-ADMIN WORK OVER GET (browser-friendly)
# -------------------------------------------------------
@router.get("/register-admin")
def register_admin(
    email: str = Query(...),
    password: str = Query(...),
    db: Session = Depends(get_db)
):
    existing = db.query(User).filter(User.email == email).first()
    if existing:
        raise HTTPException(status_code=400, detail="Admin already exists")

    admin = User(
        email=email,
        hashed_password=hash_password(password),
        is_admin=True,
    )
    db.add(admin)
    db.commit()
    return {"message": "Admin created via GET"}


# ------------------------------------
# LOGIN STILL WORKS VIA GET AS WELL
# ------------------------------------
@router.get("/login")
def login(
    email: str = Query(...),
    password: str = Query(...),
    db: Session = Depends(get_db)
):
    user = db.query(User).filter(User.email == email).first()

    if not user or not verify_password(password, user.hashed_password):
        raise HTTPException(status_code=401, detail="Invalid login")

    token = create_access_token(str(user.id))
    return {"access_token": token, "token_type": "bearer"}
