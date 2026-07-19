from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel
from auth.jwt_handler import authenticate_user, create_token

router = APIRouter(prefix="/auth", tags=["Authentication"])

class LoginRequest(BaseModel):
    username: str
    password: str

@router.post("/login")
def login(req: LoginRequest):
    user = authenticate_user(req.username, req.password)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Incorrect username or password"
        )
    token = create_token(user["username"], user["role"])
    return {"access_token": token, "token_type": "bearer", "role": user["role"]}
