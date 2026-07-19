from datetime import datetime, timedelta, timezone
from jose import jwt, JWTError
from fastapi import HTTPException, Depends, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from config import JWT_SECRET, JWT_ALGORITHM, JWT_EXPIRE_MINUTES, USERS_DB
from passlib.context import CryptContext

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
security = HTTPBearer()

def create_token(username: str, role: str) -> str:
    expire = datetime.now(timezone.utc) + timedelta(minutes=JWT_EXPIRE_MINUTES)
    return jwt.encode({"sub": username, "role": role, "exp": expire},
                      JWT_SECRET, algorithm=JWT_ALGORITHM)

def verify_token(cred: HTTPAuthorizationCredentials = Depends(security)):
    try:
        payload = jwt.decode(cred.credentials, JWT_SECRET, algorithms=[JWT_ALGORITHM])
        return payload
    except JWTError:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired access token",
            headers={"WWW-Authenticate": "Bearer"},
        )

def require_role(required_role: str):
    def role_checker(payload: dict = Depends(verify_token)):
        user_role = payload.get("role")
        # Admins bypass all role restrictions
        if user_role != required_role and user_role != "admin":
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail="Action requires elevated privileges"
            )
        return payload
    return role_checker

def authenticate_user(username: str, password: str):
    user = USERS_DB.get(username)
    if not user or user["password"] != password:
        return None
    return {"username": username, "role": user["role"]}
