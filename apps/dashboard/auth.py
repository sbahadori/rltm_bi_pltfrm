from __future__ import annotations

import os
try:
    from apps.dashboard.settings import get_settings
except ImportError:  # pragma: no cover
    from .settings import get_settings

from datetime import datetime, timedelta, timezone
from typing import Any

from fastapi import Depends, HTTPException, status
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from jose import JWTError, jwt
from passlib.context import CryptContext

# ─────────────────────────────────────────────────────────────
# Config
# ─────────────────────────────────────────────────────────────

settings = get_settings()

SECRET_KEY = settings.dashboard_secret_key

if not SECRET_KEY:
    if settings.app_env in {"prod", "production"}:
        raise RuntimeError("DASHBOARD_SECRET_KEY is required in production.")
    SECRET_KEY = "dev-only-rltm-bi-dashboard-secret-change-me"

ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = settings.dashboard_token_expire_minutes

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
bearer_scheme = HTTPBearer(auto_error=False)

# ─────────────────────────────────────────────────────────────
# Password helpers
# ─────────────────────────────────────────────────────────────

def hash_password(password: str) -> str:
    return pwd_context.hash(password)


def verify_password(plain: str, hashed: str) -> bool:
    return pwd_context.verify(plain, hashed)


# ─────────────────────────────────────────────────────────────
# Token helpers
# ─────────────────────────────────────────────────────────────

def create_access_token(data: dict[str, Any]) -> str:
    to_encode = data.copy()
    expire = datetime.now(timezone.utc) + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode["exp"] = expire
    return jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)


def _decode_token(token: str) -> dict[str, Any]:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except JWTError as exc:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid or expired token",
            headers={"WWW-Authenticate": "Bearer"},
        ) from exc


# ─────────────────────────────────────────────────────────────
# FastAPI dependency injectors
# ─────────────────────────────────────────────────────────────

def get_current_user(
    credentials: HTTPAuthorizationCredentials | None = Depends(bearer_scheme),
) -> dict[str, Any]:
    if credentials is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Not authenticated",
            headers={"WWW-Authenticate": "Bearer"},
        )
    return _decode_token(credentials.credentials)


def get_current_user_ws(token: str | None) -> dict[str, Any]:
    """
    WebSocket auth — token is passed as ?token=... query param.
    """
    if not token:
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Missing token")
    return _decode_token(token)


def require_role(*roles: str):
    """
    Dependency factory.
    Usage:  Depends(require_role("admin", "operator"))
    """
    def checker(user: dict = Depends(get_current_user)) -> dict:
        if user.get("role") not in roles:
            raise HTTPException(
                status_code=status.HTTP_403_FORBIDDEN,
                detail=f"Insufficient role. Required: {list(roles)}",
            )
        return user
    return checker