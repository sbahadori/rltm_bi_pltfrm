from __future__ import annotations

"""Authentication and dashboard-user API router."""

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

try:
    from auth import create_access_token, get_current_user, hash_password, require_role, verify_password
    from db import call_usp_one, call_usp_void
except ImportError:  # pragma: no cover
    from .auth import create_access_token, get_current_user, hash_password, require_role, verify_password
    from .db import call_usp_one, call_usp_void

router = APIRouter()


class LoginRequest(BaseModel):
    username: str
    password: str


class CreateUserRequest(BaseModel):
    username: str
    password: str
    email: str | None = None
    role: str = "viewer"


class ChangePasswordRequest(BaseModel):
    old_password: str
    new_password: str


@router.post("/auth/login")
async def login(req: LoginRequest) -> dict:
    row = call_usp_one("usp_get_dashboard_user", (req.username,))
    if not row or not verify_password(req.password, row["password_hash"]):
        raise HTTPException(status_code=status.HTTP_401_UNAUTHORIZED, detail="Invalid credentials")

    call_usp_void("usp_update_last_login", (req.username,))
    token = create_access_token(
        {
            "sub": row["username"],
            "user_id": row["user_id"],
            "role": row["role"],
        }
    )
    return {
        "access_token": token,
        "token_type": "bearer",
        "username": row["username"],
        "role": row["role"],
    }


@router.get("/auth/me")
async def me(user: dict = Depends(get_current_user)) -> dict:
    return {"username": user.get("sub"), "role": user.get("role")}


@router.post("/auth/users", dependencies=[Depends(require_role("admin"))])
async def create_user(req: CreateUserRequest, user: dict = Depends(get_current_user)) -> dict:
    call_usp_void(
        "usp_upsert_dashboard_user",
        (req.username, req.email, hash_password(req.password), req.role),
    )
    return {"created": req.username, "role": req.role}


@router.post("/auth/change-password")
async def change_password(req: ChangePasswordRequest, user: dict = Depends(get_current_user)) -> dict:
    row = call_usp_one("usp_get_dashboard_user", (user["sub"],))
    if not row or not verify_password(req.old_password, row["password_hash"]):
        raise HTTPException(status_code=400, detail="Old password incorrect")

    call_usp_void(
        "usp_upsert_dashboard_user",
        (
            user["sub"],
            row.get("email"),
            hash_password(req.new_password),
            row.get("role", "viewer"),
        ),
    )
    return {"ok": True}
