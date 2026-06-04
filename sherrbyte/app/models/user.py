"""
models/user.py — User, preference, and auth request/response schemas.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, EmailStr, Field


class Preference(BaseModel):
    topic: str
    pillar_id: int
    weight: float = 1.0


class User(BaseModel):
    id: UUID
    email: str
    name: str = ""
    bio: str = ""
    avatar_url: str = ""
    language: str = "en"
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


# ─── Auth requests ────────────────────────────────────────────────────────────
class RegisterReq(BaseModel):
    email: EmailStr
    password: str = Field(min_length=6)
    name: str = ""
    topics: list[str] = Field(default_factory=list)


class LoginReq(BaseModel):
    email: EmailStr
    password: str


class RefreshReq(BaseModel):
    refresh_token: str


class TokenPair(BaseModel):
    access_token: str
    refresh_token: str
    token_type: str = "bearer"
    user_id: str
    name: str = ""


# ─── Profile updates ──────────────────────────────────────────────────────────
class UpdateProfileReq(BaseModel):
    name: Optional[str] = None
    display_name: Optional[str] = None  # MVP alias for name
    bio: Optional[str] = None
    avatar_url: Optional[str] = None
    language: Optional[str] = None


class UpdateTopicsReq(BaseModel):
    topics: list[str] = Field(default_factory=list)
    categories: list[str] = Field(default_factory=list)  # MVP alias, merged in
