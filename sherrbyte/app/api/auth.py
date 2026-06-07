"""
api/auth.py — /auth/register, /auth/login, /auth/refresh.

Issues an access/refresh token pair on register & login. Seeds onboarding topic
preferences. Refresh exchanges a valid refresh token for a new access token.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException

from app.config import PILLAR_ALIASES, SLUG_TO_PILLAR
from app.db.supabase import db
from app.models.user import LoginReq, RefreshReq, RegisterReq, TokenPair
from app.security import (
    decode_token, hash_password, make_access_token, make_refresh_token,
    needs_rehash, verify_password,
)


def _norm_email(email: str) -> str:
    """Normalize emails so 'A@X.com ' and 'a@x.com' are the same account."""
    return (email or "").strip().lower()

log = logging.getLogger("sherbyte.auth")
router = APIRouter(prefix="/auth", tags=["auth"])


def _pillar_for_topic(topic: str) -> int:
    t = topic.lower().strip()
    return SLUG_TO_PILLAR.get(t) or PILLAR_ALIASES.get(t, 1)


async def _seed_topics(conn, user_id: str, topics: list[str]) -> None:
    for topic in topics:
        await conn.execute(
            """
            INSERT INTO user_preferences (user_id, topic, pillar_id, weight)
            VALUES ($1,$2,$3,1.0)
            ON CONFLICT (user_id, topic) DO NOTHING
            """,
            user_id, topic, _pillar_for_topic(topic),
        )


@router.post("/register", response_model=TokenPair)
async def register(req: RegisterReq) -> TokenPair:
    email = _norm_email(req.email)
    existing = await db.fetchval("SELECT 1 FROM users WHERE email=$1", email)
    if existing:
        raise HTTPException(400, "Email already registered")

    name = req.name.strip() if req.name else email.split("@")[0]
    async with db.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO users (email, password_hash, name) VALUES ($1,$2,$3) RETURNING id",
            email, hash_password(req.password), name,
        )
        user_id = str(row["id"])
        await _seed_topics(conn, user_id, req.topics)

    return TokenPair(
        access_token=make_access_token(user_id),
        refresh_token=make_refresh_token(user_id),
        user_id=user_id, name=name,
    )


@router.post("/login", response_model=TokenPair)
async def login(req: LoginReq) -> TokenPair:
    user = await db.fetchrow(
        "SELECT id, password_hash, name FROM users WHERE email=$1", _norm_email(req.email)
    )
    if not user or not verify_password(req.password, user["password_hash"]):
        raise HTTPException(401, "Invalid credentials")
    # Transparently upgrade legacy static-pepper hashes to per-user salt on login.
    if needs_rehash(user["password_hash"]):
        await db.execute(
            "UPDATE users SET password_hash=$1 WHERE id=$2",
            hash_password(req.password), user["id"],
        )
    await db.execute("UPDATE users SET last_login=now() WHERE id=$1", user["id"])
    user_id = str(user["id"])
    return TokenPair(
        access_token=make_access_token(user_id),
        refresh_token=make_refresh_token(user_id),
        user_id=user_id, name=user["name"] or "",
    )


@router.post("/refresh", response_model=TokenPair)
async def refresh(req: RefreshReq) -> TokenPair:
    user_id = decode_token(req.refresh_token, expected_kind="refresh")
    if not user_id:
        raise HTTPException(401, "Invalid refresh token")
    user = await db.fetchrow("SELECT name FROM users WHERE id=$1", user_id)
    if not user:
        raise HTTPException(401, "User no longer exists")
    return TokenPair(
        access_token=make_access_token(user_id),
        refresh_token=make_refresh_token(user_id),
        user_id=user_id, name=user["name"] or "",
    )
