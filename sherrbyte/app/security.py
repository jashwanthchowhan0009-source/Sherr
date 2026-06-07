"""
security.py — Password hashing + JWT (access/refresh) + auth dependencies.

Self-contained HMAC-signed tokens (no external JWT lib needed) carrying the user
id and an expiry. Two token types: short-lived access, long-lived refresh.
FastAPI dependencies expose the current user to routes.
"""

from __future__ import annotations

import base64
import hashlib
import hmac
import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional

from fastapi import Header, HTTPException

from app.config import settings


# ─── Passwords ────────────────────────────────────────────────────────────────
# PBKDF2-HMAC-SHA256 with a per-user random salt. New hashes are stored as
# "pbkdf2$<iterations>$<salt_hex>$<dk_hex>". Legacy hashes (bare hex, static
# pepper = jwt_secret) are still verified for backward compatibility so existing
# accounts keep working; they transparently upgrade on next login.
_PBKDF2_ITERATIONS = 200_000


def _pbkdf2(pw: str, salt: bytes, iterations: int) -> str:
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, iterations).hex()


def hash_password(pw: str) -> str:
    salt = os.urandom(16)
    dk = _pbkdf2(pw, salt, _PBKDF2_ITERATIONS)
    return f"pbkdf2${_PBKDF2_ITERATIONS}${salt.hex()}${dk}"


def _verify_legacy(pw: str) -> str:
    # Original scheme: static pepper from jwt_secret, 100k iterations, bare hex.
    return hashlib.pbkdf2_hmac("sha256", pw.encode(), settings.jwt_secret.encode(), 100_000).hex()


def verify_password(pw: str, hashed: str) -> bool:
    if hashed.startswith("pbkdf2$"):
        try:
            _, iters, salt_hex, dk_hex = hashed.split("$", 3)
            return hmac.compare_digest(_pbkdf2(pw, bytes.fromhex(salt_hex), int(iters)), dk_hex)
        except Exception:
            return False
    # Legacy static-pepper hash.
    return hmac.compare_digest(_verify_legacy(pw), hashed)


def needs_rehash(hashed: str) -> bool:
    """True if the stored hash uses the old static-pepper scheme."""
    return not hashed.startswith("pbkdf2$")


# ─── Tokens ───────────────────────────────────────────────────────────────────
def _make_token(user_id: str, kind: str, ttl: timedelta) -> str:
    payload = json.dumps({
        "id": user_id,
        "kind": kind,
        "exp": (datetime.now(timezone.utc) + ttl).isoformat(),
    })
    raw = base64.urlsafe_b64encode(payload.encode()).decode().rstrip("=")
    sig = hmac.new(settings.jwt_secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return f"{raw}.{sig}"


def make_access_token(user_id: str) -> str:
    return _make_token(user_id, "access", timedelta(minutes=settings.jwt_access_ttl_min))


def make_refresh_token(user_id: str) -> str:
    return _make_token(user_id, "refresh", timedelta(days=settings.jwt_refresh_ttl_days))


def decode_token(token: str, expected_kind: Optional[str] = None) -> Optional[str]:
    try:
        raw, sig = token.rsplit(".", 1)
        expected = hmac.new(settings.jwt_secret.encode(), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac.compare_digest(sig, expected):
            return None
        padded = raw + "=" * (-len(raw) % 4)
        payload = json.loads(base64.urlsafe_b64decode(padded))
        if datetime.fromisoformat(payload["exp"]) < datetime.now(timezone.utc):
            return None
        if expected_kind and payload.get("kind") != expected_kind:
            return None
        return payload["id"]
    except Exception:
        return None


# ─── FastAPI dependencies ─────────────────────────────────────────────────────
async def require_user(authorization: str = Header("")) -> str:
    """Hard auth — 401 if no valid access token."""
    if authorization.startswith("Bearer "):
        uid = decode_token(authorization[7:], expected_kind="access")
        if uid:
            return uid
    raise HTTPException(status_code=401, detail="Unauthorized")


async def optional_user(authorization: str = Header("")) -> Optional[str]:
    """Soft auth — returns the user id or None (for anonymous-friendly routes)."""
    if authorization.startswith("Bearer "):
        return decode_token(authorization[7:], expected_kind="access")
    return None
