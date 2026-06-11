"""
api/notifications.py — Firebase Cloud Messaging (FCM) web push.

Firebase is used ONLY for push delivery. Auth (JWT) and the Supabase DB are
untouched — these endpoints reuse the existing Bearer-token decode and store
device tokens in their own `push_tokens` table.

Everything degrades to a safe no-op until firebase-admin is installed AND the
FIREBASE_SERVICE_ACCOUNT env var (full service-account JSON) is set.
"""

from __future__ import annotations

import asyncio
import json
import logging
import os

from fastapi import APIRouter, Header, HTTPException

from app.db.supabase import db
from app.security import decode_token

log = logging.getLogger("sherbyte.fcm")
router = APIRouter(prefix="/api/notifications", tags=["notifications"])

try:
    import firebase_admin
    from firebase_admin import credentials, messaging
    _HAS_FIREBASE = True
except Exception:                       # lib not installed yet
    _HAS_FIREBASE = False

_fcm_ready = False


def _ensure_fcm() -> bool:
    """Initialize firebase-admin from FIREBASE_SERVICE_ACCOUNT (once). Returns
    True if FCM is usable. Never raises."""
    global _fcm_ready
    if _fcm_ready:
        return True
    if not _HAS_FIREBASE:
        return False
    sa = os.getenv("FIREBASE_SERVICE_ACCOUNT", "")
    if not sa:
        return False
    try:
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(sa))
            firebase_admin.initialize_app(cred)
        _fcm_ready = True
        return True
    except Exception as e:
        log.warning("Firebase init failed: %s", e)
        return False


def _uid(authorization: str):
    if authorization.startswith("Bearer "):
        return decode_token(authorization[7:], expected_kind="access")
    return None


def _require_uid(authorization: str) -> str:
    uid = _uid(authorization)
    if not uid:
        raise HTTPException(401, "Unauthorized")
    return uid


@router.post("/register")
async def register(payload: dict, authorization: str = Header("")):
    """Save (or re-assign) an FCM device token for the signed-in user."""
    uid = _require_uid(authorization)
    token = (payload.get("fcm_token") or payload.get("token") or "").strip()
    if not token:
        raise HTTPException(400, "Missing fcm_token")
    await db.execute(
        """
        INSERT INTO push_tokens (user_id, fcm_token) VALUES ($1, $2)
        ON CONFLICT (fcm_token) DO UPDATE SET user_id = $1
        """,
        uid, token,
    )
    return {"ok": True}


async def send_push(user_id: str, title: str, body: str, url: str = "/") -> int:
    """Send a push to all of a user's registered devices. Returns the count
    delivered. Auto-deletes tokens FCM reports as invalid/unregistered.
    No-op (returns 0) until firebase-admin + FIREBASE_SERVICE_ACCOUNT exist."""
    if not _ensure_fcm():
        return 0
    rows = await db.fetch("SELECT fcm_token FROM push_tokens WHERE user_id=$1", user_id)
    tokens = [r["fcm_token"] for r in rows]
    if not tokens:
        return 0
    # Data-only message → the SW (background) / onMessage (foreground) render it,
    # so there are no duplicate auto-displayed notifications.
    data = {"title": title, "body": body, "url": url}
    messages = [messaging.Message(token=t, data=data) for t in tokens]
    try:
        resp = await asyncio.to_thread(messaging.send_each, messages)
    except Exception as e:
        log.warning("FCM send failed: %s", e)
        return 0

    sent, dead = 0, []
    for tok, r in zip(tokens, resp.responses):
        if r.success:
            sent += 1
            continue
        exc = r.exception
        code = str(getattr(exc, "code", "") or "").lower()
        name = type(exc).__name__ if exc else ""
        if (name in ("UnregisteredError", "SenderIdMismatchError")
                or "not-registered" in code or "invalid-argument" in code or "not_found" in code):
            dead.append(tok)
        else:
            log.warning("FCM token failed (%s): %s", name, exc)
    if dead:
        await db.execute("DELETE FROM push_tokens WHERE fcm_token = ANY($1::text[])", dead)
        log.info("Pruned %d invalid FCM token(s)", len(dead))
    return sent


@router.post("/test")
async def test(authorization: str = Header("")):
    """Send a test push to the calling user's devices."""
    uid = _require_uid(authorization)
    if not _ensure_fcm():
        raise HTTPException(503, "Push not configured (firebase-admin / FIREBASE_SERVICE_ACCOUNT missing)")
    n = await send_push(uid, "SherrByte test 🐯", "Push notifications are working!", "/")
    if n == 0:
        raise HTTPException(404, "No registered devices for this account")
    return {"ok": True, "sent": n}
