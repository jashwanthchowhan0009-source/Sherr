"""
api/push.py — Web Push (VAPID) subscriptions + sender.

Activates automatically once VAPID_PUBLIC / VAPID_PRIVATE env vars are set and
pywebpush is installed. Until then every entry point degrades to a safe no-op
so the app & pipeline keep running.
"""

from __future__ import annotations

import json
import logging
import os

from fastapi import APIRouter, Header, HTTPException

from app.db.supabase import db
from app.security import decode_token

log = logging.getLogger("sherbyte.push")
router = APIRouter(prefix="/push", tags=["push"])

VAPID_PUBLIC = os.getenv("VAPID_PUBLIC", "")
VAPID_PRIVATE = os.getenv("VAPID_PRIVATE", "")
VAPID_SUBJECT = os.getenv("VAPID_SUBJECT", "mailto:support@thewhitetiger.in")

try:
    from pywebpush import webpush, WebPushException  # type: ignore
    _HAS_PYWEBPUSH = True
except Exception:                                    # lib not installed yet
    _HAS_PYWEBPUSH = False


def push_enabled() -> bool:
    return bool(VAPID_PUBLIC and VAPID_PRIVATE and _HAS_PYWEBPUSH)


def _uid(authorization: str):
    if authorization.startswith("Bearer "):
        return decode_token(authorization[7:], expected_kind="access")
    return None


@router.get("/vapid-public")
async def vapid_public():
    """The browser needs this to create a push subscription."""
    return {"key": VAPID_PUBLIC, "enabled": push_enabled()}


@router.post("/subscribe")
async def subscribe(payload: dict, authorization: str = Header("")):
    uid = _uid(authorization)
    if not uid:
        raise HTTPException(401, "Unauthorized")
    sub = payload.get("subscription") or payload
    endpoint = (sub or {}).get("endpoint")
    if not endpoint:
        raise HTTPException(400, "Missing subscription endpoint")
    await db.execute(
        """
        INSERT INTO push_subscriptions (user_id, endpoint, subscription)
        VALUES ($1, $2, $3)
        ON CONFLICT (endpoint) DO UPDATE SET user_id=$1, subscription=$3
        """,
        uid, endpoint, json.dumps(sub),
    )
    return {"ok": True}


@router.post("/unsubscribe")
async def unsubscribe(payload: dict):
    endpoint = (payload.get("endpoint") or "")
    if endpoint:
        await db.execute("DELETE FROM push_subscriptions WHERE endpoint=$1", endpoint)
    return {"ok": True}


async def send_push_to_user(user_id: str, data: dict) -> int:
    """Send a push to all of a user's subscriptions. Returns how many succeeded.
    No-op (returns 0) until VAPID keys + pywebpush are configured."""
    if not push_enabled():
        return 0
    rows = await db.fetch(
        "SELECT endpoint, subscription FROM push_subscriptions WHERE user_id=$1", user_id
    )
    sent = 0
    for r in rows:
        try:
            sub = r["subscription"]
            if isinstance(sub, str):
                sub = json.loads(sub)
            webpush(
                subscription_info=sub,
                data=json.dumps(data),
                vapid_private_key=VAPID_PRIVATE,
                vapid_claims={"sub": VAPID_SUBJECT},
                timeout=10,
            )
            sent += 1
        except WebPushException as e:           # 404/410 → subscription is dead
            status = getattr(getattr(e, "response", None), "status_code", None)
            if status in (404, 410):
                await db.execute("DELETE FROM push_subscriptions WHERE endpoint=$1", r["endpoint"])
            else:
                log.warning("webpush failed: %s", e)
        except Exception as e:
            log.warning("push error: %s", e)
    return sent
