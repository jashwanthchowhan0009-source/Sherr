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

# firebase-admin is imported LAZILY (inside _ensure_fcm), never at module load.
# This keeps app boot light — importing firebase-admin pulls in grpc/google libs
# that can OOM a small free-tier instance. Push stays a safe no-op until BOTH the
# lib is installed AND FIREBASE_SERVICE_ACCOUNT is set.
_fcm_ready = False
messaging = None        # bound when firebase-admin is successfully imported


def _ensure_fcm() -> bool:
    """Import + initialize firebase-admin from FIREBASE_SERVICE_ACCOUNT (once).
    Returns True if FCM is usable. Never raises, never imports at boot."""
    global _fcm_ready, messaging
    if _fcm_ready:
        return True
    sa = os.getenv("FIREBASE_SERVICE_ACCOUNT", "")
    if not sa:
        return False
    try:
        import firebase_admin
        from firebase_admin import credentials, messaging as _messaging
        messaging = _messaging
        if not firebase_admin._apps:
            cred = credentials.Certificate(json.loads(sa))
            firebase_admin.initialize_app(cred)
        _fcm_ready = True
        return True
    except Exception as e:
        log.warning("Firebase init/import failed: %s", e)
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


def _personalize(s: str, name: str) -> str:
    """Replace name placeholders with the user's first name."""
    first = (name or "there").split()[0] if (name or "").strip() else "there"
    for tok in ("{{name}}", "{name}", "[User Name]", "[user name]",
                "[Add users name]", "[Name]", "[name]"):
        s = s.replace(tok, first)
    return s


# Default reminder used by the one-tap GET trigger. [User Name] is personalized.
DAILY_REMINDER_TITLE = "📌 SherrByte Daily Reminder"
DAILY_REMINDER_BODY = (
    "Good Morning [User Name], quick daily reminder for the SherrByte app. "
    "Please take 1 minute to open the app today and scroll a bit so Google "
    "registers our daily active status."
)


async def _do_broadcast(title_tmpl: str, body_tmpl: str, url: str) -> dict:
    """Send one personalized push to every user that has a registered device."""
    if not _ensure_fcm():
        raise HTTPException(503, "Push not configured (firebase-admin / FIREBASE_SERVICE_ACCOUNT missing)")
    if not body_tmpl.strip():
        raise HTTPException(400, "Missing body")
    rows = await db.fetch(
        """
        SELECT DISTINCT u.id,
               COALESCE(NULLIF(TRIM(u.name), ''), NULLIF(u.username, ''), 'there') AS name
        FROM users u JOIN push_tokens p ON p.user_id = u.id
        """
    )
    users, delivered = 0, 0
    for r in rows:
        name = r["name"]
        n = await send_push(str(r["id"]), _personalize(title_tmpl, name),
                            _personalize(body_tmpl, name), url)
        users += 1
        delivered += n
    log.info("Broadcast: %d users, %d devices delivered", users, delivered)
    return {"ok": True, "users": users, "delivered": delivered}


def _check_admin(secret: str) -> None:
    admin = os.getenv("ADMIN_SECRET", "")
    if not admin or secret != admin:
        raise HTTPException(403, "Forbidden")


@router.post("/broadcast")
async def broadcast(payload: dict, authorization: str = Header("")):
    """Admin-only: send ONE personalized push to every user with a registered
    device. {{name}} / [User Name] in the title/body becomes the user's first
    name. Auth: ADMIN_SECRET as Bearer token or a "secret" field. Disabled until
    ADMIN_SECRET is set, so a normal user can never trigger it."""
    provided = authorization[7:] if authorization.startswith("Bearer ") else ""
    _check_admin(provided or str(payload.get("secret") or ""))
    return await _do_broadcast(
        str(payload.get("title") or "SherrByte"),
        str(payload.get("body") or ""),
        str(payload.get("url") or "/"),
    )


@router.get("/broadcast")
async def broadcast_daily(secret: str = ""):
    """One-tap trigger for the daily reminder — open
    /api/notifications/broadcast?secret=YOUR_ADMIN_SECRET in a browser, or point
    a free cron (e.g. cron-job.org) at it to send it automatically every morning.
    Sends the baked-in DAILY_REMINDER to every opted-in user."""
    _check_admin(secret)
    return await _do_broadcast(DAILY_REMINDER_TITLE, DAILY_REMINDER_BODY, "/")
