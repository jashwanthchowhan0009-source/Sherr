"""
api/compat.py — MVP-compatibility layer.

The v5 frontend (index.html) calls the old flat endpoints and expects the old
response shapes (notably `token` instead of `access_token`, and flat `/feed`,
`/trending`, `/explore`, `/interact`, `/notifications`). This router re-exposes
exactly those — and ONLY those that don't already exist in the v6 API — so the
existing UI keeps working while the frontend is migrated.

Endpoints that v6 already serves identically (/me, /search, /article/{id},
/bookmarks, /me/topics) are NOT duplicated here: the old UI hits them with the
same Bearer access token that compat login now returns.
"""

from __future__ import annotations

import logging
import os
import secrets
import smtplib
import time as _time
from email.mime.text import MIMEText
from typing import Optional

import httpx
from fastapi import APIRouter, Header, HTTPException, Query

from app.api import activity, auth, feed
from app.config import PILLARS
from app.security import hash_password
from app.db.supabase import db
from app.models.user import LoginReq, RegisterReq, UpdateTopicsReq
from app.security import decode_token

log = logging.getLogger("sherbyte.compat")
router = APIRouter(tags=["compat"])


def _uid(authorization: str) -> Optional[str]:
    if authorization.startswith("Bearer "):
        return decode_token(authorization[7:], expected_kind="access")
    return None


def _require_uid(authorization: str) -> str:
    uid = _uid(authorization)
    if not uid:
        raise HTTPException(401, "Unauthorized")
    return uid


# ─── Auth (old `token` shape) ─────────────────────────────────────────────────
@router.post("/signup")
async def signup(req: RegisterReq):
    tp = await auth.register(req)
    return {
        "token": tp.access_token,
        "access_token": tp.access_token,
        "refresh_token": tp.refresh_token,
        "user_id": tp.user_id,
        "display_name": tp.name,
        "name": tp.name,
        "username": tp.username,
        "message": "Account created",
    }


@router.post("/login")
async def login(req: LoginReq):
    tp = await auth.login(req)
    has_topics = bool(await db.fetchval(
        "SELECT 1 FROM user_preferences WHERE user_id=$1 LIMIT 1", tp.user_id
    ))
    return {
        "token": tp.access_token,
        "access_token": tp.access_token,
        "refresh_token": tp.refresh_token,
        "user_id": tp.user_id,
        "name": tp.name,
        "display_name": tp.name,
        "username": tp.username,
        "has_topics": has_topics,
    }


# ─── Flat feed family (v6 namespaces these under /feed/*) ──────────────────────
@router.get("/feed")
async def get_feed(
    page: int = Query(1, ge=1),
    limit: int = Query(20, le=50),
    scope: str = Query(""),
    pillar: int = Query(0),
    authorization: str = Header(""),
):
    res = await feed.personalized(
        page=page, limit=limit, pillar=pillar, scope=scope, user_id=_uid(authorization)
    )
    res["has_preferences"] = res.get("personalized", False)  # old field name
    return res


@router.get("/trending")
async def trending(limit: int = Query(10, le=30), authorization: str = Header("")):
    return await feed.trending(limit=limit)


@router.get("/explore")
async def explore(
    category: str = Query(""),
    pillar: int = Query(0),
    scope: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(30, le=100),
    authorization: str = Header(""),
):
    return await feed.explore(category=category, pillar=pillar, scope=scope,
                              page=page, limit=limit)


@router.get("/stats/categories")
async def stats_categories():
    """Per-category article counts + how many were created recently."""
    rows = await db.fetch("SELECT pillar_id, COUNT(*) AS c FROM info_objects GROUP BY pillar_id")
    by = {r["pillar_id"]: r["c"] for r in rows}
    cats = [{"pillar_id": pid, "slug": p["slug"], "name": p["name"], "count": int(by.get(pid, 0))}
            for pid, p in PILLARS.items()]
    recent = await db.fetchrow(
        """
        SELECT
          COUNT(*) FILTER (WHERE created_at > now() - interval '6 hours')  AS h6,
          COUNT(*) FILTER (WHERE created_at > now() - interval '12 hours') AS h12,
          COUNT(*) FILTER (WHERE created_at > now() - interval '24 hours') AS h24
        FROM info_objects
        """
    )
    return {
        "total": sum(c["count"] for c in cats),
        "categories": cats,
        "created_last": {"6h": int(recent["h6"]), "12h": int(recent["h12"]), "24h": int(recent["h24"])},
    }


# ─── Forgot password (OTP via email) ─────────────────────────────────────────
_RESET_OTPS: dict = {}          # email -> {"otp": str, "exp": epoch}  (in-memory, short-lived)
_OTP_TTL = 600                  # 10 minutes


async def _send_email(to: str, subject: str, html: str, text: str) -> bool:
    """Send via Resend (RESEND_API_KEY) or SMTP (SMTP_HOST). Returns True if sent."""
    sender = os.getenv("MAIL_FROM", "SherrByte <onboarding@resend.dev>")
    key = os.getenv("RESEND_API_KEY", "")
    if key:
        try:
            async with httpx.AsyncClient() as c:
                r = await c.post(
                    "https://api.resend.com/emails",
                    headers={"Authorization": f"Bearer {key}"},
                    json={"from": sender, "to": [to], "subject": subject, "html": html},
                    timeout=10,
                )
                if r.status_code < 300:
                    return True
                log.warning("Resend failed %s: %s", r.status_code, r.text[:200])
        except Exception as e:
            log.warning("Resend error: %s", e)
    host = os.getenv("SMTP_HOST", "")
    if host:
        try:
            msg = MIMEText(text)
            msg["Subject"], msg["From"], msg["To"] = subject, sender, to
            with smtplib.SMTP(host, int(os.getenv("SMTP_PORT", "587"))) as s:
                s.starttls()
                s.login(os.getenv("SMTP_USER", ""), os.getenv("SMTP_PASS", ""))
                s.send_message(msg)
            return True
        except Exception as e:
            log.warning("SMTP error: %s", e)
    log.info("[reset-otp] no email provider configured; OTP for %s = %s", to, _RESET_OTPS.get(to, {}).get("otp"))
    return False


@router.post("/forgot-password")
async def forgot_password(payload: dict):
    email = (payload.get("email") or "").strip().lower()
    if not email:
        raise HTTPException(400, "Email required")
    user = await db.fetchval("SELECT 1 FROM users WHERE email=$1", email)
    otp = f"{secrets.randbelow(1000000):06d}"
    _RESET_OTPS[email] = {"otp": otp, "exp": _time.time() + _OTP_TTL}
    sent = False
    if user:
        html = (f"<div style='font-family:sans-serif'><h2>SherrByte password reset</h2>"
                f"<p>Your verification code is:</p>"
                f"<p style='font-size:30px;font-weight:800;letter-spacing:6px'>{otp}</p>"
                f"<p>It expires in 10 minutes. If you didn't request this, ignore this email.</p></div>")
        sent = await _send_email(email, "Your SherrByte reset code", html, f"Your SherrByte code is {otp} (valid 10 min).")
    resp = {"ok": True}          # never reveal whether the email exists
    if os.getenv("OTP_DEBUG") and user and not sent:
        resp["debug_otp"] = otp  # launch-prep only; remove OTP_DEBUG in prod
    return resp


@router.post("/reset-password")
async def reset_password(payload: dict):
    email = (payload.get("email") or "").strip().lower()
    otp = (payload.get("otp") or "").strip()
    new_pw = payload.get("password") or ""
    if len(new_pw) < 6:
        raise HTTPException(400, "Password must be at least 6 characters")
    rec = _RESET_OTPS.get(email)
    if not rec or rec["exp"] < _time.time() or rec["otp"] != otp:
        raise HTTPException(400, "Invalid or expired code")
    await db.execute("UPDATE users SET password_hash=$1 WHERE email=$2", hash_password(new_pw), email)
    _RESET_OTPS.pop(email, None)
    return {"ok": True}


@router.post("/account/delete")
async def delete_account(authorization: str = Header("")):
    """Permanently delete the signed-in user and their data."""
    uid = _require_uid(authorization)
    for tbl in ("signals", "feeds", "user_preferences"):
        try:
            await db.execute(f"DELETE FROM {tbl} WHERE user_id=$1", uid)
        except Exception:
            pass
    await db.execute("DELETE FROM users WHERE id=$1", uid)
    return {"ok": True}


# ─── Old single /interact endpoint → v6 signals + preference nudges ───────────
_INTERACT_KIND = {
    "like": "like", "dislike": "dislike", "save": "save",
    "read": "read", "quiz_complete": "quiz", "share": "share",
}
_INTERACT_DELTA = {"like": 0.3, "save": 0.5, "share": 0.4,
                   "read": 0.1, "quiz": 0.2, "dislike": -0.4}


@router.post("/interact")
async def interact(payload: dict, authorization: str = Header("")):
    uid = _uid(authorization)
    if not uid:
        return {"status": "ok"}  # anon — silently accept, as the MVP did
    info_id = str(payload.get("article_id") or payload.get("info_object_id") or "")
    kind = _INTERACT_KIND.get(str(payload.get("action", "")))
    if not info_id or not kind:
        return {"status": "ok"}
    try:
        await db.execute(
            "INSERT INTO signals (user_id, info_object_id, kind, value) VALUES ($1,$2,$3,1.0)",
            uid, info_id, kind,
        )
        delta = _INTERACT_DELTA.get(kind, 0.0)
        if delta:
            obj = await db.fetchrow(
                "SELECT pillar_id, micro_tags FROM info_objects WHERE id=$1", info_id
            )
            if obj:
                tags = obj["micro_tags"]
                if isinstance(tags, str):
                    import json
                    tags = json.loads(tags or "[]")
                for tag in (tags or [])[:3]:
                    await db.execute(
                        """
                        INSERT INTO user_preferences (user_id, topic, pillar_id, weight, updated_at)
                        VALUES ($1,$2,$3, GREATEST(0.1, 1.0 + $4), now())
                        ON CONFLICT (user_id, topic) DO UPDATE
                          SET weight = LEAST(5.0, GREATEST(0.1, user_preferences.weight + $4)),
                              updated_at = now()
                        """,
                        uid, str(tag), obj["pillar_id"], delta,
                    )
    except Exception as e:
        # A bad/unknown info id (e.g. not a real uuid) must never fail the client.
        log.warning("interact signal skipped for %r: %s", info_id, e)
    return {"status": "ok"}


# ─── /me/feed alias (old name for updating topic prefs) ───────────────────────
@router.put("/me/feed")
async def update_feed_prefs(req: UpdateTopicsReq, authorization: str = Header("")):
    return await activity.update_topics(req, _require_uid(authorization))


# ─── Notifications (preference-driven) ────────────────────────────────────────
@router.get("/notifications")
async def notifications(authorization: str = Header("")):
    uid = _uid(authorization)
    if not uid:
        return {"notifications": []}
    prefs = await db.fetch(
        "SELECT topic, pillar_id FROM user_preferences WHERE user_id=$1 ORDER BY weight DESC LIMIT 5",
        uid,
    )
    notifs = []
    for p in prefs:
        rows = await db.fetch(
            """
            SELECT id, headline, pillar_id, image_url FROM info_objects
            WHERE topic ILIKE $1 OR micro_tags::text ILIKE $1
            ORDER BY published_at DESC LIMIT 2
            """,
            f"%{p['topic']}%",
        )
        for r in rows:
            notifs.append({
                "article_id": str(r["id"]), "headline": r["headline"],
                "topic": p["topic"],
                "color": PILLARS.get(r["pillar_id"], PILLARS[1])["color"],
                "image_url": r["image_url"],
                "message": f"New in @{p['topic']}",
            })
    return {"notifications": notifs[:20]}
