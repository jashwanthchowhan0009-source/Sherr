"""
api/activity.py — Profile + reading analytics (ported from the MVP).

Covers:
  • /me, /me (PUT), /me/topics (PUT)     — profile & onboarding prefs.
  • /activity/heartbeat                  — 30s visibility ping → screen time.
  • /me/analytics                        — screen time, streaks, category mix.
  • /me/continue                         — last unfinished article.
  • /me/activity                         — recent reading feed.

Rewritten against Postgres + the new info_objects/signals schema.
"""

from __future__ import annotations

import logging
from datetime import date, timedelta
from typing import Optional

from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel

from app.api.feed import row_to_feed_dict
from app.config import PILLARS, PILLAR_ALIASES, SLUG_TO_PILLAR
from app.db.supabase import db
from app.models.user import UpdateProfileReq, UpdateTopicsReq
from app.security import optional_user, require_user

log = logging.getLogger("sherbyte.activity")
router = APIRouter(tags=["profile", "activity"])


def _fmt_duration(sec: int) -> str:
    sec = max(0, int(sec))
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m"
    h, m = sec // 3600, (sec % 3600) // 60
    return f"{h}h {m}m" if m else f"{h}h"


def _pillar_for_topic(topic: str) -> int:
    t = topic.lower().strip()
    return SLUG_TO_PILLAR.get(t) or PILLAR_ALIASES.get(t, 1)


# ─── Profile ──────────────────────────────────────────────────────────────────
@router.get("/me")
async def get_me(user_id: str = Depends(require_user)):
    user = await db.fetchrow("SELECT * FROM users WHERE id=$1", user_id)
    if not user:
        raise HTTPException(404, "User not found")
    prefs = await db.fetch(
        "SELECT topic, pillar_id, weight FROM user_preferences WHERE user_id=$1 ORDER BY weight DESC",
        user_id,
    )
    stats = await db.fetchrow(
        """
        SELECT
            COUNT(*) FILTER (WHERE kind='read') AS reads,
            COUNT(*) FILTER (WHERE kind='like') AS likes,
            COUNT(*) FILTER (WHERE kind='save') AS saves
        FROM signals WHERE user_id=$1
        """,
        user_id,
    )
    return {
        "id": str(user["id"]), "email": user["email"], "name": user["name"],
        "display_name": user["name"], "bio": user["bio"],
        "avatar_url": user["avatar_url"], "language": user["language"],
        "created_at": user["created_at"].isoformat() if user["created_at"] else None,
        "preferences": [
            {"topic": p["topic"], "pillar_id": p["pillar_id"],
             "color": PILLARS.get(p["pillar_id"], PILLARS[1])["color"],
             "weight": round(p["weight"], 2)}
            for p in prefs
        ],
        "stats": {
            "articles_read": stats["reads"] or 0,
            "likes": stats["likes"] or 0,
            "bookmarks": stats["saves"] or 0,
        },
    }


@router.put("/me")
async def update_profile(req: UpdateProfileReq, user_id: str = Depends(require_user)):
    updates, args = [], []
    for field in ("name", "bio", "avatar_url", "language"):
        val = getattr(req, field)
        if val is not None:
            args.append(val)
            updates.append(f"{field}=${len(args)}")
    if updates:
        args.append(user_id)
        await db.execute(f"UPDATE users SET {', '.join(updates)} WHERE id=${len(args)}", *args)
    return {"status": "updated"}


@router.put("/me/topics")
async def update_topics(req: UpdateTopicsReq, user_id: str = Depends(require_user)):
    async with db.acquire() as conn:
        await conn.execute("DELETE FROM user_preferences WHERE user_id=$1", user_id)
        for topic in req.topics:
            await conn.execute(
                """
                INSERT INTO user_preferences (user_id, topic, pillar_id, weight)
                VALUES ($1,$2,$3,1.0) ON CONFLICT (user_id, topic) DO NOTHING
                """,
                user_id, topic, _pillar_for_topic(topic),
            )
    return {"status": "ok", "topics_saved": len(req.topics)}


# ─── Heartbeat ────────────────────────────────────────────────────────────────
class HeartbeatReq(BaseModel):
    duration_sec: int = 30
    info_object_id: Optional[str] = None
    scroll_pct: Optional[int] = None


@router.post("/activity/heartbeat")
async def heartbeat(req: HeartbeatReq, user_id: Optional[str] = Depends(optional_user)):
    if not user_id:
        return {"status": "anon"}
    secs = max(0, min(60, int(req.duration_sec or 0)))
    today = date.today()
    await db.execute(
        """
        INSERT INTO sessions (user_id, session_date, duration_sec, last_seen)
        VALUES ($1,$2,$3, now())
        ON CONFLICT (user_id, session_date) DO UPDATE SET
            duration_sec = sessions.duration_sec + excluded.duration_sec,
            last_seen = now()
        """,
        user_id, today, secs,
    )
    if req.info_object_id:
        sp = max(0, min(100, int(req.scroll_pct or 0)))
        await db.execute(
            """
            INSERT INTO reading_progress (user_id, info_object_id, scroll_pct, duration_sec, updated_at)
            VALUES ($1,$2,$3,$4, now())
            ON CONFLICT (user_id, info_object_id) DO UPDATE SET
                scroll_pct = GREATEST(reading_progress.scroll_pct, excluded.scroll_pct),
                duration_sec = reading_progress.duration_sec + excluded.duration_sec,
                completed = (GREATEST(reading_progress.scroll_pct, excluded.scroll_pct) >= 80),
                updated_at = now()
            """,
            user_id, req.info_object_id, sp, secs,
        )
    return {"status": "ok"}


# ─── Analytics ────────────────────────────────────────────────────────────────
@router.get("/me/analytics")
async def my_analytics(user_id: str = Depends(require_user)):
    today = date.today()
    week_ago = today - timedelta(days=6)

    row = await db.fetchrow(
        "SELECT duration_sec FROM sessions WHERE user_id=$1 AND session_date=$2",
        user_id, today,
    )
    time_today = row["duration_sec"] if row else 0

    week_rows = await db.fetch(
        "SELECT session_date, duration_sec FROM sessions WHERE user_id=$1 AND session_date >= $2 ORDER BY session_date",
        user_id, week_ago,
    )
    time_week = sum(r["duration_sec"] for r in week_rows)
    week_map = {r["session_date"]: r["duration_sec"] for r in week_rows}
    daily_sec = [
        {"date": (today - timedelta(days=6 - i)).isoformat(),
         "seconds": week_map.get(today - timedelta(days=6 - i), 0)}
        for i in range(7)
    ]

    active = await db.fetch(
        "SELECT session_date FROM sessions WHERE user_id=$1 AND duration_sec >= 60 ORDER BY session_date DESC LIMIT 180",
        user_id,
    )
    active_set = {r["session_date"] for r in active}
    current_streak = 0
    check = today if today in active_set else today - timedelta(days=1)
    while check in active_set:
        current_streak += 1
        check -= timedelta(days=1)
    longest = run = 0
    prev = None
    for d in sorted(active_set):
        run = run + 1 if prev and (d - prev).days == 1 else 1
        longest = max(longest, run)
        prev = d

    cat_rows = await db.fetch(
        """
        SELECT io.pillar_id, COUNT(*) AS c
        FROM reading_progress rp JOIN info_objects io ON io.id = rp.info_object_id
        WHERE rp.user_id=$1 AND rp.updated_at >= $2
        GROUP BY io.pillar_id
        """,
        user_id, week_ago,
    )
    total_reads = max(1, sum(r["c"] for r in cat_rows))
    categories = []
    for pid in range(1, 10):
        p = PILLARS[pid]
        found = next((r for r in cat_rows if r["pillar_id"] == pid), None)
        count = found["c"] if found else 0
        categories.append({"pillar_id": pid, "slug": p["slug"], "name": p["name"],
                           "color": p["color"], "count": count,
                           "pct": round(count * 100 / total_reads) if count else 0})
    top_category = max(categories, key=lambda c: c["count"]) if categories else None
    active_days = sum(1 for d in daily_sec if d["seconds"] > 60)

    return {
        "time_today_sec": time_today, "time_today_formatted": _fmt_duration(time_today),
        "time_week_sec": time_week, "time_week_formatted": _fmt_duration(time_week),
        "current_streak": current_streak, "longest_streak": longest,
        "daily_sec": daily_sec, "categories": categories, "top_category": top_category,
        "avg_session_minutes": round((time_week / max(1, active_days)) / 60, 1),
        "active_days_week": active_days,
    }


@router.get("/me/continue")
async def continue_reading(user_id: Optional[str] = Depends(optional_user)):
    if not user_id:
        return {"article": None}
    row = await db.fetchrow(
        """
        SELECT io.id, io.headline, io.summary, io.topic, io.pillar_id, io.micro_tags,
               io.scope, io.importance, io.sentiment, io.is_trending, io.source_name,
               io.image_url, io.thread_id, io.published_at,
               rp.scroll_pct, rp.updated_at AS last_read
        FROM reading_progress rp JOIN info_objects io ON io.id = rp.info_object_id
        WHERE rp.user_id=$1 AND rp.completed=FALSE
          AND rp.scroll_pct BETWEEN 10 AND 79
          AND rp.updated_at >= now() - interval '3 days'
        ORDER BY rp.updated_at DESC LIMIT 1
        """,
        user_id,
    )
    if not row:
        return {"article": None}
    data = row_to_feed_dict(row)
    data["scroll_pct"] = row["scroll_pct"]
    return {"article": data}


@router.get("/me/activity")
async def recent_activity(limit: int = 20, user_id: str = Depends(require_user)):
    rows = await db.fetch(
        """
        SELECT io.id, io.headline, io.summary, io.topic, io.pillar_id, io.micro_tags,
               io.scope, io.importance, io.sentiment, io.is_trending, io.source_name,
               io.image_url, io.thread_id, io.published_at,
               rp.scroll_pct, rp.duration_sec, rp.completed, rp.updated_at
        FROM reading_progress rp JOIN info_objects io ON io.id = rp.info_object_id
        WHERE rp.user_id=$1 ORDER BY rp.updated_at DESC LIMIT $2
        """,
        user_id, min(50, max(1, limit)),
    )
    out = []
    for r in rows:
        d = row_to_feed_dict(r)
        d.update({"scroll_pct": r["scroll_pct"], "duration_sec": r["duration_sec"],
                  "completed": r["completed"]})
        out.append(d)
    return {"activity": out}
