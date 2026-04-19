"""
activity.py — User activity tracking & analytics.

Tracks:
  • Screen time (per-day sessions, aggregated from 30s heartbeats)
  • Article reading progress (scroll %, duration, completion)
  • Reading streaks (consecutive active days)
  • Category breakdown (which pillars user actually reads)

All endpoints are protected. Anon users silently no-op on heartbeat.
"""

import os
import json
import sqlite3
import base64
import hashlib
import hmac as hmac_module
import logging
from datetime import datetime, date, timedelta, timezone
from typing import Optional

from fastapi import APIRouter, HTTPException, Header
from pydantic import BaseModel

log = logging.getLogger("sherbyte.activity")
router = APIRouter()

DB_PATH    = os.getenv("DB_PATH", "sherbyte.db")
JWT_SECRET = os.getenv("JWT_SECRET", "sherbyte-secret-change-in-prod")

PILLAR_META = {
    1: ("society",   "Society",   "#1E88E5"),
    2: ("economy",   "Economy",   "#FBC02D"),
    3: ("tech",      "Tech",      "#3949AB"),
    4: ("arts",      "Arts",      "#E53935"),
    5: ("nature",    "Nature",    "#43A047"),
    6: ("selfwell",  "Wellbeing", "#FB8C00"),
    7: ("philo",     "Philosophy","#8E24AA"),
    8: ("lifestyle", "Lifestyle", "#00ACC1"),
    9: ("sports",    "Sports",    "#546E7A"),
}


# ─── DB helpers ─────────────────────────────────────────────────────────
def _db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def _verify_token(token: str) -> Optional[int]:
    try:
        raw, sig = token.rsplit(".", 1)
        expected = hmac_module.new(JWT_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac_module.compare_digest(sig, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(raw + "=="))
        if datetime.fromisoformat(payload["exp"]) < datetime.now(timezone.utc):
            return None
        return payload["id"]
    except Exception:
        return None


def _auth(authorization: str, required: bool = True) -> Optional[int]:
    if authorization and authorization.startswith("Bearer "):
        uid = _verify_token(authorization[7:])
        if uid:
            return uid
    if required:
        raise HTTPException(401, "Unauthorized")
    return None


def _fmt_duration(sec: int) -> str:
    sec = max(0, int(sec))
    if sec < 60:
        return f"{sec}s"
    if sec < 3600:
        return f"{sec // 60}m"
    h, m = sec // 3600, (sec % 3600) // 60
    return f"{h}h {m}m" if m else f"{h}h"


# ─── Schema init — called from main.py startup ──────────────────────────
def init_activity_schema():
    conn = _db()
    conn.executescript("""
    CREATE TABLE IF NOT EXISTS sessions (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        session_date TEXT NOT NULL,
        duration_sec INTEGER DEFAULT 0,
        last_seen TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, session_date)
    );

    CREATE TABLE IF NOT EXISTS reading_progress (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER NOT NULL,
        article_id INTEGER NOT NULL,
        scroll_pct INTEGER DEFAULT 0,
        duration_sec INTEGER DEFAULT 0,
        completed INTEGER DEFAULT 0,
        started_at TEXT DEFAULT (datetime('now')),
        updated_at TEXT DEFAULT (datetime('now')),
        UNIQUE(user_id, article_id)
    );

    CREATE INDEX IF NOT EXISTS idx_sessions_user ON sessions(user_id, session_date DESC);
    CREATE INDEX IF NOT EXISTS idx_progress_user ON reading_progress(user_id, updated_at DESC);
    CREATE INDEX IF NOT EXISTS idx_progress_incomplete ON reading_progress(user_id, completed, updated_at DESC);
    """)
    conn.commit()
    conn.close()
    log.info("Activity schema ready")


# ─── Heartbeat: frontend pings every 30s while tab is visible ───────────
class HeartbeatReq(BaseModel):
    duration_sec: int = 30
    article_id: Optional[int] = None
    scroll_pct: Optional[int] = None


@router.post("/activity/heartbeat")
async def heartbeat(req: HeartbeatReq, authorization: str = Header("")):
    uid = _auth(authorization, required=False)
    if not uid:
        return {"status": "anon"}

    # Clamp to [0, 60] — prevents inflated metrics from bad clients
    secs = max(0, min(60, int(req.duration_sec or 0)))
    today = date.today().isoformat()

    conn = _db()
    conn.execute("""
        INSERT INTO sessions (user_id, session_date, duration_sec, last_seen)
        VALUES (?, ?, ?, datetime('now'))
        ON CONFLICT(user_id, session_date) DO UPDATE SET
            duration_sec = duration_sec + excluded.duration_sec,
            last_seen = datetime('now')
    """, (uid, today, secs))

    if req.article_id:
        sp = max(0, min(100, int(req.scroll_pct or 0)))
        conn.execute("""
            INSERT INTO reading_progress (user_id, article_id, scroll_pct, duration_sec, updated_at)
            VALUES (?, ?, ?, ?, datetime('now'))
            ON CONFLICT(user_id, article_id) DO UPDATE SET
                scroll_pct = MAX(scroll_pct, excluded.scroll_pct),
                duration_sec = duration_sec + excluded.duration_sec,
                completed = CASE WHEN MAX(scroll_pct, excluded.scroll_pct) >= 80 THEN 1 ELSE completed END,
                updated_at = datetime('now')
        """, (uid, req.article_id, sp, secs))

    conn.commit()
    conn.close()
    return {"status": "ok"}


# ─── Analytics endpoint — powers dynamic profile page ───────────────────
@router.get("/me/analytics")
async def my_analytics(authorization: str = Header("")):
    uid = _auth(authorization)
    conn = _db()
    today    = date.today()
    week_ago = (today - timedelta(days=6)).isoformat()

    # Today's screen time
    row = conn.execute(
        "SELECT duration_sec FROM sessions WHERE user_id=? AND session_date=?",
        (uid, today.isoformat())
    ).fetchone()
    time_today = row["duration_sec"] if row else 0

    # Last 7 days — for total + sparkline
    week_rows = conn.execute("""
        SELECT session_date, duration_sec FROM sessions
        WHERE user_id=? AND session_date >= ? ORDER BY session_date ASC
    """, (uid, week_ago)).fetchall()
    time_week = sum(r["duration_sec"] for r in week_rows)
    week_map = {r["session_date"]: r["duration_sec"] for r in week_rows}

    daily_sec = []
    for i in range(7):
        d = (today - timedelta(days=6 - i)).isoformat()
        daily_sec.append({"date": d, "seconds": week_map.get(d, 0)})

    # Current streak — consecutive days with ≥60s activity, ending today or yesterday
    all_dates = conn.execute("""
        SELECT session_date FROM sessions
        WHERE user_id=? AND duration_sec >= 60
        ORDER BY session_date DESC LIMIT 180
    """, (uid,)).fetchall()
    active_set = {r["session_date"] for r in all_dates}

    current_streak = 0
    check = today if today.isoformat() in active_set else today - timedelta(days=1)
    while check.isoformat() in active_set:
        current_streak += 1
        check -= timedelta(days=1)

    # Longest streak ever
    longest = run = 0
    prev = None
    for d_str in sorted(active_set):
        d_obj = date.fromisoformat(d_str)
        run = run + 1 if prev and (d_obj - prev).days == 1 else 1
        longest = max(longest, run)
        prev = d_obj

    # Articles read
    art_today = conn.execute("""
        SELECT COUNT(*) as c FROM reading_progress
        WHERE user_id=? AND date(updated_at)=? AND duration_sec >= 10
    """, (uid, today.isoformat())).fetchone()["c"]

    art_week = conn.execute("""
        SELECT COUNT(*) as c FROM reading_progress
        WHERE user_id=? AND date(updated_at) >= ? AND duration_sec >= 10
    """, (uid, week_ago)).fetchone()["c"]

    art_total = conn.execute(
        "SELECT COUNT(*) as c FROM reading_progress WHERE user_id=?", (uid,)
    ).fetchone()["c"]

    # Category breakdown over the last 7 days
    cat_rows = conn.execute("""
        SELECT a.pillar_id, COUNT(*) as c
        FROM reading_progress rp JOIN articles a ON rp.article_id = a.id
        WHERE rp.user_id=? AND date(rp.updated_at) >= ?
        GROUP BY a.pillar_id
    """, (uid, week_ago)).fetchall()

    total_reads = max(1, sum(r["c"] for r in cat_rows))
    categories = []
    for pid in range(1, 10):
        slug, name, color = PILLAR_META[pid]
        row = next((r for r in cat_rows if r["pillar_id"] == pid), None)
        count = row["c"] if row else 0
        categories.append({
            "pillar_id": pid, "slug": slug, "name": name, "color": color,
            "count": count,
            "pct": round(count * 100 / total_reads) if count else 0,
        })

    top_category = max(categories, key=lambda c: c["count"]) if categories else None
    active_days = sum(1 for d in daily_sec if d["seconds"] > 60)
    avg_session_min = round((time_week / max(1, active_days)) / 60, 1)

    conn.close()

    return {
        "time_today_sec":       time_today,
        "time_today_formatted": _fmt_duration(time_today),
        "time_week_sec":        time_week,
        "time_week_formatted":  _fmt_duration(time_week),
        "current_streak":       current_streak,
        "longest_streak":       longest,
        "articles_today":       art_today,
        "articles_week":        art_week,
        "articles_total":       art_total,
        "daily_sec":            daily_sec,
        "categories":           categories,
        "top_category":         top_category,
        "avg_session_minutes":  avg_session_min,
        "active_days_week":     active_days,
    }


# ─── Continue reading — last unfinished article ─────────────────────────
@router.get("/me/continue")
async def continue_reading(authorization: str = Header("")):
    uid = _auth(authorization, required=False)
    if not uid:
        return {"article": None}

    conn = _db()
    row = conn.execute("""
        SELECT a.*, rp.scroll_pct, rp.duration_sec as read_sec,
               rp.updated_at as last_read
        FROM reading_progress rp
        JOIN articles a ON rp.article_id = a.id
        WHERE rp.user_id=?
          AND rp.completed = 0
          AND rp.scroll_pct BETWEEN 10 AND 79
          AND datetime(rp.updated_at) >= datetime('now', '-3 days')
        ORDER BY rp.updated_at DESC LIMIT 1
    """, (uid,)).fetchone()
    conn.close()

    if not row:
        return {"article": None}

    d = dict(row)
    pid = d.get("pillar_id", 1)
    slug, name, color = PILLAR_META.get(pid, PILLAR_META[3])
    d["category"] = slug
    d["pillar_name"] = name
    d["pillar_color"] = color
    d["refined_title"]  = d.get("headline", "")
    d["cached_summary"] = d.get("summary_60", "")
    return {"article": d}


# ─── Recent reading activity — powers profile activity feed ─────────────
@router.get("/me/activity")
async def recent_activity(limit: int = 20, authorization: str = Header("")):
    uid = _auth(authorization)
    conn = _db()
    rows = conn.execute("""
        SELECT a.id, a.headline, a.image_url, a.pillar_id, a.source_name,
               rp.scroll_pct, rp.duration_sec, rp.completed, rp.updated_at
        FROM reading_progress rp
        JOIN articles a ON rp.article_id = a.id
        WHERE rp.user_id=?
        ORDER BY rp.updated_at DESC LIMIT ?
    """, (uid, min(50, max(1, limit)))).fetchall()
    conn.close()

    out = []
    for r in rows:
        d = dict(r)
        pid = d.get("pillar_id", 1)
        slug, name, color = PILLAR_META.get(pid, PILLAR_META[3])
        d["category"] = slug
        d["pillar_name"] = name
        d["pillar_color"] = color
        out.append(d)
    return {"activity": out}
