"""
api/signal.py — /signal/dwell, /signal/like, /signal/hide, /signal/mute (Learn intake).

Every interaction becomes a row in `signals`, which feeds the ALS retrain and
nudges per-topic preference weights. Hide/mute additionally write hard filters
into user_mutes so the scorer can exclude unwanted content immediately.
"""

from __future__ import annotations

import logging
import re

from fastapi import APIRouter, Depends, HTTPException, Query
from pydantic import BaseModel

from app.db.supabase import db
from app.security import require_user

log = logging.getLogger("sherbyte.signal")
router = APIRouter(prefix="/signal", tags=["signal"])

_UUID_RE = re.compile(r"^[0-9a-fA-F-]{32,36}$")


def _valid_ids(raw: str) -> list[str]:
    return [x.strip() for x in (raw or "").split(",") if _UUID_RE.match(x.strip())][:100]

# Positive signals nudge topic weights up; negatives nudge down.
WEIGHT_DELTA = {
    "like": 0.3, "save": 0.5, "share": 0.4, "read": 0.1, "quiz": 0.2,
    "dwell": 0.05, "dislike": -0.4, "hide": -0.6,
}


class DwellReq(BaseModel):
    info_object_id: str
    dwell_ms: int = 0
    scroll_pct: int = 0


class SignalReq(BaseModel):
    info_object_id: str
    kind: str  # like | dislike | save | share | quiz


class MuteReq(BaseModel):
    mute_type: str  # topic | source | entity
    value: str


async def _nudge_preferences(user_id: str, info_id: str, delta: float) -> None:
    if not delta:
        return
    obj = await db.fetchrow(
        "SELECT pillar_id, micro_tags FROM info_objects WHERE id=$1", info_id
    )
    if not obj:
        return
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
            user_id, str(tag), obj["pillar_id"], delta,
        )


@router.post("/dwell")
async def dwell(req: DwellReq, user_id: str = Depends(require_user)):
    """Reading-depth signal. dwell value is stored in seconds."""
    seconds = max(0.0, min(600.0, req.dwell_ms / 1000.0))
    await db.execute(
        "INSERT INTO signals (user_id, info_object_id, kind, value) VALUES ($1,$2,'dwell',$3)",
        user_id, req.info_object_id, seconds,
    )
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
        user_id, req.info_object_id, max(0, min(100, req.scroll_pct)), int(seconds),
    )
    if seconds >= 5:
        await _nudge_preferences(user_id, req.info_object_id, WEIGHT_DELTA["dwell"])
    return {"status": "ok"}


@router.post("")
@router.post("/")
async def signal(req: SignalReq, user_id: str = Depends(require_user)):
    if req.kind not in WEIGHT_DELTA:
        raise HTTPException(400, f"Unknown signal kind: {req.kind}")
    await db.execute(
        "INSERT INTO signals (user_id, info_object_id, kind, value) VALUES ($1,$2,$3,1.0)",
        user_id, req.info_object_id, req.kind,
    )
    await _nudge_preferences(user_id, req.info_object_id, WEIGHT_DELTA[req.kind])
    return {"status": "ok"}


@router.post("/hide")
async def hide(req: SignalReq, user_id: str = Depends(require_user)):
    await db.execute(
        "INSERT INTO signals (user_id, info_object_id, kind, value) VALUES ($1,$2,'hide',1.0)",
        user_id, req.info_object_id,
    )
    await _nudge_preferences(user_id, req.info_object_id, WEIGHT_DELTA["hide"])
    # Drop it from the cached feed immediately.
    await db.execute(
        "DELETE FROM feeds WHERE user_id=$1 AND info_object_id=$2", user_id, req.info_object_id
    )
    return {"status": "hidden"}


@router.post("/mute")
async def mute(req: MuteReq, user_id: str = Depends(require_user)):
    if req.mute_type not in ("topic", "source", "entity"):
        raise HTTPException(400, "mute_type must be topic | source | entity")
    await db.execute(
        """
        INSERT INTO user_mutes (user_id, mute_type, value) VALUES ($1,$2,$3)
        ON CONFLICT DO NOTHING
        """,
        user_id, req.mute_type, req.value,
    )
    await db.execute(
        "INSERT INTO signals (user_id, kind, value, target) VALUES ($1,'mute',1.0,$2)",
        user_id, f"{req.mute_type}:{req.value}",
    )
    return {"status": "muted", "mute_type": req.mute_type, "value": req.value}


# ─── Engagement: aggregate like/comment counts + comments ──────────────────────
class CommentReq(BaseModel):
    info_object_id: str
    body: str


@router.get("/counts")
async def counts(ids: str = Query("")):
    """Batch aggregate engagement for the feed pills: like count (distinct users
    from `signals`) + comment count, keyed by info_object id."""
    id_list = _valid_ids(ids)
    out = {i: {"likes": 0, "comments": 0} for i in id_list}
    if not id_list:
        return {"counts": out}
    try:
        for r in await db.fetch(
            "SELECT info_object_id::text AS id, COUNT(DISTINCT user_id) AS n "
            "FROM signals WHERE kind='like' AND info_object_id = ANY($1::uuid[]) "
            "GROUP BY info_object_id", id_list,
        ):
            out[r["id"]]["likes"] = r["n"]
        for r in await db.fetch(
            "SELECT info_object_id::text AS id, COUNT(*) AS n "
            "FROM comments WHERE info_object_id = ANY($1::uuid[]) "
            "GROUP BY info_object_id", id_list,
        ):
            out[r["id"]]["comments"] = r["n"]
    except Exception as e:
        log.warning("engagement counts failed: %s", e)
    return {"counts": out}


@router.get("/comments/{info_id}")
async def list_comments(info_id: str):
    if not _UUID_RE.match(info_id):
        return {"comments": []}
    rows = await db.fetch(
        """
        SELECT c.id, c.body, c.created_at, u.name, u.email
        FROM comments c JOIN users u ON u.id = c.user_id
        WHERE c.info_object_id = $1
        ORDER BY c.created_at DESC LIMIT 100
        """,
        info_id,
    )
    return {"comments": [{
        "id": str(r["id"]),
        "body": r["body"],
        "author": r["name"] or (r["email"].split("@")[0] if r["email"] else "User"),
        "created_at": r["created_at"].isoformat() if r["created_at"] else None,
    } for r in rows]}


@router.post("/comment")
async def add_comment(req: CommentReq, user_id: str = Depends(require_user)):
    body = (req.body or "").strip()[:1000]
    if not body or not _UUID_RE.match(req.info_object_id):
        raise HTTPException(400, "Invalid comment")
    row = await db.fetchrow(
        "INSERT INTO comments (info_object_id, user_id, body) VALUES ($1,$2,$3) "
        "RETURNING id, created_at",
        req.info_object_id, user_id, body,
    )
    return {"status": "ok", "id": str(row["id"]),
            "created_at": row["created_at"].isoformat() if row["created_at"] else None}
