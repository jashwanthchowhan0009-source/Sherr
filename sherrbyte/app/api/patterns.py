"""
api/patterns.py — SPRIE pattern output (Intelligence Engine V1, Step 6).

Serves the insights table (detector output) as cheap read-only SELECTs. Entity
ids are resolved to canonical names so the app can render them directly.

    GET /patterns                 — list, paginated, ?type= filter
    GET /patterns/type/{type}     — by detector type
    GET /patterns/entity/{id}     — insights involving one entity
"""

from __future__ import annotations

import json
import logging

from fastapi import APIRouter, Query

from app.db.supabase import db

log = logging.getLogger("sherbyte.patterns")
router = APIRouter(prefix="/patterns", tags=["patterns"])


async def _shape(rows) -> list[dict]:
    """Attach resolved entity names + parse explain_json for a batch of rows."""
    out = [dict(r) for r in rows]

    # Resolve all entity_ids across the batch in one query.
    all_ids = {str(i) for d in out for i in (d.get("entity_ids") or [])}
    name_map: dict[str, str] = {}
    if all_ids:
        ent_rows = await db.fetch(
            "SELECT id, canonical_name FROM entities WHERE id = ANY($1::uuid[])",
            list(all_ids),
        )
        name_map = {str(r["id"]): r["canonical_name"] for r in ent_rows}

    for d in out:
        ids = [str(i) for i in (d.get("entity_ids") or [])]
        d["entity_ids"] = ids
        d["entities"] = [name_map.get(i, i) for i in ids]
        d["id"] = str(d.get("id"))
        ej = d.get("explain_json")
        if isinstance(ej, str):
            try:
                d["explain_json"] = json.loads(ej)
            except Exception:
                d["explain_json"] = {}
        if d.get("created_at") is not None:
            d["created_at"] = d["created_at"].isoformat()
    return out


@router.get("")
async def list_patterns(
    type: str = Query(""),
    limit: int = Query(30, le=100),
    offset: int = Query(0, ge=0),
):
    if type:
        rows = await db.fetch(
            "SELECT * FROM insights WHERE type=$1 ORDER BY score DESC, created_at DESC "
            "LIMIT $2 OFFSET $3",
            type, limit, offset,
        )
    else:
        rows = await db.fetch(
            "SELECT * FROM insights ORDER BY score DESC, created_at DESC LIMIT $1 OFFSET $2",
            limit, offset,
        )
    total = await db.fetchval("SELECT COUNT(*) FROM insights")
    return {"patterns": await _shape(rows), "total": int(total or 0)}


@router.get("/type/{ptype}")
async def patterns_by_type(ptype: str, limit: int = Query(30, le=100)):
    rows = await db.fetch(
        "SELECT * FROM insights WHERE type=$1 ORDER BY score DESC, created_at DESC LIMIT $2",
        ptype, limit,
    )
    return {"patterns": await _shape(rows)}


@router.get("/entity/{entity_id}")
async def patterns_by_entity(entity_id: str, limit: int = Query(30, le=100)):
    rows = await db.fetch(
        "SELECT * FROM insights WHERE $1::uuid = ANY(entity_ids) "
        "ORDER BY score DESC, created_at DESC LIMIT $2",
        entity_id, limit,
    )
    return {"patterns": await _shape(rows)}
