"""
api/sherr.py — /sherr/brief, /sherr/explain, /sherr/thread, /sherr/generate.

Thin HTTP layer over the Sherr orchestrator (app.sherr.core). Returns the
watermarked (SGI) generation envelope. /sherr/thread narrates a whole story
thread as a timeline.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, HTTPException, Query

from app.config import WRITE_MODES
from app.db.supabase import db
from app.sherr import core
from app.sherr.router import complete_text

log = logging.getLogger("sherbyte.api.sherr")
router = APIRouter(prefix="/sherr", tags=["sherr"])


@router.get("/modes")
async def modes():
    return {"modes": {k: v["desc"] for k, v in WRITE_MODES.items()}}


@router.post("/tldr")
async def tldr(payload: dict):
    """Fast 3-line TL;DR of an article via the Gemini→Groq router. Stateless:
    the client passes the title + already-summarized body, so it works for any
    article (no DB lookup) and degrades gracefully if no LLM is configured."""
    title = (payload.get("title") or "").strip()
    text = (payload.get("text") or "").strip()
    if not text and not title:
        raise HTTPException(400, "title or text required")
    system = ("You are a news summarizer. Output EXACTLY three short bullet lines, "
              "each on its own line starting with '- ', max ~14 words each. "
              "No headings, no preamble, no markdown bold — just the three lines.")
    user = f"Headline: {title}\n\nArticle:\n{text[:4000]}\n\nWrite the 3-line TL;DR:"
    out = await complete_text(system, user, temperature=0.3, max_tokens=180)
    if not out:
        return {"tldr": None, "error": "AI unavailable"}
    lines = [l.strip().lstrip("-•*").strip() for l in out.splitlines() if l.strip()]
    return {"tldr": lines[:3]}


@router.get("/brief/{info_id}")
async def brief(info_id: str):
    try:
        return await core.generate(info_id, mode="BRIEF")
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/explain/{info_id}")
async def explain(info_id: str):
    try:
        return await core.generate(info_id, mode="EXPLAINER")
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/generate/{info_id}")
async def generate(info_id: str, mode: str = Query("BRIEF")):
    try:
        return await core.generate(info_id, mode=mode)
    except ValueError as e:
        raise HTTPException(404, str(e))


@router.get("/thread/{info_id}")
async def thread(info_id: str):
    """Narrate the full story thread that this info object belongs to."""
    obj = await db.fetchrow("SELECT thread_id FROM info_objects WHERE id=$1", info_id)
    if not obj:
        raise HTTPException(404, "info object not found")

    narrative = await core.generate(info_id, mode="THREAD")

    timeline = []
    if obj["thread_id"]:
        rows = await db.fetch(
            """
            SELECT io.id, io.headline, io.summary, io.source_name, io.published_at, sn.position
            FROM story_nodes sn JOIN info_objects io ON io.id = sn.info_object_id
            WHERE sn.thread_id=$1 ORDER BY sn.position ASC
            """,
            obj["thread_id"],
        )
        timeline = [
            {"id": str(r["id"]), "headline": r["headline"], "summary": r["summary"],
             "source_name": r["source_name"], "position": r["position"],
             "published_at": r["published_at"].isoformat() if r["published_at"] else None}
            for r in rows
        ]

    return {
        "thread_id": str(obj["thread_id"]) if obj["thread_id"] else None,
        "narrative": narrative,
        "timeline": timeline,
    }
