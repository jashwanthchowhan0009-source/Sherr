import json
import logging
from fastapi import APIRouter, Header, HTTPException, Query
from app.config import settings
# Import your database link function and the story tracking function
from app.sherr.core import get_db, link_stories, classify_scope 

log = logging.getLogger("sherbyte.admin")
router = APIRouter()

def _check_admin(token: str):
    # Dynamically verifies against your environment configurations
    if not token or token != settings.admin_token:
        raise HTTPException(403, "Invalid or missing admin token")

@router.post("/rescope")
async def admin_rescope(x_admin_token: str = Header("")):
    """Re-bucket every article into local / national / global with the current classifier."""
    _check_admin(x_admin_token)
    conn = get_db()
    rows = conn.execute("SELECT id, headline, summary_60, full_body FROM articles").fetchall()
    counts = {"local": 0, "national": 0, "global": 0}
    
    for r in rows:
        body = r["full_body"] or r["summary_60"] or ""
        sc = classify_scope(r["headline"] or "", body)
        counts[sc] = counts.get(sc, 0) + 1
        conn.execute("UPDATE articles SET scope=? WHERE id=?", (sc, r["id"]))
        
    conn.commit()
    conn.close()
    log.info("[RESCOPE] %d rows reprocessed -> %s", len(rows), counts)
    return {"rescoped": len(rows), "distribution": counts}

@router.post("/relink")
async def admin_relink(x_admin_token: str = Header("")):
    """Recompute all story threads on demand."""
    _check_admin(x_admin_token)
    conn = get_db()
    threads = link_stories(conn)
    conn.close()
    return {"threads": threads, "window_days": settings.story_window_days}