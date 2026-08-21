"""
image_service.py — article imagery with a safety switch.

    IMAGE_MODE = stock | thumbnail | art        (default: thumbnail)

  stock      Pexels stock photography, queried on the article's top entity plus its
             pillar. Licensed for this use, so hotlinking the Pexels CDN is fine.
             Results are cached BY QUERY, not by article: "Reliance Industries /
             economy" is asked once and reused across every article about it, which
             is what keeps a 200-req/hour key alive.
  thumbnail  The publisher's own og:image, deliberately constrained — capped width,
             never a full-bleed hero, publisher credit rendered on the image, and the
             whole card links to the source. This is the fair-dealing shape: a
             credited, linked thumbnail that drives traffic to the publisher, not a
             replacement for visiting them.
  art        The generated gradient. Also the fallback for the other two.

FALLBACK CHAIN: stock → art. Never blank. `thumbnail` does NOT fall back to stock —
if the publisher has no og:image we go to art rather than silently changing what kind
of image the reader is looking at.

A NOTE ON `thumbnail`. Constrained, credited and linked is the strongest fair-dealing
position, but it is a position, not an exemption: it is still the publisher's
copyrighted image on our surface without a licence. `stock` carries no such exposure,
which is why the switch exists. The default is `thumbnail` because that is what
this deployment has chosen to serve; set IMAGE_MODE=stock to take the safer one.
"""

from __future__ import annotations

import logging
import os
import re
import time
from typing import Optional

log = logging.getLogger("sherbyte.images")

# Must match main.py's default. These disagreed — "stock" here, "thumbnail"
# there — so which imagery policy applied depended on which module happened
# to read it. Unified on thumbnail, which is what the app actually serves.
IMAGE_MODE = (os.getenv("IMAGE_MODE") or "thumbnail").strip().lower()
PEXELS_API_KEY = (os.getenv("PEXELS_API_KEY") or "").strip()
VALID_MODES = ("stock", "thumbnail", "art")

_PEXELS_URL = "https://api.pexels.com/v1/search"
# Cache keyed on the QUERY. Articles about the same subject share one API call.
_QUERY_CACHE: dict[str, dict] = {}
_CACHE_TTL_SEC = 7 * 24 * 3600
# Free tier is 200/hour. Stay well under it and degrade to art rather than 429.
_MAX_CALLS_PER_HOUR = 150
_call_times: list[float] = []

# Thumbnail constraints (P0 compromise, enforced client-side too).
MAX_THUMBNAIL_WIDTH = 400

_PILLAR_TERMS = {
    "society": "government building", "economy": "financial district",
    "tech": "technology abstract", "arts": "art gallery",
    "nature": "landscape nature", "selfwell": "wellness calm",
    "sports": "stadium sport", "philo": "library books",
    "lifestyle": "city lifestyle",
}


def mode() -> str:
    """Current mode, validated. An unknown value degrades to art rather than
    guessing — a typo in an env var must not silently re-enable hotlinking."""
    m = IMAGE_MODE
    if m not in VALID_MODES:
        log.warning("IMAGE_MODE=%r is not one of %s — using 'art'", m, VALID_MODES)
        return "art"
    return m


def build_query(top_entity: str | None, pillar_slug: str | None) -> str:
    """Stock query from the article's subject and its pillar.

    Entity first because it is specific; the pillar term supplies visual context when
    the entity alone would return portraits of an unrelated namesake.
    """
    entity = re.sub(r"[^\w\s-]", "", (top_entity or "")).strip()
    pillar = _PILLAR_TERMS.get((pillar_slug or "").lower(), "news")
    return f"{entity} {pillar}".strip() if entity else pillar


def _rate_limited() -> bool:
    now = time.time()
    _call_times[:] = [t for t in _call_times if now - t < 3600]
    return len(_call_times) >= _MAX_CALLS_PER_HOUR


def cached(query: str) -> Optional[dict]:
    hit = _QUERY_CACHE.get(query)
    if hit and time.time() - hit["at"] < _CACHE_TTL_SEC:
        return hit
    return None


async def fetch_stock(query: str, client) -> Optional[dict]:
    """One Pexels lookup. Returns {url, credit, query} or None.

    None is a normal outcome — no key, rate limit reached, nothing matched, upstream
    down. Every one of those falls through to art, so imagery never blocks publishing.
    """
    if not PEXELS_API_KEY:
        return None
    hit = cached(query)
    if hit:
        return hit
    if _rate_limited():
        log.info("pexels: local rate cap reached, falling back to art")
        return None
    try:
        _call_times.append(time.time())
        r = await client.get(_PEXELS_URL, params={"query": query, "per_page": 1,
                                                  "orientation": "landscape"},
                             headers={"Authorization": PEXELS_API_KEY}, timeout=10)
        if r.status_code != 200:
            log.warning("pexels %s for %r", r.status_code, query)
            return None
        photos = (r.json() or {}).get("photos") or []
        if not photos:
            return None
        p = photos[0]
        entry = {
            "url": (p.get("src") or {}).get("large") or p.get("url"),
            # Pexels requires attribution to the photographer.
            "credit": f"Photo: {p.get('photographer', 'Pexels')} / Pexels",
            "credit_url": p.get("url", ""),
            "query": query,
            "at": time.time(),
        }
        if not entry["url"]:
            return None
        _QUERY_CACHE[query] = entry
        return entry
    except Exception as e:
        log.warning("pexels failed for %r: %s", query, e)
        return None


async def resolve_image(*, top_entity: str | None, pillar_slug: str | None,
                        source_og_image: str | None, source_name: str | None,
                        client=None) -> dict:
    """Decide this article's image. Always returns a dict; never blank.

        {"image_url", "image_source", "image_credit", "image_query"}

    image_source is stored so the render path knows which constraints to apply and so
    a mode change is auditable after the fact.
    """
    m = mode()
    art = {"image_url": "", "image_source": "art", "image_credit": "",
           "image_query": ""}

    if m == "art":
        return art

    if m == "thumbnail":
        if source_og_image:
            return {"image_url": source_og_image, "image_source": "thumbnail",
                    "image_credit": f"Image: {source_name or 'source'}",
                    "image_query": ""}
        # Deliberately NOT falling through to stock: if the publisher has no image,
        # show art rather than silently changing what the reader is looking at.
        return art

    if client is None:
        return art
    entry = await fetch_stock(build_query(top_entity, pillar_slug), client)
    if not entry:
        return art
    return {"image_url": entry["url"], "image_source": "stock",
            "image_credit": entry["credit"], "image_query": entry["query"]}


def cache_stats() -> dict:
    now = time.time()
    return {"mode": mode(), "cached_queries": len(_QUERY_CACHE),
            "calls_last_hour": len([t for t in _call_times if now - t < 3600]),
            "hourly_cap": _MAX_CALLS_PER_HOUR,
            "pexels_key_present": bool(PEXELS_API_KEY)}
