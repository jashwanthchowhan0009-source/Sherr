"""
detectors/base.py — shared insight persistence + explainability helpers.

write_insight() upserts on `signature` so nightly re-runs refresh rather than
duplicate. The *_for helpers assemble the mandatory explain_json fields (article
count, source count, top sources) and surface the source-credibility score.
"""

from __future__ import annotations

import json
import logging

log = logging.getLogger("sherbyte.detectors")


async def write_insight(conn, *, type: str, entity_ids: list, domains: list,
                        score: float, explain: dict, signature: str) -> str:
    """Insert or refresh one insight (idempotent on signature)."""
    new_id = await conn.fetchval(
        """
        INSERT INTO insights (type, entity_ids, domains, score, explain_json, signature)
        VALUES ($1, $2::uuid[], $3::text[], $4, $5::jsonb, $6)
        ON CONFLICT (signature) DO UPDATE
            SET score = EXCLUDED.score,
                explain_json = EXCLUDED.explain_json,
                domains = EXCLUDED.domains,
                updated_at = now()
        RETURNING id
        """,
        type, entity_ids, domains, float(score), json.dumps(explain), signature,
    )
    return str(new_id)


async def names_for(conn, ids: list) -> list[str]:
    """Canonical display names for entity ids, in the given order."""
    rows = await conn.fetch(
        "SELECT id, canonical_name FROM entities WHERE id = ANY($1::uuid[])", ids
    )
    m = {str(r["id"]): r["canonical_name"] for r in rows}
    return [m.get(str(i), str(i)) for i in ids]


async def domains_for(conn, ids: list, days: int) -> list[str]:
    """Distinct domains in which these entities co-occurred within the window."""
    rows = await conn.fetch(
        """
        SELECT DISTINCT domain FROM domain_signals
        WHERE entity_ids @> $1::uuid[] AND ts >= now() - ($2 || ' days')::interval
        """,
        ids, str(days),
    )
    return [r["domain"] for r in rows]


async def source_stats(conn, ids: list, days: int) -> dict:
    """Explainability core: how many articles/sources support this, who they are,
    and the credibility-weighted confidence in the evidence."""
    rows = await conn.fetch(
        """
        SELECT source_id, credibility, COUNT(*) AS c
        FROM domain_signals
        WHERE entity_ids @> $1::uuid[] AND ts >= now() - ($2 || ' days')::interval
        GROUP BY source_id, credibility
        ORDER BY c DESC
        """,
        ids, str(days),
    )
    article_count = sum(r["c"] for r in rows)
    sources = [r["source_id"] for r in rows if r["source_id"]]
    avg_cred = (
        sum((r["credibility"] or 0.0) * r["c"] for r in rows) / article_count
        if article_count else 0.5
    )
    return {
        "article_count": int(article_count),
        "source_count": len(set(sources)),
        "top_sources": sources[:5],
        "credibility": round(float(avg_cred), 3),
    }
