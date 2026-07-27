"""
pipeline/signals.py — persist Signal objects into domain_signals.

Bridges pure adapters and the DB: resolves each Signal's raw entity mentions to
canonical entity_ids (entity_resolver) and inserts the row. Takes an asyncpg conn
(connector.py convention). This is the ONLY place adapters meet the database.
"""

from __future__ import annotations

import logging
from typing import Iterable

from app.models.signal import Signal
from app.spie.knowledge.entity_resolver import resolve
from app.spie.graph import cooccurrence

log = logging.getLogger("sherbyte.signals")


async def _resolve_entity_ids(conn, sig: Signal) -> list[str]:
    ids: list[str] = []
    for e in sig.entities:
        eid = await resolve(conn, e.name, e.type)
        if eid and eid not in ids:
            ids.append(eid)
    return ids


async def persist_signal(conn, sig: Signal) -> int:
    """Resolve entities and insert one signal. Returns the new domain_signals id."""
    entity_ids = sig.entity_ids or await _resolve_entity_ids(conn, sig)
    new_id = await conn.fetchval(
        """
        INSERT INTO domain_signals
            (entity_ids, domain, ts, location, magnitude, direction, sentiment,
             embedding, source_id, credibility, confidence, novelty, ref_id)
        VALUES ($1::uuid[], $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        RETURNING id
        """,
        entity_ids, sig.domain, sig.ts, sig.location, sig.magnitude, sig.direction,
        sig.sentiment, sig.embedding, sig.source_id, sig.credibility,
        sig.confidence, sig.novelty, sig.ref_id,
    )
    # Materialize co-occurrence incrementally at ingest (best-effort — the signal
    # is already persisted, so a co-occurrence hiccup must not lose it).
    if len(entity_ids) >= 2:
        try:
            await cooccurrence.update_for_signal(conn, entity_ids, sig.ts)
        except Exception as e:
            log.warning("cooccurrence update failed for signal %s: %s", new_id, e)
    return int(new_id)


async def persist_signals(conn, sigs: Iterable[Signal]) -> int:
    """Persist a batch of signals. Returns how many rows were written."""
    n = 0
    for sig in sigs:
        try:
            await persist_signal(conn, sig)
            n += 1
        except Exception as e:
            log.warning("signal persist failed (%s): %s", sig.domain, e)
    return n
