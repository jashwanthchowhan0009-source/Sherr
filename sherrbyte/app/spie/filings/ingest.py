"""filings/ingest.py — fetch, parse, resolve, and UPSERT filings. The DB path.

The one place that writes. It fetches each source, parses it (parse.py), resolves
the company to our entity/instrument map AT INGEST, and upserts into
sherrbyte_app.filings. It adds ZERO provider calls per filing: event_class is the
rule table, and both resolvers are pure LOOKUPS that never create an entity and
never call a language model.

Idempotent by construction: ON CONFLICT (source, external_id) DO NOTHING, so the
cron can run every pass and only genuinely new filings are written.
"""

from __future__ import annotations

import json
import logging
import os

from app.spie.filings import sources as S
from app.spie.filings import parse as P
from app.spie.filings.doctor import _fetch

log = logging.getLogger("sherbyte.filings.ingest")

# Migration 028 creates sherrbyte_app.filings. run_migrations() applies it on a
# worker bootstrap, but the write and read paths that matter in practice do NOT
# bootstrap: /admin/filing-doctor (and a manual ingest) write through the raw
# engine pool, so until the first successful nightly cron the table is missing
# and every insert/select raises "relation sherrbyte_app.filings does not exist"
# — the reported blocker. Applying the migration here, idempotently, makes the
# ingest path self-sufficient regardless of who created the connection.
_MIGRATION_028 = os.path.normpath(os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "..", "..", "db", "migrations", "028_filings.sql"))


async def ensure_schema(conn) -> None:
    """Apply migration 028 (IF NOT EXISTS throughout, so a no-op once created).

    Belt for the paths that reach the filings table without a worker bootstrap.
    Best-effort: a failure here is logged and left to surface at the insert, so
    this never turns a permissions problem into a silent swallow."""
    try:
        with open(_MIGRATION_028, encoding="utf-8") as fh:
            await conn.execute(fh.read())
    except Exception as ex:                     # noqa: BLE001
        log.warning("ensure_schema (028_filings) failed: %s", ex)

_INSERT = """
INSERT INTO sherrbyte_app.filings
  (source, external_id, company_code, company_name, filing_type, event_class,
   subject, filing_date, url, entity_id, symbol, feed_class, raw)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8::date, $9, $10::uuid, $11, 'financial',
        $12::jsonb)
ON CONFLICT (source, external_id) DO NOTHING
RETURNING id
"""


async def _resolve(conn, f: P.Filing, index: dict) -> tuple:
    """(entity_id, symbol) for a filing, both best-effort and both create=False.

    entity_id: resolve the company name against our entity graph WITHOUT creating
    it — a filing about a company we have never seen contributes no fabricated
    node, exactly like instrument_map's honesty rule.

    symbol: the instrument ticker the filing's own text reaches, via the SAME
    seeded keyword edges the analog engine uses (event_library.linked_symbols).
    Most mid-cap filings reach no priced instrument and get NULL — that is the
    known ~13-symbol limit stated in CLAUDE.md, not a bug.
    """
    from app.spie.knowledge import entity_resolver
    from app.spie.analog import event_library

    entity_id = None
    name = (f.company_name or "").strip() or (f.company_code or "").strip()
    if name:
        try:
            entity_id = await entity_resolver.resolve(conn, name, "ORG", create=False)
        except Exception as ex:                    # resolution is best-effort
            log.debug("entity resolve failed for %r: %s", name, ex)

    symbol = None
    syms = event_library.linked_symbols(f.subject or "", f.company_name or "", index)
    if syms:
        symbol = syms[0]
    return entity_id, symbol


async def _upsert(conn, f: P.Filing, entity_id, symbol) -> bool:
    row = f.to_row()
    new_id = await conn.fetchval(
        _INSERT,
        row["source"], row["external_id"], row["company_code"],
        row["company_name"], row["filing_type"], row["event_class"],
        row["subject"], row["filing_date"], row["url"],
        entity_id, symbol, json.dumps(row["raw"]),
    )
    return new_id is not None


async def ingest_source(conn, client, src: S.Source, index: dict) -> dict:
    fetched = await _fetch(client, src)
    body = fetched["text"]
    pr = P.parse(src.kind, src.name, body) if body is not None else P.ParseResult()

    written = 0
    resolved_syms = 0
    resolved_entities = 0
    for f in pr.filings:
        entity_id, symbol = await _resolve(conn, f, index)
        if entity_id:
            resolved_entities += 1
        if symbol:
            resolved_syms += 1
        try:
            if await _upsert(conn, f, entity_id, symbol):
                written += 1
        except Exception as ex:                    # a bad row costs only itself
            log.warning("filing upsert failed (%s/%s): %s",
                        src.name, f.external_id, ex)
            pr.failures.append(P._fail(f.raw, f"upsert failed: {ex}"))

    return {
        "source": src.name,
        "http_status": fetched["status"],
        "error": fetched["error"],
        "parsed": pr.parsed,
        "failed": pr.failed,
        "written": written,
        "resolved_entities": resolved_entities,
        "resolved_symbols": resolved_syms,
    }


async def run(conn, *, only: str | None = None) -> dict:
    """Ingest every source (or just `only`). Returns a per-source summary."""
    import httpx
    from app.spie.analog import event_library

    # The table must exist before the first upsert — the caller may be the
    # endpoint's raw pool, which never ran migrations.
    await ensure_schema(conn)

    # Build the keyword -> ticker index ONCE, not per filing.
    index = event_library.symbol_index()

    srcs = [s for s in S.SOURCES if (only is None or s.name == only)]
    per_source = []
    async with httpx.AsyncClient(follow_redirects=True) as client:
        for src in srcs:
            try:
                per_source.append(await ingest_source(conn, client, src, index))
            except Exception as ex:
                log.error("ingest_source %s failed: %s", src.name, ex, exc_info=True)
                per_source.append({"source": src.name, "error": str(ex),
                                   "parsed": 0, "written": 0})

    return {
        "ok": True,
        "written": sum(r.get("written", 0) for r in per_source),
        "parsed": sum(r.get("parsed", 0) for r in per_source),
        "per_source": per_source,
    }
