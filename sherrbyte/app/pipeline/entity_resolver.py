"""
pipeline/entity_resolver.py — canonical entity resolution (Intelligence Engine V1, Step 1).

Collapses every entity mention to ONE canonical entity id before any co-occurrence
or correlation work. "Tata Motors Ltd", "Tata Motors" and "TaMo" must all resolve to
the same id — otherwise the counts driving every downstream detector are corrupt.

Two layers:
  • Pure, deterministic normalization (no DB, no I/O) — fully unit-testable:
        normalize_name()  → a canonical string key
        coarse_type()     → collapses noisy spaCy labels to ORG/PERSON/GPE/MISC
        resolve_key()     → applies seeded synonyms, returns (norm_key, type, display)
  • Async DB resolution against the entities / entity_aliases tables (migration 010):
        resolve(conn, name, type)      → entity_id (uuid str)
        resolve_many(conn, mentions)   → [entity_id, ...]
        seed_aliases(conn)             → load the curated synonym seeds
        backfill(conn)                 → resolve every info_objects.entities mention

The async functions take an asyncpg connection explicitly (same convention as
pipeline/connector.py) so this module imports nothing heavy at top level and its
pure core stays importable in isolation.
"""

from __future__ import annotations

import logging
import re
from typing import Iterable, Optional

log = logging.getLogger("sherbyte.entity")

# ─── Coarse type buckets ──────────────────────────────────────────────────────
# spaCy emits ~18 labels; many are noise for resolution. Collapse to four coarse
# types. Same normalized name under two coarse types stays distinct on purpose
# (e.g. "Jordan" the country vs "Jordan" the person).
_COARSE = {
    "ORG": "ORG",
    "PERSON": "PERSON", "PER": "PERSON",
    "GPE": "GPE", "LOC": "GPE", "FAC": "GPE",
}


def coarse_type(label: Optional[str]) -> str:
    return _COARSE.get((label or "").strip().upper(), "MISC")


# ─── Deterministic name normalization ─────────────────────────────────────────
# Corporate/legal suffixes that carry no identity — stripped so "Apple Inc." and
# "Apple" collapse. Kept as trailing-token removal (not substring) so we never
# mangle a name that legitimately contains one of these words mid-string.
# Only unambiguous legal/corporate suffixes — NOT descriptive words like
# "Industries", "Group" or "Holdings", which are genuine parts of a name
# ("Reliance Industries", "Adani Group"). Those short forms are handled by the
# curated seed synonyms instead, so we never over-strip a real name here.
_SUFFIX_TOKENS = {
    "ltd", "limited", "inc", "incorporated", "corp", "corporation", "co",
    "company", "plc", "llc", "lp", "llp", "pvt", "private", "gmbh",
    "sa", "ag", "nv",
}
_PUNCT_RE = re.compile(r"[^\w\s]", flags=re.UNICODE)
_WS_RE = re.compile(r"\s+")


def normalize_name(raw: str) -> str:
    """Deterministic, idempotent canonical key for an entity surface form.

    lowercase → '&'→'and' → strip punctuation → collapse whitespace →
    drop a leading 'the' → drop trailing corporate suffix tokens.
    """
    if not raw:
        return ""
    s = raw.strip().lower().replace("&", " and ")
    s = _PUNCT_RE.sub(" ", s)
    s = _WS_RE.sub(" ", s).strip()
    if not s:
        return ""
    tokens = s.split(" ")
    if len(tokens) > 1 and tokens[0] == "the":
        tokens = tokens[1:]
    # Peel trailing suffix tokens, but never reduce to nothing.
    while len(tokens) > 1 and tokens[-1] in _SUFFIX_TOKENS:
        tokens = tokens[:-1]
    return " ".join(tokens)


# ─── Curated synonym seeds ────────────────────────────────────────────────────
# High-value Indian-audience entities whose short forms (TaMo, RBI, INFY) would
# never collapse from normalization alone. Each canonical is inserted once with
# source='seed'; its aliases point at it. Extend freely — resolution reads these
# through SEED_LOOKUP, so adding a row needs no code change elsewhere.
_SEED: list[dict] = [
    {"canonical": "Reserve Bank of India", "type": "ORG", "aliases": ["RBI"]},
    {"canonical": "Securities and Exchange Board of India", "type": "ORG", "aliases": ["SEBI"]},
    {"canonical": "Tata Motors", "type": "ORG", "aliases": ["TaMo", "Tata Motors Ltd"]},
    {"canonical": "Reliance Industries", "type": "ORG", "aliases": ["RIL", "Reliance"]},
    {"canonical": "Infosys", "type": "ORG", "aliases": ["INFY"]},
    {"canonical": "Tata Consultancy Services", "type": "ORG", "aliases": ["TCS"]},
    {"canonical": "State Bank of India", "type": "ORG", "aliases": ["SBI"]},
    {"canonical": "HDFC Bank", "type": "ORG", "aliases": ["HDFC"]},
    {"canonical": "Bharatiya Janata Party", "type": "ORG", "aliases": ["BJP"]},
    {"canonical": "Indian National Congress", "type": "ORG", "aliases": ["Congress"]},
    {"canonical": "Narendra Modi", "type": "PERSON", "aliases": ["Modi", "PM Modi"]},
    {"canonical": "Indian Space Research Organisation", "type": "ORG", "aliases": ["ISRO"]},
    {"canonical": "Adani Group", "type": "ORG", "aliases": ["Adani"]},
    {"canonical": "Nifty 50", "type": "MISC", "aliases": ["Nifty", "Nifty50"]},
    {"canonical": "Sensex", "type": "MISC", "aliases": ["BSE Sensex"]},
]


def _build_seed_lookup() -> dict:
    """norm(alias) → (canonical_norm, canonical_display, coarse_type). The canonical
    form maps to itself too, so a full-name mention also lands on the seed identity."""
    lut: dict[str, tuple] = {}
    for e in _SEED:
        cdisplay = e["canonical"]
        ctype = coarse_type(e["type"]) if e["type"] in _COARSE else e["type"]
        cnorm = normalize_name(cdisplay)
        forms = [cdisplay] + e.get("aliases", [])
        for form in forms:
            nf = normalize_name(form)
            if nf:
                lut[nf] = (cnorm, cdisplay, ctype)
    return lut


SEED_LOOKUP = _build_seed_lookup()


def resolve_key(name: str, type: str = "MISC") -> tuple[str, str, str]:
    """Pure resolution: return (norm_key, coarse_type, canonical_display).

    Applies the curated seed synonyms first (so 'TaMo' → Tata Motors), else falls
    back to deterministic normalization with the mention's own casing preserved as
    the display form. norm_key == "" means the mention is empty/unusable.
    """
    base = normalize_name(name)
    if not base:
        return "", coarse_type(type), ""
    seed = SEED_LOOKUP.get(base)
    if seed:
        cnorm, cdisplay, ctype = seed
        return cnorm, ctype, cdisplay
    return base, coarse_type(type), name.strip()


# ─── Async DB resolution (entities / entity_aliases) ──────────────────────────
async def resolve(conn, name: str, type: str = "MISC", *, create: bool = True) -> Optional[str]:
    """Resolve one mention to a canonical entity id (uuid str), creating it if new.

    Lookup order: alias table → (norm_key, type) → insert. Every path records the
    surface form as an alias so the next identical mention resolves in one hop.
    Concurrency-safe via ON CONFLICT. Returns None for empty mentions (or when
    create=False and the entity does not exist yet).
    """
    norm_key, ctype, display = resolve_key(name, type)
    if not norm_key:
        return None

    # 1) Known surface form / seeded synonym → straight to its entity.
    row = await conn.fetchrow(
        """
        SELECT ea.entity_id FROM entity_aliases ea
        JOIN entities e ON e.id = ea.entity_id
        WHERE ea.norm_alias = $1 AND e.type = $2
        """,
        norm_key, ctype,
    )
    if row:
        eid = row["entity_id"]
        await conn.execute(
            "UPDATE entities SET mention_count = mention_count + 1, updated_at = now() WHERE id = $1",
            eid,
        )
        await _record_alias(conn, name, ctype_entity_id=eid)
        return str(eid)

    # 2) Existing canonical entity by (norm_key, type)?  3) else create it.
    if create:
        eid = await conn.fetchval(
            """
            INSERT INTO entities (canonical_name, type, norm_key, mention_count)
            VALUES ($1, $2, $3, 1)
            ON CONFLICT (norm_key, type)
            DO UPDATE SET mention_count = entities.mention_count + 1, updated_at = now()
            RETURNING id
            """,
            display or norm_key, ctype, norm_key,
        )
    else:
        eid = await conn.fetchval(
            "SELECT id FROM entities WHERE norm_key = $1 AND type = $2", norm_key, ctype,
        )
        if eid is None:
            return None

    await _record_alias(conn, name, ctype_entity_id=eid)
    return str(eid)


async def _record_alias(conn, surface: str, ctype_entity_id) -> None:
    """Persist a surface form → entity link (best-effort, dedup on conflict)."""
    norm_alias = normalize_name(surface)
    if not norm_alias:
        return
    await conn.execute(
        """
        INSERT INTO entity_aliases (alias, norm_alias, entity_id, source)
        VALUES ($1, $2, $3, 'auto')
        ON CONFLICT (norm_alias, entity_id) DO NOTHING
        """,
        surface.strip(), norm_alias, ctype_entity_id,
    )


async def resolve_many(conn, mentions: Iterable) -> list[str]:
    """Resolve a batch of mentions. Each item is a str, a (name, type) tuple, or an
    object/dict with .name/.type (e.g. the understander's Entity). Order preserved;
    empty mentions dropped."""
    out: list[str] = []
    for m in mentions:
        name, mtype = _unpack_mention(m)
        eid = await resolve(conn, name, mtype)
        if eid:
            out.append(eid)
    return out


def _unpack_mention(m) -> tuple[str, str]:
    if isinstance(m, str):
        return m, "MISC"
    if isinstance(m, (tuple, list)) and m:
        return (m[0], m[1] if len(m) > 1 else "MISC")
    if isinstance(m, dict):
        return m.get("name", "") or m.get("canonical", ""), m.get("type", "MISC")
    return getattr(m, "name", "") or getattr(m, "canonical", ""), getattr(m, "type", "MISC")


async def seed_aliases(conn) -> int:
    """Insert the curated seed entities + their aliases (source='seed'). Idempotent —
    re-running only fills gaps. Returns the number of seed entities ensured."""
    n = 0
    for e in _SEED:
        cdisplay = e["canonical"]
        ctype = coarse_type(e["type"]) if e["type"] in _COARSE else e["type"]
        cnorm = normalize_name(cdisplay)
        eid = await conn.fetchval(
            """
            INSERT INTO entities (canonical_name, type, norm_key, mention_count)
            VALUES ($1, $2, $3, 0)
            ON CONFLICT (norm_key, type) DO UPDATE SET updated_at = now()
            RETURNING id
            """,
            cdisplay, ctype, cnorm,
        )
        for form in [cdisplay] + e.get("aliases", []):
            nf = normalize_name(form)
            if not nf:
                continue
            await conn.execute(
                """
                INSERT INTO entity_aliases (alias, norm_alias, entity_id, source)
                VALUES ($1, $2, $3, 'seed')
                ON CONFLICT (norm_alias, entity_id) DO NOTHING
                """,
                form, nf, eid,
            )
        n += 1
    log.info("entity seed ensured: %d canonical entities", n)
    return n


async def backfill(conn, batch: int = 500) -> dict:
    """Resolve every entity mention already stored in info_objects.entities (JSONB
    [{name,type,canonical}]) through the resolver, populating entities/entity_aliases.
    Chunked to avoid long locks. Returns {objects, mentions, entities}."""
    await seed_aliases(conn)
    objects = mentions = 0
    last_id = "00000000-0000-0000-0000-000000000000"
    while True:
        rows = await conn.fetch(
            """
            SELECT id, entities FROM info_objects
            WHERE id > $1 AND entities IS NOT NULL
            ORDER BY id ASC LIMIT $2
            """,
            last_id, batch,
        )
        if not rows:
            break
        for r in rows:
            last_id = r["id"]
            objects += 1
            ents = r["entities"]
            if isinstance(ents, str):
                import json
                try:
                    ents = json.loads(ents)
                except Exception:
                    ents = []
            for ent in ents or []:
                if await resolve(conn, _unpack_mention(ent)[0], _unpack_mention(ent)[1]):
                    mentions += 1
    total = await conn.fetchval("SELECT COUNT(*) FROM entities")
    log.info("entity backfill: %d objects, %d mentions → %d entities", objects, mentions, total)
    return {"objects": objects, "mentions": mentions, "entities": total}
