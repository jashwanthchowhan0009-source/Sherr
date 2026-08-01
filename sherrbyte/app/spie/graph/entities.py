"""
graph/entities.py — corpus-aware entity admission (P1.1).

Replaces the hand-maintained blocklist. A list of 14 banned words was never going to
hold: "It's" got in, then "One", then "Two", and each fix was another literal added
after someone spotted it in production. The corpus already contains the signal needed
to decide this, so the rules here are measured rather than enumerated.

FOUR GATES, cheapest first:

  1. NER TYPE WHITELIST — ORG, PERSON, GPE, PRODUCT, EVENT, LAW, NORP. Everything
     else (DATE, CARDINAL, PERCENT, TIME…) is not a graph entity.
  2. SURFACE NORMALISATION — strip possessives and contractions, so "India's" and
     "India" are one entity rather than two, and "It's" reduces to a stopword.
  3. DOCUMENT-FREQUENCY CEILING — reject anything appearing in more than
     MAX_DOCUMENT_FREQUENCY of documents. This is what kills generic words WITHOUT a
     list: a term in a third of all articles carries no information about any of them.
     It also handles words we have never thought of, which is the point.
  4. SUPPORT FLOOR — at least MIN_DOCUMENTS distinct documents from
     MIN_PUBLISHER_DOMAINS distinct publishers. One outlet's house style is not
     evidence that something is an entity.

The DF table is computed over the corpus and cached; recompute nightly.
"""

from __future__ import annotations

import logging
import re

from app.spie.knowledge.entity_resolver import normalize_name

log = logging.getLogger("sherbyte.entities")

# spaCy labels that can denote a real-world thing worth putting in a graph.
ALLOWED_NER_TYPES = {"ORG", "PERSON", "GPE", "PRODUCT", "EVENT", "LAW", "NORP",
                     # Our own coarse buckets map onto the same idea.
                     "MISC"}

# A term in more than this share of documents describes the corpus, not the story.
MAX_DOCUMENT_FREQUENCY = 0.15
MIN_DOCUMENTS = 3
MIN_PUBLISHER_DOMAINS = 2

# Possessives and contraction tails. Stripped BEFORE the DF lookup so "India's" and
# "India" share one count instead of splitting it.
_POSSESSIVE_RE = re.compile(r"[’']s\b", re.IGNORECASE)
_CONTRACTION_RE = re.compile(r"[’'](s|re|ve|ll|d|t|m)\b", re.IGNORECASE)
_LEADING_ARTICLE_RE = re.compile(r"^(the|a|an)\s+", re.IGNORECASE)


def canonical_surface(name: str) -> str:
    """Strip possessives, contractions and a leading article, then normalise.

    "India's" → "india", "The Reserve Bank" → "reserve bank", "It's" → "it".
    Contractions collapse to their stem, which is a stopword, so they fall out at the
    DF gate instead of needing to be listed.
    """
    if not name:
        return ""
    s = _POSSESSIVE_RE.sub("", name.strip())
    s = _CONTRACTION_RE.sub("", s)
    s = _LEADING_ARTICLE_RE.sub("", s)
    return normalize_name(s)


def type_allowed(ner_type: str | None) -> bool:
    return (ner_type or "MISC").strip().upper() in ALLOWED_NER_TYPES


def is_admissible(name: str, ner_type: str | None, stats: dict | None,
                  total_docs: int) -> tuple[bool, str]:
    """Should this mention enter the graph? Returns (ok, reason).

    `stats` is this surface form's row from the DF table:
        {"documents": int, "domains": int}
    None means "never seen", which fails the support floor rather than passing by
    default — an unknown term is not admitted on the assumption it is fine.
    """
    if not type_allowed(ner_type):
        return False, f"ner type {ner_type} not in whitelist"

    surface = canonical_surface(name)
    if not surface or len(surface) <= 2:
        return False, "surface too short after normalisation"

    if not stats:
        return False, "no corpus support"

    docs = int(stats.get("documents") or 0)
    domains = int(stats.get("domains") or 0)

    if total_docs > 0:
        df = docs / total_docs
        if df > MAX_DOCUMENT_FREQUENCY:
            return False, (f"document frequency {df:.2%} exceeds "
                           f"{MAX_DOCUMENT_FREQUENCY:.0%}")
    if docs < MIN_DOCUMENTS:
        return False, f"only {docs} documents (need {MIN_DOCUMENTS})"
    if domains < MIN_PUBLISHER_DOMAINS:
        return False, f"only {domains} publisher domain(s) (need {MIN_PUBLISHER_DOMAINS})"
    return True, "ok"


# ─── the DF table ─────────────────────────────────────────────────────────────
async def compute_document_frequency(conn, *, days: int = 365) -> dict:
    """Documents and distinct publisher domains per entity, over the corpus.

    Distinct DOCUMENTS, not mentions: an entity named nine times in one article has a
    document frequency of one. Counting mentions would let a single repetitive article
    promote a term.
    """
    rows = await conn.fetch(
        """
        SELECT e.canonical_name AS name,
               COUNT(DISTINCT ds.ref_id) AS documents,
               COUNT(DISTINCT split_part(ds.source_id, ':', 1)) AS domains
        FROM domain_signals ds
        JOIN entities e ON e.id = ANY(ds.entity_ids)
        WHERE ds.domain = 'news'
          AND ds.ts >= now() - ($1 || ' days')::interval
        GROUP BY 1
        """, str(int(days)))
    total = await conn.fetchval(
        """
        SELECT COUNT(DISTINCT ref_id) FROM domain_signals
        WHERE domain = 'news' AND ts >= now() - ($1 || ' days')::interval
        """, str(int(days)))
    table = {canonical_surface(r["name"]): {"documents": int(r["documents"]),
                                            "domains": int(r["domains"])}
             for r in rows}
    return {"total_documents": int(total or 0), "entities": table}


async def refresh_and_report(conn, *, days: int = 365) -> dict:
    """Recompute the table and report what the ceiling would reject. Nightly job."""
    df = await compute_document_frequency(conn, days=days)
    total = df["total_documents"]
    over = [(n, s["documents"] / total) for n, s in df["entities"].items()
            if total and s["documents"] / total > MAX_DOCUMENT_FREQUENCY]
    over.sort(key=lambda x: x[1], reverse=True)
    thin = sum(1 for s in df["entities"].values()
               if s["documents"] < MIN_DOCUMENTS or s["domains"] < MIN_PUBLISHER_DOMAINS)
    log.info("entity DF: %d terms over the %.0f%% ceiling, %d below the support floor",
             len(over), MAX_DOCUMENT_FREQUENCY * 100, thin)
    return {"total_documents": total, "entities": len(df["entities"]),
            "over_ceiling": [{"entity": n, "df": round(d, 4)} for n, d in over[:25]],
            "below_support_floor": thin, "table": df["entities"]}


# ─── cached DF table ──────────────────────────────────────────────────────────
# Computed on first use and reused for the process lifetime. The nightly job calls
# refresh() to replace it; without that job the cache still populates itself on the
# first resolve_many of a run, so the filter works from day one rather than waiting
# for a scheduler nobody has wired up yet.
_CACHE: dict = {"entities": {}, "total_documents": 0, "loaded": False}
# Below this many documents, frequency is not a meaningful statistic.
_MIN_DOCS_TO_TRUST = 20


async def ensure_loaded(conn, *, days: int = 365) -> dict:
    """Populate the DF cache if empty. Idempotent, safe to call once per batch."""
    if _CACHE["loaded"]:
        return _CACHE
    try:
        df = await compute_document_frequency(conn, days=days)
    except Exception as e:
        log.warning("DF table unavailable (%s) — admitting on type/shape only", e)
        _CACHE.update(loaded=True, degraded=True)
        return _CACHE
    _CACHE.update(entities=df["entities"], total_documents=df["total_documents"],
                  loaded=True, degraded=False)
    log.info("entity DF cache: %d surfaces over %d documents",
             len(df["entities"]), df["total_documents"])
    return _CACHE


def refresh(table: dict, total_documents: int) -> None:
    """Replace the cache — used by the nightly recompute."""
    _CACHE.update(entities=table, total_documents=total_documents,
                  loaded=True, degraded=False)


def invalidate() -> None:
    _CACHE.update(entities={}, total_documents=0, loaded=False, degraded=False)


def admits(name: str, ner_type: str | None) -> tuple[bool, str]:
    """Admission decision against the cached table.

    On a corpus too small for frequency to mean anything — a fresh install, or the DF
    query failing — the ceiling and support floor are SKIPPED and only the type and
    shape rules apply. Applying a 15% ceiling to 6 documents would reject everything
    appearing twice, which is worse than not filtering at all.
    """
    total = _CACHE.get("total_documents", 0)
    if _CACHE.get("degraded") or total < _MIN_DOCS_TO_TRUST:
        if not type_allowed(ner_type):
            return False, f"ner type {ner_type} not in whitelist"
        surface = canonical_surface(name)
        if not surface or len(surface) <= 2:
            return False, "surface too short after normalisation"
        return True, "ok (corpus too small for frequency rules)"
    return is_admissible(name, ner_type,
                         _CACHE["entities"].get(canonical_surface(name)), total)
