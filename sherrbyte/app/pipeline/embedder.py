"""
pipeline/embedder.py — Stage 03b: EMBED.

Vectorizes info objects with MiniLM (all-MiniLM-L6-v2 → 384-d) and writes the
result into the pgvector column. These embeddings power three things:
  • Connect  — story threading via cosine similarity.
  • RAG      — consensus retrieval for the Sherr writer.
  • Personalize — content-based scoring.

The model is loaded lazily and cached. If sentence-transformers isn't installed
the embedder degrades to a deterministic hashing embedding so the pipeline keeps
running (similarity quality drops, but nothing crashes).
"""

from __future__ import annotations

import hashlib
import logging

from app.config import settings
from app.db.supabase import db

log = logging.getLogger("sherbyte.embedder")

_MODEL = None
_MODEL_TRIED = False


def _get_model():
    global _MODEL, _MODEL_TRIED
    if _MODEL_TRIED:
        return _MODEL
    _MODEL_TRIED = True
    try:
        from sentence_transformers import SentenceTransformer
        _MODEL = SentenceTransformer(settings.embed_model)
        log.info("Embedding model loaded: %s", settings.embed_model)
    except Exception as e:
        log.warning("sentence-transformers unavailable, using hash fallback: %s", e)
        _MODEL = None
    return _MODEL


def _hash_embedding(text: str) -> list[float]:
    """Deterministic, bounded fallback vector (not semantic, but stable)."""
    dim = settings.embed_dim
    vec = [0.0] * dim
    for tok in text.lower().split():
        h = int(hashlib.md5(tok.encode()).hexdigest(), 16)
        vec[h % dim] += 1.0
    norm = sum(v * v for v in vec) ** 0.5 or 1.0
    return [v / norm for v in vec]


def embed_text(text: str) -> list[float]:
    return embed_batch([text])[0]


def embed_batch(texts: list[str]) -> list[list[float]]:
    model = _get_model()
    if model is None:
        return [_hash_embedding(t) for t in texts]
    vecs = model.encode(
        texts, batch_size=settings.embed_batch_size,
        normalize_embeddings=True, show_progress_bar=False,
    )
    return [v.tolist() for v in vecs]


def _embed_input(headline: str, summary: str, topic: str) -> str:
    return f"{headline}. {summary}. {topic}".strip()


async def embed_info_object(info_object_id: str, headline: str,
                            summary: str, topic: str) -> list[float]:
    vec = embed_text(_embed_input(headline, summary, topic))
    await db.execute(
        "UPDATE info_objects SET embedding=$1 WHERE id=$2", vec, info_object_id
    )
    return vec


async def embed_pending(limit: int = 128) -> int:
    """Embed any info objects missing a vector (used by the embed worker)."""
    rows = await db.fetch(
        """
        SELECT id, headline, summary, topic FROM info_objects
        WHERE embedding IS NULL ORDER BY created_at DESC LIMIT $1
        """,
        limit,
    )
    if not rows:
        return 0
    texts = [_embed_input(r["headline"], r["summary"], r["topic"]) for r in rows]
    vecs = embed_batch(texts)
    async with db.acquire() as conn:
        for r, v in zip(rows, vecs):
            await conn.execute("UPDATE info_objects SET embedding=$1 WHERE id=$2", v, r["id"])
    log.info("[EMBED] vectorized %d info objects", len(rows))
    return len(rows)
