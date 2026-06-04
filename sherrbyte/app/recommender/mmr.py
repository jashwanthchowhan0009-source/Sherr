"""
recommender/mmr.py — Maximal Marginal Relevance re-ranker.

After the hybrid scorer ranks candidates by relevance, MMR re-orders them to
balance relevance against diversity, so the feed doesn't show ten near-identical
takes on the same story.

    MMR = λ · relevance(i) − (1−λ) · max_j∈selected cosine(i, j)

λ = MMR_LAMBDA (1.0 = pure relevance, 0.0 = pure diversity).
"""

from __future__ import annotations

import numpy as np

from app.config import settings


def _cosine(a: np.ndarray, b: np.ndarray) -> float:
    na, nb = np.linalg.norm(a), np.linalg.norm(b)
    if na == 0 or nb == 0:
        return 0.0
    return float(np.dot(a, b) / (na * nb))


def rerank(candidates: list[dict], top_k: int,
           lambda_: float | None = None) -> list[dict]:
    """Re-rank candidates by MMR.

    Each candidate dict must carry 'score' (relevance) and 'embedding'
    (list[float] or None). Candidates without embeddings are ranked by raw score
    only and appended after the diverse set.
    """
    lambda_ = settings.mmr_lambda if lambda_ is None else lambda_
    with_emb = [c for c in candidates if c.get("embedding")]
    without = [c for c in candidates if not c.get("embedding")]

    if not with_emb:
        return sorted(candidates, key=lambda c: c["score"], reverse=True)[:top_k]

    pool = sorted(with_emb, key=lambda c: c["score"], reverse=True)
    embeddings = {id(c): np.asarray(c["embedding"], dtype=np.float32) for c in pool}

    selected: list[dict] = []
    remaining = pool[:]
    # Normalize relevance to [0,1] for a fair trade-off with cosine.
    scores = np.array([c["score"] for c in pool], dtype=np.float32)
    lo, hi = float(scores.min()), float(scores.max())
    span = (hi - lo) or 1.0

    while remaining and len(selected) < top_k:
        best, best_val = None, -1e9
        for c in remaining:
            rel = (c["score"] - lo) / span
            if selected:
                div = max(_cosine(embeddings[id(c)], embeddings[id(s)]) for s in selected)
            else:
                div = 0.0
            mmr = lambda_ * rel - (1 - lambda_) * div
            if mmr > best_val:
                best_val, best = mmr, c
        selected.append(best)
        remaining.remove(best)

    # Top up with any embedding-less candidates if room remains.
    if len(selected) < top_k:
        selected.extend(sorted(without, key=lambda c: c["score"], reverse=True))
    return selected[:top_k]
