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


def rerank(candidates: list[dict], top_k: int,
           lambda_: float | None = None) -> list[dict]:
    """Re-rank candidates by MMR (vectorized).

    Each candidate dict must carry 'score' (relevance) and 'embedding'
    (list[float] or None). Candidates without embeddings are ranked by raw score
    only and appended after the diverse set.

    The selection is done with numpy matrix ops instead of a per-pair Python
    loop: each round computes the new pick's cosine against the whole pool in one
    shot and folds it into a running max-similarity vector. That turns the old
    O(top_k · pool · selected) Python loop (tens of seconds on a small CPU for a
    few-hundred-item pool — which timed out /feed) into O(top_k · pool) fast
    numpy work.
    """
    lambda_ = settings.mmr_lambda if lambda_ is None else lambda_
    with_emb = [c for c in candidates if c.get("embedding")]
    without = [c for c in candidates if not c.get("embedding")]

    if not with_emb:
        return sorted(candidates, key=lambda c: c["score"], reverse=True)[:top_k]

    pool = sorted(with_emb, key=lambda c: c["score"], reverse=True)
    n = len(pool)

    # Row-normalized embedding matrix → cosine becomes a plain dot product.
    emb = np.asarray([c["embedding"] for c in pool], dtype=np.float32)
    norms = np.linalg.norm(emb, axis=1, keepdims=True)
    norms[norms == 0] = 1.0
    emb /= norms

    # Relevance normalized to [0,1] for a fair trade-off against cosine.
    rel = np.array([c["score"] for c in pool], dtype=np.float32)
    lo, hi = float(rel.min()), float(rel.max())
    rel = (rel - lo) / ((hi - lo) or 1.0)

    k = min(top_k, n)
    available = np.ones(n, dtype=bool)
    max_sim = np.zeros(n, dtype=np.float32)   # running max cosine to any selected
    chosen: list[int] = []

    for _ in range(k):
        mmr = lambda_ * rel - (1.0 - lambda_) * max_sim
        mmr[~available] = -np.inf
        i = int(np.argmax(mmr))
        chosen.append(i)
        available[i] = False
        # Fold the new pick's similarity to every candidate into the running max.
        max_sim = np.maximum(max_sim, emb @ emb[i])

    selected = [pool[i] for i in chosen]

    # Top up with any embedding-less candidates if room remains.
    if len(selected) < top_k:
        selected.extend(sorted(without, key=lambda c: c["score"], reverse=True))
    return selected[:top_k]
