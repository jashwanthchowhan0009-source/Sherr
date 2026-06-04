from datetime import datetime, timedelta, timezone

from app.recommender.mmr import rerank
from app.recommender.temporal import freshness


def test_freshness_decays_with_age():
    now = datetime.now(timezone.utc)
    fresh = freshness(now, now=now)
    old = freshness(now - timedelta(hours=24), now=now)
    assert 0 < old < fresh <= 1.0


def test_mmr_prefers_diverse_items():
    # Two near-identical high-score items + one different lower-score item.
    candidates = [
        {"id": "a", "score": 1.0, "embedding": [1.0, 0.0, 0.0]},
        {"id": "b", "score": 0.95, "embedding": [0.99, 0.01, 0.0]},  # near-dup of a
        {"id": "c", "score": 0.8, "embedding": [0.0, 1.0, 0.0]},     # diverse
    ]
    out = rerank(candidates, top_k=2, lambda_=0.5)
    ids = [c["id"] for c in out]
    assert ids[0] == "a"
    # Diversity should pull the orthogonal item ahead of the near-duplicate.
    assert ids[1] == "c"


def test_mmr_without_embeddings_falls_back_to_score():
    candidates = [
        {"id": "a", "score": 0.2, "embedding": None},
        {"id": "b", "score": 0.9, "embedding": None},
    ]
    out = rerank(candidates, top_k=2)
    assert out[0]["id"] == "b"
