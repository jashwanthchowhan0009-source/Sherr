"""
models/article.py — Raw article schema (Collect stage).

An Article is the unprocessed unit straight from a feed/scraper. It carries the
three dedup signatures (url_hash, title_hash, simhash) computed at collection
time so the deduplicator can act before anything expensive (NER, LLM) runs.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class ArticleIn(BaseModel):
    """A freshly collected article, pre-persistence."""
    url: str
    url_hash: str
    title: str
    title_hash: str
    simhash: Optional[int] = None
    body: str = ""
    image_url: str = ""
    video_url: str = ""
    source_name: str = ""
    scope: str = "global"
    published_at: datetime = Field(default_factory=datetime.utcnow)


class Article(ArticleIn):
    """A persisted article row."""
    id: UUID
    collected_at: datetime
    processed: bool = False

    model_config = {"from_attributes": True}
