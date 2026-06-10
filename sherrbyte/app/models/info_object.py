"""
models/info_object.py — The constructed information unit (Construct stage).

This is the heart of the pipeline. An InfoObject is the normalized, structured
representation of one real-world event:

    InfoObject { headline · wwww · entities · topic · importance · embedding[384] }

Everything downstream (Connect, Personalize, Narrate, Deliver) operates on
InfoObjects, not raw articles.
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field

from app.config import PILLARS


class Entity(BaseModel):
    name: str
    type: str = "MISC"          # PERSON | ORG | GPE | EVENT | MISC ...
    canonical: str = ""         # normalized form ("PM Modi" → "Narendra Modi")


class WWWW(BaseModel):
    """Who / What / Where / When / Why — extracted by the Understand stage."""
    who: str = ""
    what: str = ""
    where: str = ""
    when: str = ""
    why: str = ""


class InfoObjectIn(BaseModel):
    """A constructed info object, pre-persistence (no id / embedding yet)."""
    article_id: Optional[UUID] = None
    headline: str
    summary: str = ""
    body: str = ""
    wwww: WWWW = Field(default_factory=WWWW)
    entities: list[Entity] = Field(default_factory=list)
    topic: str = ""
    pillar_id: int = 1
    micro_tags: list[str] = Field(default_factory=list)
    scope: str = "global"
    importance: float = 0.0
    sentiment: str = "neutral"
    is_trending: bool = False
    source_name: str = ""
    image_url: str = ""
    video_url: str = ""
    published_at: datetime = Field(default_factory=datetime.utcnow)


class InfoObject(InfoObjectIn):
    """A persisted info object row, with embedding and thread linkage."""
    id: UUID
    thread_id: Optional[UUID] = None
    embedding: Optional[list[float]] = None
    engagement: int = 0
    created_at: datetime

    model_config = {"from_attributes": True}

    def pillar_meta(self) -> dict:
        return PILLARS.get(self.pillar_id, PILLARS[1])

    def to_feed_dict(self) -> dict:
        """Frontend-facing shape with pillar metadata expanded."""
        p = self.pillar_meta()
        return {
            "id": str(self.id),
            "headline": self.headline,
            "summary": self.summary,
            "topic": self.topic,
            "pillar_id": self.pillar_id,
            "pillar_name": p["name"],
            "pillar_color": p["color"],
            "pillar_emoji": p["emoji"],
            "category": p["slug"],
            "micro_tags": self.micro_tags,
            "scope": self.scope,
            "importance": round(self.importance, 3),
            "sentiment": self.sentiment,
            "is_trending": self.is_trending,
            "image_url": self.image_url,
            "source_name": self.source_name,
            "thread_id": str(self.thread_id) if self.thread_id else None,
            "published_at": self.published_at.isoformat() if self.published_at else None,
        }
