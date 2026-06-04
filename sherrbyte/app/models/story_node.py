"""
models/story_node.py — Story graph schemas (Connect stage).

A StoryThread is an evolving narrative; StoryNodes are its info objects placed
on a timeline, each optionally linked to the prior node it follows (backstory).
"""

from __future__ import annotations

from datetime import datetime
from typing import Optional
from uuid import UUID

from pydantic import BaseModel, Field


class StoryThread(BaseModel):
    id: UUID
    title: str
    topic: str = ""
    pillar_id: int = 1
    node_count: int = 0
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class StoryNode(BaseModel):
    id: UUID
    thread_id: UUID
    info_object_id: UUID
    parent_node_id: Optional[UUID] = None
    position: int = 0
    similarity: float = 0.0
    created_at: Optional[datetime] = None

    model_config = {"from_attributes": True}


class ThreadTimeline(BaseModel):
    """A thread with its ordered member info objects — what /sherr/thread returns."""
    thread: StoryThread
    nodes: list[dict] = Field(default_factory=list)  # info_object feed dicts in order
