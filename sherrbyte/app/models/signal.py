"""
models/signal.py — the universal Signal schema (Intelligence Engine V1, Step 2).

Every adapter converts raw domain data into Signal objects; every detector reads
only Signals. `entities` carries raw (name, type) mentions as the adapter sees
them; `entity_ids` is filled at persist time by the entity resolver. This keeps
adapters pure (no DB) while the stored row (domain_signals) holds canonical ids.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field


class SignalEntity(BaseModel):
    name: str
    type: str = "MISC"          # PERSON | ORG | GPE | MISC


class Signal(BaseModel):
    entities: list[SignalEntity] = Field(default_factory=list)
    domain: str
    ts: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    location: Optional[str] = None
    magnitude: float = 0.0
    direction: int = 0                       # +1 / -1 / 0
    sentiment: Optional[float] = None        # -1..1 where applicable
    embedding: Optional[list[float]] = None
    source_id: str = ""
    credibility: float = 0.5                 # 0..1 source trust
    confidence: float = 0.5                  # 0..1 adapter confidence
    novelty: float = 0.0                     # 0..1 (computed at persist if unset)
    ref_id: Optional[str] = None             # provenance back-link

    # Filled by the persistence layer after entity resolution.
    entity_ids: list[str] = Field(default_factory=list)

    @property
    def entity_names(self) -> list[str]:
        return [e.name for e in self.entities]
