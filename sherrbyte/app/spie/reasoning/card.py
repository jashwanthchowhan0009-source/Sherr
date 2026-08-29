"""
reasoning/card.py — a DecisionCard, built only after the math has already fired.

THE ORDER MATTERS AND IS ENFORCED HERE.

    1. tick_anomaly scores the daily return.        Deterministic.
    2. edges.traverse finds downstream entities.    Deterministic.
    3. news_match finds REAL articles.              Deterministic.
    4. Only if 1 fired AND 3 returned >= MIN_ARTICLES does an LLM get called.
    5. The LLM receives a FIXED payload: symbol, pct_change, z, headlines and
       the graph path. It never sees raw prices, never sees the series, and
       never decides whether any of this was significant.

If the math is silent, build() returns None and no LLM call is made. Silence is
a valid output, and a card is never rendered from an unvalidated response.

WHY signal_strength AND NOT confidence. reasoning/confidence.py computes a
calibrated 0..1 confidence for the reasoned insights, and that number is a
probability-like statement about evidence. This is not that. signal_strength is
a transparent 0-100 ranking score — a way to sort today's cards — and calling it
a confidence or printing it as a percentage would invite reading it as "77%
likely to be true", which it is not and can never be.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import Optional

from pydantic import BaseModel, Field, ValidationError

log = logging.getLogger("sherbyte.reasoning.card")

# How many LLM calls this process has made. Read by /admin/sherr-i-status, so
# "did the math stay silent" is answerable without reading logs.
LLM_CALLS = {"attempted": 0, "succeeded": 0, "validation_retries": 0, "failed": 0}


def reset_counters() -> None:
    for k in LLM_CALLS:
        LLM_CALLS[k] = 0


class Evidence(BaseModel):
    title: str
    source: str = ""
    url: str = ""
    published_at: Optional[datetime] = None


class DecisionCard(BaseModel):
    observation: str
    evidence: list[Evidence] = Field(default_factory=list)
    affected_entities: list[str] = Field(default_factory=list)
    signal_strength: int = Field(ge=0, le=100)
    what_to_watch: str
    computed_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))


# ─── signal strength ─────────────────────────────────────────────────────────
# Deliberately simple and fully documented: a reader who wants to know why one
# card sits above another can reconstruct this from the numbers on the card.
#
#   magnitude   |z| past the trigger, saturating at 6 sigma          0..50
#   corroboration  how many real articles matched, saturating at 5   0..30
#   independence   how many distinct sources, saturating at 4        0..20
#
# It is a RANKING score, not a probability. Nothing multiplies it by 100 and
# calls it a percentage.
W_MAGNITUDE, W_ARTICLES, W_SOURCES = 50, 30, 20
Z_SATURATION, ARTICLE_SATURATION, SOURCE_SATURATION = 6.0, 5, 4


def signal_strength(z: float, article_count: int, source_count: int) -> int:
    z_part = min(abs(float(z or 0.0)), Z_SATURATION) / Z_SATURATION
    a_part = min(int(article_count or 0), ARTICLE_SATURATION) / ARTICLE_SATURATION
    s_part = min(int(source_count or 0), SOURCE_SATURATION) / SOURCE_SATURATION
    return int(round(W_MAGNITUDE * z_part + W_ARTICLES * a_part + W_SOURCES * s_part))


def explain_signal_strength() -> dict:
    """The formula, shipped alongside the number so it is auditable."""
    return {
        "scale": "0-100 ranking score, NOT a probability and NOT a percentage",
        "magnitude": f"up to {W_MAGNITUDE} — |z| saturating at {Z_SATURATION}",
        "corroboration": f"up to {W_ARTICLES} — articles saturating at {ARTICLE_SATURATION}",
        "independence": f"up to {W_SOURCES} — distinct sources saturating at {SOURCE_SATURATION}",
    }


# ─── the payload the LLM is allowed to see ───────────────────────────────────
def build_payload(anomaly, articles: list, paths: list,
                  display_name: str = "") -> dict:
    """Structured facts only. No price series, no judgement, no instruction to
    decide whether this matters — that decision was already made upstream.

    `symbol` is the DISPLAY name when the caller has one: a card reading
    "CL=F rose 8.27%" names a ticker the reader has never seen, where
    "WTI Crude rose 8.27%" is the same fact in their language.
    """
    return {
        "symbol": display_name or anomaly.symbol,
        "market_type": anomaly.market_type,
        "direction": "rose" if anomaly.direction > 0 else "fell",
        "pct_change": round(abs(anomaly.pct_change), 2),
        "z_score": anomaly.z,
        "baseline_days": anomaly.window_n,
        "observed_on": anomaly.ts.date().isoformat() if anomaly.ts else None,
        "headlines": [a["title"] for a in articles][:6],
        "sources": sorted({a.get("source", "") for a in articles if a.get("source")}),
        "graph_paths": paths[:6],
    }


PROMPT = """You are writing one factual card for a market-intelligence feed.

The significance of this move has ALREADY been established by a statistical
test. Do not re-judge it, do not hedge about whether it matters, and do not
predict what happens next.

Write from this payload and nothing else:

{payload}

Return ONLY a JSON object:
{{
  "observation": "1-2 sentences. What moved, by how much, and what the headlines
                  below were about. Past tense. Never 'will', 'expect' or
                  'forecast'. Never claim causation — the news and the move
                  coincided.",
  "what_to_watch": "One sentence naming a concrete, checkable thing a reader
                    could look at next. Not advice. Not a prediction."
}}"""


async def _ask_llm(payload: dict) -> Optional[dict]:
    """One call through the app's provider cascade and key rotation."""
    import os
    import sys
    root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(
        os.path.dirname(os.path.abspath(__file__))))))
    if root not in sys.path:
        sys.path.insert(0, root)
    import httpx
    import ai_processor

    LLM_CALLS["attempted"] += 1
    prompt = PROMPT.format(payload=json.dumps(payload, indent=2, default=str))
    async with httpx.AsyncClient() as client:
        # The cascade already handles provider fallback and per-key rotation.
        return await ai_processor._call_cascade("Sherr-I card", prompt, client)


def _coerce(raw: dict, anomaly, articles: list, entities: list) -> DecisionCard:
    """Validate the LLM's two prose fields; every other field is OURS.

    The model supplies `observation` and `what_to_watch` and nothing else.
    Evidence, entities and signal_strength are computed here, so no response —
    valid or not — can inflate the score or invent a citation.
    """
    sources = {a.get("source", "") for a in articles if a.get("source")}
    return DecisionCard(
        observation=str((raw or {}).get("observation", "")).strip(),
        what_to_watch=str((raw or {}).get("what_to_watch", "")).strip(),
        evidence=[Evidence(title=a["title"], source=a.get("source", ""),
                           url=a.get("url", ""), published_at=a.get("published_at"))
                  for a in articles],
        affected_entities=entities,
        signal_strength=signal_strength(anomaly.z, len(articles), len(sources)),
    )


async def build(anomaly, articles: list, paths: list, entities: list,
                *, min_articles: int = 2,
                display_name: str = "") -> Optional[DecisionCard]:
    """A card, or None. None is a normal and frequent outcome.

    Returns None WITHOUT calling the LLM when there is no anomaly or fewer than
    `min_articles` real articles. Returns None WITH one retry when the response
    fails validation — an unvalidated card is never rendered.
    """
    if anomaly is None or len(articles or []) < min_articles:
        return None

    payload = build_payload(anomaly, articles, paths, display_name)
    for attempt in (1, 2):
        try:
            raw = await _ask_llm(payload)
        except Exception as e:
            log.warning("card LLM call failed: %s", e)
            raw = None
        if raw:
            try:
                card = _coerce(raw, anomaly, articles, entities)
                if card.observation and card.what_to_watch:
                    LLM_CALLS["succeeded"] += 1
                    return card
                raise ValidationError.from_exception_data("DecisionCard", [])
            except (ValidationError, TypeError, ValueError) as e:
                log.warning("card failed validation (attempt %d): %s", attempt, e)
        if attempt == 1:
            LLM_CALLS["validation_retries"] += 1
    LLM_CALLS["failed"] += 1
    return None
