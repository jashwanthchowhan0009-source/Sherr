"""
sherr/writer.py — Narrative generation.

Given an info object id, a write mode, and RAG consensus, produce the narrative
through the LLM router and return the raw text. Enforces the mode's word budget
as a final guardrail. Falls back to the info object's own summary/body if no LLM
provider is available so the endpoint always returns something useful.
"""

from __future__ import annotations

import logging

from app.config import WRITE_MODES
from app.sherr.prompts import MODE_PROMPTS, build_user_prompt, normalize_mode
from app.sherr.router import complete_text
from app.text_utils import truncate_to_words

log = logging.getLogger("sherbyte.writer")


def _word_budget(mode: str) -> int:
    return WRITE_MODES[mode]["max_words"]


async def write(headline: str, summary: str, body: str, wwww: dict,
                consensus: list[str], mode: str) -> str:
    mode = normalize_mode(mode)
    system = MODE_PROMPTS[mode]
    user = build_user_prompt(headline, summary, body, wwww, consensus)

    text = await complete_text(
        system=system, user=user,
        temperature=0.4,
        max_tokens=min(2048, _word_budget(mode) * 3),
    )

    if not text:
        # Graceful fallback — no LLM configured / both providers down.
        text = summary or body or headline

    return truncate_to_words(text.strip(), _word_budget(mode))
