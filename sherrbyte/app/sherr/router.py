"""
sherr/router.py — LLM provider cascade: Gemini → Groq.

A single choke point for every LLM call in the codebase (understander + writer).
Tries Gemini 2.5 Flash first (with structured-output schema when given), falls
back to Groq, and returns None if neither is configured or both fail — callers
are expected to degrade gracefully.

Public API:
    await complete_json(system, user, schema, temperature) -> dict | None
    await complete_text(system, user, temperature, max_tokens) -> str | None
    provider_status() -> dict
"""

from __future__ import annotations

import json
import logging
import re
from typing import Optional

import httpx

from app.config import settings

log = logging.getLogger("sherbyte.router")


# ─── Gemini ───────────────────────────────────────────────────────────────────
async def _gemini(system: str, user: str, schema: Optional[dict],
                  temperature: float, max_tokens: int,
                  client: httpx.AsyncClient) -> Optional[str]:
    if not settings.gemini_api_key:
        return None
    url = (
        f"https://generativelanguage.googleapis.com/v1beta/models/"
        f"{settings.gemini_model}:generateContent?key={settings.gemini_api_key}"
    )
    gen_cfg: dict = {"temperature": temperature, "maxOutputTokens": max_tokens}
    if schema is not None:
        gen_cfg["responseMimeType"] = "application/json"
        gen_cfg["responseSchema"] = schema
    payload = {
        "systemInstruction": {"parts": [{"text": system}]},
        "contents": [{"role": "user", "parts": [{"text": user}]}],
        "generationConfig": gen_cfg,
    }
    try:
        r = await client.post(url, json=payload, timeout=30)
        if r.status_code != 200:
            log.warning("Gemini HTTP %d: %s", r.status_code, r.text[:200])
            return None
        parts = r.json().get("candidates", [{}])[0].get("content", {}).get("parts", [])
        return parts[0].get("text", "").strip() if parts else None
    except Exception as e:
        log.warning("Gemini call failed: %s", e)
        return None


# ─── Groq ─────────────────────────────────────────────────────────────────────
async def _groq(system: str, user: str, json_mode: bool,
                temperature: float, max_tokens: int,
                client: httpx.AsyncClient) -> Optional[str]:
    if not settings.groq_api_key:
        return None
    body = {
        "model": settings.groq_model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}
    try:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {settings.groq_api_key}",
                "Content-Type": "application/json",
            },
            json=body, timeout=25,
        )
        if r.status_code != 200:
            log.warning("Groq HTTP %d: %s", r.status_code, r.text[:200])
            return None
        return r.json()["choices"][0]["message"]["content"].strip()
    except Exception as e:
        log.warning("Groq call failed: %s", e)
        return None


def _parse_json(text: Optional[str]) -> Optional[dict]:
    if not text:
        return None
    text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text.strip()).strip()
    try:
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.warning("LLM JSON parse failed: %s", e)
        return None


# ─── Public API ───────────────────────────────────────────────────────────────
async def complete_json(system: str, user: str, schema: Optional[dict] = None,
                        temperature: float = 0.3, max_tokens: int = 1024) -> Optional[dict]:
    """Structured completion. Gemini (schema-constrained) → Groq (json mode)."""
    async with httpx.AsyncClient() as client:
        text = await _gemini(system, user, schema, temperature, max_tokens, client)
        result = _parse_json(text)
        if result is not None:
            return result
        text = await _groq(system, user, True, temperature, max_tokens, client)
        return _parse_json(text)


async def complete_text(system: str, user: str,
                        temperature: float = 0.4, max_tokens: int = 1024) -> Optional[str]:
    """Free-form completion. Gemini → Groq."""
    async with httpx.AsyncClient() as client:
        text = await _gemini(system, user, None, temperature, max_tokens, client)
        if text:
            return text
        return await _groq(system, user, False, temperature, max_tokens, client)


def provider_status() -> dict:
    return {
        "primary": settings.ai_primary,
        "gemini": bool(settings.gemini_api_key),
        "groq": bool(settings.groq_api_key),
        "model": settings.gemini_model if settings.gemini_api_key
                 else (settings.groq_model if settings.groq_api_key else "none"),
    }
