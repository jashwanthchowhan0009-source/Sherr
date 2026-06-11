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

import asyncio
import json
import logging
import random
import re
import time
from collections import deque
from typing import Optional

import httpx

from app.config import settings

log = logging.getLogger("sherbyte.router")


# ─── Groq rate limiting ─────────────────────────────────────────────────────
class _GroqLimiter:
    """Keeps Groq usage under a tokens-per-minute ceiling and caps the number of
    in-flight requests. ``reserve`` blocks until the requested token budget fits
    inside a rolling 60-second window, so concurrent callers self-throttle
    instead of all firing at once and tripping 429s."""

    def __init__(self, tpm: int, max_concurrency: int):
        self.tpm = max(tpm, 1)
        self._sem = asyncio.Semaphore(max(max_concurrency, 1))
        self._lock = asyncio.Lock()
        self._events: deque[tuple[float, int]] = deque()   # (monotonic_ts, tokens)
        self._used = 0

    async def reserve(self, tokens: int) -> None:
        # A single request larger than the whole budget would never fit — clamp
        # it so we wait at most one window rather than deadlocking forever.
        tokens = max(1, min(tokens, self.tpm))
        while True:
            async with self._lock:
                now = time.monotonic()
                while self._events and now - self._events[0][0] >= 60.0:
                    self._used -= self._events.popleft()[1]
                if self._used + tokens <= self.tpm:
                    self._events.append((now, tokens))
                    self._used += tokens
                    return
                wait = 60.0 - (now - self._events[0][0])
            await asyncio.sleep(max(0.05, min(wait, 5.0)))

    async def __aenter__(self) -> "_GroqLimiter":
        await self._sem.acquire()
        return self

    async def __aexit__(self, *exc) -> None:
        self._sem.release()


_groq_limiter = _GroqLimiter(settings.groq_tpm_limit, settings.groq_max_concurrency)


def _estimate_tokens(*texts: str) -> int:
    """Rough token estimate (~4 chars/token) — good enough for budgeting."""
    return sum(len(t) for t in texts) // 4 + 8


def _retry_after_seconds(resp: httpx.Response, attempt: int) -> float:
    """Backoff delay for a 429: honor a server ``Retry-After`` when present,
    otherwise exponential backoff with full jitter (random in [0, cap])."""
    ra = resp.headers.get("Retry-After")
    if ra:
        try:
            return min(float(ra), settings.groq_backoff_max_sec) + random.uniform(0, 0.5)
        except ValueError:
            pass
    cap = min(settings.groq_backoff_base_sec * (2 ** attempt), settings.groq_backoff_max_sec)
    return random.uniform(0, cap)


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
            # Quota/rate-limit (429 or RESOURCE_EXHAUSTED) is the expected
            # free-tier failure — fail over to Groq quietly rather than spamming.
            if r.status_code == 429 or "RESOURCE_EXHAUSTED" in r.text:
                log.warning("Gemini quota exceeded — failing over to Groq")
            else:
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
                client: httpx.AsyncClient, model: Optional[str] = None) -> Optional[str]:
    if not settings.groq_api_key:
        return None
    body = {
        "model": model or settings.groq_model,
        "messages": [
            {"role": "system", "content": system},
            {"role": "user", "content": user},
        ],
        "temperature": temperature,
        "max_tokens": max_tokens,
    }
    if json_mode:
        body["response_format"] = {"type": "json_object"}

    # Stay under the TPM ceiling before we ever hit the wire (reserve worst case:
    # prompt tokens + the completion we asked for).
    await _groq_limiter.reserve(_estimate_tokens(system, user) + max_tokens)

    async with _groq_limiter:                      # cap concurrent in-flight calls
        for attempt in range(settings.groq_max_retries + 1):
            try:
                r = await client.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {settings.groq_api_key}",
                        "Content-Type": "application/json",
                    },
                    json=body, timeout=25,
                )
            except Exception as e:
                log.warning("Groq call failed: %s", e)
                return None

            if r.status_code == 429:
                if attempt >= settings.groq_max_retries:
                    log.warning("Groq 429 — gave up after %d retries", attempt)
                    return None
                delay = _retry_after_seconds(r, attempt)
                log.warning("Groq 429 — backing off %.1fs (retry %d/%d)",
                            delay, attempt + 1, settings.groq_max_retries)
                await asyncio.sleep(delay)
                continue
            if r.status_code != 200:
                log.warning("Groq HTTP %d: %s", r.status_code, r.text[:200])
                return None
            try:
                return r.json()["choices"][0]["message"]["content"].strip()
            except Exception as e:
                log.warning("Groq response parse failed: %s", e)
                return None
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
    """Structured completion. Gemini (schema-constrained) → Groq (json mode).

    On Gemini quota/failure the call fails over to Groq's fast, low-cost model.
    If both are unavailable this returns None and the caller (e.g. the
    understander) degrades to deterministic rule-based extraction — the
    ingestion pipeline never crashes on a rate limit.
    """
    async with httpx.AsyncClient() as client:
        text = await _gemini(system, user, schema, temperature, max_tokens, client)
        result = _parse_json(text)
        if result is not None:
            return result
        text = await _groq(system, user, True, temperature, max_tokens, client,
                           model=settings.groq_fallback_model)
        return _parse_json(text)


async def complete_text(system: str, user: str,
                        temperature: float = 0.4, max_tokens: int = 1024) -> Optional[str]:
    """Free-form completion. Gemini → Groq (fast fallback model)."""
    async with httpx.AsyncClient() as client:
        text = await _gemini(system, user, None, temperature, max_tokens, client)
        if text:
            return text
        return await _groq(system, user, False, temperature, max_tokens, client,
                           model=settings.groq_fallback_model)


def provider_status() -> dict:
    return {
        "primary": settings.ai_primary,
        "gemini": bool(settings.gemini_api_key),
        "groq": bool(settings.groq_api_key),
        "model": settings.gemini_model if settings.gemini_api_key
                 else (settings.groq_model if settings.groq_api_key else "none"),
    }
