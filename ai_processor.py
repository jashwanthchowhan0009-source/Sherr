"""
ai_processor.py — Gemini 2.5 Flash primary with Groq fallback.
Uses structured output (responseSchema) so JSON is guaranteed valid.
"""

import os
import json
import asyncio
import logging
import re
from typing import Optional
import httpx

from text_utils import (
    clean_html_fragments,
    extract_sentences,
    truncate_to_words,
    word_count,
    summary_conflicts_with_title,
)

log = logging.getLogger("sherbyte.ai")

GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")
GROK_API_KEY   = os.getenv("GROK_API_KEY", "")
GEMINI_MODEL   = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
GROQ_MODEL     = os.getenv("GROQ_MODEL",   "llama-3.3-70b-versatile")

VALID_CATEGORIES = [
    "society", "economy", "tech", "arts", "nature",
    "selfwell", "philo", "lifestyle", "sports"
]

# Gemini structured output schema — guarantees valid JSON shape
_GEMINI_SCHEMA = {
    "type": "object",
    "properties": {
        "refined_title": {"type": "string"},
        "summary":       {"type": "string"},
        "full_body":     {"type": "string"},
        "category":      {"type": "string", "enum": VALID_CATEGORIES},
        "topic_tags":    {"type": "array",  "items": {"type": "string"}},
        "is_trending":   {"type": "boolean"},
        "sentiment":     {"type": "string", "enum": ["positive", "neutral", "negative"]},
        "when_info":     {"type": "string"},
        "where_info":    {"type": "string"},
    },
    "required": ["refined_title", "summary", "full_body", "category"]
}

SYSTEM_INSTRUCTION = """You are SherByte's senior news editor for an Indian audience.
Transform raw news into polished, structured content. Your output feeds directly into a mobile news app.

STRICT RULES:

1. refined_title — Maximum 12 words. Active voice. Concrete and specific.
   BAD: "Breaking: Big News About Tech Company"
   GOOD: "Nvidia posts record Q4 earnings, stock climbs 8%"
   Never use prefixes like "Breaking:", "Exclusive:", "Headline:", "Watch:", "Just In:".

2. summary — EXACTLY 2 factual sentences totaling 40-55 words.
   - MUST NOT begin with or restate the title.
   - Sentence 1: what specifically happened.
   - Sentence 2: immediate consequence, context, or next step.
   - No rhetorical questions, no "read on", no "find out".

3. full_body — 4-6 short paragraphs, 150-200 words total.
   - Cover WHO, WHAT, WHEN, WHERE, HOW.
   - Factual only. No speculation, no editorial opinion.
   - Use plain paragraphs separated by blank lines. No markdown.

4. category — Choose EXACTLY ONE slug from this list. This is not a suggestion.
   - society  = politics, elections, governance, courts, diplomacy, protests, education policy, military conflict
   - economy  = stocks, crypto, banking, IPOs, earnings, funding rounds, trade, real estate
   - tech     = AI, software, hardware, space, cybersecurity, gadgets, scientific research
   - arts     = films, music, books, TV series, theatre, galleries, creative award shows
   - nature   = climate, wildlife, natural disasters (floods/quakes/cyclones), environment, animals
   - selfwell = physical health, mental health, fitness, nutrition, medicine, disease, hospitals
   - philo    = religion, spirituality, philosophy, ethics debates, mythology
   - lifestyle= travel, food, fashion, social trends, celebrity gossip (non-artistic), influencers
   - sports   = cricket, football, F1, IPL, Olympics, tennis, all athletics, esports, gaming

   Disambiguation examples:
   - "Bank IPO" → economy (NOT tech).
   - "Elon Musk rocket launch" → tech.
   - "Elon Musk divorce" → lifestyle.
   - "Actor wins Oscar" → arts.
   - "Actor contracts virus" → selfwell.
   - "Virat Kohli scores century" → sports.
   - "Flood in Kerala" → nature.
   - "Supreme Court ruling on flood relief" → society.

5. topic_tags — 2-5 specific proper nouns or concepts from the article. Examples: "Bitcoin", "Supreme Court", "Nifty 50", "ISRO", "IPL".

6. is_trending — true ONLY for: major breaking events, record-breaking outcomes, national/global impact, or unprecedented announcements. Routine news is false.

7. sentiment — positive | neutral | negative. Based on the event itself, not the prose.

8. when_info — "April 16, 2026" or "Thursday morning" if article states it, else "".

9. where_info — "City, Country" or "State, Country" if present, else "Not specified".

Output the JSON object only. No markdown. No commentary."""


async def _call_gemini(title: str, body: str, client: httpx.AsyncClient) -> Optional[dict]:
    if not GEMINI_API_KEY:
        return None

    url = f"https://generativelanguage.googleapis.com/v1beta/models/{GEMINI_MODEL}:generateContent?key={GEMINI_API_KEY}"

    payload = {
        "systemInstruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
        "contents": [{
            "role": "user",
            "parts": [{"text": f"ARTICLE TITLE: {title}\n\nARTICLE BODY: {body[:2500]}"}]
        }],
        "generationConfig": {
            "temperature": 0.35,
            "maxOutputTokens": 1024,
            "responseMimeType": "application/json",
            "responseSchema": _GEMINI_SCHEMA,
        }
    }

    try:
        r = await client.post(url, json=payload, timeout=30)
        if r.status_code != 200:
            log.warning("Gemini HTTP %d: %s", r.status_code, r.text[:250])
            return None
        data = r.json()
        candidates = data.get("candidates", [])
        if not candidates:
            log.warning("Gemini returned no candidates")
            return None
        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            return None
        text = parts[0].get("text", "").strip()
        return json.loads(text)
    except json.JSONDecodeError as e:
        log.warning("Gemini JSON parse failed: %s", e)
        return None
    except Exception as e:
        log.warning("Gemini call failed: %s", e)
        return None


async def _call_groq(title: str, body: str, client: httpx.AsyncClient) -> Optional[dict]:
    if not GROK_API_KEY:
        return None

    prompt = SYSTEM_INSTRUCTION + f"""

ARTICLE TITLE: {title}

ARTICLE BODY: {body[:2000]}

Return ONLY a single JSON object matching the schema. No markdown, no code fences."""

    try:
        r = await client.post(
            "https://api.groq.com/openai/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {GROK_API_KEY}",
                "Content-Type":  "application/json",
            },
            json={
                "model":           GROQ_MODEL,
                "messages":        [{"role": "user", "content": prompt}],
                "temperature":     0.35,
                "max_tokens":      900,
                "response_format": {"type": "json_object"},
            },
            timeout=25,
        )
        if r.status_code != 200:
            log.warning("Groq HTTP %d: %s", r.status_code, r.text[:200])
            return None
        data = r.json()
        text = data["choices"][0]["message"]["content"].strip()
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text).strip()
        return json.loads(text)
    except Exception as e:
        log.warning("Groq fallback failed: %s", e)
        return None


def _rule_based_fallback(title: str, body: str, fallback_category: str = "tech") -> dict:
    body_clean = clean_html_fragments(body)
    summary    = extract_sentences(body_clean, 2) or title
    summary    = truncate_to_words(summary, 55)
    full       = truncate_to_words(body_clean, 180) or title
    return {
        "refined_title": truncate_to_words(title, 12),
        "summary":       summary,
        "full_body":     full,
        "category":      fallback_category,
        "topic_tags":    [],
        "is_trending":   False,
        "sentiment":     "neutral",
        "when_info":     "",
        "where_info":    "Not specified",
    }


def _validate_and_fix(result: dict, title: str, body: str, fallback_category: str = "tech") -> dict:
    """Defensive layer: fix anything the LLM got subtly wrong."""
    if not isinstance(result, dict):
        return _rule_based_fallback(title, body, fallback_category)

    result.setdefault("refined_title", title)
    result.setdefault("summary",       "")
    result.setdefault("full_body",     "")
    result.setdefault("category",      fallback_category)
    result.setdefault("topic_tags",    [])
    result.setdefault("is_trending",   False)
    result.setdefault("sentiment",     "neutral")
    result.setdefault("when_info",     "")
    result.setdefault("where_info",    "Not specified")

    # Enforce valid category
    if result["category"] not in VALID_CATEGORIES:
        result["category"] = fallback_category

    # Trim refined_title
    if word_count(result["refined_title"]) > 14:
        result["refined_title"] = truncate_to_words(result["refined_title"], 12)

    # Fill in a bad/empty summary
    if not result["summary"] or word_count(result["summary"]) < 10:
        body_clean = clean_html_fragments(body)
        alt = extract_sentences(body_clean, 2)
        result["summary"] = alt or result["refined_title"]

    # Summary that just restates the title → use next sentences from the body
    if summary_conflicts_with_title(result["summary"], result["refined_title"]):
        body_clean = clean_html_fragments(body)
        all_sentences = re.split(r'(?<=[.!?])\s+', body_clean.strip())
        alt = ' '.join(all_sentences[1:3]).strip()
        if alt and word_count(alt) >= 15 and not summary_conflicts_with_title(alt, result["refined_title"]):
            result["summary"] = alt

    if word_count(result["summary"]) > 65:
        result["summary"] = truncate_to_words(result["summary"], 55)

    # Fill in empty full_body
    if not result["full_body"] or word_count(result["full_body"]) < 40:
        body_clean = clean_html_fragments(body)
        result["full_body"] = truncate_to_words(body_clean, 180) or result["summary"]

    # Normalize tags
    if not isinstance(result["topic_tags"], list):
        result["topic_tags"] = []
    result["topic_tags"] = [
        str(t).strip() for t in result["topic_tags"]
        if t and isinstance(t, (str, int, float))
    ][:5]

    # Bool coercion
    result["is_trending"] = bool(result["is_trending"])

    return result


# ─── Public API ──────────────────────────────────────────────────────────

async def process_article(title: str, body: str, fallback_category: str = "tech") -> dict:
    """Process a single article. Gemini → Groq → rule-based."""
    body_clean = clean_html_fragments(body)

    async with httpx.AsyncClient() as client:
        result = await _call_gemini(title, body_clean, client)
        if result:
            return _validate_and_fix(result, title, body_clean, fallback_category)

        result = await _call_groq(title, body_clean, client)
        if result:
            return _validate_and_fix(result, title, body_clean, fallback_category)

    return _rule_based_fallback(title, body_clean, fallback_category)


async def process_batch(articles: list[dict], concurrency: int = 5) -> list[dict]:
    """
    Process many articles in parallel. 5x faster than sequential.
    Each article dict needs: {'title': str, 'body': str, 'fallback_category': str}
    Returns list of processed results in the same order.
    """
    sem = asyncio.Semaphore(concurrency)
    results: list[Optional[dict]] = [None] * len(articles)

    async with httpx.AsyncClient() as client:
        async def one(idx: int, article: dict):
            async with sem:
                title    = article.get("title", "")
                body     = clean_html_fragments(article.get("body", ""))
                fallback = article.get("fallback_category", "tech")
                try:
                    r = await _call_gemini(title, body, client)
                    if not r:
                        r = await _call_groq(title, body, client)
                    if not r:
                        r = _rule_based_fallback(title, body, fallback)
                    results[idx] = _validate_and_fix(r, title, body, fallback)
                except Exception as e:
                    log.warning("Batch item %d failed: %s", idx, e)
                    results[idx] = _rule_based_fallback(title, body, fallback)

        await asyncio.gather(*[one(i, a) for i, a in enumerate(articles)])

    return [r for r in results if r is not None]


def available_providers() -> dict:
    """For /health endpoint — which AI providers are configured."""
    return {
        "gemini":    bool(GEMINI_API_KEY),
        "groq":      bool(GROK_API_KEY),
        "primary":   "gemini" if GEMINI_API_KEY else ("groq" if GROK_API_KEY else "rule-based"),
        "model":     GEMINI_MODEL if GEMINI_API_KEY else (GROQ_MODEL if GROK_API_KEY else "none"),
    }
