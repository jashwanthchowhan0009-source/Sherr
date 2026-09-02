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
from datetime import datetime, timezone
import httpx

import key_pool

from text_utils import (
    MIN_ORIGINAL_WORDS,
    clean_html_fragments,
    extract_sentences,
    truncate_to_words,
    word_count,
    summary_conflicts_with_title,
)

log = logging.getLogger("sherbyte.ai")

# ─── model ids ───────────────────────────────────────────────────────────────
# EVERY ONE OF THESE IS AN ENV VAR, AND HAS BEEN. The value below is only the
# fallback when the variable is unset — so a model retirement is fixed by
# setting GEMINI_MODEL on the service, with no deploy and no code change.
#
# A RETIRED MODEL IS THE WORST FAILURE THIS FILE HAS. The provider answers 404
# or 400, _call_provider returns None, _rule_based_fallback supplies the
# placeholder, and 25,000 articles quietly stay on it. Two things make that
# visible now: the refusal is recorded in PROVIDER_ERRORS with its status and
# body, and /admin/body-audit reports the model each provider is ACTUALLY using
# so a stale id can be read off the endpoint instead of inferred from silence.
#
# Defaults verified 2026-09-02. THEY WILL GO STALE — that is the nature of the
# thing, which is why the env var is the real answer and this is only a floor.
# gemini-2.5-flash (retiring 2026-10-20) and grok-2-latest (does not exist) are
# what these replaced.
MODEL_DEFAULT = {
    "gemini": "gemini-3.1-flash-lite",
    "groq":   "llama-3.1-8b-instant",
    "openai": "gpt-4o-mini",
    "grok":   "grok-4.3",
}

# Which env var overrides which model, for the audit to report back. A provider
# whose model is still the built-in default is worth seeing as such: it is the
# one that goes stale without anyone touching it.
MODEL_ENV_VAR = {"gemini": "GEMINI_MODEL", "groq": "GROQ_MODEL",
                 "openai": "OPENAI_MODEL", "grok": "GROK_MODEL"}


def model_for(provider: str) -> str:
    """The model id this provider will actually be called with, read NOW.

    Resolved per call rather than captured at import. Two reasons, and the
    second is the one that matters: it means /admin/body-audit reports what a
    request would really use rather than what the env looked like at boot, and
    it means a test can set the variable without reloading the module — which
    corrupts every other module holding a reference to this one.
    """
    return os.getenv(MODEL_ENV_VAR[provider], MODEL_DEFAULT[provider]).strip() \
        or MODEL_DEFAULT[provider]


# Module-level names kept for the call sites and tests that already read them.
GEMINI_MODEL   = model_for("gemini")
GROQ_MODEL     = model_for("groq")
OPENAI_MODEL   = model_for("openai")
GROK_MODEL     = model_for("grok")

# Every key the environment carries, per provider, collected once at import.
# GEMINI_API_KEY_4 / GEMINI_API_KEY_9 / GROQ_API_KEY_4 / GPT_API_KEY_4 were all
# invisible before this: the module read one fixed name per provider, so the
# spares were dead weight and a single 429 took the whole rewrite pass down.
KEYS = key_pool.PoolSet()

# Back-compat: a few call sites and tests read these names directly. They are
# now "the first key in the pool" rather than "the only key".
GEMINI_API_KEY = KEYS.get("gemini").current() or ""
GROK_API_KEY   = KEYS.get("grok").current() or ""

# Copyright-safe placeholders. When no AI rewrite is available we NEVER fall back
# to the source article's text — we show these neutral, original strings instead.
_SAFE_SUMMARY = "Sherr AI is preparing an original summary of this story."
_SAFE_BODY = (
    "Sherr AI is preparing an original, plain-language summary of this story — "
    "the key facts, who is involved and why it matters will appear here shortly. "
    "Use the source link to read the full report at the original publisher."
)

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

0. ORIGINALITY — this overrides every other rule.
   READ the source, EXTRACT the facts, then WRITE FRESH PROSE in your own words.
   Never copy sentences or clauses from the source. Never lightly reword them.
   You may reproduce at most 25 consecutive words verbatim, and only when ALL of
   these hold: it is a direct statement by a named person, it is wrapped in double
   quotation marks, and the speaker is named in the same sentence.
   Everything outside such a quote must be your own phrasing. Output that reuses the
   source's wording is rejected automatically and the article is not published.

1. refined_title — Maximum 12 words. Active voice. Concrete and specific.
   It must be YOUR headline, not the publisher's. Use a different word order and a
   different angle from the source title. It must NOT be a substring of the source
   title, and must NOT share any run of 5 consecutive words with it.
   BAD: "Breaking: Big News About Tech Company"
   GOOD: "Nvidia posts record Q4 earnings, stock climbs 8%"
   Never use prefixes like "Breaking:", "Exclusive:", "Headline:", "Watch:", "Just In:".

2. summary — EXACTLY 2 factual sentences totaling 40-55 words.
   - MUST NOT begin with or restate the title.
   - Sentence 1: what specifically happened.
   - Sentence 2: immediate consequence, context, or next step.
   - No rhetorical questions, no "read on", no "find out".

3. full_body — 2-3 sentences, 40-70 words. ONE paragraph.

   YOU ARE WORKING FROM A SHORT NEWS BLURB, NOT A FULL ARTICLE. Usually 30-40
   words. Write ONLY what those words support.

   - ABSTRACTIVE, never extractive: synthesise the facts into new sentences.
   - Cover only the WHO / WHAT / WHEN / WHERE the source actually states.
   - NEVER INVENT. No detail, figure, quote, date, cause, consequence or
     background that is not in the source. If the source does not say why
     something happened, do not say why. A shorter, thinner body is CORRECT
     when the source is thin — inventing detail to reach a word count is the
     single worst thing you can do here.
   - If the source gives you almost nothing, write one accurate sentence and
     stop. Do not pad.
   - Factual only. No speculation, no editorial opinion. No markdown.

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


# ─── provider calls ───────────────────────────────────────────────────────────
# Each takes an explicit key and returns (result, status):
#   result — the parsed dict, or None
#   status — the HTTP status, so the pool wrapper can tell "this key is rate
#            limited" (rotate) from "the service is down" (do not rotate, the
#            next key would fail identically and spend the pool for nothing).

async def _gemini_once(key: str, title: str, body: str, client) -> tuple:
    # model_for, not the boot-time constant: the audit reports model_for's
    # answer, and the request must use the same one or the two disagree.
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model_for('gemini')}:generateContent?key={key}")
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
            log.warning("Gemini HTTP %d: %s", r.status_code, r.text[:200])
            _record_error("gemini", r.status_code, r.text)
            return None, r.status_code
        candidates = (r.json() or {}).get("candidates", [])
        if not candidates:
            log.warning("Gemini returned no candidates")
            _record_error("gemini", 200, f"no candidates: {r.text[:300]}")
            return None, 200
        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            return None, 200
        return json.loads(parts[0].get("text", "").strip()), 200
    except json.JSONDecodeError as e:
        log.warning("Gemini JSON parse failed: %s", e)
        _record_error("gemini", 200, f"JSONDecodeError: {e}")
        return None, 200
    except Exception as e:
        log.warning("Gemini call failed: %s", e)
        _record_error("gemini", 0, f"{type(e).__name__}: {e}")
        return None, 0


# Every provider failure used to end at a log.warning and then vanish: the
# cascade returned None, _rule_based_fallback supplied the placeholder, and the
# caller had no way to learn which provider refused it or why. A pass that
# rewrites nothing looked identical to one with nothing to rewrite.
#
# So each refusal is recorded here — provider, HTTP status, and the response
# body verbatim — and /admin/body-audit reads it back.
PROVIDER_ERRORS: list = []
_MAX_PROVIDER_ERRORS = 20


def _record_error(provider: str, status: int, detail: str) -> None:
    PROVIDER_ERRORS.insert(0, {
        "provider": provider,
        "status": status,          # 0 == the request never got a response
        "error": (detail or "")[:500],
        "at": datetime.now(timezone.utc).isoformat(),
    })
    del PROVIDER_ERRORS[_MAX_PROVIDER_ERRORS:]


def last_provider_errors(n: int = 5) -> list:
    return PROVIDER_ERRORS[:n]


async def _openai_chat_once(key: str, title: str, body: str, client, *,
                            url: str, model: str, label: str) -> tuple:
    """Groq, OpenAI and Grok all speak the OpenAI chat-completions shape, so they
    share one caller and differ only by endpoint, model and key."""
    prompt = SYSTEM_INSTRUCTION + f"""

ARTICLE TITLE: {title}

ARTICLE BODY: {body[:2000]}

Return ONLY a single JSON object matching the schema. No markdown, no code fences."""
    try:
        r = await client.post(
            url,
            headers={"Authorization": f"Bearer {key}",
                     "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.35,
                  "max_tokens": 900,
                  "response_format": {"type": "json_object"}},
            timeout=25,
        )
        if r.status_code != 200:
            log.warning("%s HTTP %d: %s", label, r.status_code, r.text[:200])
            _record_error(label, r.status_code, r.text)
            return None, r.status_code
        text = (r.json()["choices"][0]["message"]["content"] or "").strip()
        text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text).strip()
        return json.loads(text), 200
    except Exception as e:
        log.warning("%s call failed: %s", label, e)
        _record_error(label, 0, f"{type(e).__name__}: {e}")
        return None, 0


# provider -> a coroutine (key, title, body, client) -> (result, status)
_PROVIDER_CALLS = {
    "gemini": _gemini_once,
    "groq":   lambda k, t, b, c: _openai_chat_once(
        k, t, b, c, url="https://api.groq.com/openai/v1/chat/completions",
        model=model_for("groq"), label="Groq"),
    "openai": lambda k, t, b, c: _openai_chat_once(
        k, t, b, c, url="https://api.openai.com/v1/chat/completions",
        model=model_for("openai"), label="OpenAI"),
    # xAI, NOT Groq. The code used to read GROK_API_KEY and post it to
    # api.groq.com, so whichever of the two keys existed, one provider was
    # always being called with the other's credential.
    "grok":   lambda k, t, b, c: _openai_chat_once(
        k, t, b, c, url="https://api.x.ai/v1/chat/completions",
        model=model_for("grok"), label="Grok"),
}


async def _call_provider(provider: str, title: str, body: str, client):
    """Try every key in one provider's pool, rotating on key-specific failures.

    Rotation happens ONLY on 401/403/429 — the statuses that mean this key is
    the problem. On anything else the next key would fail identically, and
    burning the pool to learn that costs the fallback its keys too.
    """
    pool = KEYS.get(provider)
    if not pool.size:
        return None
    fn = _PROVIDER_CALLS[provider]
    pool.reset()
    for _ in range(pool.size):
        key = pool.current()
        if not key:
            return None
        result, status = await fn(key, title, body, client)
        if result:
            return result
        if status in key_pool.ROTATE_STATUSES:
            if not pool.rotate(f"HTTP {status}"):
                log.warning("%s: all %d key(s) exhausted (last HTTP %d)",
                            provider, pool.size, status)
                return None
            continue
        return None
    return None


async def _call_cascade(title: str, body: str, client):
    """Every configured provider in order, each with its own key rotation."""
    for provider in KEYS.configured():
        result = await _call_provider(provider, title, body, client)
        if result:
            return result
    return None


# NOTE: there are deliberately no per-provider _call_gemini / _call_groq
# wrappers any more. Nothing outside this module called them, and leaving them
# meant a test could monkeypatch _call_gemini, watch the patch have no effect
# because the cascade dispatches through _PROVIDER_CALLS, and still pass.
# Patch _PROVIDER_CALLS[provider] for one provider, or _call_cascade for all.


# Pillar ids the keyword classifier returns, mapped onto the category slugs this
# module speaks. Kept as a mapping rather than duplicating the keyword table:
# scripts/publish_pending.py owns that table and the drain already uses it, and a
# second copy would drift.
_PILLAR_TO_CATEGORY = {
    1: "society", 2: "economy", 3: "tech", 4: "arts", 5: "nature",
    6: "selfwell", 7: "philo", 8: "lifestyle", 9: "sports",
}


def _classify_by_keyword(title: str, body: str, fallback_category: str) -> tuple:
    """(category, evidence) from the shared keyword table, or the caller's default.

    Imported lazily and defensively: this runs on the path where everything else
    has already failed, so it must not be the thing that raises.
    """
    try:
        import os as _os
        import sys as _sys
        _here = _os.path.dirname(_os.path.abspath(__file__))
        _scripts = _os.path.join(_here, "scripts")
        if _scripts not in _sys.path:
            _sys.path.insert(0, _scripts)
        from publish_pending import classify           # noqa: PLC0415
        pillar, evidence = classify(title or "", body or "")
        return _PILLAR_TO_CATEGORY.get(pillar, fallback_category), evidence
    except Exception as e:                              # pragma: no cover
        log.warning("keyword classifier unavailable, using default category: %s", e)
        return fallback_category, {"matched": [], "reason": "classifier unavailable"}


def _rule_based_fallback(title: str, body: str, fallback_category: str = "tech",
                         publishable: bool = False) -> dict:
    """Both providers are down. Classify by keyword and hand back a usable row.

    COPYRIGHT: the body is never the publisher's. `full_body` is our own stub, as
    it always was — an outage is not a licence to reproduce someone's article.

    What changed is the headline. This used to return refined_title="" so the
    caller parked the row as pending_rewrite, on the reasoning that the
    publisher's headline is theirs. That is true, and it is also why the corpus
    sat at 1600+ parked rows and the feed served nothing when the providers went
    down: with no title there is nothing to publish. `publishable=True` keeps the
    publisher's headline and marks the row for the aggregator posture — headline
    with visible credit and an outbound link, body ours — which is the same
    posture scripts/publish_pending.py and the startup drain already use. The row
    records ai_fallback so it stays distinguishable from something that actually
    cleared the gate, and a later rewrite pass can find it with one query.
    """
    category, evidence = _classify_by_keyword(title, body, fallback_category)
    out = {
        "refined_title": "",
        "summary":       _SAFE_SUMMARY,
        "full_body":     _SAFE_BODY,
        "category":      category,
        "topic_tags":    [],
        "is_trending":   False,
        "sentiment":     "neutral",
        "when_info":     "",
        "where_info":    "Not specified",
        "ai_fallback":   True,
        "classifier":    evidence,
    }
    if publishable:
        out["refined_title"] = (title or "").strip()
        out["publish_as_aggregator"] = True
    return out


def _validate_and_fix(result: dict, title: str, body: str, fallback_category: str = "tech") -> dict:
    """Defensive layer: fix anything the LLM got subtly wrong."""
    if not isinstance(result, dict):
        # A provider answered with something unusable, which is a provider failure
        # like any other — publish rather than park.
        return _rule_based_fallback(title, body, fallback_category, publishable=True)

    # Same rule as the fallback: absent means park it, never inherit the source.
    result.setdefault("refined_title", "")
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

    # Fill in a bad/empty summary — copyright-safe: use our own title or a
    # neutral placeholder, never the source article's sentences.
    if not result["summary"] or word_count(result["summary"]) < 10:
        result["summary"] = result["refined_title"] or _SAFE_SUMMARY

    if word_count(result["summary"]) > 65:
        result["summary"] = truncate_to_words(result["summary"], 55)

    # Fill in an empty or too-short full_body — never fall back to the source
    # text.
    #
    # THE FLOOR IS MIN_ORIGINAL_WORDS, NOT 40. It was 40, while body_state
    # accepted an original body at 25, so a real 30-word rewrite was replaced
    # with the placeholder here and then classified a stub downstream. Both
    # modules now read the same constant; a test asserts they cannot drift.
    if not result["full_body"] or word_count(result["full_body"]) < MIN_ORIGINAL_WORDS:
        result["full_body"] = _SAFE_BODY

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
        result = await _call_cascade(title, body_clean, client)
        if result:
            return _validate_and_fix(result, title, body_clean, fallback_category)

    # Every configured provider failed, each having spent its own keys — a 4xx on
    # a retired model id, an outage, or no key at all. Parking the row here is
    # what emptied the feed, so classify by keyword and publish on the
    # aggregator posture instead.
    log.warning("all AI providers failed for %r (%s) — rule-based fallback",
                (title or "")[:60], KEYS.describe())
    return _rule_based_fallback(title, body_clean, fallback_category, publishable=True)


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
                    r = await _call_cascade(title, body, client)
                    if not r:
                        # publishable=True, exactly as process_article does.
                        # Without it _rule_based_fallback returns
                        # refined_title="" — and run_ai_batch writes that
                        # straight into `headline`. Every article processed
                        # while the providers were down got a BLANK TITLE, which
                        # is why the feed showed cards with an image, a byline
                        # and no headline at all.
                        r = _rule_based_fallback(title, body, fallback,
                                                 publishable=True)
                    results[idx] = _validate_and_fix(r, title, body, fallback)
                except Exception as e:
                    log.warning("Batch item %d failed: %s", idx, e)
                    results[idx] = _rule_based_fallback(title, body, fallback,
                                                        publishable=True)

        await asyncio.gather(*[one(i, a) for i, a in enumerate(articles)])

    return [r for r in results if r is not None]


# Live lookups, not the boot-time constants: setting GEMINI_MODEL on the service
# takes effect on the next request rather than the next restart.
_MODEL_FOR = {p: (lambda p=p: model_for(p)) for p in MODEL_DEFAULT}


def available_providers() -> dict:
    """Which providers are configured, and how DEEP each pool is.

    Sizes rather than booleans: "gemini: true" was the same answer whether one
    key was configured or three, which is exactly the difference between a 429
    ending the pass and a 429 costing one retry. A size is still truthy, so
    callers testing `if providers["gemini"]` keep working.
    """
    sizes = KEYS.sizes()
    configured = KEYS.configured()
    primary = configured[0] if configured else "rule-based"
    return {
        **sizes,
        "total_keys": sum(sizes.values()),
        "primary":    primary,
        # The primary's model, kept for callers that already read this key.
        "model":      _MODEL_FOR[primary]() if primary in _MODEL_FOR else "none",
        # EVERY configured provider's model, with whether it came from the
        # environment or from the built-in default. One stale id in a fallback
        # provider is invisible in "model" alone — and the fallback is exactly
        # what gets used on the day the primary breaks.
        "models":     {p: {"model": _MODEL_FOR[p](),
                           "source": ("env:" + MODEL_ENV_VAR[p]
                                      if os.getenv(MODEL_ENV_VAR[p])
                                      else "built-in default")}
                       for p in configured if p in _MODEL_FOR},
        # The order a request actually walks, so a log line about a fallback can
        # be checked against what was configured.
        "cascade":    configured or ["rule-based"],
    }
