"""
ai_processor.py — Gemini 2.5 Flash primary with Groq fallback.
Uses structured output (responseSchema) so JSON is guaranteed valid.
"""

import os
import json
import asyncio
import logging
import random
import re
from typing import Optional
from datetime import datetime, timezone
import httpx

import key_pool
import telugu

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
    # The BIG model, not the small one: a small model is fast but too weak to
    # REFRAME a story — it dry-restates the source, so the headline comes back
    # near-identical and the body reads like the wire copy. Slower on the free
    # tier (lower daily tokens), but the quality is the whole point.
    #
    # llama-3.3-70b-versatile was this value and Groq SHUT IT DOWN for the free
    # and developer tiers on 2026-08-16 (console.groq.com/docs/deprecations),
    # which named openai/gpt-oss-120b as its replacement. A retired id does not
    # fail loudly: the call 404s, the cascade drops to the next provider, and the
    # only symptom is a backlog that stops clearing. Override with GROQ_MODEL.
    "groq":   "openai/gpt-oss-120b",
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

# One cross-domain "dot" (lowercase types for the single-article Gemini schema).
_DOT_SCHEMA_LC = {
    "type": "object",
    "properties": {
        "summary":     {"type": "string"},
        "impact":      {"type": "string", "enum": ["up", "down", "mixed", "neutral"]},
        "instruments": {"type": "array", "items": {
            "type": "object",
            "properties": {"name": {"type": "string"}, "ticker": {"type": "string"}},
        }},
    },
}

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
        # The WWWH skeleton (full-article mode fills these; blurb mode may not).
        "what_info":     {"type": "string"},
        "why_info":      {"type": "string"},
        "how_info":      {"type": "string"},
        "who_subject":   {"type": "string"},
        "who_affected":  {"type": "array", "items": {"type": "string"}},
        # The myFeed dossier panes — a causal timeline and the cross-domain read,
        # grounded in this one source. Both optional; empty is a valid answer and
        # renders as 'pending' rather than an error.
        "strings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "stage":  {"type": "string"},
                    "title":  {"type": "string"},
                    "detail": {"type": "string"},
                },
            },
        },
        "dots": {
            "type": "object",
            "properties": {
                "market_debt":       _DOT_SCHEMA_LC,
                "policy_governance": _DOT_SCHEMA_LC,
                "tech_culture":      _DOT_SCHEMA_LC,
                "asymmetric_catch":  {"type": "object",
                                      "properties": {"summary": {"type": "string"}}},
            },
        },
        # The Telugu card — a TRANSLATION of the three fields above, asked for in
        # the same request so it costs no extra provider call (telugu.py). Always
        # optional: a model that cannot translate omits it and the row simply has
        # no Telugu copy.
        "telugu": {
            "type": "object",
            "properties": {
                "refined_title": {"type": "string"},
                "summary":       {"type": "string"},
                "full_body":     {"type": "string"},
            },
        },
    },
    "required": ["refined_title", "summary", "full_body", "category"]
}

SYSTEM_INSTRUCTION = """You are SherByte's senior news editor for an Indian audience.
Transform raw news into polished, structured content. Your output feeds directly into a mobile news app.

VOICE: write like a sharp human journalist, not a wire aggregator. Lead with the
single most consequential or genuinely surprising TRUE fact — the angle a reader
would miss from the source's own headline — so the opening earns the next line.
This is a HOOK made of facts, never of hype: it lives entirely inside rules 0 and
3 below. Curiosity is allowed; invention, teasing ("read on", "you won't
believe"), and any fact the source does not state are not.

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
   It must be YOUR headline, not the publisher's. Find the HOOK ANGLE — the stake,
   the tension, or the "why this matters" the publisher's own headline buries —
   and lead with THAT, not with the announcement's framing. Different word order
   is not enough: change the ANGLE. It must NOT be a substring of the source
   title, and must NOT share any run of 5 consecutive words with it.
   BAD  (restates the announcement): "Halle Berry, Regina King and Meg Ryan Named Jurors for Tribeca-Chanel Program"
   GOOD (leads with the stake):       "Three Oscar winners will pick Hollywood's next women directors"
   BAD: "Breaking: Big News About Tech Company"
   GOOD: "Nvidia's record quarter just reset the AI-chip race"
   Never use prefixes like "Breaking:", "Exclusive:", "Headline:", "Watch:", "Just In:".

2. summary — EXACTLY 2 factual sentences totaling 40-55 words.
   - MUST NOT begin with or restate the title.
   - Sentence 1: the HOOK — the most consequential or surprising true fact of
     what specifically happened, told as a journalist would open the story.
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

10. strings — a SHORT causal timeline of THIS story, as an array of 2-4 steps,
    each {stage, title, detail}. Same originality + no-invention rules as the
    body: build it ONLY from facts the source states.
    - stage: one of "Origin", "Background", "Escalation", "The Spark", "Present",
      "What's next" — pick what fits each step.
    - title: 2-5 words naming the step.
    - detail: ONE past-tense (or, for the last step, present/future) sentence of
      what happened, in your own words.
    - Order oldest → newest; the final step is the present event.
    - If the source gives no prior context, return FEWER steps (even just one
      "Present" step). NEVER invent history, causes, or a future the source does
      not state. An empty array [] is a valid answer.

11. dots — the cross-domain read of THIS story, an object with these OPTIONAL keys:
    "market_debt", "policy_governance", "tech_culture" — each {summary (one
    sentence), impact ("up"|"down"|"mixed"|"neutral"), instruments (array of
    {name, ticker}, only if a real, named instrument is involved)} — and
    "asymmetric_catch" {summary} — the angle mainstream coverage tends to miss.
    - Include a sector ONLY if the story genuinely touches it AND the source
      supports the claim. Most stories touch one or two; OMIT the rest.
    - NEVER fabricate a market/instrument impact for a story with no market angle
      (an arts or sports story usually has none — leave market_debt out).
    - Factual, compliant, no speculation about prices or direction beyond what the
      source states. An empty object {} is a valid answer.

Output the JSON object only. No markdown. No commentary."""


# ─── FULL-ARTICLE mode: read the report, rebuild it as WWWH, write it fresh ──
# When article_reader managed to read the publisher's whole article, the model is
# no longer squeezing a 40-word blurb — it has the facts. The owner's spec
# (sbb.pdf) for how a SherrByte article is written:
#
#   What happened      [subject -> object]
#   Where & when       [location & time]
#   Why it happened    [reason / cause]
#   How it happened    [process / sequence]
#
# That skeleton is extracted first, then the body is written FROM the skeleton,
# descriptively, with a human hook — "the way people like to read" — and never
# with the publisher's sentences.
FULL_TEXT_MIN_WORDS = 120

_FULL_BODY_RULE = """3. full_body — 110-170 words, 2 or 3 short paragraphs separated by a blank line.

   YOU ARE WORKING FROM THE PUBLISHER'S FULL ARTICLE. Read all of it first.
   Then build the WWWH skeleton (rules 12-16) and write the body FROM THAT
   SKELETON, not from the article's sentences.

   - Paragraph 1 — THE HOOK + WHAT HAPPENED: open with the single most
     human, consequential or surprising true fact (a person, a number, a
     stake), then say plainly who did what to whom, where and when.
   - Paragraph 2 — WHY and HOW: the cause or reason the article gives, and
     the sequence of how it unfolded. If the article gives no reason, say
     what is known and do not guess.
   - Optional paragraph 3 — what it means now or what comes next, ONLY if
     the article states it.
   - Plain, warm, specific English for an Indian reader. Short sentences.
     Explain any jargon in a few words. No hype words ("shocking",
     "massive"), no teasing, no rhetorical questions, no markdown.
   - ABSTRACTIVE, never extractive: new sentences, new order, your words.
     Do not keep the article's sentence structure with swapped synonyms.
   - NEVER INVENT. Every name, number, date and place must be in the article."""

_WWWH_RULES = """

12. what_info — WHAT HAPPENED, as ONE sentence in the form subject -> action ->
    object, in your own words. Example: "A jury led by Ira Sachs gave Mike
    Leigh's film three top awards."
13. who_subject — the main actor (person/organisation) who did it. "" if none.
14. who_affected — array of the people/organisations/places it was done to or
    that it affects, as named in the article. [] if none.
15. why_info — WHY IT HAPPENED: the reason or cause the article states, one
    sentence. "" if the article does not state one — never guess a motive.
16. how_info — HOW IT HAPPENED: the process or sequence of events, one or two
    sentences, oldest step first. "" if the article does not describe one.

when_info and where_info (rules 8-9) are the WHERE & WHEN of the skeleton — fill
them from the article whenever it states them."""


def _build_full_instruction(base: str) -> str:
    start = base.index("3. full_body")
    end = base.index("4. category")
    head = base[:start] + _FULL_BODY_RULE + "\n\n" + base[end:]
    closing = "Output the JSON object only."
    i = head.rindex(closing)
    return head[:i].rstrip() + _WWWH_RULES + "\n\n" + head[i:]


SYSTEM_INSTRUCTION_FULL = _build_full_instruction(SYSTEM_INSTRUCTION)


# ─── RESEARCH mode: write from the evidence brief, like an analyst ───────────
# Adopted 2026-10-15. The cards read like a wire copy because the writer's whole
# input was the publisher's own page: everything it could add was either a
# paraphrase (rejected by the originality gate) or an invention (rejected by the
# writer gate). `research.build_brief` supplies what the page does not carry —
# our own earlier coverage of the same entities, and the instrument's MEASURED
# move against its own normal day — and this instruction is how that brief
# becomes prose.
#
# READER-FRIENDLY, NOT A DESK NOTE (the product owner's call): plain English,
# three short paragraphs, every number already verified upstream. The model does
# not reason about prices and never sees a raw series; it uses the numbers the
# brief hands it, in the past tense, or it uses none.
_RESEARCH_BODY_RULE = """3. full_body — 130-180 words, exactly 3 short paragraphs separated by a blank
   line. You are writing as a market and geopolitics analyst explaining the
   story to an ordinary reader, NOT as a wire service and NOT as a trading desk.

   A RESEARCH BRIEF is appended after the article body, between
   "=== SHERR RESEARCH BRIEF" and "=== END BRIEF ===". Everything in it is
   already verified against our own corpus and our own price store. It is the
   ONLY source of any figure you did not find in the article itself.

   - Paragraph 1 — WHAT HAPPENED: the consequential fact in plain words, who
     did what to whom, where and when. Lead with the most human or most
     consequential true detail. Include the single number that matters most, if
     there is one.
   - Paragraph 2 — WHAT CAME BEFORE, AND WHY IT MATTERS: use OUR EARLIER
     COVERAGE from the brief to say how this fits what already happened, then
     the mechanism — who is affected and through what, named specifically. If
     the brief lists no earlier coverage, give the article's own context and do
     not invent a history.
   - Paragraph 3 — WHAT WAS MEASURED, AND WHAT TO WATCH: if the brief gives a
     MEASURED MARKET MOVE, state it in the past tense, with the instrument
     named and the comparison the brief makes ("beyond"/"inside its normal
     range"). If the brief gives none, say plainly what has not moved or is not
     measured here, and never imply a move. Close with what WOULD show the
     story developing — a condition, stated conditionally.

   HARD RULES for this mode, on top of rule 0:
   - NEVER state a figure that is not in the article or in the brief. Not one.
   - NEVER say a market or price will do anything. No direction claim about the
     future, no target, no probability, no "expected to". Past tense and
     conditional only.
   - NEVER present an instrument the brief lists as "NAMED but NOT PRICED" as
     having moved.
   - Do not restate our earlier coverage as if it happened now.
   - Plain, warm, specific English for an Indian reader. Short sentences.
     Explain any jargon in a few words. No hype, no teasing, no rhetorical
     questions, no markdown, no bullet points inside the body.

   COMPLIANCE — a market story is screened by the SAME blocklist the engine
   applies to its own cards, and ONE hit withholds the whole card. These words
   are ordinary in market journalism and are still forbidden here, anywhere in
   the text, quotes included:
   - advice words: buy, sell, hold, invest, recommend, advice, target,
     allocate, allocation, portfolio, position, entry, exit, stop loss,
     upside, downside, undervalued, overvalued, bullish, bearish, rally,
     correction, opportunity
   - forward-looking words: will, should, expect, expected to, likely to,
     going to, set to, poised to, predict, prediction, forecast
   - causal shortcuts: causes, caused, causing, due to, impact on
   The check is on WHOLE WORDS, so "hold" is blocked while "held" is not — but
   do not go looking for the loophole: write "stayed above" rather than "hold
   above", and use rose / fell / was flat for a move. Use "after" or "following" where you would write "due to". State a
   reason as something a named party said ("ministers pointed to weaker
   demand"), not as a cause you assert.
   NEVER write a figure with a currency symbol or code — "$90", "Rs 7,500",
   "INR 90" each read as a price target and are blocked. Write the number with
   its unit in words instead: "about 90 dollars a barrel", "roughly 7,500
   rupees a gram"."""


def _build_research_instruction(base: str) -> str:
    """The FULL instruction with its body rule swapped for the Research one.

    Built FROM the full instruction rather than written again, so the originality
    rule, the category list, the entity rules and the strings/dots contract have
    exactly one definition. A second copy is a second thing to forget to update.
    """
    start = base.index("3. full_body")
    end = base.index("4. category")
    return base[:start] + _RESEARCH_BODY_RULE + "\n\n" + base[end:]


SYSTEM_INSTRUCTION_RESEARCH = _build_research_instruction(SYSTEM_INSTRUCTION_FULL)


# ─── TELUGU: one more field, never one more request ──────────────────────────
# Adopted 2026-10-15. The product owner asked for English by default and Telugu
# "if possible". The only affordable shape is a translation in the SAME request
# that writes the story — a second call would halve the drain's throughput
# against a 15-request-a-minute ceiling (see telugu.py for the full reasoning,
# including why the compliance gate can only be applied to the English half).
#
# Appended to the finished instruction rather than numbered into it, so the
# blurb, full-article and Research prompts all carry it without renumbering the
# WWWH rules that are inserted at different points in each.
_TELUGU_RULE = """

TELUGU — one extra field, and it is a TRANSLATION, not a second article.

18. telugu — an object {refined_title, summary, full_body} carrying rules 1, 2
    and 3 above IN TELUGU. Omit the field entirely if you cannot do this well;
    an omitted field is a correct answer and costs the card nothing.
    - TRANSLATE WHAT YOU WROTE. Do not write a different story, do not add a
      fact, a figure, a name or a sentence that is not in your English answer,
      and do not drop a paragraph. Same meaning, same order, same tense.
    - EVERY FIGURE MUST ALREADY BE IN YOUR ENGLISH ANSWER. A number that appears
      only in the Telugu is treated as invented and the translation is dropped.
    - Write in TELUGU SCRIPT. Keep proper nouns and tickers in their usual form
      (Brent, Nifty 50, OPEC+, USDINR) rather than transliterating them.
    - Natural newspaper Telugu for an ordinary reader in the Telugu states, not
      a word-for-word rendering of the English and not Sanskritised prose. Where
      an English financial term has no everyday Telugu word, keep the English
      word in Latin script and explain it in two or three Telugu words.
    - The SAME compliance rules apply, in both languages: no advice, no buy or
      sell, no recommendation, no target, no forecast or prediction, and no
      figure next to a currency symbol or code. Keep the past tense and the
      conditional of the English; never translate a measured past move into
      something that reads as what will happen next."""


def with_telugu(instruction: str) -> str:
    """The instruction with the Telugu rule inserted before its closing line.

    Read at CALL time, never captured at import: TELUGU_ENABLED has to be able
    to turn the field off without a reload, and this repo has already been
    bitten by module-level configuration that other modules hold references to.
    """
    if not telugu.enabled():
        return instruction
    closing = "Output the JSON object only."
    i = instruction.rfind(closing)
    if i < 0:
        return instruction + _TELUGU_RULE
    return instruction[:i].rstrip() + _TELUGU_RULE + "\n\n" + instruction[i:]


def is_full_text(body: str) -> bool:
    return len((body or "").split()) >= FULL_TEXT_MIN_WORDS


RESEARCH_MODE = "research"


def instruction_for(body: str, mode: str = None) -> str:
    """The full-article (WWWH) prompt when we hold the article, else the blurb one."""
    if mode == RESEARCH_MODE:
        return with_telugu(SYSTEM_INSTRUCTION_RESEARCH)
    return with_telugu(SYSTEM_INSTRUCTION_FULL if is_full_text(body)
                       else SYSTEM_INSTRUCTION)


def _out_tokens(body: str, mode: str = None, *, gemini: bool = False) -> int:
    """The output ceiling, raised when the request also asks for Telugu."""
    long_form = is_full_text(body) or mode == RESEARCH_MODE
    base = (2048 if gemini else 1600) if long_form else (1024 if gemini else 900)
    return int(base * 1.8) if telugu.enabled() else base


def body_limit(body: str) -> int:
    return 7000 if is_full_text(body) else 2500


# ─── provider calls ───────────────────────────────────────────────────────────
# Each takes an explicit key and returns (result, status):
#   result — the parsed dict, or None
#   status — the HTTP status, so the pool wrapper can tell "this key is rate
#            limited" (rotate) from "the service is down" (do not rotate, the
#            next key would fail identically and spend the pool for nothing).

async def _gemini_once(key: str, title: str, body: str, client, *,
                       mode: str = None) -> tuple:
    # model_for, not the boot-time constant: the audit reports model_for's
    # answer, and the request must use the same one or the two disagree.
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model_for('gemini')}:generateContent?key={key}")
    payload = {
        "systemInstruction": {"parts": [{"text": instruction_for(body, mode)}]},
        "contents": [{
            "role": "user",
            "parts": [{"text": f"ARTICLE TITLE: {title}\n\nARTICLE BODY: {body[:body_limit(body)]}"}]
        }],
        "generationConfig": {
            "temperature": 0.35,
            # A translated card is close to twice the output, and a response
            # cut off mid-JSON loses the ENGLISH card too — so the ceiling moves
            # with the field rather than hoping the answer fits.
            "maxOutputTokens": _out_tokens(body, mode, gemini=True),
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


def _retry_after_seconds(resp) -> Optional[float]:
    """Seconds to wait before retrying a 429, from the provider's own headers.

    Groq (and the OpenAI shape generally) return `retry-after` and/or
    `x-ratelimit-reset-*` when a request is rate limited — usually the free-tier
    tokens-per-minute cap. Honouring it turns a 429 from a lost rewrite into a
    slightly slower one."""
    try:
        for h in ("retry-after", "x-ratelimit-reset-tokens", "x-ratelimit-reset-requests"):
            v = resp.headers.get(h)
            if not v:
                continue
            v = v.strip().lower().rstrip("s")
            try:
                return float(v)
            except ValueError:
                # e.g. "1m30" style — fall through to the default backoff
                pass
    except Exception:
        pass
    return None


async def _openai_chat_once(key: str, title: str, body: str, client, *,
                            url: str, model: str, label: str,
                            mode: str = None) -> tuple:
    """Groq, OpenAI and Grok all speak the OpenAI chat-completions shape, so they
    share one caller and differ only by endpoint, model and key.

    A 429 is RETRIED here (honouring Retry-After), not surfaced immediately: on a
    free tier — Groq especially — the tokens-per-minute cap bounces bursts, and
    without this each bounce fell straight back to the placeholder stub (the feed
    then never got an original body). Up to 4 attempts, capped waits."""
    prompt = instruction_for(body, mode) + f"""

ARTICLE TITLE: {title}

ARTICLE BODY: {body[:body_limit(body) if (is_full_text(body) or mode == RESEARCH_MODE) else 2000]}

Return ONLY a single JSON object matching the schema. No markdown, no code fences."""
    payload = {"model": model,
               "messages": [{"role": "user", "content": prompt}],
               "temperature": 0.35,
               "max_tokens": _out_tokens(body, mode),
               "response_format": {"type": "json_object"}}
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    for attempt in range(4):
        try:
            r = await client.post(url, headers=headers, json=payload, timeout=30)
            if r.status_code == 429:
                _record_error(label, 429, r.text)
                if attempt < 3:
                    wait = _retry_after_seconds(r)
                    if wait is None:
                        wait = 4 * (2 ** attempt)          # 4, 8, 16s
                    await asyncio.sleep(min(wait, 65))
                    continue
                return None, 429
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
    return None, 429


# provider -> a coroutine (key, title, body, client) -> (result, status)
_PROVIDER_CALLS = {
    "gemini": _gemini_once,
    "groq":   lambda k, t, b, c, **kw: _openai_chat_once(
        k, t, b, c, url="https://api.groq.com/openai/v1/chat/completions",
        model=model_for("groq"), label="Groq", **kw),
    "openai": lambda k, t, b, c, **kw: _openai_chat_once(
        k, t, b, c, url="https://api.openai.com/v1/chat/completions",
        model=model_for("openai"), label="OpenAI", **kw),
    # xAI, NOT Groq. The code used to read GROK_API_KEY and post it to
    # api.groq.com, so whichever of the two keys existed, one provider was
    # always being called with the other's credential.
    "grok":   lambda k, t, b, c, **kw: _openai_chat_once(
        k, t, b, c, url="https://api.x.ai/v1/chat/completions",
        model=model_for("grok"), label="Grok", **kw),
}


async def _call_provider(provider: str, title: str, body: str, client,
                         mode: str = None):
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
        # mode is passed ONLY when set. A test that monkeypatches
        # _PROVIDER_CALLS[provider] with the documented 4-argument lambda keeps
        # working; adding an unconditional kwarg would break every one of them.
        if mode:
            result, status = await fn(key, title, body, client, mode=mode)
        else:
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


async def _call_cascade(title: str, body: str, client, mode: str = None):
    """Every configured provider in order, each with its own key rotation."""
    for provider in KEYS.configured():
        result = await _call_provider(provider, title, body, client, mode)
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
    for k in ("what_info", "why_info", "how_info", "who_subject"):
        v = result.get(k)
        result[k] = v.strip() if isinstance(v, str) else ""
    wa = result.get("who_affected")
    if isinstance(wa, str):
        wa = [wa] if wa.strip() else []
    result["who_affected"] = [str(x).strip() for x in (wa or [])
                              if isinstance(x, (str, int, float)) and str(x).strip()][:8]

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
                # Per-item, because one drain tick mixes Research rows with
                # ordinary ones and they must not share an instruction.
                mode     = article.get("mode") or None
                try:
                    r = await _call_cascade(title, body, client, mode)
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


# ─── MULTI-SOURCE SYNTHESIS ──────────────────────────────────────────────────
# A separate call path from process_article, on purpose. That one takes ONE
# article and asks for a rewrite; this takes a cluster of articles about one
# event and asks for a new briefing written from the facts they agree on.
#
# It shares the key pools and the 401/403/429 rotation with everything else here
# — one quota, one rotation policy — but nothing else. In particular it does NOT
# fall back to _rule_based_fallback: the rule-based path writes a placeholder,
# and a placeholder returned from a synthesis call would be written to every row
# in the cluster at once. A failed synthesis returns None and the caller leaves
# the rows alone for the next tick.

async def _gemini_synth_once(key: str, prompt: str, client) -> tuple:
    import synthesis                                             # noqa: PLC0415
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model_for('gemini')}:generateContent?key={key}")
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            # Lower than the rewrite's 0.35. Synthesis is a factual merge, and
            # temperature is where invented detail comes from.
            "temperature": 0.2,
            "maxOutputTokens": 1024,
            "responseMimeType": "application/json",
            "responseSchema": synthesis.SYNTHESIS_SCHEMA,
        },
    }
    try:
        r = await client.post(url, json=payload, timeout=45)
        if r.status_code != 200:
            _record_error("gemini", r.status_code, r.text)
            return None, r.status_code
        candidates = (r.json() or {}).get("candidates", [])
        if not candidates:
            _record_error("gemini", 200, f"no candidates: {r.text[:300]}")
            return None, 200
        parts = candidates[0].get("content", {}).get("parts", [])
        if not parts:
            return None, 200
        return parts[0].get("text", "").strip(), 200
    except Exception as e:                                        # noqa: BLE001
        _record_error("gemini", 0, f"{type(e).__name__}: {e}")
        return None, 0


async def _openai_synth_once(key: str, prompt: str, client, *,
                             url: str, model: str, label: str) -> tuple:
    try:
        r = await client.post(
            url,
            headers={"Authorization": f"Bearer {key}",
                     "Content-Type": "application/json"},
            json={"model": model,
                  "messages": [{"role": "user", "content": prompt}],
                  "temperature": 0.2,
                  "max_tokens": 900,
                  "response_format": {"type": "json_object"}},
            timeout=45,
        )
        if r.status_code != 200:
            _record_error(label, r.status_code, r.text)
            return None, r.status_code
        return (r.json()["choices"][0]["message"]["content"] or "").strip(), 200
    except Exception as e:                                        # noqa: BLE001
        _record_error(label, 0, f"{type(e).__name__}: {e}")
        return None, 0


_SYNTH_CALLS = {
    "gemini": _gemini_synth_once,
    "groq":   lambda k, p, c: _openai_synth_once(
        k, p, c, url="https://api.groq.com/openai/v1/chat/completions",
        model=model_for("groq"), label="Groq"),
    "openai": lambda k, p, c: _openai_synth_once(
        k, p, c, url="https://api.openai.com/v1/chat/completions",
        model=model_for("openai"), label="OpenAI"),
    "grok":   lambda k, p, c: _openai_synth_once(
        k, p, c, url="https://api.x.ai/v1/chat/completions",
        model=model_for("grok"), label="Grok"),
}


_HOOK_CHECK = None
_HOOK_CHECK_LOADED = False


def _hook_check():
    """The compliance blocklist the News-node hook is screened against.

    ONE BLOCKLIST, not a second copy (CLAUDE.md): this reuses the engine's
    `narrative.violates_language_rules` rather than re-listing the banned words
    here. The engine lives under sherrbyte/ so its package root is added to the
    path the same way the market backfills reach it. Resolved once and cached; if
    the engine cannot be imported the hook is left unscreened rather than the
    whole synthesis failing — but the import is in-repo, so that is a last resort.
    """
    global _HOOK_CHECK, _HOOK_CHECK_LOADED
    if _HOOK_CHECK_LOADED:
        return _HOOK_CHECK
    _HOOK_CHECK_LOADED = True
    try:
        import sys                                               # noqa: PLC0415
        engine = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "sherrbyte")
        if engine not in sys.path:
            sys.path.insert(0, engine)
        from app.spie.reasoning.narrative import (               # noqa: PLC0415
            violates_language_rules)
        _HOOK_CHECK = violates_language_rules
    except Exception as e:                                       # noqa: BLE001
        log.warning("hook language check unavailable, hooks left unscreened: %s", e)
        _HOOK_CHECK = None
    return _HOOK_CHECK


async def synthesize(prompt: str, *, n_sources: int = 0) -> Optional[dict]:
    """One synthesis call for one cluster. Returns the validated dict, or None.

    ONE CLUSTER IS ONE REQUEST — that is the unit the drain's rate limit counts,
    and it is why synthesis is cheaper per article than the single rewrite: five
    articles that used to cost five requests now cost one.
    """
    import synthesis                                             # noqa: PLC0415
    hook_check = _hook_check()
    async with httpx.AsyncClient() as client:
        for provider in KEYS.configured():
            fn = _SYNTH_CALLS.get(provider)
            if fn is None:
                continue
            pool = KEYS.get(provider)
            if not pool.size:
                continue
            pool.reset()
            for _ in range(pool.size):
                key = pool.current()
                if not key:
                    break
                text, status = await fn(key, prompt, client)
                if text:
                    try:
                        return synthesis.parse_synthesis(
                            text, n_sources=n_sources, hook_check=hook_check)
                    except synthesis.SynthesisRejected as e:
                        # A rejected answer is a provider failure, recorded like
                        # one so /admin/body-audit can show why nothing was
                        # written instead of reporting a silent zero.
                        _record_error(provider, 200, f"synthesis rejected: {e}")
                        return None
                if status in key_pool.ROTATE_STATUSES:
                    if not pool.rotate(f"HTTP {status}"):
                        break
                    continue
                break
    return None


# ─── SECTION 1: THE NEWS WRITER (SHERR_WRITING_SPEC.md) ───────────────────────
# A separate writer from process_article. That one is the general rewrite that
# produces the whole card (title, summary, category, tags, strings, dots). This
# one implements Section 1 of SHERR_WRITING_SPEC.md exactly: it returns the
# structured NEWS contract (headline / body / why_it_matters / numbers_used /
# entities / primary_source_attribution) that the Section 4 quality gate scores
# before publish. Its why_it_matters is what the card's why-it-matters line
# renders from.
#
# The prompt below is the spec's Section 1.3 text, VERBATIM — the spec is
# binding and says "Use it as-is." Do not paraphrase it; adjust the wording in
# the spec, never here.

# writer_id = model name plus prompt version (spec 1.4). Bumping the prompt bumps
# this, so /admin/writer-doctor can tell one prompt generation's failures from
# another's.
NEWS_PROMPT_VERSION = "news-v1"

NEWS_PROMPT = """You are the SherrByte NEWS writer. You receive raw source text about a
real-world event. You produce an original, factual restatement of that event
in SherrByte's own words.

ABSOLUTE RULES

1. Never reproduce a sentence, clause or distinctive phrase from the source.
   Read for facts, then write from scratch. Matching wording is a failure
   even when the meaning is correct.
2. Never state a fact the source does not contain. No background you know
   from elsewhere, no inferred figures, no rounded-up numbers, no names the
   source does not name.
3. Never predict, forecast, advise or evaluate. Describe what happened and
   what is verifiably in effect. Do not say what will happen next, and do not
   characterise anything as good, bad, strong or weak.
4. Never use editorial adjectives. Write as a wire service writes: flat,
   specific, unhurried.
5. If the source contains conflicting figures or accounts, state the conflict
   in the body rather than choosing one.

WHAT TO WRITE

headline: 6-14 words stating the event. No questions, no teases, no colons.
Do not reuse more than four consecutive words from the source headline.

body: 60-80 words, at least three sentences.
  Sentence 1 - what happened, who, when, where.
  Sentence 2 - the mechanism: how it happened, or how it takes effect.
  Sentence 3 - context or scale: the figure, comparison or timeframe that
  tells a reader how large this is. This sentence must contain something
  concrete, not a generality.

why_it_matters: one sentence, 12-28 words, restating sentence 3 of the body
as significance. Introduce nothing new. Do not speculate about consequences.

numbers_used: for every number appearing in your headline, body or
why_it_matters, give an object with the value, its unit, and source_span -
an exact substring copied from the source text where that number appears.
This is the only place you may copy source text, and it is never published.
If you used no numbers, return an empty array.

entities: the subject (primary actor) and affected (other named parties).
Named entities only.

primary_source_attribution: the publication these facts came from.

OUTPUT

Return one JSON object and nothing else. No markdown fences, no explanation.

{
  "headline": "",
  "body": "",
  "why_it_matters": "",
  "numbers_used": [],
  "entities": {"subject": "", "affected": []},
  "primary_source_attribution": ""
}

SOURCE TEXT:
"""

# Structured-output schema for Gemini — guarantees the shape parses. The gate's
# G7_SHAPE still runs (a provider without structured output, or an empty field,
# is still a failure), so this is an aid, not the check.
_NEWS_SCHEMA = {
    "type": "object",
    "properties": {
        "headline":       {"type": "string"},
        "body":           {"type": "string"},
        "why_it_matters": {"type": "string"},
        "numbers_used": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "value":       {"type": "string"},
                    "unit":        {"type": "string"},
                    "source_span": {"type": "string"},
                },
            },
        },
        "entities": {
            "type": "object",
            "properties": {
                "subject":  {"type": "string"},
                "affected": {"type": "array", "items": {"type": "string"}},
            },
        },
        "primary_source_attribution": {"type": "string"},
    },
    "required": ["headline", "body", "why_it_matters", "entities",
                 "primary_source_attribution"],
}


def news_writer_id(provider: str) -> str:
    """model name plus prompt version, e.g. gemini-3.1-flash-lite/news-v1."""
    return f"{model_for(provider)}/{NEWS_PROMPT_VERSION}"


def _news_corrective_block(failures: list) -> str:
    """The corrective context appended on a regeneration (spec 4.3).

    Additive — it does not alter the verbatim prompt above. It hands the writer
    the failed rule IDs and their reasons so the second attempt can fix exactly
    what tripped, rather than rolling the dice again.
    """
    if not failures:
        return ""
    lines = "\n".join(
        f"- {f.get('rule')}: {f.get('reason')}" for f in failures if f.get("rule"))
    return ("\n\nYOUR PREVIOUS ATTEMPT FAILED THESE CHECKS. Return corrected JSON "
            "that fixes every one of them, keeping all the rules above:\n" + lines
            + "\n")


async def _news_backoff(resp, attempt: int) -> None:
    """Exponential backoff with jitter on a 429, honouring Retry-After (spec 4.4)."""
    wait = _retry_after_seconds(resp)
    if wait is None:
        wait = 2.0 * (2 ** attempt)                    # 2, 4, 8, 16s
    wait = min(wait, 60.0)
    wait += random.uniform(0.0, wait * 0.25)           # jitter, so retries desync
    await asyncio.sleep(wait)


async def _news_gemini_once(key: str, prompt: str, client) -> tuple:
    url = (f"https://generativelanguage.googleapis.com/v1beta/models/"
           f"{model_for('gemini')}:generateContent?key={key}")
    payload = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            # Low temperature: this is a factual restatement, and temperature is
            # where invented figures come from.
            "temperature": 0.2,
            "maxOutputTokens": 1024,
            "responseMimeType": "application/json",
            "responseSchema": _NEWS_SCHEMA,
        },
    }
    for attempt in range(4):
        try:
            r = await client.post(url, json=payload, timeout=40)
            if r.status_code == 429:
                _record_error("gemini", 429, r.text)
                if attempt < 3:
                    await _news_backoff(r, attempt)
                    continue
                return None, 429
            if r.status_code != 200:
                _record_error("gemini", r.status_code, r.text)
                return None, r.status_code
            candidates = (r.json() or {}).get("candidates", [])
            if not candidates:
                _record_error("gemini", 200, f"no candidates: {r.text[:300]}")
                return None, 200
            parts = candidates[0].get("content", {}).get("parts", [])
            if not parts:
                return None, 200
            return json.loads(parts[0].get("text", "").strip()), 200
        except json.JSONDecodeError as e:
            _record_error("gemini", 200, f"JSONDecodeError: {e}")
            return None, 200
        except Exception as e:                                     # noqa: BLE001
            _record_error("gemini", 0, f"{type(e).__name__}: {e}")
            return None, 0
    return None, 429


async def _news_openai_once(key: str, prompt: str, client, *,
                            url: str, model: str, label: str) -> tuple:
    payload = {"model": model,
               "messages": [{"role": "user", "content": prompt}],
               "temperature": 0.2,
               "max_tokens": 1024,
               "response_format": {"type": "json_object"}}
    headers = {"Authorization": f"Bearer {key}", "Content-Type": "application/json"}
    for attempt in range(4):
        try:
            r = await client.post(url, headers=headers, json=payload, timeout=40)
            if r.status_code == 429:
                _record_error(label, 429, r.text)
                if attempt < 3:
                    await _news_backoff(r, attempt)
                    continue
                return None, 429
            if r.status_code != 200:
                _record_error(label, r.status_code, r.text)
                return None, r.status_code
            text = (r.json()["choices"][0]["message"]["content"] or "").strip()
            text = re.sub(r"^```(?:json)?\s*|\s*```$", "", text).strip()
            return json.loads(text), 200
        except Exception as e:                                     # noqa: BLE001
            _record_error(label, 0, f"{type(e).__name__}: {e}")
            return None, 0
    return None, 429


_NEWS_CALLS = {
    "gemini": _news_gemini_once,
    "groq":   lambda k, p, c: _news_openai_once(
        k, p, c, url="https://api.groq.com/openai/v1/chat/completions",
        model=model_for("groq"), label="Groq"),
    "openai": lambda k, p, c: _news_openai_once(
        k, p, c, url="https://api.openai.com/v1/chat/completions",
        model=model_for("openai"), label="OpenAI"),
    "grok":   lambda k, p, c: _news_openai_once(
        k, p, c, url="https://api.x.ai/v1/chat/completions",
        model=model_for("grok"), label="Grok"),
}


async def news_writer(source_text: str, *, corrective: Optional[list] = None,
                      client=None) -> tuple:
    """Run the Section 1 NEWS writer over one source text.

    Returns (result, writer_id): `result` is the parsed contract dict, or None on
    total failure (every provider/key exhausted). `writer_id` is model+prompt
    version (spec 1.4), returned even on failure so the gate can record which
    writer produced nothing.

    Shares the key pools and 401/403/429 rotation with the rest of ai_processor —
    one quota, one rotation policy. It does NOT fall back to a rule-based body: a
    failed NEWS write returns None and the caller handles the fallback (spec 4.3),
    because a placeholder returned here would be scored as if the model wrote it.
    """
    prompt = NEWS_PROMPT + (source_text or "")
    if corrective:
        prompt += _news_corrective_block(corrective)

    configured = KEYS.configured()
    primary = configured[0] if configured else "gemini"

    async def _run(cl):
        for provider in configured:
            fn = _NEWS_CALLS.get(provider)
            if fn is None:
                continue
            pool = KEYS.get(provider)
            if not pool.size:
                continue
            pool.reset()
            for _ in range(pool.size):
                key = pool.current()
                if not key:
                    break
                result, status = await fn(key, prompt, cl)
                if result is not None:
                    return result, news_writer_id(provider)
                if status in key_pool.ROTATE_STATUSES:
                    if not pool.rotate(f"HTTP {status}"):
                        break
                    continue
                break
        return None, news_writer_id(primary)

    if client is not None:
        return await _run(client)
    async with httpx.AsyncClient() as cl:
        return await _run(cl)
