"""
pipeline/understander.py — Stage 02: UNDERSTAND.

Turns a raw article into structured understanding:
  • NER          — named entities (spaCy if available, regex fallback).
  • WWWW         — Who / What / Where / When / Why (LLM, with rule fallback).
  • Topic + Pillar classification (LLM, with keyword-rule fallback).
  • Sentiment + scope.

The LLM path goes through the Gemini→Groq router; if no provider is configured
(or the call fails) we degrade gracefully to deterministic rules so the pipeline
never stalls.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field

from app.config import PILLAR_ALIASES, PILLARS, SLUG_TO_PILLAR, VALID_CATEGORY_SLUGS
from app.models.article import ArticleIn
from app.models.info_object import Entity, WWWW
from app.sherr.router import complete_json
from app.text_utils import clean_html_fragments, extract_sentences

log = logging.getLogger("sherbyte.understander")


@dataclass
class Understanding:
    headline: str
    summary: str
    body: str
    wwww: WWWW
    entities: list[Entity] = field(default_factory=list)
    topic: str = ""
    pillar_id: int = 1
    micro_tags: list[str] = field(default_factory=list)
    scope: str = "global"
    sentiment: str = "neutral"
    is_trending: bool = False


# ─── Rule-based classifier (fallback) ─────────────────────────────────────────
PILLAR_KEYWORDS: dict[int, list[str]] = {
    1: ["election", "parliament", "government", "minister", "senate", "vote", "democracy",
        "constitution", "treaty", "diplomat", "judiciary", "supreme court", "president",
        "prime minister", "cabinet", "lok sabha", "united nations", "nato", "sanctions",
        "military", "army", "defence", "protest", "coup", "policy",
        "terror", "terrorism", "uapa", "arrest", "arrested", "police", "crime",
        "murder", "attack", "killed", "court", "custody", "militant", "extremist",
        "smuggling", "raid", "riot", "border", "war", "espionage", "sedition"],
    2: ["stock market", "share price", "nifty", "sensex", "nasdaq", "bitcoin", "crypto",
        "ethereum", "blockchain", "startup", "venture capital", "ipo", "merger",
        "acquisition", "earnings", "inflation", "interest rate", "gdp", "recession",
        "rbi", "federal reserve", "budget", "gst", "bank", "fintech", "real estate",
        "net worth", "trillionaire", "billionaire", "market cap", "shares"],
    3: ["artificial intelligence", "machine learning", "chatgpt", "openai", "llm",
        "quantum computing", "robotics", "spacex", "isro", "nasa", "satellite",
        "cybersecurity", "data breach", "semiconductor", "electric vehicle", "nvidia",
        "software", "app update", "5g", "gene editing"],
    4: ["box office", "oscar", "grammy", "emmy", "music album", "concert", "netflix",
        "streaming", "art exhibition", "museum", "fashion week", "book launch",
        "bestseller", "k-pop", "bollywood", "hollywood", "anime", "film festival", "cannes"],
    5: ["climate change", "global warming", "carbon emissions", "wildlife", "endangered",
        "national park", "earthquake", "tsunami", "hurricane", "cyclone", "flood",
        "drought", "wildfire", "deforestation", "renewable energy", "biodiversity"],
    6: ["mental health", "depression", "anxiety", "therapy", "yoga", "meditation",
        "mindfulness", "weight loss", "obesity", "diet", "hospital", "doctor",
        "vaccine", "covid", "pandemic", "cancer", "diabetes", "surgery", "fitness"],
    7: ["philosophy", "buddhism", "hinduism", "christianity", "islam", "sikhism",
        "religion", "spirituality", "astrology", "temple", "church", "mosque",
        "mythology", "stoic", "ethics"],
    8: ["travel", "tourism", "hotel", "restaurant", "cuisine", "chef", "fashion trend",
        "celebrity", "dating app", "home decor", "remote work", "influencer", "content creator",
        "reality show", "reality tv", "reality star", "relationship", "gossip",
        "viral video", "social media star", "lifestyle", "fashion"],
    9: ["cricket", "ipl", "test match", "odi", "t20", "football", "fifa", "premier league",
        "champions league", "formula 1", "f1", "grand prix", "olympic", "world cup",
        "tennis", "wimbledon", "nba", "esports", "gaming", "wicket",
        "gta", "playstation", "xbox", "nintendo", "video game", "gameplay", "rockstar games"],
}

# Source-feed → pillar hints. Used as the default bucket when keyword matching
# finds nothing, so topic-specific feeds (e.g. "BBC Sport", "NYT Business") land
# in the right category instead of all defaulting to tech. Keys are matched as
# case-insensitive substrings of the source name.
_SOURCE_PILLAR_HINTS: list[tuple[str, int]] = [
    ("sport", 9), ("espn", 9), ("ign", 9), ("gamespot", 9),
    ("business", 2), ("moneycontrol", 2), ("economic times", 2), ("mint", 2),
    ("forbes", 2), ("fortune", 2),
    ("tech", 3), ("verge", 3), ("wired", 3), ("ars technica", 3), ("engadget", 3),
    ("gadgets", 3), ("science daily", 3), ("nasa", 3),
    ("health", 6), ("healthline", 6), ("medical", 6),
    ("arts", 4), ("culture", 4), ("variety", 4), ("deadline", 4), ("rolling stone", 4),
    ("science", 5), ("earthsky", 5), ("yale e360", 5),
    ("life", 8),
    ("world", 1), ("guardian", 1), ("jazeera", 1),
]


def _source_pillar(source_name: str) -> int:
    s = (source_name or "").lower()
    for needle, pid in _SOURCE_PILLAR_HINTS:
        if needle in s:
            return pid
    return 1  # neutral general-news bucket (society) rather than tech


_INDIA_WORDS = ["india", "delhi", "mumbai", "bangalore", "chennai", "hyderabad", "kolkata",
                "indian", "modi", "bjp", "congress", "rupee", "nifty", "sensex", "kerala", "tamil"]
_LOCAL_WORDS = ["city", "district", "local", "municipal", "village", "town", "ward", "panchayat"]
_GLOBAL_WORDS = ["world", "global", "international", "nato", "china", "russia", "europe",
                 "america", "washington", "beijing", "moscow", "london"]


def classify_pillar(title: str, body: str, source_name: str = "") -> tuple[int, list[str]]:
    text = f"{title} {body}".lower()
    scores = {pid: 0 for pid in PILLARS}
    tags: list[str] = []
    for pid, kws in PILLAR_KEYWORDS.items():
        for kw in kws:
            if kw in text:
                scores[pid] += 2
                tags.append(kw.title())
    best = max(scores, key=scores.get)
    if scores[best] == 0:
        best = _source_pillar(source_name)  # fall back to the feed's natural pillar
    return best, list(dict.fromkeys(tags))[:8]


def classify_scope(title: str, body: str) -> str:
    text = f"{title} {body}".lower()
    i = sum(1 for w in _INDIA_WORDS if w in text)
    l = sum(1 for w in _LOCAL_WORDS if w in text)
    g = sum(1 for w in _GLOBAL_WORDS if w in text)
    if l >= 2 and i > 0:
        return "local"
    return "national" if i > g else "global"


# ─── NER ──────────────────────────────────────────────────────────────────────
_NLP = None
_SPACY_TRIED = False


def _get_spacy():
    global _NLP, _SPACY_TRIED
    if _SPACY_TRIED:
        return _NLP
    _SPACY_TRIED = True
    try:
        import spacy
        _NLP = spacy.load("en_core_web_sm")
    except Exception as e:
        log.info("spaCy model unavailable, using regex NER fallback: %s", e)
        _NLP = None
    return _NLP


def extract_entities(text: str) -> list[Entity]:
    nlp = _get_spacy()
    if nlp is not None:
        doc = nlp(text[:5000])
        seen, out = set(), []
        for ent in doc.ents:
            key = ent.text.strip().lower()
            if key in seen or len(ent.text.strip()) < 2:
                continue
            seen.add(key)
            out.append(Entity(name=ent.text.strip(), type=ent.label_, canonical=ent.text.strip()))
        return out[:15]
    # Regex fallback: runs of Capitalized Words. Internal hyphens and apostrophes
    # are part of the token ("Spider-Man", "Coca-Cola", "O'Brien") — without this
    # the match stopped at the hyphen and emitted "Spider" + "Man" as two entities.
    _WORD = r"[A-Z][a-zA-Z]*(?:[-'’][A-Za-z]+)*"
    candidates = re.findall(rf"\b({_WORD}(?:\s+{_WORD}){{0,3}})\b", text)
    seen, out = set(), []
    for c in candidates:
        key = c.lower()
        if key in seen or len(c) < 3:
            continue
        seen.add(key)
        out.append(Entity(name=c, type="MISC", canonical=c))
    return out[:15]


# ─── LLM extraction schema ────────────────────────────────────────────────────
_UNDERSTAND_SCHEMA = {
    "type": "object",
    "properties": {
        "refined_title": {"type": "string"},
        "summary":       {"type": "string"},
        "who":           {"type": "string"},
        "what":          {"type": "string"},
        "where":         {"type": "string"},
        "when":          {"type": "string"},
        "why":           {"type": "string"},
        "category":      {"type": "string", "enum": VALID_CATEGORY_SLUGS},
        "topic_tags":    {"type": "array", "items": {"type": "string"}},
        "sentiment":     {"type": "string", "enum": ["positive", "neutral", "negative"]},
        "is_trending":   {"type": "boolean"},
    },
    "required": ["refined_title", "summary", "category"],
}

_UNDERSTAND_SYSTEM = """You are SherByte's news understanding engine for an Indian audience.
Extract a clean, structured understanding of the article. Rules:
- refined_title: <= 12 words, active voice, no "Breaking:"/"Watch:" prefixes.
- summary: exactly 2 factual sentences, 40-55 words, must NOT restate the title.
- who/what/where/when/why: short factual phrases ("" if genuinely absent).
- category: pick exactly one slug. Guide — society = governance, politics, crime,
  law, conflict, police, policy, public affairs; economy = business, markets,
  finance, trade; tech = the technology / science itself — gadgets, software,
  AI, apps, space science, research (NOT a tech company's finances); arts = films, TV & streaming
  shows, music, books, theatre, visual art and the creative industry (the works
  and their makers); nature = environment, climate, wildlife, space; selfwell =
  health, fitness, mental well-being; philo = philosophy, religion, spirituality,
  ethics (ideas only — NEVER news about crime, terror, politics or events);
  lifestyle = celebrity gossip & personal lives, relationships, reality-TV
  off-screen, fashion, food, travel, influencers, social-media trends; sports =
  sports & gaming.
  Crime / terror / political / legal / conflict stories are always 'society'.
  Celebrity gossip, relationships and reality-TV personal life are 'lifestyle',
  not 'arts' — 'arts' is for the creative works themselves.
  A tech company's wealth / stock / valuation / net-worth / markets story is
  'economy', not 'tech'. Video games & gaming (GTA, PlayStation, Xbox, Steam,
  esports) are 'sports', not 'tech'. Films, TV and celebrities are 'arts', not 'tech'.
- topic_tags: 2-5 specific proper nouns/concepts.
- is_trending: true only for major/record-breaking/national-or-global-impact events.
Output JSON only."""


# Hard-news markers that never appear in genuine philosophy/belief content — used
# to catch the LLM occasionally filing a crime/terror/political story under philo.
_HARD_NEWS_WORDS = (
    "terror", "terrorist", "uapa", "arrest", "police", "murder", "killed",
    "attack", "blast", "court", "custody", "militant", "extremist", "smuggl",
    "raid", "election", "parliament", "minister", "war", "troops", "border",
    "sanction", "riot", "crime", "sedition", "espionage",
)


def _looks_hard_news(text: str) -> bool:
    t = text.lower()
    return any(w in t for w in _HARD_NEWS_WORDS)


# "tech" is the most over-assigned bucket — the LLM drops a tech-company's
# wealth story, a video game, or an entertainment piece into it. These markers
# reroute those obvious cases. Gaming/wealth terms rarely occur in real
# science/technology coverage, so the override is safe.
_GAMING_WORDS = (
    "gta", "playstation", " xbox", "nintendo", "rockstar games", "video game",
    "gameplay", "esports", "steam deck", "game pass", "call of duty",
)
_WEALTH_WORDS = (
    "net worth", "trillionaire", "billionaire", "market cap", "valuation",
    "shares plunge", "shares fell", "stock rout", "wipeout", "richest",
)
_FILM_WORDS = (
    "box office", "movie", "actor", "actress", "casting", "james bond", "007",
    "hollywood", "bollywood", "trailer", "sequel", "film festival",
)


def _has(text: str, words: tuple) -> bool:
    t = text.lower()
    return any(w in t for w in words)


async def understand(article: ArticleIn) -> Understanding:
    """Produce structured understanding for one article. LLM-first, rule-fallback."""
    body = clean_html_fragments(article.body)
    entities = extract_entities(f"{article.title}. {body}")

    # Deterministic baseline (always computed; used directly if the LLM is off).
    rule_pillar, rule_tags = classify_pillar(article.title, body, article.source_name)
    scope = classify_scope(article.title, body)

    llm = await complete_json(
        system=_UNDERSTAND_SYSTEM,
        user=f"ARTICLE TITLE: {article.title}\n\nARTICLE BODY: {body[:2500]}",
        schema=_UNDERSTAND_SCHEMA,
        temperature=0.3,
    )

    if llm:
        slug = str(llm.get("category", "")).lower()
        pillar = SLUG_TO_PILLAR.get(slug) or PILLAR_ALIASES.get(slug, rule_pillar)
        blob = f"{article.title} {body[:500]}"
        # The LLM occasionally files a crime/terror/political story under
        # "Philosophy & Belief"; those markers never appear in real philosophy,
        # so anchor such stories back to Society & Governance.
        if pillar == 7 and _looks_hard_news(blob):
            pillar = 1
        # "tech" is over-assigned: reroute a video game to Sports & Gaming and a
        # tech-company wealth/markets story to Business & Economy.
        elif pillar == 3:
            if _has(blob, _GAMING_WORDS):
                pillar = 9
            elif _has(blob, _WEALTH_WORDS):
                pillar = 2
            elif _has(blob, _FILM_WORDS):
                pillar = 4
        tags = [str(t).strip() for t in (llm.get("topic_tags") or []) if str(t).strip()]
        tags = list(dict.fromkeys(tags + rule_tags))[:8]
        return Understanding(
            headline=(llm.get("refined_title") or article.title).strip(),
            summary=(llm.get("summary") or extract_sentences(body, 2)).strip(),
            body=body,
            wwww=WWWW(
                who=llm.get("who", ""), what=llm.get("what", ""),
                where=llm.get("where", ""), when=llm.get("when", ""),
                why=llm.get("why", ""),
            ),
            entities=entities,
            topic=(tags[0] if tags else PILLARS[pillar]["name"]),
            pillar_id=pillar,
            micro_tags=tags,
            scope=scope,
            sentiment=llm.get("sentiment", "neutral"),
            is_trending=bool(llm.get("is_trending", False)),
        )

    # Pure rule-based fallback.
    return Understanding(
        headline=article.title,
        summary=extract_sentences(body, 2) or article.title,
        body=body,
        wwww=WWWW(),
        entities=entities,
        topic=(rule_tags[0] if rule_tags else PILLARS[rule_pillar]["name"]),
        pillar_id=rule_pillar,
        micro_tags=rule_tags,
        scope=scope,
        sentiment="neutral",
        is_trending=False,
    )
