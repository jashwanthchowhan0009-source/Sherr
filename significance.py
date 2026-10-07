"""significance.py — is this story worth a SherrByte card?

The corpus ingests ~1,700 articles a day across ~110 feeds. The AI writer runs
on Gemini's free tier and can write a few hundred. Before this module the drain
took them newest-first, so the budget went to whatever arrived last: city crime,
match scores, celebrity casting, gaming guides — and the stories the product is
about (geopolitics, macro, markets, AI, space and science) were released raw by
publish_pending, with the publisher's headline repeated in every pane.

This scores each article 0-100 from what ingest already has (source, headline,
blurb, feed_class) so the scarce writer budget, and the myFeed surface, go to
what matters globally. It is:

  * PURE — no DB, no network, no model. Cheap enough to run on every row at
    ingest and on a backfill of tens of thousands of rows.
  * EXPLAINABLE — `score_article` returns the reasons with the number, so a
    surprising score is debuggable from /admin/significance without guessing.
  * CONSERVATIVE — it decides PRIORITY and myFeed eligibility. Nothing is
    deleted; a low-scoring story still lives in Explore.

The four signals, in order of weight:

  1. Source tier   — wires, world desks, regulators, financial and science press
                     carry consequential events far more often than city desks,
                     entertainment trade papers and gaming sites.
  2. Topic         — geopolitics, macro/central banks, markets and trade,
                     energy/commodities, AI/tech, space/science, defence, climate
                     policy.
  3. Big entities  — countries, multilateral bodies, central banks and
                     regulators, the largest companies and the AI labs.
  4. Junk penalty  — crime, accidents, local civic disputes, celebrity,
                     match reports, product deals, gaming guides, lifestyle.

Regulatory micro-filings (SEBI recovery certificates, adjudication orders
against one individual) are public record but not news a reader wants; they are
capped low here and belong to the filings pipeline (Sherr-I), not myFeed.
"""
from __future__ import annotations

import os
import re

# ─── thresholds (read at call time, never cached at import — see CLAUDE.md) ──

def feed_min() -> int:
    """Minimum score for a card to appear in myFeed."""
    return int(os.getenv("SIG_FEED_MIN", "42"))


def rewrite_min() -> int:
    """Minimum score for a story to spend AI-writer budget."""
    return int(os.getenv("SIG_REWRITE_MIN", os.getenv("SIG_FEED_MIN", "42")))


UNSCORED = -1      # column default; a row ingest has not scored yet


# ─── 1. source tiers ─────────────────────────────────────────────────────────
# Tier A: wires, world desks, regulators/central banks, financial and science
# press. Tier B: national dailies and general tech/business — mixed quality,
# the content decides. Tier C: entertainment, gaming, sport, lifestyle.
_TIER_A = {
    "ap news", "reuters india business", "bbc world", "bbc news", "bbc business",
    "bbc science", "al jazeera", "deutsche welle", "france 24", "nyt world",
    "nyt business", "cnn world", "abc international", "npr news", "the guardian",
    "guardian business", "guardian science", "foreign policy", "war on the rocks",
    "defense news", "breaking defense", "financial times", "economic times",
    "et markets", "mint", "mint markets", "business standard",
    "business standard markets", "moneycontrol", "moneycontrol markets",
    "moneycontrol business", "ndtv profit (bq)", "fortune", "oilprice.com",
    "mining.com", "carbon brief", "energy monitor", "nasa", "new scientist",
    "science daily", "mit technology review", "ieee spectrum", "nature",
    "rbi releases", "rbi notifications", "rbi speeches", "sebi releases",
    "sebi orders", "mca notices", "ibbi notices", "fed / fomc", "us treasury",
    "eia energy", "opec", "lbma metals", "bse announcements", "nse announcements",
}
_TIER_B = {
    "the hindu", "indian express", "times of india", "hindustan times", "ndtv",
    "forbes", "nyt tech", "bbc tech", "wired", "the verge", "techcrunch",
    "ars technica", "venturebeat", "engadget", "gadgets 360", "bleeping computer",
    "the hacker news", "krebs on security", "securityweek", "dark reading",
    "guardian climate", "inside climate news", "climate home news", "yale e360",
    "grist", "cleantechnica", "electrek", "renewable energy world",
    "the war zone", "the air current", "stat news", "earthsky", "bbc health",
    "nyt health",
}
_TIER_C = {
    "deadline", "variety", "rolling stone", "ign", "gamespot", "espn",
    "bbc sport", "guardian sport", "nyt sports", "ndtv sports", "bbc arts",
    "nyt arts", "guardian culture", "guardian life", "healthline",
    "medical news today", "simple flying", "the aviation geek",
    "sophos naked security", "sans isc",
}
_TIER_POINTS = {"A": 30, "B": 18, "C": 0, "?": 12}


def source_tier(source_name: str) -> str:
    s = (source_name or "").strip().lower()
    if s in _TIER_A:
        return "A"
    if s in _TIER_B:
        return "B"
    if s in _TIER_C:
        return "C"
    return "?"


# ─── 2. topics ───────────────────────────────────────────────────────────────
# Each topic is a set of whole-word / phrase patterns. A story earns the topic's
# points once, however many of its words match, so a keyword-stuffed blurb cannot
# run the score up. Two topics max count (geopolitics + markets is the typical
# combination of a story that matters).
_TOPICS: dict[str, tuple[int, tuple[str, ...]]] = {
    "geopolitics": (22, (
        r"summit", r"sanctions?", r"tariffs?", r"trade (?:deal|war|talks|pact)",
        r"ceasefire", r"invasion", r"diplomat\w*", r"foreign minister",
        r"prime minister", r"president", r"election", r"parliament",
        r"bilateral", r"treaty", r"embassy", r"geopolitic\w*", r"border (?:clash|dispute|talks)",
        r"war", r"missile", r"nuclear", r"military", r"troops", r"airstrikes?",
        r"refugees?", r"coup", r"annex\w*", r"strait of hormuz", r"red sea",
        r"strait", r"(?:drone|missile|rocket|terror|air|naval) (?:attacks?|strikes?)", r"attacks? on (?:its |the )?(?:airports?|refiner\w*|ships?|vessels?|tankers?|base)", r"drones?", r"houthis?", r"hamas",
        r"hezbollah", r"taliban", r"militants?", r"insurgen\w*", r"shipping lanes?",
        r"vessels?", r"tankers?", r"hostages?", r"genocide", r"envoy", r"juntas?",
        r"self-reliance", r"trade (?:with|between)", r"exports?", r"imports?",
    )),
    "macro": (22, (
        r"inflation", r"interest rates?", r"rate (?:cut|hike)", r"repo rate",
        r"monetary policy", r"central bank", r"gdp", r"recession", r"fiscal",
        r"budget deficit", r"current account", r"forex reserves?", r"bond yields?",
        r"treasury yields?", r"rupee", r"dollar index", r"currency", r"imf",
        r"world bank", r"unemployment", r"payrolls", r"cpi", r"wpi", r"pmi",
    )),
    "markets": (20, (
        r"sensex", r"nifty", r"nasdaq", r"s&p 500", r"dow jones", r"stock market",
        r"shares? (?:rose|fell|jumped|slid|surged|dropped)", r"ipo", r"earnings",
        r"quarterly (?:results|profit|revenue)", r"q[1-4] (?:results|profit|revenue)",
        r"market cap\w*", r"acquisition", r"merger", r"buyback", r"valuation",
        r"investors?", r"fii", r"fpi", r"mutual funds?", r"crypto\w*", r"bitcoin",
        r"stake", r"funding round", r"raises? \$",
    )),
    "energy_commodities": (18, (
        r"crude", r"brent", r"oil prices?", r"opec\+?", r"natural gas", r"lng",
        r"gold prices?", r"silver", r"copper", r"lithium", r"rare earths?",
        r"commodit\w*", r"refiner\w*", r"coal", r"power grid", r"electricity prices?",
        r"pipelines?", r"bpd", r"barrels?", r"oil fields?", r"nuclear power",
    )),
    "ai_tech": (24, (
        r"artificial intelligence", r"\bai\b", r"openai", r"anthropic",
        r"deepmind", r"large language model", r"\bllm\w*", r"chatgpt", r"gemini",
        r"semiconductors?", r"chips?", r"chipmakers?", r"gpus?", r"data cent(?:er|re)s?",
        r"quantum", r"mistral", r"deepseek", r"\bxai\b", r"grok", r"llama",
        r"claude", r"copilot", r"perplexity", r"hugging face", r"ai (?:models?|agents?|chips?|labs?|safety|regulation)",
        r"cyberattack", r"ransomware", r"data breach", r"antitrust",
    )),
    "space_science": (24, (
        r"isro", r"nasa", r"esa", r"spacex", r"rocket", r"launch(?:ed)? (?:a |the )?(?:satellite|mission)",
        r"satellite", r"orbit", r"moon", r"mars", r"asteroid", r"telescope",
        r"astronom\w*", r"discovery", r"scientists?", r"researchers? (?:found|discover)",
        r"study (?:finds|shows)", r"genome", r"vaccine", r"breakthrough",
        r"fusion", r"particle", r"species", r"nobel", r"physics", r"chemistry",
        r"neutrinos?", r"climate scientists?", r"peer-reviewed",
    )),
    "climate_policy": (14, (
        r"climate (?:change|deal|summit|policy|finance)", r"cop\d\d", r"emissions",
        r"carbon (?:tax|market|price|credits?)", r"net zero", r"renewables?",
        r"heatwave", r"cyclone", r"el ni[nñ]o", r"monsoon",
    )),
}
_MAX_TOPICS = 2


# ─── 3. big entities ─────────────────────────────────────────────────────────
_ENTITIES = (
    # multilateral + central banks + regulators
    r"brics", r"g7", r"g20", r"united nations", r"\bun\b", r"nato", r"wto", r"opec",
    r"european union", r"\beu\b", r"asean", r"quad", r"imf", r"world bank",
    r"federal reserve", r"\bfed\b", r"ecb", r"bank of japan", r"people'?s bank of china",
    r"\brbi\b", r"reserve bank", r"\bsebi\b", r"\bsec\b", r"finance ministry",
    # countries (the ones that move markets and headlines)
    r"india", r"china", r"united states", r"\bus\b", r"\bu\.s\.", r"russia",
    r"ukraine", r"israel", r"iran", r"saudi", r"uae", r"japan", r"germany",
    r"france", r"britain", r"\buk\b", r"pakistan", r"taiwan", r"north korea",
    r"south korea", r"brazil", r"turkey", r"gaza", r"qatar", r"egypt",
    # companies + labs
    r"apple", r"microsoft", r"nvidia", r"alphabet", r"google", r"amazon",
    r"meta", r"tesla", r"tsmc", r"samsung", r"intel", r"\bamd\b", r"openai",
    r"anthropic", r"reliance", r"tata", r"infosys", r"\btcs\b", r"adani",
    r"hdfc", r"icici", r"\bsbi\b", r"aramco", r"exxon", r"shell", r"jpmorgan",
    r"goldman", r"blackrock", r"berkshire", r"boeing", r"airbus",
)
_ENTITY_POINTS = 6
_ENTITY_CAP = 18


# ─── 4. junk ─────────────────────────────────────────────────────────────────
_JUNK: tuple[tuple[int, tuple[str, ...]], ...] = (
    # crime / accidents / local civic — the "local accident, dispute between
    # five people" class
    (30, (r"murder\w*", r"stabb\w*", r"robbery", r"theft", r"burglar\w*",
          r"molest\w*", r"assault\w*", r"kidnapp\w*", r"dowry", r"suicide",
          r"found dead", r"body found", r"road accident", r"accident",
          r"collided", r"hit-and-run", r"drown\w*", r"electrocut\w*",
          r"police (?:said|arrested|booked|registered)", r"arrested",
          r"booked for", r"fir (?:lodged|registered)", r"brawl", r"clash between",
          r"family dispute", r"neighbou?rs?", r"civic body", r"municipal",
          r"ward", r"potholes?", r"waterlogging", r"traffic (?:jam|snarl)",
          r"gram panchayat", r"village", r"residents of", r"water supply",
          r"power cut", r"tells? (?:the )?court", r"told (?:the )?court", r"jailed",
          r"sentenced", r"convicted", r"pleads? guilty", r"\bscam\b", r"pulled towards")),
    # entertainment / celebrity
    (30, (r"box office", r"trailer", r"teaser", r"casting", r"cast as",
          r"red carpet", r"celebrity", r"actor", r"actress", r"bollywood",
          r"hollywood", r"netflix series", r"season \d+", r"episode",
          r"album", r"concert", r"tour dates", r"dating", r"wedding",
          r"divorce", r"reality show", r"bigg boss")),
    # sport results
    (25, (r"\bvs\.?\b", r"match report", r"scored", r"goals?", r"wickets?",
          r"innings", r"semi-?final", r"quarter-?final", r"playoffs?",
          r"transfer (?:window|news)", r"fantasy (?:team|picks)", r"ipl",
          r"premier league", r"la liga", r"nba", r"nfl", r"grand slam", r"odis?",
          r"t20i?s?", r"test match", r"centur(?:y|ies) record", r"cricket\w*",
          r"tennis", r"olympic sports?", r"on court", r"\bopen exit")),
    # retail stock-tip spam — markets-flavoured but not events, and the
    # "buy/target/should you buy" framing is also outside the SEBI posture
    (35, (r"stocks? to buy", r"should you buy", r"buy rating", r"target price",
          r"share price live", r"live updates", r"stock market prediction",
          r"picks \d+ (?:\w+ )?stocks", r"\d+ stocks (?:to|that|for)",
          r"stocks in news", r"stocks to watch", r"multibagger", r"\d+-day (?:sma|ema)",
          r"technical (?:view|analysis)", r"intraday", r"penny stocks?")),
    # gaming
    (30, (r"xbox", r"playstation", r"\bps5\b", r"nintendo", r"video ?games?",
          r"gamers?", r"gaming", r"minecraft", r"fortnite", r"gta")),
    # shopping / lifestyle / how-to
    (30, (r"\bdeal\b", r"\bdeals\b", r"discount", r"\bsale\b", r"best .* to buy",
          r"price drop", r"review:", r"hands-on", r"unboxing", r"how to",
          r"walkthrough", r"guide:", r"tips for", r"recipe", r"horoscope",
          r"zodiac", r"weight loss", r"skincare", r"workout", r"wordle",
          r"crossword", r"quiz", r"gameplay", r"patch notes", r"easter egg",
          r"first impressions", r"launched in india", r"price in india",
          r"price,? (?:specifications|features)", r"\d+mah", r"\d+hz display")),
)

# Regulatory micro-filings: one entity, one order, boilerplate title.
_FILING_PATTERNS = (
    r"general remittance advice", r"recovery certificate", r"adjudication order",
    r"settlement order in respect of", r"order in the matter of",
    r"in respect of (?:mr|ms|m/s)\.?", r"\(pan:? ", r"exemption order",
    r"notice of attachment", r"release order",
)
_FILING_CAP = 20


def _compile(pats):
    return re.compile(r"\b(?:" + "|".join(pats) + r")\b", re.IGNORECASE)


_TOPIC_RX = {k: (pts, _compile(p)) for k, (pts, p) in _TOPICS.items()}
_ENTITY_RX = [re.compile(r"\b" + p + r"\b" if not p.startswith(r"\b") else p,
                         re.IGNORECASE) for p in _ENTITIES]
_JUNK_RX = [(pts, _compile(p)) for pts, p in _JUNK]
_FILING_RX = _compile(_FILING_PATTERNS)


def score_article(headline: str, summary: str = "", source_name: str = "",
                  feed_class: str = "general") -> tuple[int, dict]:
    """Score one story 0-100. Returns (score, reasons).

    The HEADLINE is weighted over the blurb: topics and junk are matched on the
    headline first; the blurb can add a topic the headline missed but cannot add
    a junk penalty on its own (blurbs mention "police" or "match" in passing far
    more often than the story is about them).
    """
    head = (headline or "").strip()
    blurb = (summary or "").strip()[:400]
    text = f"{head} {blurb}"
    reasons: dict = {}

    tier = source_tier(source_name)
    score = _TIER_POINTS[tier]
    reasons["tier"] = tier

    # topics — best two, headline or blurb
    hits = []
    for name, (pts, rx) in _TOPIC_RX.items():
        if rx.search(text):
            hits.append((pts, name))
    hits.sort(reverse=True)
    topics = hits[:_MAX_TOPICS]
    score += sum(p for p, _ in topics)
    reasons["topics"] = [n for _, n in topics]

    # entities — distinct matches, capped
    ents = 0
    for rx in _ENTITY_RX:
        if rx.search(text):
            ents += 1
    ent_pts = min(ents * _ENTITY_POINTS, _ENTITY_CAP)
    score += ent_pts
    reasons["entities"] = ents

    if (feed_class or "") == "financial":
        score += 8
        reasons["financial"] = True

    # junk — headline only, largest single penalty plus half of any second
    pens = sorted((pts for pts, rx in _JUNK_RX if rx.search(head)), reverse=True)
    if pens:
        penalty = pens[0] + (pens[1] // 2 if len(pens) > 1 else 0)
        score -= penalty
        reasons["junk_penalty"] = penalty

    # tier C with no hard-news topic stays out, whatever the entity count
    if tier == "C" and not topics:
        score = min(score, 20)
        reasons["tier_c_cap"] = True

    if _FILING_RX.search(head):
        score = min(score, _FILING_CAP)
        reasons["filing"] = True

    score = max(0, min(100, int(score)))
    return score, reasons


def score(headline: str, summary: str = "", source_name: str = "",
          feed_class: str = "general") -> int:
    return score_article(headline, summary, source_name, feed_class)[0]
