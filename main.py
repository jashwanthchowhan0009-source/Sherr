"""
SherByte Backend — v5.0 (Premium)
9-Pillar Taxonomy | Gemini 2.5 Flash pipeline | Personalized Feed | JWT Auth

Fixes vs v4.1:
  • No more DB wipe on startup (was destroying user data every deploy)
  • Fixed the duplicated prompt bug that crashed grok_rewrite on import
  • Title-hash dedup prevents the same story from 3 sources × 3 articles
  • Concurrent Gemini batch processing (5 in parallel)
  • Safe ALTER TABLE migrations for new columns
  • LRU cache on /pillars and /topics (static data, was hitting DB every call)
  • is_trending + sentiment stored per article, exposed to frontend
  • refined_title and cached_summary aliases on every article response

Run: python main.py   or   uvicorn main:app --host 0.0.0.0 --port $PORT
"""

import os, sys, json, math, hashlib, asyncio, logging, random, re, sqlite3, time
import hmac as hmac_module
import base64
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from functools import lru_cache
from typing import Optional

import httpx
import feedparser
from dotenv import load_dotenv

from activity import router as activity_router, init_activity_schema
from markets  import router as markets_router

from text_utils import clean_html_fragments, title_fingerprint
# P0.4 — the originality gate. One implementation, loaded via the root shim so the
# sqlite app does not pull in the sherrbyte package's asyncpg/Supabase dependencies.
from originality import (
    MAX_CONTIGUOUS_RUN, MAX_NGRAM_OVERLAP, headline_is_original,
    originality_check)
# What is actually in a body, and what a rewrite can be written FROM. The drain
# overwrites full_body with a stub, so feeding full_body to the AI pass makes it
# summarize its own placeholder — see body_state.py.
import body_state
# One canonical shape for articles.published_at. Four formats used to reach that
# column; see timestamps.py for the two bugs that produced.
import timestamps

# Which imagery the feed serves: stock | thumbnail | art. See image_service.py.
IMAGE_MODE = (os.getenv("IMAGE_MODE") or "thumbnail").strip().lower()
from ai_processor import process_batch, available_providers

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sherbyte")

# ─── ENV ─────────────────────────────────────────────────────────────────────
NEWSAPI_KEY     = os.getenv("NEWSAPI_KEY", "")
# A default signing secret is a backdoor, not a convenience: the value is in the
# repository, so anyone who can read it can mint a token for any user id. Dev keeps a
# generated ephemeral secret (tokens die on restart, which is fine locally); prod
# refuses to boot without a real one rather than starting up quietly forgeable.
JWT_SECRET = os.getenv("JWT_SECRET", "").strip()
if not JWT_SECRET:
    if (os.getenv("ENV") or "dev").lower() in ("prod", "production"):
        raise RuntimeError(
            "JWT_SECRET is not set. Refusing to start in production with a "
            "predictable signing key — set JWT_SECRET to a long random value.")
    import secrets as _secrets
    JWT_SECRET = _secrets.token_urlsafe(48)
    logging.getLogger("sherbyte").warning(
        "JWT_SECRET unset — using an ephemeral dev secret; tokens reset on restart.")
OPENWEATHER_KEY = os.getenv("OPENWEATHER_KEY", "")
# Comma-separated allowlist. Defaults cover local dev and the deployed frontends;
# override with CORS_ORIGINS in any other environment.
CORS_ORIGINS = [o.strip() for o in (os.getenv(
    "CORS_ORIGINS",
    "https://sherrbyte.vercel.app,https://sherrbyte.com,https://www.sherrbyte.com,"
    "http://localhost:3000,http://localhost:5173,http://127.0.0.1:5500"
) or "").split(",") if o.strip()]

DB_PATH         = os.getenv("DB_PATH", "sherbyte.db")

# Knobs for the AI cycle
AI_BATCH_SIZE   = int(os.getenv("AI_BATCH_SIZE", "50"))
AI_CONCURRENCY  = int(os.getenv("AI_CONCURRENCY", "5"))
COLLECT_INTERVAL_MIN = int(os.getenv("COLLECT_INTERVAL_MIN", "25"))

# The feed serves only status='published'. Ingest writes every article as
# pending_rewrite and the AI pass is what promotes it, so whenever that pass is
# behind the backlog grows and the feed empties out — which is what a reader sees
# as "no articles, no images".
#
# This used to hold a floor of 25, on the reasoning that a handful of pending rows
# is the rewrite pass being briefly behind and draining those would race it. In
# practice the pass was pointed at a decommissioned Groq model for long enough to
# park the entire corpus, so the floor only decided how long an empty feed stayed
# empty. It is gone: every boot drains whatever is pending, with no cap on how
# many. Set PENDING_DRAIN_THRESHOLD to a positive number to restore a floor, or
# leave it at 0 for none. DISABLE_PENDING_DRAIN=1 turns the pass off entirely.
PENDING_DRAIN_THRESHOLD = int(os.getenv("PENDING_DRAIN_THRESHOLD", "0"))
# How many bodies the nightly rewrite sweep attempts. Bounded because the free
# Gemini/Groq tiers are rate limited and a free Render instance should not spend
# its whole night on AI calls; a full corpus pass is several nights, or several
# calls to /admin/reprocess-bodies.
BODY_REPROCESS_NIGHTLY = int(os.getenv("BODY_REPROCESS_NIGHTLY", "150"))
# How long boot will wait for the drain before carrying on without it. The
# server does not accept connections until lifespan returns, so this is also
# the longest the health check can be left unable to connect.
DRAIN_BOOT_BUDGET_S = float(os.getenv("DRAIN_BOOT_BUDGET_S", "20"))
DISABLE_PENDING_DRAIN = (os.getenv("DISABLE_PENDING_DRAIN", "") or "").strip().lower() \
    in ("1", "true", "yes")

# Admin token guarding the maintenance endpoints (/admin/*). These endpoints
# reprocess, republish and re-scope the whole corpus, so a default that ships in
# a public repo is the same as no token at all. Same posture as JWT_SECRET:
# mandatory in production, ephemeral in dev.
ADMIN_TOKEN     = os.getenv("ADMIN_TOKEN", "").strip()
if not ADMIN_TOKEN:
    if (os.getenv("ENV") or "dev").lower() in ("prod", "production"):
        raise RuntimeError(
            "ADMIN_TOKEN is not set. Refusing to start in production with a "
            "publicly-known admin token — set ADMIN_TOKEN to a long random value.")
    import secrets as _asecrets
    ADMIN_TOKEN = _asecrets.token_urlsafe(32)
    logging.getLogger("sherbyte").warning(
        "ADMIN_TOKEN unset — dev token for this process only: %s", ADMIN_TOKEN)

# Optional: proxy /patterns to a SEPARATE engine deployment. Not needed in the
# single-service setup — there, DATABASE_URL below is what matters.
ENGINE_URL      = os.getenv("ENGINE_URL", "").rstrip("/")

# The Sherr-I engine's Postgres (Supabase). The workers (app.workers.*) write insights
# here; this app reads them here. This — not ENGINE_URL — is what makes /patterns
# return real insights in the single-service deployment.
SHERR_I_DATABASE_URL = (os.getenv("SHERR_I_DATABASE_URL") or os.getenv("DATABASE_URL") or "").strip()
_spie_pool = None


def _sanitize_pg_dsn(dsn: str) -> str:
    """Strip query params raw asyncpg rejects (pgbouncer / SQLAlchemy-only ones) so
    a pasted Supabase *pooler* URL connects cleanly."""
    from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit
    drop = {"pgbouncer", "prepared_statement_cache_size", "statement_cache_size",
            "prepared_statements", "prepare_threshold"}
    p = urlsplit(dsn)
    if not p.query:
        return dsn
    kept = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
            if k.lower() not in drop]
    return urlunsplit((p.scheme, p.netloc, p.path, urlencode(kept), p.fragment))


async def get_spie_pool():
    """Lazy asyncpg pool for the engine's Postgres. Returns None if unconfigured
    or unreachable (caller then reports source='unavailable', never fake data)."""
    global _spie_pool
    if _spie_pool is not None or not SHERR_I_DATABASE_URL:
        return _spie_pool
    try:
        import asyncpg
        _spie_pool = await asyncpg.create_pool(
            dsn=_sanitize_pg_dsn(SHERR_I_DATABASE_URL),
            min_size=1, max_size=4, timeout=20.0, command_timeout=20.0,
            statement_cache_size=0,      # Supabase transaction pooler safety
        )
        log.info("Sherr-I Postgres pool ready (insights source)")
    except Exception as e:
        log.warning("Sherr-I Postgres unavailable: %s", e)
        _spie_pool = None
    return _spie_pool


# Nothing older than this reaches the page. An insight is a claim about what is
# happening NOW — "FIFA and X are newly connected" stops being that the moment
# it is three days old, and the 2026-07-30 seed rows were still surfacing months
# later as though they were today's findings.
#
# 72 hours because the detectors run nightly: two missed runs still leave a
# populated page, three means the engine has genuinely stopped and an empty page
# is the honest answer.
PATTERN_MAX_AGE_HOURS = int(os.getenv("PATTERN_MAX_AGE_HOURS", "72"))


async def _spie_patterns(type: str, limit: int, offset: int,
                         max_age_hours: int = None) -> Optional[dict]:
    """Read real insights from the engine's Postgres, resolving entity ids to
    canonical names. Returns None when the DB isn't usable.

    FILTERED IN SQL, NOT IN THE UI. A client-side filter still ships the stale
    rows over the wire, still counts them in `total`, and is one forgotten
    caller away from putting them back on screen — and /patterns has more than
    one caller (the Sherr-I page and Explore's Connections section).
    """
    # Reads public.insights — the engine's own table, over a raw asyncpg pool with
    # the default search_path. Deliberate: this app's sherrbyte_app schema has no
    # insights table (its seed-tier demo rows live in demo_insights).
    pool = await get_spie_pool()
    if pool is None:
        return None
    # 0 disables the window — for an admin reading the raw table, never the app.
    hours = PATTERN_MAX_AGE_HOURS if max_age_hours is None else int(max_age_hours)

    # Built once and used for BOTH the page and the count. `total` used to be a
    # bare COUNT(*) over the whole table, so a filtered request reported a
    # number that had nothing to do with the rows beside it — the page saying
    # "319 patterns detected" above four cards is exactly that bug.
    where, args = [], []
    if type:
        args.append(type)
        where.append(f"type=${len(args)}")
    if hours > 0:
        args.append(str(int(hours)))
        where.append(f"created_at >= now() - (${len(args)} || ' hours')::interval")
    clause = (" WHERE " + " AND ".join(where)) if where else ""

    try:
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                f"SELECT * FROM insights{clause} ORDER BY score DESC, created_at DESC "
                f"LIMIT ${len(args) + 1} OFFSET ${len(args) + 2}",
                *args, limit, offset)
            total = await conn.fetchval(
                f"SELECT COUNT(*) FROM insights{clause}", *args)

            out = [dict(r) for r in rows]
            ids = {str(i) for d in out for i in (d.get("entity_ids") or [])}
            names = {}
            if ids:
                erows = await conn.fetch(
                    "SELECT id, canonical_name FROM entities WHERE id = ANY($1::uuid[])",
                    list(ids))
                names = {str(r["id"]): r["canonical_name"] for r in erows}

        for d in out:
            eids = [str(i) for i in (d.get("entity_ids") or [])]
            d["entity_ids"] = eids
            d["entities"] = [names.get(i, i) for i in eids]
            d["id"] = str(d.get("id"))
            ej = d.get("explain_json")
            if isinstance(ej, (str, bytes)):
                try:
                    d["explain_json"] = json.loads(ej)
                except Exception:
                    d["explain_json"] = {}
            if d.get("created_at") is not None:
                d["created_at"] = str(d["created_at"])
        return {"patterns": out, "total": int(total or 0), "source": "engine"}
    except Exception as e:
        log.warning("Sherr-I insights query failed: %s", e)
        return None

# Story-thread ("string") linking window — how far back we cluster related news.
STORY_WINDOW_DAYS = int(os.getenv("STORY_WINDOW_DAYS", "45"))
# Above this a "story thread" is an over-merged cluster, not a story. A real
# running story is a few dozen updates at the very most; thousands means the
# clustering collapsed and the thread is noise.
MAX_THREAD_SIZE = int(os.getenv("MAX_THREAD_SIZE", "40"))

# ─── TAXONOMY: 9 PILLARS ─────────────────────────────────────────────────────
PILLARS = {
    1: {"name": "Society & Governance",  "color": "#1E88E5", "emoji": "🏛️",  "slug": "society"},
    2: {"name": "Business & Economy",    "color": "#FBC02D", "emoji": "💼",  "slug": "economy"},
    3: {"name": "Science & Technology",  "color": "#3949AB", "emoji": "🔬",  "slug": "tech"},
    4: {"name": "Arts & Culture",        "color": "#E53935", "emoji": "🎭",  "slug": "arts"},
    5: {"name": "Natural World",         "color": "#43A047", "emoji": "🌿",  "slug": "nature"},
    6: {"name": "Self & Well-being",     "color": "#FB8C00", "emoji": "🧘",  "slug": "selfwell"},
    7: {"name": "Philosophy & Belief",   "color": "#8E24AA", "emoji": "🔮",  "slug": "philo"},
    8: {"name": "Society & Lifestyle",   "color": "#00ACC1", "emoji": "✨",  "slug": "lifestyle"},
    9: {"name": "Sports & Gaming",       "color": "#546E7A", "emoji": "⚽",  "slug": "sports"},
}

FRONTEND_SLUG_MAP = {
    "society": 1, "economy": 2, "tech": 3, "arts": 4, "nature": 5,
    "selfwell": 6, "philo": 7, "lifestyle": 8, "sports": 9,
    "science": 3, "business": 2, "wellbeing": 6, "philosophy": 7, "governance": 1,
}
SLUG_TO_PILLAR = {v["slug"]: k for k, v in PILLARS.items()}

SUB_PILLARS = {
    1: ["Power & Politics", "Education & Justice"],
    2: ["Markets & Finance", "Startups & Industry"],
    3: ["Digital Frontiers", "Physical Sciences"],
    4: ["Aesthetics & Design", "Media & Entertainment"],
    5: ["Biology & Zoology", "Earth & Environment"],
    6: ["Mind & Body", "Lifestyle & Habits"],
    7: ["Spirituality", "Philosophical Inquiry"],
    8: ["Modern Living", "Cultural Trends"],
    9: ["Athletic Performance", "Gaming & Interactive"],
}

# Micro-topics trimmed for brevity; your existing MICRO_TOPICS dict is preserved.
# (If you want, keep the full dict from v4.1 — this only affects classification fallback.)
MICRO_TOPICS: dict[str, int] = {
    # Pillar 1
    "Elections": 1, "Supreme Court": 1, "Parliament": 1, "Geopolitics": 1, "Diplomacy": 1,
    "Lok Sabha": 1, "NATO": 1, "G20": 1, "Politics": 1, "Government": 1, "Law": 1, "Education": 1,
    # Pillar 2
    "Stock Market": 2, "Nifty 50": 2, "Sensex": 2, "Bitcoin": 2, "Cryptocurrency": 2, "Ethereum": 2,
    "IPO": 2, "Startup": 2, "Venture Capital": 2, "FinTech": 2, "Inflation": 2, "GDP": 2,
    "Real Estate": 2, "E-Commerce": 2, "Economy": 2, "Finance": 2, "Business": 2,
    # Pillar 3
    "Artificial Intelligence": 3, "ChatGPT": 3, "OpenAI": 3, "LLM": 3, "Quantum Computing": 3,
    "SpaceX": 3, "ISRO": 3, "NASA": 3, "Cybersecurity": 3, "Robotics": 3, "Semiconductors": 3,
    "5G": 3, "Nvidia": 3, "AI": 3, "Software": 3, "Space": 3,
    # Pillar 4
    "Bollywood": 4, "Oscar": 4, "Grammy": 4, "Netflix": 4, "Anime": 4, "K-Pop": 4,
    "Marvel": 4, "Film Festival": 4, "Cinema": 4, "Music": 4, "Art": 4, "Literature": 4,
    # Pillar 5
    "Climate Change": 5, "Global Warming": 5, "Wildlife": 5, "Conservation": 5,
    "Earthquake": 5, "Cyclone": 5, "Flood": 5, "Tsunami": 5, "Renewable Energy": 5,
    "Biodiversity": 5, "Nature": 5, "Environment": 5, "Animals": 5,
    # Pillar 6
    "Mental Health": 6, "Meditation": 6, "Yoga": 6, "Fitness": 6, "Nutrition": 6,
    "Vaccine": 6, "COVID": 6, "Cancer": 6, "Diabetes": 6, "Health": 6, "Wellness": 6,
    # Pillar 7
    "Philosophy": 7, "Stoicism": 7, "Buddhism": 7, "Hinduism": 7, "Christianity": 7,
    "Islam": 7, "Sikhism": 7, "Spirituality": 7, "Religion": 7, "Ethics": 7, "Mythology": 7,
    # Pillar 8
    "Travel": 8, "Food": 8, "Fashion": 8, "Restaurant": 8, "Celebrity": 8,
    "Social Media": 8, "TikTok": 8, "Instagram": 8, "Lifestyle": 8, "Tourism": 8,
    # Pillar 9
    "Cricket": 9, "IPL": 9, "Football": 9, "Premier League": 9, "F1": 9, "Formula 1": 9,
    "Olympics": 9, "NBA": 9, "Tennis": 9, "Wimbledon": 9, "FIFA": 9, "Esports": 9,
    "Gaming": 9, "Sports": 9, "Virat Kohli": 9, "Messi": 9, "Ronaldo": 9,
    # Cybersecurity extensions (Pillar 3)
    "Data Breach": 3, "Ransomware": 3, "Phishing": 3, "Malware": 3,
    "Zero-Day": 3, "Vulnerability": 3, "DDoS": 3, "APT": 3,
    "Cyber Attack": 3, "Encryption": 3, "Patch Tuesday": 3,
    "Aviation Safety": 3, "Aircraft": 3, "Airline": 3, "SpaceX Launch": 3,
    # Military / geopolitics extensions (Pillar 1)
    "Drone Strike": 1, "Nuclear Weapons": 1, "Missile Test": 1,
    "Ukraine War": 1, "Gaza War": 1, "Taiwan Strait": 1,
    "NATO Summit": 1, "Defense Budget": 1, "Ceasefire": 1,
    "War Crimes": 1, "Refugee Crisis": 1, "Arms Deal": 1,
    "Military Alliance": 1, "Coup": 1, "Peacekeeping": 1,
    # Energy / climate extensions (Pillar 5)
    "Oil Prices": 5, "OPEC": 5, "LNG": 5, "Energy Crisis": 5,
    "Nuclear Energy": 5, "Solar Farm": 5, "Wind Farm": 5,
    "Energy Security": 5, "Carbon Capture": 5, "Electric Grid": 5,
    "Coal": 5, "Uranium": 5, "Paris Agreement": 5,
    "Carbon Budget": 5, "Sea Level Rise": 5, "Heat Wave": 5,
    "Arctic Ice": 5, "COP30": 5, "Fossil Fuels": 5,
}

# ─── FAST RULE-BASED CLASSIFIER (fallback before AI runs) ────────────────────
PILLAR_EXCLUSIVE_KEYWORDS = {
    1: ["election","parliament","government","minister","senate","vote","democracy",
        "constitution","treaty","diplomat","legislation","judiciary","supreme court",
        "president","prime minister","cabinet","political party","bjp","congress party",
        "lok sabha","rajya sabha","united nations","nato","geopolitics","sanctions",
        "military","army","defence","protest","coup","chief minister","governor",
        "drone strike","missile strike","ceasefire","war crimes","refugee","nato summit",
        "defense budget","arms deal","peacekeeping","military alliance","nuclear test",
        "ukraine war","taiwan strait","iran deal","un security council"],
    2: ["stock market","share price","nifty","sensex","nasdaq","bitcoin","cryptocurrency",
        "crypto","ethereum","blockchain","startup","venture capital","funding round",
        "ipo","merger","acquisition","quarterly earnings","inflation","interest rate",
        "gdp","recession","rbi","federal reserve","sebi","budget","gst","bank",
        "mutual fund","hedge fund","e-commerce","fintech","real estate","supply chain"],
    3: ["artificial intelligence","machine learning","deep learning","chatgpt","openai",
        "llm","neural network","quantum computing","crispr","gene editing","robotics",
        "spacex","isro","nasa","rocket launch","satellite","cybersecurity","data breach",
        "ransomware","smartphone launch","5g","6g","semiconductor","electric vehicle",
        "nuclear fusion","github","app update","nvidia","tpu",
        "zero-day","malware","phishing","cyber attack","ddos","apt group","hacking",
        "vulnerability","patch","exploit","darkweb","aviation accident","aircraft crash"],
    4: ["box office","oscar","grammy","emmy","bafta","music album","concert tour",
        "netflix series","amazon prime","disney+","streaming platform","art exhibition",
        "museum","gallery","fashion week","book launch","bestseller","broadway",
        "k-pop","bollywood film","hollywood movie","anime","film festival","cannes"],
    5: ["climate change","global warming","carbon emissions","greenhouse gas",
        "wildlife conservation","endangered species","national park","earthquake",
        "tsunami","hurricane","cyclone","tornado","flood","drought","wildfire",
        "deforestation","renewable energy","coral reef","biodiversity",
        "oil price","opec","lng","natural gas","energy crisis","nuclear plant",
        "solar energy","wind energy","carbon capture","electric grid","heat wave",
        "sea level rise","arctic ice","paris agreement","carbon budget","coal",
        "uranium","fossil fuel","energy security","petrochemical"],
    6: ["mental health","depression","anxiety disorder","therapy","yoga class",
        "meditation","mindfulness","weight loss","obesity","diet plan","hospital",
        "doctor","treatment","vaccine","covid","pandemic","cancer","diabetes",
        "heart disease","surgery","fitness routine","gym"],
    7: ["philosophy debate","buddhism","hinduism","christianity","islam","sikhism",
        "religion","spirituality","astrology","horoscope","meditation center",
        "monastery","temple","church","mosque","mythology","occult","stoic"],
    8: ["travel destination","tourism","hotel review","restaurant review","food festival",
        "cuisine","chef","fashion trend","celebrity","dating app","home decor",
        "remote work","digital nomad","music festival","influencer","content creator"],
    9: ["cricket match","ipl","test match","odi","t20","football match","fifa",
        "premier league","champions league","formula 1","f1 race","grand prix",
        "olympic games","gold medal","world cup","tennis match","wimbledon",
        "basketball game","nba finals","esports","gaming championship","wicket",
        "goal scored","sports injury"],
}


def classify_article(title: str, body: str) -> tuple[int, list[str]]:
    text = (title + " " + body).lower()
    scores = {pid: 0 for pid in range(1, 10)}

    for pid, kws in PILLAR_EXCLUSIVE_KEYWORDS.items():
        for kw in kws:
            if kw in text:
                scores[pid] += 2

    matched_tags = []
    for topic, pid in MICRO_TOPICS.items():
        if topic.lower() in text:
            scores[pid] += 3
            if topic not in matched_tags:
                matched_tags.append(topic)

    best_pillar = max(scores, key=scores.get)

    if scores[best_pillar] == 0:
        title_lower = title.lower()
        quick_map = [
            (9, ["cricket","football","ipl","match","score","wicket","goal","tennis","f1"]),
            (1, ["government","minister","election","court","parliament","policy"]),
            (2, ["market","bank","economy","stock","profit","revenue"]),
            (4, ["film","movie","music","actor","album","oscar","concert"]),
            (5, ["climate","wildlife","nature","flood","earthquake","cyclone"]),
            (6, ["health","hospital","mental","fitness","vaccine","disease"]),
            (8, ["travel","food","fashion","celebrity","restaurant","trend"]),
            (7, ["religion","spiritual","philosophy","temple","church"]),
            (3, ["tech","ai","app","phone","software","launch","cyber"]),
        ]
        for pid, kws in quick_map:
            if any(kw in title_lower for kw in kws):
                best_pillar = pid
                break

    return best_pillar, list(dict.fromkeys(matched_tags))[:10]


def classify_scope(title: str, body: str) -> str:
    text = (title + " " + body).lower()
    india_words = ["india","delhi","mumbai","bengaluru","bangalore","chennai","hyderabad",
                   "kolkata","pune","ahmedabad","jaipur","lucknow","indian","modi","bjp",
                   "congress","rupee","nifty","sensex","rbi","lok sabha","kerala","tamil",
                   "karnataka","maharashtra","gujarat","punjab","bihar","bengal","odisha",
                   "isro","supreme court","new delhi"]
    local_words = ["district","municipal","village","town","ward","panchayat","corporation",
                   "locality","neighbourhood","neighborhood","civic","metro station","tehsil"]
    global_words = ["world","global","international","nato","china","russia","ukraine","europe",
                    "european","america","american","washington","beijing","moscow","london",
                    "united nations","white house","pentagon","brussels","tokyo","israel","gaza"]
    i = sum(1 for w in india_words if w in text)
    l = sum(1 for w in local_words if w in text)
    g = sum(1 for w in global_words if w in text)
    # Local: clearly sub-national civic reporting with an India signal.
    if l >= 1 and i >= 1:
        return "local"
    # National: any India signal that isn't outweighed by global framing.
    if i >= 1 and i >= g:
        return "national"
    return "global"


def _scope_clause(scope: str, col: str = "scope"):
    """Inclusive scope filter used by the feeds.

    global   → broadest, no geographic filter (all stories)
    national → India-focused stories, including the local ones beneath them
    local    → strictly local civic stories
    """
    s = (scope or "").lower()
    if s == "national":
        return f" AND {col} IN ('national','local')", []
    if s == "local":
        return f" AND {col}=?", ["local"]
    return "", []   # global / unknown → no filter


# ─── IMAGE EXTRACTION ────────────────────────────────────────────────────────
def extract_image(entry, pillar_id: int) -> str:
    if hasattr(entry, "media_content") and entry.media_content:
        mc = entry.media_content[0]
        if isinstance(mc, dict) and mc.get("url", "").startswith("http"):
            return mc["url"]
    if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
        mt = entry.media_thumbnail[0]
        if isinstance(mt, dict) and mt.get("url", "").startswith("http"):
            return mt["url"]
    if hasattr(entry, "enclosures") and entry.enclosures:
        for enc in entry.enclosures:
            if isinstance(enc, dict) and enc.get("type", "").startswith("image"):
                url = enc.get("url", "")
                if url.startswith("http"):
                    return url
    html = getattr(entry, "summary", "") or ""
    if hasattr(entry, "content") and entry.content:
        html += entry.content[0].get("value", "")
    img_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', html)
    if img_match:
        url = img_match.group(1)
        if url.startswith("http"):
            return url
    if hasattr(entry, "links"):
        for link in entry.links:
            if isinstance(link, dict) and link.get("type", "").startswith("image"):
                href = link.get("href", "")
                if href.startswith("http"):
                    return href
    return ""  # Let frontend render the category-gradient fallback


# ─── RSS FEEDS ───────────────────────────────────────────────────────────────
RSS_FEEDS = [
    ("https://feeds.feedburner.com/ndtvnews-top-stories", "NDTV"),
    ("https://timesofindia.indiatimes.com/rssfeedstopstories.cms", "Times of India"),
    ("https://www.thehindu.com/feeder/default.rss", "The Hindu"),
    ("https://www.hindustantimes.com/rss/topnews/rssfeed.xml", "Hindustan Times"),
    ("https://indianexpress.com/feed/", "Indian Express"),
    ("https://www.livemint.com/rss/RSS.xml", "Mint"),
    ("https://feeds.feedburner.com/gadgets360-latest", "Gadgets 360"),
    ("https://techcrunch.com/feed/", "TechCrunch"),
    ("https://www.wired.com/feed/rss", "Wired"),
    ("https://feeds.arstechnica.com/arstechnica/index", "Ars Technica"),
    ("https://www.theverge.com/rss/index.xml", "The Verge"),
    ("https://www.engadget.com/rss.xml", "Engadget"),
    ("https://economictimes.indiatimes.com/rssfeedsdefault.cms", "Economic Times"),
    ("https://www.moneycontrol.com/rss/latestnews.xml", "MoneyControl"),
    ("https://www.business-standard.com/rss/latest.rss", "Business Standard"),
    ("https://www.forbes.com/innovation/feed/", "Forbes"),
    ("https://fortune.com/feed/", "Fortune"),
    ("https://feeds.bbci.co.uk/news/rss.xml", "BBC News"),
    ("https://feeds.bbci.co.uk/news/world/rss.xml", "BBC World"),
    ("https://feeds.bbci.co.uk/news/technology/rss.xml", "BBC Tech"),
    ("https://feeds.bbci.co.uk/news/science_and_environment/rss.xml", "BBC Science"),
    ("https://feeds.bbci.co.uk/news/business/rss.xml", "BBC Business"),
    ("https://feeds.bbci.co.uk/news/health/rss.xml", "BBC Health"),
    ("https://feeds.bbci.co.uk/news/entertainment_and_arts/rss.xml", "BBC Arts"),
    ("https://feeds.bbci.co.uk/news/sports/rss.xml", "BBC Sport"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/World.xml", "NYT World"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml", "NYT Tech"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Business.xml", "NYT Business"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Health.xml", "NYT Health"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Arts.xml", "NYT Arts"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Sports.xml", "NYT Sports"),
    ("https://www.theguardian.com/world/rss", "The Guardian"),
    ("https://www.theguardian.com/uk/sport/rss", "Guardian Sport"),
    ("https://www.theguardian.com/science/rss", "Guardian Science"),
    ("https://www.theguardian.com/business/rss", "Guardian Business"),
    ("https://www.theguardian.com/culture/rss", "Guardian Culture"),
    ("https://www.theguardian.com/lifeandstyle/rss", "Guardian Life"),
    ("https://www.aljazeera.com/xml/rss/all.xml", "Al Jazeera"),
    ("https://www.espn.com/espn/rss/news", "ESPN"),
    ("https://www.ndtv.com/rss/sports", "NDTV Sports"),
    ("https://www.sciencedaily.com/rss/top.xml", "Science Daily"),
    ("https://earthsky.org/category/astronomy/feed", "EarthSky"),
    ("https://www.nasa.gov/rss/dyn/breaking_news.rss", "NASA"),
    ("https://rss.medicalnewstoday.com/featurednews.xml", "Medical News Today"),
    ("https://www.healthline.com/rss/news", "Healthline"),
    ("https://variety.com/feed/", "Variety"),
    ("https://deadline.com/feed/", "Deadline"),
    ("https://www.rollingstone.com/music/music-news/feed/", "Rolling Stone"),
    ("https://feeds.feedburner.com/ign/games-all", "IGN"),
    ("https://www.gamespot.com/feeds/mashup/", "GameSpot"),
    ("https://e360.yale.edu/feed", "Yale E360"),
    # --- CYBERSECURITY (Pillar 3) ---
    ("https://feeds.feedburner.com/TheHackersNews", "The Hacker News"),
    ("https://krebsonsecurity.com/feed/", "Krebs on Security"),
    ("https://www.bleepingcomputer.com/feed/", "Bleeping Computer"),
    ("https://www.darkreading.com/rss.xml", "Dark Reading"),
    ("https://www.securityweek.com/feed/", "SecurityWeek"),
    ("https://isc.sans.edu/rssfeed_full.xml", "SANS ISC"),
    ("https://nakedsecurity.sophos.com/feed/", "Sophos Naked Security"),
    # --- MILITARY & GEOPOLITICS (Pillar 1) ---
    ("https://breakingdefense.com/feed/", "Breaking Defense"),
    ("https://warontherocks.com/feed/", "War on the Rocks"),
    ("https://foreignpolicy.com/feed/", "Foreign Policy"),
    ("https://thewarzone.com/feed/", "The War Zone"),
    ("https://www.defensenews.com/rss/", "Defense News"),
    # --- ENERGY (Pillar 5) ---
    ("https://oilprice.com/rss/main", "OilPrice.com"),
    ("https://cleantechnica.com/feed/", "CleanTechnica"),
    ("https://www.renewableenergyworld.com/feed/", "Renewable Energy World"),
    ("https://electrek.co/feed/", "Electrek"),
    ("https://www.energymonitor.ai/feed/", "Energy Monitor"),
    # --- CLIMATE (Pillar 5) ---
    ("https://www.carbonbrief.org/feed/", "Carbon Brief"),
    ("https://insideclimatenews.org/feed/", "Inside Climate News"),
    ("https://www.climatechangenews.com/feed/", "Climate Home News"),
    ("https://grist.org/feed/", "Grist"),
    ("https://www.theguardian.com/environment/climate-change/rss", "Guardian Climate"),
    # --- AVIATION / AEROSPACE (Pillar 3) ---
    ("https://simpleflying.com/feed/", "Simple Flying"),
    ("https://theaviationgeek.com/feed/", "The Aviation Geek"),
    ("https://theaircurrent.com/feed/", "The Air Current"),
    # --- GLOBAL NEWS (Pillar 1) ---
    ("https://rss.dw.com/rdf/rss-en-all", "Deutsche Welle"),
    ("https://rss.cnn.com/rss/edition.rss", "CNN World"),
    ("https://feeds.apnews.com/rss/topnews", "AP News"),
    ("https://www.france24.com/en/rss", "France 24"),
    ("https://feeds.npr.org/1001/rss.xml", "NPR News"),
    ("https://abcnews.go.com/abcnews/internationalheadlines", "ABC International"),
    # --- EMERGING TECH (Pillar 3) ---
    ("https://www.technologyreview.com/topnews.rss", "MIT Technology Review"),
    ("https://spectrum.ieee.org/feeds/feed.rss", "IEEE Spectrum"),
    ("https://venturebeat.com/feed/", "VentureBeat"),
    # --- SCIENCE & HEALTH (Pillars 5 & 6) ---
    ("https://www.statnews.com/feed/", "STAT News"),
    ("https://www.newscientist.com/feed/home/", "New Scientist"),
]

# ─── DATABASE ────────────────────────────────────────────────────────────────
CREATE_TABLES = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    email TEXT UNIQUE NOT NULL,
    password TEXT NOT NULL,
    name TEXT DEFAULT '',
    bio TEXT DEFAULT '',
    avatar_url TEXT DEFAULT '',
    language TEXT DEFAULT 'en',
    created_at TEXT DEFAULT (datetime('now')),
    last_login TEXT
);

CREATE TABLE IF NOT EXISTS topics (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT UNIQUE NOT NULL,
    slug TEXT NOT NULL,
    pillar_id INTEGER NOT NULL,
    sub_pillar TEXT DEFAULT '',
    color TEXT NOT NULL,
    emoji TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS articles (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    url TEXT UNIQUE NOT NULL,
    title_hash TEXT,
    headline TEXT NOT NULL,
    summary_60 TEXT DEFAULT '',
    full_body TEXT DEFAULT '',
    source_summary TEXT DEFAULT '',
    when_info TEXT DEFAULT '',
    where_info TEXT DEFAULT '',
    what_info TEXT DEFAULT '',
    how_info TEXT DEFAULT '',
    image_url TEXT DEFAULT '',
    source_name TEXT DEFAULT '',
    pillar_id INTEGER DEFAULT 1,
    micro_tags TEXT DEFAULT '[]',
    scope TEXT DEFAULT 'global',
    is_trending INTEGER DEFAULT 0,
    sentiment TEXT DEFAULT 'neutral',
    -- Left as datetime('now') ON PURPOSE. pgcompat translates this one; it has
    -- no rule for strftime, so a "canonical" DDL default silently failed the
    -- whole CREATE TABLE on Postgres and the articles table was never created.
    -- The default is a fallback nothing reaches anyway — both ingest paths pass
    -- an explicit canonical value — and after normalise_published_at() runs the
    -- Postgres default becomes now() on a real timestamptz column.
    published_at TEXT DEFAULT (datetime('now')),
    collected_at TEXT DEFAULT (datetime('now')),
    ai_processed INTEGER DEFAULT 0,
    reprocessed INTEGER DEFAULT 0,
    story_id INTEGER DEFAULT 0,
    engagement INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_preferences (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    topic_name TEXT NOT NULL,
    pillar_id INTEGER NOT NULL,
    weight REAL DEFAULT 1.0,
    UNIQUE(user_id, topic_name)
);

CREATE TABLE IF NOT EXISTS user_interactions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    article_id INTEGER NOT NULL,
    action TEXT NOT NULL,
    timestamp TEXT DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS feeds (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    article_id INTEGER NOT NULL,
    score REAL DEFAULT 0.0,
    computed_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, article_id)
);

CREATE TABLE IF NOT EXISTS bookmarks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id INTEGER NOT NULL,
    article_id INTEGER NOT NULL,
    saved_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, article_id)
);

-- Sherr-I demo rows for the "seed" tier, and NOTHING ELSE.
--
-- NAMED demo_insights, NOT insights, deliberately. This app runs its DDL through
-- pgcompat with search_path=sherrbyte_app, while the engine writes and
-- _spie_patterns reads public.insights over a raw asyncpg pool. Calling this
-- table `insights` therefore created a SECOND table of that name, in a different
-- schema, holding _SAMPLE_INSIGHTS demo rows — shadowing the real one on every
-- production boot. Nothing read the shadow, so nothing failed; it just sat there
-- waiting for someone to join the wrong one. Two same-named tables in two schemas
-- is the two-database bug that has already cost this repo once.
--
-- The name is now unambiguous in both directions: `insights` is always the
-- engine's table in public, `demo_insights` is always the local seed tier.
CREATE TABLE IF NOT EXISTS demo_insights (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    type TEXT NOT NULL,
    entities TEXT DEFAULT '[]',
    domains TEXT DEFAULT '[]',
    score REAL DEFAULT 0,
    explain_json TEXT DEFAULT '{}',
    signature TEXT UNIQUE,
    created_at TEXT DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_demo_insights_type ON demo_insights(type, score DESC);
CREATE INDEX IF NOT EXISTS idx_articles_pillar ON articles(pillar_id);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_title_hash ON articles(title_hash);
CREATE INDEX IF NOT EXISTS idx_articles_trending ON articles(is_trending);
CREATE INDEX IF NOT EXISTS idx_articles_story ON articles(story_id);
CREATE INDEX IF NOT EXISTS idx_feeds_user ON feeds(user_id, score DESC);
CREATE INDEX IF NOT EXISTS idx_prefs_user ON user_preferences(user_id);
"""

# Safe migrations for users upgrading from v4.1
_MIGRATIONS = [
    "ALTER TABLE articles ADD COLUMN title_hash TEXT",
    "ALTER TABLE articles ADD COLUMN is_trending INTEGER DEFAULT 0",
    "ALTER TABLE articles ADD COLUMN sentiment TEXT DEFAULT 'neutral'",
    # Copyright scrub flag — 1 once a row's body is guaranteed AI-written (fresh
    # pipeline output or a /admin/reprocess pass), so legacy rows are re-run once.
    "ALTER TABLE articles ADD COLUMN reprocessed INTEGER DEFAULT 0",
    # Story-thread id ("string" feature) — groups related articles into a
    # chronological thread. 0 = not part of any multi-article thread.
    "ALTER TABLE articles ADD COLUMN story_id INTEGER DEFAULT 0",
    # ── P0 originality gate ───────────────────────────────────────────────────
    # The publisher's headline, kept for the originality diff and never rendered.
    # `headline` is OURS; if rewriting fails the row is parked, not backfilled from
    # here — a source headline must never reach the feed by fallback.
    "ALTER TABLE articles ADD COLUMN source_headline TEXT DEFAULT ''",
    # published | pending_rewrite | blocked_originality. Only 'published' is served.
    "ALTER TABLE articles ADD COLUMN status TEXT DEFAULT 'published'",
    # The audit trail: the metrics the gate computed, stored whether it passed or not.
    "ALTER TABLE articles ADD COLUMN originality_json TEXT DEFAULT ''",
    "ALTER TABLE articles ADD COLUMN originality_overlap REAL DEFAULT -1",
    "ALTER TABLE articles ADD COLUMN originality_run INTEGER DEFAULT -1",
    "ALTER TABLE articles ADD COLUMN originality_checked_at TEXT DEFAULT ''",
    # ── imagery ───────────────────────────────────────────────────────────────
    # Which kind of image this row carries: stock | thumbnail | art. Stored rather
    # than inferred so the render path knows which constraints to apply, and so a
    # mode change stays auditable after the fact.
    "ALTER TABLE articles ADD COLUMN image_source TEXT DEFAULT 'art'",
    "ALTER TABLE articles ADD COLUMN image_credit TEXT DEFAULT ''",
    "ALTER TABLE articles ADD COLUMN image_query TEXT DEFAULT ''",
    # The publisher's og:image, stored but NOT necessarily rendered. Whether it is
    # shown is IMAGE_MODE's decision at render time; keeping it here means flipping
    # modes is an env-var change, not a re-crawl.
    "ALTER TABLE articles ADD COLUMN source_image_url TEXT DEFAULT ''",
]

# Publisher image URLs are never persisted again (P0.1). Existing rows are scrubbed
# on boot: a hotlinked hero is both a copyright exposure and a referrer leak, and
# leaving old rows intact would keep serving them.
# Legacy scrub: rows collected before image_source existed carry an unattributed
# publisher hotlink. Rows written since are labelled and governed by IMAGE_MODE, so
# they are left alone — otherwise every boot would wipe thumbnail mode's own output.
_IMAGE_SCRUB = (
    "UPDATE articles SET image_url = '' "
    "WHERE image_url <> '' AND (image_source IS NULL OR image_source = '') "
    "AND image_url NOT LIKE '%sherrbyte%'"
)


# Sample Sherr-I pattern output (shape matches the real engine's insights.explain_json).
_SAMPLE_INSIGHTS = [
    {
        "type": "temporal_correlation",
        "entities": ["Monsoon (Mumbai)", "Vegetable prices"],
        "domains": ["weather", "commodities"],
        "score": 0.72,
        "signature": "temporal:monsoon-mumbai:veg-prices:3",
        "explain_json": {
            "why": "Historically observed: Monsoon (Mumbai) movements have been "
                   "followed by Vegetable prices movements about 3 day(s) later "
                   "(correlation 0.72 over 12 overlapping days, in ≥2 separate "
                   "periods). This is a detected correlation, not causation, and is "
                   "not a prediction.",
            "leader_entity": "Monsoon (Mumbai)", "follower_entity": "Vegetable prices",
            "lag_days": 3, "r": 0.72, "windows_tested": [0, 1, 2, 3, 7],
            "observations": 12, "article_count": 34, "source_count": 6,
            "top_sources": ["open-meteo:rainfall", "yahoo", "The Hindu", "livemint"],
            "credibility": 0.88, "confidence": 0.72,
        },
    },
    {
        "type": "temporal_correlation",
        "entities": ["Crude Oil", "USD/INR"],
        "domains": ["commodities", "forex"],
        "score": 0.61,
        "signature": "temporal:crude:usdinr:1",
        "explain_json": {
            "why": "Historically observed: Crude Oil movements have been followed by "
                   "USD/INR movements about 1 day(s) later (correlation 0.61 over 21 "
                   "overlapping days, in ≥2 separate periods). Detected correlation, "
                   "not causation.",
            "leader_entity": "Crude Oil", "follower_entity": "USD/INR",
            "lag_days": 1, "r": 0.61, "windows_tested": [0, 1, 2, 3, 7],
            "observations": 21, "article_count": 18, "source_count": 4,
            "top_sources": ["yahoo", "Bloomberg", "Reuters"],
            "credibility": 0.93, "confidence": 0.61,
        },
    },
    {
        "type": "emergence",
        "entities": ["Reserve Bank of India", "Fintech lending"],
        "domains": ["news", "economy"],
        "score": 7.0,
        "signature": "emergence:rbi:fintech-lending",
        "explain_json": {
            "why": "Reserve Bank of India and Fintech lending co-occurred 7 times in "
                   "the last 7 days, with no appearances together in the preceding 90 "
                   "days — a newly emerging connection.",
            "article_count": 7, "source_count": 5,
            "top_sources": ["The Hindu", "livemint", "Business Standard", "Reuters"],
            "credibility": 0.9, "confidence": 0.7,
        },
    },
]


# Render's free tier gives the service an EPHEMERAL filesystem, so sherbyte.db is
# destroyed on every deploy and the feed resets to zero. No amount of draining or
# re-ingesting fixes that; the storage has to leave local disk. When DATABASE_URL
# is a postgres URL every read and write goes to Postgres instead, through a
# sqlite3-shaped adapter (pgcompat) so the ~80 existing call sites keep working
# unmodified. sqlite remains only as the local-development backend.
import pgcompat                                            # noqa: E402

ARTICLES_DSN = (os.getenv("DATABASE_URL")
                or os.getenv("SHERR_I_DATABASE_URL") or "").strip()
USE_POSTGRES = pgcompat.is_postgres_url(ARTICLES_DSN)


def get_db():
    if USE_POSTGRES:
        return pgcompat.connect(ARTICLES_DSN)
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


# ─── published_at normalisation ──────────────────────────────────────────────
# `ORDER BY published_at DESC` on a TEXT column is a byte comparison, and this
# column carried four different formats — see timestamps.py. Two bugs came out
# of it: the feed sorted a six-hour-old article above a four-hour-old one, and
# every naive stamp was read by the browser as local time, adding the reader's
# UTC offset to every age.
#
# Run at boot, idempotent, and safe on both backends. On Postgres it finishes by
# making the column a real timestamptz, after which the database does the
# comparison and no format can drift back in.
_PA_BACKFILL_SQL_PG = """
UPDATE articles
   SET published_at = to_char(published_at::timestamptz AT TIME ZONE 'UTC',
                              'YYYY-MM-DD"T"HH24:MI:SS') || '+00:00'
 WHERE published_at IS NOT NULL AND published_at <> ''
   AND published_at !~ '^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\+00:00$'
   AND published_at ~ '^\\d{4}-\\d{2}-\\d{2}'
"""


def repair_blank_headlines(conn) -> dict:
    """Restore headlines that a failed AI pass blanked.

    process_batch's rule-based fallback returned refined_title="" whenever both
    providers were down, and run_ai_batch wrote that into `headline` — so those
    articles render as a card with an image, a byline and NOTHING ELSE. The
    publisher's headline is still in source_headline (kept for the originality
    diff), which is exactly what the aggregator posture publishes anyway:
    their headline, under credit, with our body.

    Idempotent. Rows with no source_headline either are unrecoverable and are
    unpublished rather than left as blank cards in the feed.
    """
    out = {"restored": 0, "unpublished": 0}
    try:
        cur = conn.execute(
            "UPDATE articles SET headline = source_headline "
            "WHERE COALESCE(TRIM(headline), '') = '' "
            "AND COALESCE(TRIM(source_headline), '') <> ''")
        out["restored"] = getattr(cur, "rowcount", 0) or 0
        # Nothing to show and nothing to restore from: a titleless card is worse
        # than one fewer card.
        cur = conn.execute(
            "UPDATE articles SET status = 'pending_rewrite' "
            "WHERE COALESCE(TRIM(headline), '') = '' AND status = 'published'")
        out["unpublished"] = getattr(cur, "rowcount", 0) or 0
        conn.commit()
    except Exception as e:
        log.warning("[HEADLINE] repair skipped: %s", e)
        out["error"] = str(e)
    return out


def normalise_published_at(conn) -> dict:
    """Rewrite every non-canonical published_at, then make the column
    timestamptz on Postgres. Returns what it did, for the boot log."""
    out = {"backend": "postgres" if USE_POSTGRES else "sqlite",
           "rewritten": 0, "column_type": None, "altered": False}
    try:
        if USE_POSTGRES:
            # Type FIRST. Once the column is timestamptz the text backfill below
            # is not just unnecessary, it is invalid — `published_at <> ''`
            # against a timestamptz raises, and every boot would log an error
            # that reads like a failure while nothing was actually wrong.
            row = conn.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_schema='sherrbyte_app' AND table_name='articles' "
                "AND column_name='published_at'").fetchone()
            current = (row or {}).get("data_type") if row else None
            out["column_type"] = current
            if current and current.lower() != "text":
                return out                      # already migrated, nothing to do

            cur = conn.execute(_PA_BACKFILL_SQL_PG)
            out["rewritten"] = getattr(cur, "rowcount", 0) or 0
            # ALTER ... USING aborts the whole statement on one unparseable
            # row, so those are resolved first — but NOT to NULL. Postgres sorts
            # NULLS FIRST under DESC, so a row with an unreadable stamp would
            # have been promoted to the TOP of the feed. It falls back to
            # collected_at, and to the epoch when even that is unusable: we do
            # not know when it was published, so it must not lead the feed.
            conn.execute(
                "UPDATE articles SET published_at = COALESCE("
                "  NULLIF(regexp_replace(COALESCE(collected_at,''), "
                "         '^(\\d{4}-\\d{2}-\\d{2})[T ](\\d{2}:\\d{2}:\\d{2}).*$', "
                "         '\\1T\\2+00:00'), COALESCE(collected_at,'')), "
                "  '1970-01-01T00:00:00+00:00') "
                "WHERE published_at IS NULL OR published_at = '' "
                "   OR published_at !~ '^[0-9]{4}-[0-9]{2}-[0-9]{2}'")
            if True:
                # The DROP DEFAULT is required, not tidiness: the column carries
                # a text default (now()::text), and Postgres refuses the type
                # change with "default ... cannot be cast automatically" while
                # it is attached.
                conn.execute("ALTER TABLE articles ALTER COLUMN published_at "
                             "DROP DEFAULT")
                conn.execute("ALTER TABLE articles ALTER COLUMN published_at "
                             "TYPE timestamptz USING published_at::timestamptz")
                conn.execute("ALTER TABLE articles ALTER COLUMN published_at "
                             "SET DEFAULT now()")
                out["altered"] = True
                out["column_type"] = "timestamp with time zone"
        else:
            # sqlite has no timestamp type, so the canonical string IS the fix.
            rows = conn.execute(
                "SELECT id, published_at FROM articles "
                "WHERE published_at IS NOT NULL AND published_at <> ''").fetchall()
            for r in rows:
                canon = timestamps.to_canonical(r["published_at"])
                if canon and canon != r["published_at"]:
                    conn.execute("UPDATE articles SET published_at=? WHERE id=?",
                                 (canon, r["id"]))
                    out["rewritten"] += 1
            out["column_type"] = "text"
        conn.commit()
    except Exception as e:
        log.warning("[PUBLISHED_AT] normalisation skipped: %s", e)
        out["error"] = str(e)
    return out


def init_db():
    conn = get_db()
    conn.executescript(CREATE_TABLES)
    for stmt in _MIGRATIONS:
        try:
            conn.execute(stmt)
        except Exception:
            # "column already exists" is the expected case on every boot after the
            # first. Postgres raises its own error type, not sqlite3's, so this
            # cannot stay narrowed to OperationalError.
            pass
    # A card with no headline is the worst thing the feed can show. Repair
    # before anything is served.
    try:
        hp = repair_blank_headlines(conn)
        if hp.get("restored") or hp.get("unpublished"):
            log.info("[HEADLINE] %s", hp)
    except Exception as e:
        log.warning("[HEADLINE] repair failed: %s", e)

    # published_at must be one shape before anything sorts on it. Idempotent:
    # a second boot finds every row already canonical and rewrites none.
    try:
        pa = normalise_published_at(conn)
        if pa.get("rewritten") or pa.get("altered"):
            log.info("[PUBLISHED_AT] %s", pa)
    except Exception as e:
        log.warning("[PUBLISHED_AT] failed: %s", e)

    # P0.1 backfill: null out every publisher-hosted hero already on disk. Idempotent,
    # and cheap enough to run on every boot — the alternative is a one-shot script
    # somebody forgets to run before a deploy.
    try:
        # In thumbnail mode publisher images ARE what we serve, so blanking them
        # on every boot just fights the renderer. The scrub exists for the modes
        # that do not serve them.
        scrubbed = 0 if IMAGE_MODE == "thumbnail" else conn.execute(_IMAGE_SCRUB).rowcount
        if scrubbed:
            log.info("P0.1 scrubbed %d hotlinked publisher images", scrubbed)
    except Exception as e:
        # Not sqlite3.OperationalError: on Postgres the driver raises its own
        # type, so a narrowed except here would let a scrub failure abort init_db
        # and take the whole boot with it.
        log.warning("image scrub skipped: %s", e)
    conn.commit()

    # Seed topics table
    cur = conn.cursor()
    for topic_name, pid in MICRO_TOPICS.items():
        p = PILLARS[pid]
        slug = topic_name.lower().replace(" ", "-").replace("&", "and").replace("+", "plus")
        try:
            cur.execute(
                "INSERT OR IGNORE INTO topics (name, slug, pillar_id, color, emoji) VALUES (?,?,?,?,?)",
                (topic_name, slug, pid, p["color"], p["emoji"])
            )
        except Exception:
            pass

    # Backfill title_hash for any rows missing it
    try:
        missing = conn.execute("SELECT id, headline FROM articles WHERE title_hash IS NULL OR title_hash=''").fetchall()
        for row in missing:
            h = title_fingerprint(row["headline"])
            conn.execute("UPDATE articles SET title_hash=? WHERE id=?", (h, row["id"]))
    except Exception as e:
        log.warning("title_hash backfill skipped: %s", e)

    # Seed a few sample Sherr-I insights so the app shows real pattern output
    # before the full Postgres engine is deployed. Idempotent via signature.
    try:
        for ins in _SAMPLE_INSIGHTS:
            conn.execute(
                "INSERT OR IGNORE INTO demo_insights (type, entities, domains, score, explain_json, signature) "
                "VALUES (?,?,?,?,?,?)",
                (ins["type"], json.dumps(ins["entities"]), json.dumps(ins["domains"]),
                 ins["score"], json.dumps(ins["explain_json"]), ins["signature"]),
            )
    except Exception as e:
        log.warning("insight seed skipped: %s", e)

    conn.commit()
    conn.close()
    # Name the backend that is actually in use. "DB ready at sherbyte.db" while
    # every query goes to Postgres is the kind of log line that costs an hour.
    if USE_POSTGRES:
        log.info("DB ready: postgres, schema %s (sqlite path unused)",
                 pgcompat.APP_SCHEMA)
    else:
        log.info("DB ready: sqlite at %s (EPHEMERAL on Render — set DATABASE_URL "
                 "to persist across deploys)", DB_PATH)

# ─── AUTH ────────────────────────────────────────────────────────────────────
# Passwords were stored as a bare, unsalted SHA-256. That is a single fast hash: a
# commodity GPU tries billions per second, so every password in the table — and every
# place a user reused it — was recoverable from a database leak in minutes. Rainbow
# tables handle the common ones with no compute at all.
#
# PBKDF2-HMAC-SHA256 instead: per-user random salt (kills rainbow tables and makes
# each password a separate attack) and a deliberately slow iteration count (kills the
# throughput). stdlib, so no new dependency — bcrypt/argon2 are stronger per unit of
# work, but the security gap between "unsalted SHA-256" and "PBKDF2 at 600k" is the
# one that matters here, and this ships today.
PBKDF2_ITERATIONS = 600_000        # OWASP 2023 guidance for PBKDF2-HMAC-SHA256
_HASH_PREFIX = "pbkdf2_sha256"


def hash_password(pw: str) -> str:
    """`pbkdf2_sha256$<iterations>$<salt_hex>$<hash_hex>` — self-describing, so the
    iteration count can be raised later without breaking existing rows."""
    salt = os.urandom(16)
    dk = hashlib.pbkdf2_hmac("sha256", pw.encode(), salt, PBKDF2_ITERATIONS)
    return f"{_HASH_PREFIX}${PBKDF2_ITERATIONS}${salt.hex()}${dk.hex()}"


def check_password(pw: str, hashed: str) -> bool:
    """Verify against either format.

    Legacy rows are still accepted so nobody is locked out by the upgrade; the login
    path rehashes them on the next successful sign-in (see needs_rehash). Legacy
    verification is deliberately NOT removed yet — that happens once the table is
    fully migrated.
    """
    if not hashed:
        return False
    if hashed.startswith(_HASH_PREFIX + "$"):
        try:
            _, iters, salt_hex, want = hashed.split("$", 3)
            dk = hashlib.pbkdf2_hmac("sha256", pw.encode(),
                                     bytes.fromhex(salt_hex), int(iters))
            return hmac_module.compare_digest(dk.hex(), want)
        except Exception:
            return False
    # Legacy unsalted SHA-256.
    return hmac_module.compare_digest(hashlib.sha256(pw.encode()).hexdigest(), hashed)


def needs_rehash(hashed: str) -> bool:
    """True for a legacy hash, or one below the current iteration count."""
    if not hashed or not hashed.startswith(_HASH_PREFIX + "$"):
        return True
    try:
        return int(hashed.split("$", 2)[1]) < PBKDF2_ITERATIONS
    except Exception:
        return True


def make_token(user_id: int) -> str:
    payload = json.dumps({
        "id": user_id,
        "exp": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()
    })
    raw = base64.urlsafe_b64encode(payload.encode()).decode()
    sig = hmac_module.new(JWT_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
    return f"{raw}.{sig}"


def verify_token(token: str) -> Optional[int]:
    try:
        raw, sig = token.rsplit(".", 1)
        expected = hmac_module.new(JWT_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()
        if not hmac_module.compare_digest(sig, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(raw + "=="))
        if datetime.fromisoformat(payload["exp"]) < datetime.now(timezone.utc):
            return None
        return payload["id"]
    except Exception:
        return None


# ─── NEWS COLLECTION ─────────────────────────────────────────────────────────
async def fetch_feed_async(feed_url: str, source_name: str, client: httpx.AsyncClient) -> list[dict]:
    articles = []
    try:
        r = await client.get(
            feed_url,
            headers={"User-Agent": "SherByte/5.0 (+https://sherbyte.in)"},
            timeout=12,
        )
        if r.status_code != 200:
            return articles
        feed = await asyncio.get_event_loop().run_in_executor(None, feedparser.parse, r.text)
        for entry in feed.entries[:36]:
            title = getattr(entry, "title", "").strip()
            summary = getattr(entry, "summary", "") or ""
            link = getattr(entry, "link", "").strip()
            if not title or not link:
                continue

            clean = clean_html_fragments(summary)
            pid, tags = classify_article(title, clean)
            scope = classify_scope(title, clean)
            img = extract_image(entry, pid)

            # Canonical from the start: feedparser's published_parsed is a UTC
            # struct_time, and datetime.now() here was SERVER-LOCAL — two
            # different meanings written into one column.
            pub_date = timestamps.now_canonical()
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    pub_date = timestamps.to_canonical(
                        datetime(*entry.published_parsed[:6], tzinfo=timezone.utc))
                except Exception:
                    pass

            articles.append({
                "url": link,
                "title_hash": title_fingerprint(title),
                "headline": title,
                # 0.2: the publisher's headline is kept ONLY for the originality diff.
                # `headline` is overwritten by our own once the AI pass runs; until then
                # the row is parked and excluded from the feed.
                "source_headline": title,
                "status": "pending_rewrite",
                "summary_60": clean[:400],
                "full_body": clean,
                "source_summary": clean[:200],
                "when_info": pub_date,
                "where_info": "Not specified",
                "what_info": title,
                "how_info": "",
                # image_url is what we RENDER; source_image_url is what the
                # publisher offered. IMAGE_MODE decides which one wins.
                "image_url": "",
                "source_image_url": img or "",
                "source_name": source_name,
                "pillar_id": pid,
                "micro_tags": json.dumps(tags),
                "scope": scope,
                "published_at": pub_date,
                "ai_processed": 0,
            })
    except Exception as e:
        log.warning("RSS %s failed: %s", source_name, e)
    return articles


async def collect_rss() -> list[dict]:
    all_articles = []
    batch_size = 10
    async with httpx.AsyncClient(timeout=15, follow_redirects=True) as client:
        for i in range(0, len(RSS_FEEDS), batch_size):
            batch = RSS_FEEDS[i:i + batch_size]
            tasks = [fetch_feed_async(url, name, client) for url, name in batch]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for res in results:
                if isinstance(res, list):
                    all_articles.extend(res)
            await asyncio.sleep(0.4)
    log.info("[RSS] Collected %d raw from %d feeds", len(all_articles), len(RSS_FEEDS))
    return all_articles


async def collect_newsapi() -> list[dict]:
    if not NEWSAPI_KEY:
        return []
    articles = []
    queries = [
        f"https://newsapi.org/v2/top-headlines?language=en&pageSize=40&apiKey={NEWSAPI_KEY}",
        f"https://newsapi.org/v2/top-headlines?country=in&pageSize=40&apiKey={NEWSAPI_KEY}",
        f"https://newsapi.org/v2/everything?q=india+politics+economy&language=en&pageSize=30&sortBy=publishedAt&apiKey={NEWSAPI_KEY}",
        f"https://newsapi.org/v2/everything?q=sports+cricket+ipl&language=en&pageSize=30&sortBy=publishedAt&apiKey={NEWSAPI_KEY}",
    ]
    async with httpx.AsyncClient(timeout=15) as client:
        for url in queries:
            try:
                r = await client.get(url)
                data = r.json()
                for a in data.get("articles", []):
                    title = (a.get("title") or "").strip()
                    body = a.get("content") or a.get("description") or ""
                    link = (a.get("url") or "").strip()
                    if not title or not link or "[Removed]" in title:
                        continue
                    clean = clean_html_fragments(body)
                    pid, tags = classify_article(title, clean)
                    scope = classify_scope(title, clean)
                    articles.append({
                        "url": link,
                        "title_hash": title_fingerprint(title),
                        "headline": title,
                        # 0.2: the publisher's headline is kept ONLY for the originality diff.
                        # `headline` is overwritten by our own once the AI pass runs; until then
                        # the row is parked and excluded from the feed.
                        "source_headline": title,
                        "status": "pending_rewrite",
                # 0.2: the publisher's headline is kept ONLY for the originality diff.
                # `headline` is overwritten by our own once the AI pass runs; until then
                # the row is parked and excluded from the feed.
                "source_headline": title,
                "status": "pending_rewrite",
                        "summary_60": clean[:400],
                        "full_body": clean,
                        "source_summary": (a.get("description") or "")[:200],
                        "when_info": a.get("publishedAt", datetime.now().isoformat()),
                        "where_info": "Not specified",
                        "what_info": title,
                        "how_info": "",
                        "image_url": "",
                        "source_image_url": a.get("urlToImage") or "",

                        "source_name": (a.get("source") or {}).get("name", "NewsAPI"),
                        "pillar_id": pid,
                        "micro_tags": json.dumps(tags),
                        "scope": scope,
                        "published_at": timestamps.to_canonical(
                            a.get("publishedAt")) or timestamps.now_canonical(),
                        "ai_processed": 0,
                    })
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning("NewsAPI failed: %s", e)
    return articles


def _insert_with_dedup(conn, article: dict) -> bool:
    """Insert an article, skipping if URL or title-hash already exists."""
    # Title-hash dedup — catches the same story from 3 different sources
    existing = conn.execute(
        "SELECT id FROM articles WHERE title_hash=?", (article["title_hash"],)
    ).fetchone()
    if existing:
        return False
    # Defaults so any caller that predates 0.2 still inserts — and parks, rather than
    # publishing an unchecked row by omission.
    article.setdefault("source_headline", article.get("headline", ""))
    article.setdefault("source_image_url", "")
    article.setdefault("status", "pending_rewrite")
    try:
        cur = conn.execute("""
            INSERT OR IGNORE INTO articles
            (url, title_hash, headline, source_headline, status, summary_60, full_body,
             source_summary, when_info, where_info, what_info, how_info, image_url,
             source_image_url, source_name, pillar_id, micro_tags, scope, published_at)
            VALUES(:url, :title_hash, :headline, :source_headline, :status, :summary_60,
                   :full_body, :source_summary, :when_info, :where_info, :what_info,
                   :how_info, :image_url, :source_image_url, :source_name, :pillar_id,
                   :micro_tags, :scope, :published_at)
        """, article)
        # cur.rowcount, NOT conn.total_changes. total_changes is CUMULATIVE for
        # the connection, so once a single row had ever landed it stayed > 0 and
        # every subsequent call reported success — including the ones ON CONFLICT
        # skipped as duplicates. That made "[CYCLE] inserted=N" count the whole
        # batch every cycle regardless of what was actually written. rowcount is
        # per-statement, and is 0 for a skipped duplicate on both backends.
        return (cur.rowcount or 0) > 0
    except Exception as e:
        # WARNING, not debug. A broken INSERT here means the collector reports
        # "0 new articles inserted" every cycle while looking perfectly healthy —
        # which is exactly how a placeholder-dialect mismatch stayed invisible.
        log.warning("[DB] insert failed for %s: %s", (article.get("url") or "?")[:80], e)
        return False


def _gate_article(headline: str, body: str, source_headline: str,
                  source_body: str, ai_result: dict | None = None) -> tuple[str, dict]:
    """Run both originality gates. Returns (status, audit).

    status is 'published' only when the headline is genuinely ours AND the body
    clears the overlap gate. A failed BODY check stays absolute: a body that
    overlaps the source is a reproduction, and nothing overrides that.

    The one exception is the headline check, and only when both AI providers are
    down. The rewrite is what makes a headline ours, so with no provider there is
    nothing to compare and every article parks — which is precisely how the corpus
    reached 1600+ parked rows and the feed served nothing. In that case we publish
    on the AGGREGATOR posture instead: the publisher's headline kept with visible
    credit and an outbound link, the body our own stub, never theirs. That is the
    same posture scripts/publish_pending.py and the startup drain already take,
    and the audit records it so these rows stay findable for a later rewrite.
    """
    head_ok, head_m = headline_is_original(headline, source_headline)
    body_ok, body_m = originality_check(body, source_body)
    audit = {"status": None, "headline": head_m, "body": body_m}

    if head_ok and body_ok:
        status = "published"
    elif not body_ok:
        status = "blocked_originality"
    elif ai_result and ai_result.get("publish_as_aggregator"):
        status = "published"
        audit["ai_fallback"] = True
        audit["posture"] = "aggregator"
        audit["classifier"] = ai_result.get("classifier", {})
        audit["note"] = ("both AI providers failed; published with the publisher's "
                         "headline under credit and our own body stub")
    else:
        status = "pending_rewrite"

    audit["status"] = status
    return status, audit


# Mirrors ai_processor._SAFE_BODY. Our own words, used wherever a body cannot be
# AI-written — never the publisher's text.
_SAFE_BODY_TEXT = (
    "Sherr AI is preparing an original, plain-language summary of this story — "
    "the key facts, who is involved and why it matters will appear here shortly. "
    "Use the source link to read the full report at the original publisher."
)


def _apply_aggregator_posture(result: dict, row) -> None:
    """Attach the credit and the source link that the aggregator posture requires.

    Our stub already replaces the publisher's prose; this adds the attribution
    that makes keeping their HEADLINE defensible. Mutates `result` in place so
    both AI batch paths get it from one call.
    """
    try:
        src = (row["source_name"] if "source_name" in row.keys() else "") or ""
        url = (row["url"] if "url" in row.keys() else "") or ""
    except Exception:
        src, url = "", ""
    credit = f"Source: {src or 'the original publisher'}"
    result["full_body"] = f"{_SAFE_BODY_TEXT}\n\n{credit}\n{url}".strip()


async def run_ai_batch(conn):
    """Pull unprocessed articles and refine them with Gemini in parallel."""
    rows = conn.execute(
        # source_name and url are here for the aggregator fallback: a credit
        # line and an outbound link are what make it aggregation rather than
        # a bare reproduction, so they are not optional.
        "SELECT id, headline, source_headline, full_body, summary_60, "
        "source_summary, pillar_id, micro_tags, "
        "source_name, url FROM articles WHERE ai_processed=0 "
        "ORDER BY collected_at DESC LIMIT ?",
        (AI_BATCH_SIZE,)
    ).fetchall()

    if not rows:
        return 0

    providers = available_providers()
    if providers["primary"] == "rule-based":
        log.info("[AI] No API keys configured — skipping refinement pass")
        return 0

    batch_input = []
    for row in rows:
        fallback_slug = PILLARS.get(row["pillar_id"], PILLARS[3])["slug"]
        batch_input.append({
            "title": row["headline"],
            # NOT row["full_body"]: on a row the startup drain already released,
            # that is our own stub, and summarizing a stub yields another stub.
            # source_material() prefers the longest surviving publisher text.
            "body": body_state.source_material(
                row["headline"], row["summary_60"], row["source_summary"],
                row["full_body"]),
            "fallback_category": fallback_slug,
        })

    log.info("[AI] Processing %d articles via %s (concurrency=%d)",
             len(batch_input), providers["primary"], AI_CONCURRENCY)

    try:
        processed = await process_batch(batch_input, concurrency=AI_CONCURRENCY)
    except Exception as e:
        log.error("[AI] Batch failed entirely: %s", e)
        return 0

    success = 0
    for row, result in zip(rows, processed):
        try:
            new_pid = SLUG_TO_PILLAR.get(result["category"], row["pillar_id"])
            # Merge tags: AI-generated + originally classified
            existing_tags = json.loads(row["micro_tags"] or "[]")
            all_tags = list(dict.fromkeys(result["topic_tags"] + existing_tags))[:10]

            # ── 0.2 + 0.4: both gates run before anything can be published ──
            src_head = row["source_headline"] or row["headline"] or ""
            status, audit = _gate_article(
                result["refined_title"], result["full_body"], src_head,
                row["full_body"] or "", ai_result=result)
            if audit.get("posture") == "aggregator":
                _apply_aggregator_posture(result, row)

            # source_summary IS DELIBERATELY ABSENT FROM THIS UPDATE.
            #
            # It used to be set to result["summary"] — OUR text — under a
            # comment reading "kept for back-compat". That overwrote the
            # publisher's own words, and this schema keeps no other copy of
            # them: ingest writes clean[:200] into source_summary and nothing
            # else preserves the source.
            #
            # Two things broke as a result. body_state.classify uses
            # source_summary as the ORIGINALITY REFERENCE, so once a row had
            # been through this pass the gate was comparing our body against
            # our own summary — checking text against itself. And
            # source_material() had nothing left to rewrite FROM, so every
            # retry regenerated the placeholder. Rows now reported as
            # "no_source_material" are the ones this line destroyed; for those
            # the publisher text is gone and only re-ingest recovers it.
            conn.execute("""
                UPDATE articles SET
                    headline=?, summary_60=?, full_body=?,
                    when_info=?, where_info=?, pillar_id=?, micro_tags=?,
                    is_trending=?, sentiment=?, ai_processed=1, reprocessed=1,
                    status=?, originality_json=?, originality_overlap=?,
                    originality_run=?, originality_checked_at=?
                WHERE id=?
            """, (
                # A headline is the ONE field a card cannot render without.
                # process_batch's fallback returned refined_title="" whenever the
                # providers were down, and this wrote it straight in — which is
                # why the feed showed cards with an image, a byline and NO TITLE.
                # Never blank it; keep whatever the row already had.
                (result.get("refined_title") or "").strip()
                    or (row["headline"] or "").strip()
                    or (row["source_headline"] or "").strip(),
                result["summary"],
                result["full_body"],
                result.get("when_info", ""),
                result.get("where_info", "Not specified"),
                new_pid,
                json.dumps(all_tags),
                1 if result["is_trending"] else 0,
                result["sentiment"],
                status,
                json.dumps(audit),
                audit["body"]["overlap"],
                audit["body"]["longest_run"],
                datetime.now(timezone.utc).isoformat(),
                row["id"],
            ))
            success += 1
        except Exception as e:
            log.warning("[AI] Update failed for id %d: %s", row["id"], e)

    conn.commit()
    log.info("[AI] %d/%d articles refined", success, len(rows))
    return success


# ─── STORY THREADS ("the string") ────────────────────────────────────────────
# Lightweight, embedding-free clustering: articles that share ≥2 significant
# terms (proper-noun-ish tokens + AI topic tags) within a rolling window are
# linked into one chronological thread. No external model needed at runtime.
_STORY_STOPWORDS = set((
    "the a an and or of to in on for with at by from as is are was were be been "
    "being this that these those it its he she they them his her their our your "
    "you we new say says said report reports amid over after before into out up "
    "down off than then when what which who whom how why will would can could may "
    "might must not no yes but if about first also more most other some such only "
    "just very now get got make made back two one year years day days week weeks"
).split())


def _story_terms(headline: str, tags: list) -> set:
    """Significant terms used to decide if two articles belong to one thread."""
    terms = set()
    for t in (tags or []):
        t = str(t).strip().lower()
        if len(t) >= 3:
            terms.add(t)
    for w in re.findall(r"[a-z0-9]{4,}", (headline or "").lower()):
        if w not in _STORY_STOPWORDS:
            terms.add(w)
    return terms


def link_stories(conn, window_days: int = STORY_WINDOW_DAYS) -> int:
    """Cluster recent AI-processed articles into story threads via shared terms.

    Uses union-find over an inverted term index. Over-generic terms (appearing
    in many articles) are ignored so unrelated stories don't merge. Returns the
    number of multi-article threads formed.
    """
    rows = conn.execute(
        "SELECT id, headline, micro_tags, pillar_id FROM articles "
        "WHERE ai_processed=1 AND status='published' AND published_at >= datetime('now', ?) "
        "ORDER BY id ASC",
        (f"-{int(window_days)} days",)
    ).fetchall()
    if len(rows) < 2:
        return 0

    # Threads stay within one category — a story only links articles of the
    # same pillar, so unrelated stories that merely share a word don't merge.
    pillar_of = {r["id"]: r["pillar_id"] for r in rows}

    inverted: dict[str, list] = {}
    for r in rows:
        try:
            tags = json.loads(r["micro_tags"] or "[]")
        except Exception:
            tags = []
        for term in _story_terms(r["headline"], tags):
            inverted.setdefault(term, []).append(r["id"])

    parent = {r["id"]: r["id"] for r in rows}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[max(ra, rb)] = min(ra, rb)

    # Count shared terms per co-occurring pair; skip terms that are too generic
    # (would merge everything) or unique (link nothing).
    pair_shared: dict[tuple, int] = {}
    for term, ids in inverted.items():
        if len(ids) < 2 or len(ids) > 40:
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = ids[i], ids[j]
                key = (a, b) if a < b else (b, a)
                pair_shared[key] = pair_shared.get(key, 0) + 1

    MIN_SHARED = 2
    for (a, b), shared in pair_shared.items():
        if shared >= MIN_SHARED and pillar_of.get(a) == pillar_of.get(b):
            union(a, b)

    roots = {r["id"]: find(r["id"]) for r in rows}
    sizes: dict[int, int] = {}
    for root in roots.values():
        sizes[root] = sizes.get(root, 0) + 1

    for aid, root in roots.items():
        sid = root if sizes[root] >= 2 else 0
        conn.execute("UPDATE articles SET story_id=? WHERE id=?", (sid, aid))
    conn.commit()

    threads = sum(1 for s in sizes.values() if s >= 2)
    log.info("[STORY] linked %d articles into %d threads", len(rows), threads)
    return threads


async def collect_news():
    log.info("[CRON] Collection cycle start")
    try:
        rss_articles = await collect_rss()
        news_articles = await collect_newsapi()
        all_articles = rss_articles + news_articles

        # In-batch dedup by title_hash (same story from 3 sources this cycle)
        seen_hashes = set()
        unique = []
        for a in all_articles:
            if a["title_hash"] in seen_hashes:
                continue
            seen_hashes.add(a["title_hash"])
            unique.append(a)

        log.info("[DEDUP] %d unique of %d raw (dropped %d intra-batch dupes)",
                 len(unique), len(all_articles), len(all_articles) - len(unique))

        conn = get_db()
        loop = asyncio.get_event_loop()

        # OFF THE EVENT LOOP. The driver is synchronous, so this loop is one
        # blocking round trip per article — over the Supabase pooler, for a batch
        # of a hundred-odd, that is seconds of a completely stalled loop during
        # which nothing else is served. Combined with a health check that also
        # hit the database, it was enough for Render to time out its probe and
        # restart the instance mid-cycle, forever.
        def _write_batch() -> int:
            n = 0
            for a in unique:
                if _insert_with_dedup(conn, a):
                    n += 1
            conn.commit()
            return n

        new_count = await loop.run_in_executor(None, _write_batch)
        log.info("[DB] %d new articles inserted", new_count)

        # AI refinement pass
        await run_ai_batch(conn)

        # What actually came out the far end. "N inserted" says nothing about
        # whether any of it is servable, and servable is the only thing the feed
        # cares about. Two more queries, so also off the loop.
        def _summary() -> str:
            by_status = {r["status"] or "?": r["c"] for r in conn.execute(
                "SELECT status, COUNT(*) AS c FROM articles GROUP BY status")}
            servable = conn.execute(
                "SELECT COUNT(*) AS c FROM articles "
                "WHERE ai_processed=1 AND status='published'").fetchone()["c"]
            return "inserted=%d servable=%d by_status=%s" % (
                new_count, servable, by_status)

        try:
            log.info("[CYCLE] %s", await loop.run_in_executor(None, _summary))
        except Exception as e:
            log.warning("[CYCLE] status summary failed: %s", e)

        # Link related articles into chronological story threads ("the string").
        # Another whole-corpus synchronous walk — same reason, same treatment.
        try:
            await loop.run_in_executor(None, link_stories, conn)
        except Exception as e:
            log.warning("[STORY] linking failed: %s", e)

        # Stats
        for pid in range(1, 10):
            cnt = conn.execute(
                "SELECT COUNT(*) as c FROM articles WHERE pillar_id=?", (pid,)
            ).fetchone()["c"]
            log.info("  Pillar %d [%s]: %d", pid, PILLARS[pid]["slug"], cnt)

        conn.close()
        log.info("[CRON] Cycle complete")
    except Exception as e:
        log.error("[CRON] collect_news crashed: %s", e, exc_info=True)


# ─── FEED ALGORITHM ──────────────────────────────────────────────────────────
def compute_feed_for_user(user_id: int):
    conn = get_db()
    prefs = conn.execute(
        "SELECT topic_name, pillar_id, weight FROM user_preferences WHERE user_id=?",
        (user_id,)
    ).fetchall()
    if not prefs:
        conn.close()
        return

    pref_pillars = {}
    pref_topics = {}
    for p in prefs:
        pref_pillars[p["pillar_id"]] = pref_pillars.get(p["pillar_id"], 0) + p["weight"]
        pref_topics[p["topic_name"].lower()] = p["weight"]

    cutoff = (datetime.now() - timedelta(days=7)).isoformat()
    articles = conn.execute(
        "SELECT id, pillar_id, micro_tags, published_at, engagement, is_trending "
        "FROM articles WHERE published_at > ? ORDER BY published_at DESC LIMIT 500",
        (cutoff,)
    ).fetchall()

    for art in articles:
        pillar_score = pref_pillars.get(art["pillar_id"], 0)
        tags = json.loads(art["micro_tags"] or "[]")
        tag_score = sum(pref_topics.get(t.lower(), 0) for t in tags)
        # Both sides AWARE. This was fromisoformat() against a naive
        # datetime.now(): once published_at carries an offset that subtraction
        # raises, the except swallows it, and every article silently scores the
        # same 0.5 recency — the ranking quietly stops ranking.
        pub = timestamps.parse(art["published_at"])
        if pub is None:
            recency = 0.5
        else:
            hours_ago = max(0.0, (datetime.now(timezone.utc) - pub).total_seconds() / 3600)
            recency = 1.0 / (1.0 + math.log1p(hours_ago / 4))
        engagement_boost = math.log1p(art["engagement"]) * 0.1
        trending_boost = 0.5 if art["is_trending"] else 0
        serendipity = 0.1 * (abs(hash(str(art["id"]) + str(user_id))) % 100) / 100
        score = (pillar_score * 2 + tag_score * 3) * recency + engagement_boost + trending_boost + serendipity
        if score > 0.05:
            conn.execute(
                "INSERT OR REPLACE INTO feeds (user_id, article_id, score, computed_at) "
                "VALUES(?, ?, ?, datetime('now'))",
                (user_id, art["id"], score)
            )
    conn.commit()
    conn.close()


# ─── FASTAPI APP ─────────────────────────────────────────────────────────────
from fastapi import FastAPI, HTTPException, Header, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    init_activity_schema()
    asyncio.create_task(collect_news())
    scheduler.add_job(collect_news, "interval", minutes=COLLECT_INTERVAL_MIN, id="collect_news")
    # Explore page feeds — six jobs on their own intervals, plus one warm-up pass so
    # the first request hits a populated cache instead of waiting for the slowest job.
    try:
        import explore_feeds
        explore_feeds.register_jobs(scheduler)
        asyncio.create_task(explore_feeds.refresh_all())
    except Exception as e:
        log.error("explore feeds not started: %s", e)
    # Sherr-I's historical price store: one daily close per symbol, appended
    # nightly. Inert without a Postgres DSN (the job reports "skipped" rather
    # than raising), so local sqlite development is unaffected. Seed the 90-day
    # history it measures against with scripts/backfill_ticks.py.
    try:
        import market_ticks
        market_ticks.register_jobs(scheduler)
    except Exception as e:
        log.error("market ticks job not started: %s", e)
    # The Sherr-I detector pass, in THIS process. The engine's own scheduler is
    # in sherrbyte/app/main.py, which render.yaml does not start, and the GitHub
    # Actions cron cannot reach the database — so nothing had ever run the
    # detectors in production and every insight carried the seed date. 02:10 UTC
    # is after market_ticks_daily (01:30), which is one of the inputs.
    try:
        scheduler.add_job(detectors_job, "cron", hour=2, minute=10,
                          id="sherr_i_detectors", replace_existing=True,
                          max_instances=1)
        log.info("Sherr-I detectors scheduled @ 02:10 UTC")
    except Exception as e:
        log.error("detector job not started: %s", e)
    # Bodies still holding the drain's stub or the publisher's own text. Nightly
    # and bounded; /admin/reprocess-bodies is the same pass on demand.
    try:
        scheduler.add_job(body_reprocess_job, "cron", hour=3, minute=20,
                          id="body_reprocess", replace_existing=True,
                          max_instances=1)
        log.info("Body reprocess scheduled @ 03:20 UTC (%d/run)",
                 BODY_REPROCESS_NIGHTLY)
    except Exception as e:
        log.error("body reprocess job not started: %s", e)
    scheduler.start()
    log.info("Scheduler: collect every %d min", COLLECT_INTERVAL_MIN)
    # Inline, because the point is that the feed is servable by the time the first
    # request arrives — as a background task it was one more thing that could be
    # starved before it ran.
    #
    # But BOUNDED. Uvicorn does not accept connections until lifespan startup
    # returns, so a slow drain holds the port closed and Render's health check
    # cannot even connect — which is the same boot loop by a different route. On
    # sqlite this was a third of a second; over the Supabase pooler it is a round
    # trip per row and a large backlog can run long. So it gets a budget, and if
    # it overruns, boot continues and the drain finishes in the background.
    try:
        await asyncio.wait_for(
            asyncio.shield(asyncio.get_event_loop().run_in_executor(
                None, _drain_pending_if_stalled)),
            timeout=DRAIN_BOOT_BUDGET_S)
    except asyncio.TimeoutError:
        log.warning("[DRAIN] still running after %ss — continuing boot, "
                    "the drain finishes in the background",
                    DRAIN_BOOT_BUDGET_S)
    except Exception as e:
        log.error("[DRAIN] boot drain failed: %s", e)
    log.info("AI providers: %s", available_providers())
    # One census at boot: "how many articles still show a stub" is the number
    # this deployment has had no way to see.
    try:
        _c = get_db()
        try:
            _a = body_state.audit(_c)
            log.info("[BODY] census: %s | needs rewrite: %d of %d published",
                     _a["published_by_state"], _a["needs_rewrite"],
                     sum(_a["published_by_state"].values()))
        finally:
            _c.close()
    except Exception as e:
        log.warning("[BODY] census failed: %s", e)
    yield
    scheduler.shutdown()


app = FastAPI(title="SherByte API", version="5.0.0", lifespan=lifespan)
app.add_middleware(
    # allow_origins=["*"] WITH allow_credentials=True is rejected outright by every
    # browser (the spec forbids the pair), so credentialed calls were silently failing
    # while the wildcard still advertised the API to any origin. Pin real origins.
    CORSMiddleware, allow_origins=CORS_ORIGINS, allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

app.include_router(activity_router)
app.include_router(markets_router)

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def get_current_user(authorization: str = "") -> int:
    """Lenient: public endpoints stay readable without a session."""
    if authorization and authorization.startswith("Bearer "):
        uid = verify_token(authorization[7:])
        if uid:
            return uid
    return 1  # Anonymous user default


def require_user(authorization: str = "") -> int:
    """Strict: for endpoints that describe a specific person.

    The lenient path above turns a missing OR expired token into anonymous
    uid=1, and when no row with that id exists the handler then answers 404
    "User not found". A 404 is indistinguishable from a broken route, and the
    client only refreshes its token on a 401 — so a stale session left the app
    permanently signed out of itself instead of quietly renewing. 401 is both
    the truth and the thing the client knows how to act on.
    """
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "Sign in to continue")
    uid = verify_token(authorization[7:])
    if not uid:
        raise HTTPException(401, "Session expired — sign in again")
    return uid


def article_row_to_dict(row) -> dict:
    d = dict(row)
    # One shape on the wire whatever the column now holds: a datetime once the
    # ALTER has run on Postgres, a canonical string on sqlite. The client parses
    # this with new Date(), which needs the explicit offset — a naive value is
    # read as the READER's local time, which is what made every card read
    # "1d ago" for an IST reader.
    for _k in ("published_at", "collected_at"):
        if d.get(_k) is not None:
            d[_k] = timestamps.to_iso(d[_k]) or ""
    pid = d.get("pillar_id", 1)
    pillar = PILLARS.get(pid, PILLARS[1])
    d["pillar_name"]  = pillar["name"]
    d["pillar_color"] = pillar["color"]
    d["pillar_emoji"] = pillar["emoji"]
    d["pillar_slug"]  = pillar["slug"]
    d["category"]     = pillar["slug"]
    # Aliases for frontend normalizers
    d["refined_title"]  = d.get("headline", "")
    d["cached_summary"] = d.get("summary_60", "")
    d["isTrending"]     = bool(d.get("is_trending", 0))
    d["story_id"]       = d.get("story_id", 0) or 0
    # The visible byline is always our own brand (bodies are AI-written, not the
    # publisher's text); the original URL stays available as a verify link.
    d["orig_source"]    = d.get("source_name", "")

    # Imagery is resolved at SERVE time, not at ingest, so flipping IMAGE_MODE takes
    # effect on the next request instead of requiring a re-crawl. thumbnail mode
    # surfaces the publisher image WITH credit; anything else leaves image_url as
    # stored and the client falls back to generated art.
    if IMAGE_MODE == "thumbnail":
        if not d.get("image_url"):
            src_img = d.get("source_image_url") or ""
            if src_img:
                d["image_url"] = src_img
                d["image_source"] = "thumbnail"
        # Rows ingested before the gate existed carry an image_url with no
        # image_source. The client refuses an unlabelled third-party URL — by
        # design — so every one of those articles rendered as generated art and
        # the app looked like it had no images at all. In thumbnail mode a stored
        # publisher image IS a credited thumbnail; label it so it can render.
        elif not d.get("image_source"):
            d["image_source"] = "thumbnail"
        if d.get("image_url") and not d.get("image_credit"):
            d["image_credit"] = f"Image: {d['orig_source'] or 'source'}"
    d["source_image_url"] = d.get("source_image_url") or ""

    d["source_name"]    = "SherrByte News"
    d["source"]         = "SherrByte News"
    try:
        d["micro_tags"] = json.loads(d.get("micro_tags") or "[]")
    except Exception:
        d["micro_tags"] = []
    return d


# ─── CACHED STATIC ENDPOINTS ─────────────────────────────────────────────────
@lru_cache(maxsize=1)
def _topics_payload():
    result = []
    for pid, pillar in PILLARS.items():
        result.append({
            "id": pid, "name": pillar["name"], "color": pillar["color"],
            "emoji": pillar["emoji"], "slug": pillar["slug"],
            "sub_pillars": SUB_PILLARS.get(pid, []),
            "topics": [
                {"name": t, "slug": t.lower().replace(" ", "-"), "color": pillar["color"]}
                for t, p in MICRO_TOPICS.items() if p == pid
            ],
        })
    return {"pillars": result}


@lru_cache(maxsize=1)
def _pillars_payload():
    return {"pillars": [{**v, "id": k, "sub_pillars": SUB_PILLARS.get(k, [])}
                        for k, v in PILLARS.items()]}


# ─── PYDANTIC MODELS ─────────────────────────────────────────────────────────
class SignupReq(BaseModel):
    email: str
    password: str
    name: str = ""
    topics: list[str] = []


class LoginReq(BaseModel):
    email: str
    password: str


class InteractReq(BaseModel):
    article_id: int
    action: str
    category: str = ""
    duration_sec: int = 0


class UpdateProfileReq(BaseModel):
    name: Optional[str] = None
    bio: Optional[str] = None
    display_name: Optional[str] = None
    avatar_url: Optional[str] = None
    language: Optional[str] = None
    link: Optional[str] = None


class UpdateTopicsReq(BaseModel):
    topics: list[str] = []
    categories: list[str] = []


# ─── ROUTES ──────────────────────────────────────────────────────────────────
@app.post("/signup")
async def signup(req: SignupReq):
    conn = get_db()
    if conn.execute("SELECT id FROM users WHERE email=?", (req.email,)).fetchone():
        conn.close()
        raise HTTPException(400, "Email already registered")
    pw_hash = hash_password(req.password)
    # RETURNING rather than lastrowid: sqlite has supported it since 3.35 and
    # Postgres has no lastrowid at all, so this is the one form both backends
    # answer identically.
    cur = conn.execute(
        "INSERT INTO users (email, password, name) VALUES(?, ?, ?) RETURNING id",
        (req.email, pw_hash, req.name or req.email.split("@")[0])
    )
    row = cur.fetchone()
    user_id = row["id"] if row is not None else cur.lastrowid
    for topic in req.topics:
        pid = MICRO_TOPICS.get(topic, 1)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO user_preferences (user_id, topic_name, pillar_id, weight) VALUES(?,?,?,1.0)",
                (user_id, topic, pid)
            )
        except Exception:
            pass
    conn.commit()
    conn.close()
    asyncio.create_task(asyncio.to_thread(compute_feed_for_user, user_id))
    return {"token": make_token(user_id), "user_id": user_id,
            "display_name": req.name or req.email.split("@")[0], "message": "Account created"}


# ─── Auth rate limiting ───────────────────────────────────────────────────────
# Without this, /auth/login is an unmetered password oracle: PBKDF2 makes each guess
# expensive for the ATTACKER, but nothing stopped them from making unlimited guesses
# against a known email. Two independent buckets — per client IP and per account —
# because one attacker with many IPs and many attackers on one IP are different
# attacks. In-memory, so it resets on redeploy and does not span replicas; that is a
# deliberate trade (no new infra) and it still removes the trivial case.
_AUTH_HITS: dict = {}
AUTH_MAX_ATTEMPTS = 8
AUTH_WINDOW_SEC = 300


def _client_ip(request) -> str:
    # Render terminates TLS upstream, so the socket peer is the proxy.
    fwd = (request.headers.get("x-forwarded-for") or "").split(",")[0].strip()
    return fwd or (request.client.host if request.client else "unknown")


def auth_rate_limit(*keys: str) -> None:
    """Raise 429 once any bucket exceeds the window. Call before verifying a password."""
    now = time.time()
    for key in keys:
        if not key:
            continue
        hits = [t for t in _AUTH_HITS.get(key, []) if now - t < AUTH_WINDOW_SEC]
        if len(hits) >= AUTH_MAX_ATTEMPTS:
            retry = int(AUTH_WINDOW_SEC - (now - hits[0]))
            raise HTTPException(429, f"Too many attempts. Try again in {retry}s.",
                                headers={"Retry-After": str(max(retry, 1))})
        hits.append(now)
        _AUTH_HITS[key] = hits
    # Bound the dict so a spray across many keys cannot grow it without limit.
    if len(_AUTH_HITS) > 10_000:
        for k in [k for k, v in _AUTH_HITS.items()
                  if not v or now - v[-1] > AUTH_WINDOW_SEC]:
            _AUTH_HITS.pop(k, None)


@app.post("/auth/login")
@app.post("/login")
async def login(req: LoginReq, request: Request):
    auth_rate_limit(f"ip:{_client_ip(request)}", f"acct:{(req.email or '').lower()}")
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email,)).fetchone()
    if not user or not check_password(req.password, user["password"]):
        conn.close()
        raise HTTPException(401, "Invalid credentials")
    # We have the plaintext exactly once per login — the only moment a legacy hash can
    # be upgraded without forcing a password reset.
    if needs_rehash(user["password"]):
        conn.execute("UPDATE users SET password=? WHERE id=?",
                     (hash_password(req.password), user["id"]))
        log.info("upgraded password hash for user %s", user["id"])
    conn.execute("UPDATE users SET last_login=datetime('now') WHERE id=?", (user["id"],))
    conn.commit()
    pref_count = conn.execute(
        "SELECT COUNT(*) as c FROM user_preferences WHERE user_id=?", (user["id"],)
    ).fetchone()["c"]
    conn.close()
    return {"token": make_token(user["id"]), "user_id": user["id"], "name": user["name"],
            "display_name": user["name"], "email": user["email"], "has_topics": pref_count > 0}


@app.post("/auth/register")
async def register(req: SignupReq):
    return await signup(req)


@app.get("/topics")
async def get_topics():
    return _topics_payload()


@app.get("/feed")
async def get_feed(
    page: int = Query(1, ge=1),
    limit: int = Query(20, le=50),
    scope: str = Query(""),
    pillar: int = Query(0),
    authorization: str = Header(""),
):
    uid = get_current_user(authorization)
    offset = (page - 1) * limit
    conn = get_db()
    prefs = conn.execute("SELECT COUNT(*) as c FROM user_preferences WHERE user_id=?", (uid,)).fetchone()
    has_p = prefs["c"] > 0

    if has_p:
        await asyncio.get_event_loop().run_in_executor(None, compute_feed_for_user, uid)
        q = "SELECT a.*, f.score FROM articles a JOIN feeds f ON a.id=f.article_id WHERE f.user_id=? AND a.ai_processed=1 AND a.status='published'"
        p = [uid]
        sc_sql, sc_params = _scope_clause(scope, "a.scope")
        q += sc_sql; p += sc_params
        if pillar:
            q += " AND a.pillar_id=?"; p.append(pillar)
        q += " ORDER BY f.score DESC, a.published_at DESC, a.id DESC LIMIT ? OFFSET ?"
        p += [limit + 1, offset]
        rows = conn.execute(q, p).fetchall()
        if len(rows) < 5:
            rows = conn.execute(
                "SELECT *, 1.0 as score FROM articles WHERE ai_processed=1 AND status='published' "
        "AND COALESCE(TRIM(headline),'') <> '' ORDER BY published_at DESC, id DESC LIMIT ? OFFSET ?",
                (limit + 1, offset)
            ).fetchall()
    else:
        rows = conn.execute(
            "SELECT *, 1.0 as score FROM articles WHERE ai_processed=1 AND status='published' "
        "AND COALESCE(TRIM(headline),'') <> '' ORDER BY published_at DESC, id DESC LIMIT ? OFFSET ?",
            (limit + 1, offset)
        ).fetchall()

    conn.close()
    has_more = len(rows) > limit
    return {"articles": [article_row_to_dict(r) for r in rows[:limit]],
            "page": page, "has_more": has_more, "has_preferences": has_p}


@app.get("/explore")
async def explore_feed(
    category: str = Query(""),
    pillar: int = Query(0),
    scope: str = Query(""),
    page: int = Query(1, ge=1),
    limit: int = Query(30, le=100),
    authorization: str = Header(""),
):
    get_current_user(authorization)
    offset = (page - 1) * limit
    conn = get_db()
    # Scope soft-fallback, matching /feed. Ingest classifies almost everything
    # 'global', so a reader on Local was served an empty Explore page — every
    # category row reading "No stories yet" while the same articles sat one
    # filter away. An empty scope broadens rather than starving.
    if scope and scope.lower() != "global":
        sc_sql, sc_params = _scope_clause(scope)
        have = conn.execute(
            "SELECT COUNT(*) AS c FROM articles WHERE ai_processed=1 "
            "AND status='published'" + sc_sql, sc_params).fetchone()["c"]
        if have == 0:
            scope = ""
    q = "SELECT * FROM articles WHERE ai_processed=1 AND status='published'"
    p = []
    if category and not pillar:
        resolved = FRONTEND_SLUG_MAP.get(category.lower())
        if resolved:
            pillar = resolved
    if pillar:
        q += " AND pillar_id=?"; p.append(pillar)
    sc_sql, sc_params = _scope_clause(scope)
    q += sc_sql; p += sc_params
    q += " ORDER BY published_at DESC, id DESC LIMIT ? OFFSET ?"
    p += [limit + 1, offset]
    rows = conn.execute(q, p).fetchall()
    conn.close()
    has_more = len(rows) > limit
    return {"articles": [article_row_to_dict(r) for r in rows[:limit]], "has_more": has_more}


# ─── Explore page snapshot ────────────────────────────────────────────────────
# ─── /live/* ─────────────────────────────────────────────────────────────────
# The Explore weather tile, the Word of the Day card and the dictionary lookup
# have been calling these three since they were written; the routes were never
# added, so all three 404'd and each tile sat on its loading state forever.
#
# explore_feeds already fetches weather and word_of_day on a schedule, but its
# shapes are not these: it pins one latitude/longitude, and it returns the
# definition flat while the card reads meanings[0].part_of_speech. Rather than
# reshape a cached payload into something it is not, these fetch directly — the
# weather tile is per-reader by nature (it takes the browser's coordinates) and
# a dictionary lookup is a user action, neither of which a shared 12-hour cache
# can answer.

# Reused from the Explore pipeline rather than kept as a second copy — a
# divergent list would mean the tile and the card disagree about the day's word.
def _word_list() -> list:
    try:
        import explore_feeds                                  # noqa: PLC0415
        words = getattr(explore_feeds._mod, "WORD_LIST", None)
        if words:
            return list(words)
    except Exception as e:
        log.warning("[LIVE] word list unavailable, using fallback: %s", e)
    return ["serendipity", "ephemeral", "quixotic", "lucid", "candid",
            "resilient", "eloquent", "pragmatic", "nuance", "salient"]


WORD_OF_DAY_LIST = _word_list()
_wod_cache: dict = {}


def _dict_payload(word: str, entry: dict) -> dict:
    """dictionaryapi.dev -> the shape the cards read.

    The card does meanings[0].part_of_speech; the upstream nests definitions one
    level deeper than that. Flattening it here keeps that knowledge in one place.
    """
    out = []
    for m in (entry.get("meanings") or [])[:4]:
        d0 = (m.get("definitions") or [{}])[0]
        out.append({"part_of_speech": m.get("partOfSpeech") or "",
                    "definition": d0.get("definition") or "",
                    "example": d0.get("example") or ""})
    return {"word": entry.get("word") or word,
            "phonetic": entry.get("phonetic") or next(
                (p.get("text") for p in (entry.get("phonetics") or [])
                 if p.get("text")), ""),
            "meanings": out}


async def _lookup_word(word: str) -> dict:
    """404 when the word genuinely has no entry, 503 when the upstream is the
    problem. Letting a transport error escape turns an outage into an unhandled
    500 with a stack trace, and tells the caller nothing about which it was."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get(
                f"https://api.dictionaryapi.dev/api/v2/entries/en/{word}")
    except Exception as e:
        log.warning("[LIVE] dictionary upstream failed for %r: %s", word, e)
        raise HTTPException(503, f"dictionary upstream unavailable: {e}")
    if r.status_code == 404:
        raise HTTPException(404, f"no dictionary entry for {word!r}")
    if r.status_code != 200:
        raise HTTPException(503, f"dictionary upstream returned {r.status_code}")
    try:
        return _dict_payload(word, (r.json() or [{}])[0])
    except Exception as e:
        raise HTTPException(503, f"unreadable dictionary payload: {e}")


@app.get("/live/weather")
async def live_weather(lat: float = Query(19.076), lon: float = Query(72.877)):
    """Current conditions for the reader's coordinates (Mumbai if not supplied)."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            r = await client.get("https://api.open-meteo.com/v1/forecast", params={
                "latitude": lat, "longitude": lon,
                "current": "temperature_2m,relative_humidity_2m,weather_code,"
                           "wind_speed_10m,apparent_temperature",
                "daily": "temperature_2m_max,temperature_2m_min",
                "timezone": "auto", "forecast_days": 3})
            r.raise_for_status()
            d = r.json() or {}
            cur = d.get("current") or {}

            # The tile shows a place name, and coordinates are not one. A failed
            # reverse geocode costs the label, not the temperature.
            city = ""
            try:
                g = await client.get(
                    "https://geocoding-api.open-meteo.com/v1/search",
                    params={"latitude": lat, "longitude": lon, "count": 1})
                if g.status_code == 200:
                    hit = ((g.json() or {}).get("results") or [{}])[0]
                    city = hit.get("name") or ""
            except Exception:
                pass

        daily = d.get("daily") or {}
        return {
            "lat": lat, "lon": lon, "city": city,
            "temp_c": cur.get("temperature_2m"),
            "feels_c": cur.get("apparent_temperature"),
            "humidity": cur.get("relative_humidity_2m"),
            "wind_kph": cur.get("wind_speed_10m"),
            "code": cur.get("weather_code"),
            "high_c": (daily.get("temperature_2m_max") or [None])[0],
            "low_c": (daily.get("temperature_2m_min") or [None])[0],
        }
    except Exception as e:
        log.warning("[LIVE] weather failed: %s", e)
        raise HTTPException(503, f"weather upstream unavailable: {e}")


@app.get("/live/word-of-day")
async def live_word_of_day():
    """One word per UTC day, so every reader sees the same one and a restart
    does not reshuffle it. Cached because it cannot change within the day."""
    day = int(time.time() // 86400)
    hit = _wod_cache.get(day)
    if hit:
        return hit
    word = WORD_OF_DAY_LIST[random.Random(day).randrange(len(WORD_OF_DAY_LIST))]
    try:
        payload = await _lookup_word(word)
    except HTTPException:
        raise                       # already the right status and message
    except Exception as e:
        log.warning("[LIVE] word-of-day failed for %r: %s", word, e)
        raise HTTPException(503, f"dictionary upstream unavailable: {e}")
    _wod_cache.clear()          # only ever one day in flight
    _wod_cache[day] = payload
    return payload


@app.get("/live/dictionary/{word}")
async def live_dictionary(word: str):
    w = (word or "").strip().lower()[:64]
    if not w or not w.replace("-", "").replace("'", "").isalpha():
        raise HTTPException(400, "word must be alphabetic")
    return await _lookup_word(w)


@app.get("/explore/snapshot")
async def explore_snapshot():
    """All Explore sections from cache in one call — no upstream API on this path."""
    import explore_feeds
    return await explore_feeds.snapshot()


@app.post("/admin/explore/refresh")
async def explore_refresh(name: str = Query("")):
    """Force a refresh: all sections, or one by name."""
    import explore_feeds
    if name:
        if name not in explore_feeds.FETCHERS:
            raise HTTPException(404, f"unknown section: {name}")
        return await explore_feeds.refresh(name)
    return await explore_feeds.refresh_all()


@app.get("/explore/pillars")
async def explore_by_pillar(
    per: int = Query(6, ge=1, le=20),
    scope: str = Query(""),
    authorization: str = Header(""),
):
    """Every pillar, with the SAME number of articles each.

    The old page built its rows by slicing one flat recency-ordered feed, so a pillar
    with a busy news day crowded out the quiet ones and the rows came out ragged. Here
    each pillar gets its own top-N, and `per` is clamped to what the THINNEST pillar
    can actually supply — otherwise "equal" would mean padding some rows with older
    material while others stay fresh, which is a different kind of uneven.
    """
    get_current_user(authorization)
    conn = get_db()
    try:
        sc_sql, sc_params = _scope_clause(scope)
        base = ("SELECT COUNT(*) AS c FROM articles "
                "WHERE ai_processed=1 AND status='published' AND pillar_id=?" + sc_sql)
        avail = {pid: conn.execute(base, [pid] + sc_params).fetchone()["c"]
                 for pid in PILLARS}
        stocked = [c for c in avail.values() if c > 0]
        # Clamp to the thinnest STOCKED pillar; an empty pillar is reported as empty
        # rather than dragging every other row to zero.
        n = min(per, min(stocked)) if stocked else 0

        out = []
        for pid, meta in PILLARS.items():
            rows = conn.execute(
                "SELECT * FROM articles WHERE ai_processed=1 AND status='published' "
                "AND pillar_id=?" + sc_sql +
                " ORDER BY published_at DESC, id DESC LIMIT ?",
                [pid] + sc_params + [n]).fetchall() if n else []
            out.append({
                "pillar_id": pid, "name": meta["name"], "slug": meta["slug"],
                "color": meta["color"], "emoji": meta["emoji"],
                "available": avail[pid],
                "articles": [article_row_to_dict(r) for r in rows],
            })
        return {"per_pillar": n, "requested": per, "pillars": out,
                # Surfaced so a short row is explainable rather than looking broken.
                "limited_by": (min(avail, key=lambda k: avail[k] if avail[k] else 10**9)
                               if stocked and n < per else None)}
    finally:
        conn.close()


@app.get("/trending")
async def trending_feed(
    limit: int = Query(10, le=30),
    authorization: str = Header(""),
):
    """Only trending articles — premium feature flagged by the AI pass."""
    get_current_user(authorization)
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM articles WHERE is_trending=1 AND ai_processed=1 AND status='published' ORDER BY published_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}


def insight_row_to_dict(row) -> dict:
    d = dict(row)
    for k in ("entities", "domains"):
        try:
            d[k] = json.loads(d.get(k) or "[]")
        except Exception:
            d[k] = []
    try:
        d["explain_json"] = json.loads(d.get("explain_json") or "{}")
    except Exception:
        d["explain_json"] = {}
    return d


# ─── P0.5 — originality audit ─────────────────────────────────────────────────
@app.get("/admin/originality")
async def admin_originality():
    """Counts by publish status plus the gate's own metrics.

    This is the launch checklist in one call: `blocked_originality` and
    `pending_rewrite` must both be zero on the served feed, and `unchecked` tells us
    how much of the corpus predates the gate and still needs the backfill.
    """
    conn = get_db()
    try:
        by_status = {r["status"] or "published": r["c"] for r in conn.execute(
            "SELECT status, COUNT(*) AS c FROM articles GROUP BY status")}
        checked = conn.execute(
            "SELECT COUNT(*) AS c, AVG(originality_overlap) AS avg_overlap, "
            "MAX(originality_overlap) AS max_overlap, MAX(originality_run) AS max_run "
            "FROM articles WHERE originality_overlap >= 0").fetchone()
        hotlinked = conn.execute(
            "SELECT COUNT(*) AS c FROM articles "
            "WHERE image_url <> '' AND image_url NOT LIKE '%sherrbyte%'").fetchone()["c"]
        total = conn.execute("SELECT COUNT(*) AS c FROM articles").fetchone()["c"]
        return {
            "total": total,
            "by_status": by_status,
            "passed": by_status.get("published", 0),
            "blocked_originality": by_status.get("blocked_originality", 0),
            "pending_rewrite": by_status.get("pending_rewrite", 0),
            "checked": checked["c"],
            "unchecked": total - (checked["c"] or 0),
            "avg_overlap": round(checked["avg_overlap"], 4) if checked["avg_overlap"] is not None else None,
            "max_overlap": checked["max_overlap"],
            "max_contiguous_run": checked["max_run"],
            # Must be 0. Any non-zero value is a live copyright exposure.
            "hotlinked_images": hotlinked,
            "thresholds": {"overlap": MAX_NGRAM_OVERLAP,
                           "longest_run": MAX_CONTIGUOUS_RUN},
        }
    finally:
        conn.close()


@app.get("/patterns")
async def patterns(
    type: str = Query(""),
    limit: int = Query(30, le=100),
    offset: int = Query(0, ge=0),
    max_age_hours: int = Query(None, ge=0, le=8760),
):
    """Sherr-I pattern output — the Intelligence Engine's insights, most significant
    first. Optional ?type=emergence|temporal_correlation.

    Resolution order:
      1. The engine's Postgres (DATABASE_URL / SHERR_I_DATABASE_URL) — the workers write
         insights there and this app reads them from the same DB. Single-service
         deployments need nothing else; ENGINE_URL is NOT required.
      2. ENGINE_URL — only for a separate engine deployment.
      3. Local sqlite demo rows — clearly labelled, never a silent substitute.

    Every response carries `source`: "engine" | "unavailable" | "seed".
    """
    # 1. Same-service path: read the real insights straight from Supabase.
    if SHERR_I_DATABASE_URL:
        data = await _spie_patterns(type, limit, offset, max_age_hours)
        if data is not None:
            return data
        if not ENGINE_URL:
            return {"patterns": [], "total": 0, "source": "unavailable",
                    "detail": "Configured Postgres (DATABASE_URL) is not reachable "
                              "or has no insights table yet."}

    if ENGINE_URL:
        try:
            params = {"limit": limit, "offset": offset}
            if type:
                params["type"] = type
            async with httpx.AsyncClient(timeout=20) as client:
                r = await client.get(f"{ENGINE_URL}/patterns", params=params)
                if r.status_code == 200:
                    data = r.json()
                    data["source"] = "engine"
                    return data
                log.warning("engine /patterns HTTP %d", r.status_code)
                detail = f"engine returned HTTP {r.status_code}"
        except Exception as e:
            log.warning("engine /patterns proxy failed: %s", e)
            detail = str(e)
        # An engine IS configured but is not answering. Never paper over that with
        # demo rows — the caller must be able to tell real from fake.
        return {"patterns": [], "total": 0, "source": "unavailable",
                "engine_url": ENGINE_URL, "detail": detail}

    conn = get_db()
    # Same window on the local path: a stale demo row is exactly as wrong on
    # screen as a stale real one.
    hours = PATTERN_MAX_AGE_HOURS if max_age_hours is None else int(max_age_hours)
    where, p = [], []
    if type:
        where.append("type=?"); p.append(type)
    if hours > 0:
        where.append("created_at >= datetime('now', ?)"); p.append(f"-{int(hours)} hours")
    clause = (" WHERE " + " AND ".join(where)) if where else ""
    q = ("SELECT * FROM demo_insights" + clause
         + " ORDER BY score DESC, created_at DESC LIMIT ? OFFSET ?")
    rows = conn.execute(q, p + [limit, offset]).fetchall()
    total = conn.execute("SELECT COUNT(*) AS c FROM demo_insights" + clause, p).fetchone()["c"]
    conn.close()
    return {"patterns": [insight_row_to_dict(r) for r in rows], "total": total,
            "source": "seed",
            "detail": "No ENGINE_URL configured — these are local demo rows, not live insights."}


@app.get("/patterns/type/{ptype}")
async def patterns_by_type(ptype: str, limit: int = Query(30, le=100),
                           max_age_hours: int = Query(None, ge=0, le=8760)):
    """Second door to the same table — so it carries the same window. A filter
    only one endpoint enforces is a filter one caller can walk around."""
    return await patterns(type=ptype, limit=limit, offset=0,
                          max_age_hours=max_age_hours)


@app.get("/article/{article_id}")
async def get_article(article_id: int, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    row = conn.execute("SELECT * FROM articles WHERE id=?", (article_id,)).fetchone()
    if not row:
        conn.close()
        raise HTTPException(404, "Article not found")
    conn.execute("UPDATE articles SET engagement=engagement+1 WHERE id=?", (article_id,))
    conn.execute(
        "INSERT OR IGNORE INTO user_interactions (user_id, article_id, action) VALUES(?,?,'read')",
        (uid, article_id)
    )
    conn.commit()
    conn.close()
    return article_row_to_dict(row)


@app.post("/interact")
async def interact(req: InteractReq, authorization: str = Header("")):
    uid = get_current_user(authorization)
    if req.action not in ("like", "dislike", "save", "read", "quiz_complete"):
        raise HTTPException(400, "Invalid action")
    conn = get_db()
    art = conn.execute("SELECT pillar_id, micro_tags FROM articles WHERE id=?", (req.article_id,)).fetchone()
    if not art:
        conn.close()
        return {"status": "ok"}
    conn.execute(
        "INSERT OR REPLACE INTO user_interactions (user_id, article_id, action) VALUES(?,?,?)",
        (uid, req.article_id, req.action)
    )
    delta = {"like": 0.3, "save": 0.5, "read": 0.1, "dislike": -0.4, "quiz_complete": 0.2}.get(req.action, 0)
    if delta:
        pid = art["pillar_id"]
        tags = json.loads(art["micro_tags"] or "[]")
        for tag in tags[:3]:
            existing = conn.execute(
                "SELECT id, weight FROM user_preferences WHERE user_id=? AND topic_name=?",
                (uid, tag)
            ).fetchone()
            if existing:
                new_w = max(0.1, min(5.0, existing["weight"] + delta))
                conn.execute("UPDATE user_preferences SET weight=? WHERE id=?", (new_w, existing["id"]))
            else:
                conn.execute(
                    "INSERT INTO user_preferences (user_id, topic_name, pillar_id, weight) VALUES(?,?,?,?)",
                    (uid, tag, pid, max(0.1, 1.0 + delta))
                )
    conn.commit()
    conn.close()
    return {"status": "ok"}


@app.get("/search")
async def search(q: str = Query(""), authorization: str = Header("")):
    get_current_user(authorization)
    if not q:
        return {"articles": []}
    conn = get_db()
    q_like = f"%{q}%"
    rows = conn.execute(
        "SELECT * FROM articles WHERE (headline LIKE ? OR summary_60 LIKE ?) AND ai_processed=1 AND status='published' "
        "ORDER BY published_at DESC LIMIT 25",
        (q_like, q_like)
    ).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}


@app.get("/me")
async def get_me(authorization: str = Header("")):
    uid = require_user(authorization)
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        # A valid token for a row that is gone is also a dead session, not a
        # missing page.
        conn.close()
        raise HTTPException(401, "Session no longer valid — sign in again")
    prefs = conn.execute(
        "SELECT topic_name, pillar_id, weight FROM user_preferences WHERE user_id=? ORDER BY weight DESC",
        (uid,)
    ).fetchall()
    stats = conn.execute("""
        SELECT COUNT(*) as ic,
            COUNT(CASE WHEN action='read' THEN 1 END) as articles_read,
            COUNT(CASE WHEN action='like' THEN 1 END) as likes
        FROM user_interactions WHERE user_id=?
    """, (uid,)).fetchone()
    bm_count = conn.execute("SELECT COUNT(*) as c FROM bookmarks WHERE user_id=?", (uid,)).fetchone()
    conn.close()
    return {
        "id": user["id"], "email": user["email"], "name": user["name"],
        "display_name": user["name"], "bio": user["bio"],
        "avatar_url": user["avatar_url"], "language": user["language"],
        "created_at": user["created_at"],
        "preferences": [
            {"topic": p["topic_name"], "pillar_id": p["pillar_id"],
             "color": PILLARS.get(p["pillar_id"], PILLARS[1])["color"],
             "weight": round(p["weight"], 2)}
            for p in prefs
        ],
        "stats": {
            "articles_read": stats["articles_read"] or 0,
            "likes": stats["likes"] or 0,
            "bookmarks": bm_count["c"] or 0
        },
    }


@app.put("/me")
async def update_profile(req: UpdateProfileReq, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    updates = {}
    display = req.display_name or req.name
    if display: updates["name"] = display
    if req.bio: updates["bio"] = req.bio
    if req.avatar_url: updates["avatar_url"] = req.avatar_url
    if req.language: updates["language"] = req.language
    if updates:
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE users SET {set_clause} WHERE id=?", list(updates.values()) + [uid])
        conn.commit()
    conn.close()
    return {"status": "updated"}


@app.put("/me/topics")
@app.put("/me/feed")
async def update_topics(req: UpdateTopicsReq, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    conn.execute("DELETE FROM user_preferences WHERE user_id=?", (uid,))
    for topic in req.topics:
        pid = MICRO_TOPICS.get(topic, 1)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO user_preferences (user_id, topic_name, pillar_id, weight) VALUES(?,?,?,1.0)",
                (uid, topic, pid)
            )
        except Exception:
            pass
    conn.commit()
    conn.close()
    asyncio.create_task(asyncio.to_thread(compute_feed_for_user, uid))
    return {"status": "ok", "topics_saved": len(req.topics)}


@app.get("/bookmarks")
async def get_bookmarks(authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    rows = conn.execute("""
        SELECT a.* FROM articles a JOIN bookmarks b ON a.id=b.article_id
        WHERE b.user_id=? ORDER BY b.saved_at DESC
    """, (uid,)).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}


@app.post("/bookmarks/{article_id}")
async def toggle_bookmark(article_id: int, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM bookmarks WHERE user_id=? AND article_id=?",
        (uid, article_id)
    ).fetchone()
    if existing:
        conn.execute("DELETE FROM bookmarks WHERE user_id=? AND article_id=?", (uid, article_id))
        saved = False
    else:
        conn.execute("INSERT INTO bookmarks (user_id, article_id) VALUES(?,?)", (uid, article_id))
        saved = True
    conn.commit()
    conn.close()
    return {"saved": saved}


@app.get("/notifications")
async def get_notifications(authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    prefs = conn.execute(
        "SELECT topic_name, pillar_id FROM user_preferences WHERE user_id=? ORDER BY weight DESC LIMIT 5",
        (uid,)
    ).fetchall()
    notifs = []
    for p in prefs:
        rows = conn.execute(
            "SELECT id, headline, pillar_id, image_url FROM articles "
            "WHERE micro_tags LIKE ? ORDER BY published_at DESC LIMIT 2",
            (f'%{p["topic_name"]}%',)
        ).fetchall()
        for r in rows:
            pid = r["pillar_id"]
            notifs.append({
                "article_id": r["id"], "headline": r["headline"],
                "topic": p["topic_name"], "color": PILLARS.get(pid, PILLARS[1])["color"],
                "image_url": r["image_url"], "message": f"New in @{p['topic_name']}"
            })
    conn.close()
    return {"notifications": notifs[:20]}


def _check_admin(token: str):
    if not token or token != ADMIN_TOKEN:
        raise HTTPException(403, "Invalid or missing admin token")


@app.post("/admin/reprocess")
async def admin_reprocess(
    limit: int = Query(50),
    force: int = Query(0),
    x_admin_token: str = Header(""),
):
    """Re-run the AI writer over legacy rows so no verbatim source text remains.

    Targets rows that predate the copyright fix (reprocessed=0). Each row's body
    is passed back through the AI writer, which produces an original SherrByte
    rewrite — or, if the AI providers are unavailable, a neutral placeholder.
    Idempotent: a row is only ever reprocessed once. Call repeatedly (with
    `limit`) to drain the backlog in batches.
    """
    _check_admin(x_admin_token)

    providers = available_providers()
    ai_up = providers["primary"] != "rule-based"
    # Guard: without working AI keys, reprocessing would overwrite every targeted
    # row with a placeholder. Require force=1 to opt into that scrub explicitly.
    if not ai_up and not force:
        conn = get_db()
        remaining = conn.execute(
            "SELECT COUNT(*) c FROM articles WHERE ai_processed=1 AND status='published' AND COALESCE(reprocessed,0)=0"
        ).fetchone()["c"]
        conn.close()
        return {
            "reprocessed": 0, "remaining": remaining, "ai": providers["primary"],
            "note": "AI providers unavailable — pass force=1 to scrub these rows "
                    "to neutral placeholders anyway, or configure GEMINI/GROQ keys first.",
        }

    limit = max(1, min(int(limit), 200))
    conn = get_db()
    rows = conn.execute(
        # source_name/url: same reason as the batch pass — the aggregator
        # fallback needs a credit line and a link, and _apply_aggregator_posture
        # reads them off this row.
        "SELECT id, headline, full_body, summary_60, source_summary, pillar_id, "
        "micro_tags, source_name, url "
        "FROM articles WHERE ai_processed=1 AND status='published' "
        "AND COALESCE(reprocessed,0)=0 ORDER BY id ASC LIMIT ?",
        (limit,)
    ).fetchall()

    if not rows:
        conn.close()
        return {"reprocessed": 0, "remaining": 0, "ai": providers["primary"],
                "note": "nothing pending — all rows already reprocessed"}

    batch_input = [{
        "title": r["headline"],
        # Same reason as run_ai_batch: full_body on a drained row is our stub.
        "body": body_state.source_material(
            r["headline"], r["summary_60"], r["source_summary"], r["full_body"]),
        "fallback_category": PILLARS.get(r["pillar_id"], PILLARS[3])["slug"],
    } for r in rows]

    processed = await process_batch(batch_input, concurrency=AI_CONCURRENCY)

    done = 0
    for r, result in zip(rows, processed):
        try:
            new_pid = SLUG_TO_PILLAR.get(result["category"], r["pillar_id"])
            existing_tags = json.loads(r["micro_tags"] or "[]")
            all_tags = list(dict.fromkeys(result["topic_tags"] + existing_tags))[:10]
            # ── 0.2 + 0.4: both gates run before anything can be published ──
            #
            # `r`, NOT `row`. This block read a name that is never bound in this
            # function, so every iteration raised NameError, was swallowed by the
            # except below, and logged "[REPROCESS] update failed". The endpoint
            # has never updated a single row.
            src_head = r["source_headline"] or r["headline"] or ""
            status, audit = _gate_article(
                result["refined_title"], result["full_body"], src_head,
                r["full_body"] or "", ai_result=result)
            if audit.get("posture") == "aggregator":
                _apply_aggregator_posture(result, r)

            # source_summary is absent here for the same reason as in
            # run_ai_batch: it holds the publisher's only surviving text, and
            # overwriting it destroys both the originality reference and the
            # material a retry would rewrite from.
            #
            # The value list was also five short of the placeholders — status
            # and the four originality columns were never supplied — so even
            # with the name fixed this statement could not have run.
            conn.execute("""
                UPDATE articles SET
                    headline=?, summary_60=?, full_body=?,
                    when_info=?, where_info=?, pillar_id=?, micro_tags=?,
                    is_trending=?, sentiment=?, ai_processed=1, reprocessed=1,
                    status=?, originality_json=?, originality_overlap=?,
                    originality_run=?, originality_checked_at=?
                WHERE id=?
            """, (
                (result.get("refined_title") or "").strip()
                    or (r["headline"] or "").strip()
                    or (r["source_headline"] or "").strip(),
                result["summary"],
                result["full_body"],
                result.get("when_info", ""),
                result.get("where_info", "Not specified"),
                new_pid,
                json.dumps(all_tags),
                1 if result["is_trending"] else 0,
                result["sentiment"],
                status,
                json.dumps(audit),
                audit["body"]["overlap"],
                audit["body"]["longest_run"],
                datetime.now(timezone.utc).isoformat(),
                r["id"],
            ))
            done += 1
        except Exception as e:
            log.warning("[REPROCESS] update failed for id %s: %s", r["id"], e)

    conn.commit()
    remaining = conn.execute(
        "SELECT COUNT(*) c FROM articles WHERE ai_processed=1 AND status='published' AND COALESCE(reprocessed,0)=0"
    ).fetchone()["c"]

    # Content changed — refresh story threads so headlines/tags re-cluster.
    try:
        link_stories(conn)
    except Exception as e:
        log.warning("[REPROCESS] relink failed: %s", e)

    conn.close()
    log.info("[REPROCESS] %d rows reprocessed, %d remaining", done, remaining)
    return {"reprocessed": done, "remaining": remaining, "ai": providers["primary"]}


def _run_drain_for_backend(*, dry_run: bool = False, limit=None) -> dict:
    """Drain through get_db(), whatever it is connected to.

    THE DRAIN FOLLOWS THE SCHEMA, NOT THE DRIVER. This used to branch on
    USE_POSTGRES and call run_postgres, which queries info_objects joined to
    articles — the Sherr-I engine's two-table layout. But this app carries its
    OWN single-table schema to Postgres through pgcompat, in its own
    sherrbyte_app schema, and info_objects does not exist there. So the drain
    reported found=0 against a backlog of 2343 rows: it was reading a table that
    was not the one filling up.

    One connection, one query, one table. pgcompat translates the '?' and the
    rest, so the sqlite-shaped statements are correct on both.
    """
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
    import publish_pending                                   # noqa: PLC0415
    conn = get_db()
    try:
        out = publish_pending.drain_articles(
            conn, mode="aggregator", dry_run=dry_run, limit=limit)
    finally:
        conn.close()
    out["backend"] = "postgres" if USE_POSTGRES else "sqlite"
    return out


def _drain_pending_if_stalled() -> dict:
    """Release EVERY pending_rewrite article into the feed. No threshold, no cap.

    Aggregator posture only: the publisher's headline is kept with visible credit
    and an outbound link, the body is replaced with our own stub. This never
    republishes somebody else's prose, and it never touches blocked_originality —
    those failed the BODY gate, and releasing them would recreate the exact
    exposure the gate exists to prevent. That one exclusion is not a threshold; it
    is the line between aggregating and copying.

    Never raises. It runs inline during lifespan, and a backlog drain must not be
    able to stop the app from booting.
    """
    if DISABLE_PENDING_DRAIN:
        log.info("[DRAIN] skipped: DISABLE_PENDING_DRAIN is set")
        return {"skipped": "disabled"}
    try:
        conn = get_db()
        try:
            pending = conn.execute(
                "SELECT COUNT(*) c FROM articles WHERE status='pending_rewrite'"
            ).fetchone()["c"]
        finally:
            conn.close()
        if not pending:
            log.info("[DRAIN] released 0 articles (nothing pending)")
            return {"skipped": "nothing pending", "pending": 0, "published": 0}

        sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
        # Follow whichever backend get_db() is using. Hardcoding run_sqlite here
        # meant that once DATABASE_URL was set the drain opened a sqlite file
        # that no longer exists and cheerfully reported "released 0 articles"
        # while the real backlog sat untouched in Postgres.
        out = _run_drain_for_backend()
        log.info("[DRAIN] released %s articles", out.get("published"))
        out["pending_before"] = pending
        return out
    except Exception as e:
        log.error("[DRAIN] failed, feed left as-is: %s", e, exc_info=True)
        return {"error": str(e), "published": 0}


@app.get("/admin/flush-pending")
async def admin_flush_pending():
    """Run the drain on demand and report what happened.

    Deliberately unauthenticated so it can be opened from a phone browser while
    the feed is being brought back up. That is a real exposure and it is meant to
    be temporary: this is a corpus-wide write reachable by anyone with the URL,
    and a GET at that, so a crawler or a link preview can fire it. It is bounded
    rather than dangerous — it only ever moves pending_rewrite to published on the
    aggregator posture, it cannot touch blocked_originality, and once the backlog
    is drained a second call is a no-op — but it should be deleted or moved behind
    _check_admin as soon as the feed is healthy. /admin/publish-pending is the
    same operation with the token check and a dry-run default.
    """
    out = await asyncio.get_event_loop().run_in_executor(
        None, _drain_pending_if_stalled)

    # Report the resulting corpus state too, so one call answers both "did it run"
    # and "is the feed actually servable now" — the latter being the real question.
    conn = get_db()
    try:
        one = lambda q: conn.execute(q).fetchone()["c"]
        out["now"] = {
            "servable": one("SELECT COUNT(*) c FROM articles "
                            "WHERE ai_processed=1 AND status='published'"),
            "pending_rewrite": one("SELECT COUNT(*) c FROM articles "
                                   "WHERE status='pending_rewrite'"),
            "blocked_originality": one("SELECT COUNT(*) c FROM articles "
                                       "WHERE status='blocked_originality'"),
            "not_ai_processed": one("SELECT COUNT(*) c FROM articles "
                                    "WHERE COALESCE(ai_processed,0)=0"),
            "total": one("SELECT COUNT(*) c FROM articles"),
        }
    except Exception as e:
        out["now"] = {"error": str(e)}
    finally:
        conn.close()
    return out


# ─── Sherr-I: price-store backfill + engine diagnosis ────────────────────────
# Both exist because the deployment has no shell. Render's free tier gives no
# console, so "run the backfill" and "why is /patterns not saying engine" are
# otherwise unanswerable from a phone.

# The backfill in flight. One at a time: two concurrent runs would fight over the
# same rows and double the upstream rate-limit pressure for no gain.
_ticks_task: Optional[asyncio.Task] = None
_ticks_result: dict = {}


async def _run_ticks_backfill(days: int, only: str) -> None:
    global _ticks_result
    try:
        import market_ticks
        result = await market_ticks.backfill(days, only=only or None)
        _ticks_result = result
        log.info("[TICKS] backfill finished: %s", result)
    except Exception as e:
        _ticks_result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        log.error("[TICKS] backfill failed: %s", e, exc_info=True)


@app.get("/admin/backfill-ticks")
async def admin_backfill_ticks(
    x_admin_token: str = Header(""),
    token: str = Query(""),
    days: int = Query(90, ge=1, le=3650),
    only: str = Query(""),
    restart: int = Query(0),
):
    """Seed sherrbyte_app.market_ticks with daily closes, and report progress.

    THE FIRST CALL STARTS THE RUN AND RETURNS IMMEDIATELY. Fetching ~57 symbols
    takes minutes, and Render kills a request that outlives the health-check
    window — so this never blocks on the work. Poll the same URL for progress;
    when it finishes, the same URL returns the report.

        GET /admin/backfill-ticks?token=...            -> {"status": "started"}
        GET /admin/backfill-ticks?token=...            -> {"status": "running", ...}
        GET /admin/backfill-ticks?token=...            -> {"status": "complete", ...}
        GET /admin/backfill-ticks?token=...&restart=1  -> run again
        GET /admin/backfill-ticks?token=...&only=crypto&days=30

    Run as an asyncio task rather than in a thread-pool executor: the backfill is
    already async httpx with its own bounded concurrency, so an executor would
    only wrap it in a second event loop. Same non-blocking result, one less
    moving part — and the same shape lifespan already uses for the Explore warm-up.
    """
    _check_admin(x_admin_token or token)          # ?token= for the address bar
    global _ticks_task, _ticks_result

    import market_ticks

    running = _ticks_task is not None and not _ticks_task.done()

    # An in-flight run reports progress. It is never restarted implicitly —
    # two concurrent runs would fight over the same rows and double the
    # upstream rate-limit pressure for nothing.
    if running and not restart:
        return {"status": "running", "progress": market_ticks.progress(),
                "hint": "poll this URL; the report appears here when it finishes"}

    # A finished run keeps answering with its report until a restart is asked
    # for, so the result survives however long it takes someone to look.
    if _ticks_task is not None and not running and not restart:
        out = {"status": "complete", "progress": market_ticks.progress(),
               "result": _ticks_result}
        try:
            out["report"] = await market_ticks.report()
        except Exception as e:
            out["report"] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        return out

    if not market_ticks.configured():
        return {"status": "unconfigured",
                "detail": "market_ticks is Postgres-only and neither "
                          "DATABASE_URL nor SHERR_I_DATABASE_URL is set."}

    if running and restart:
        _ticks_task.cancel()
    _ticks_result = {}
    _ticks_task = asyncio.create_task(_run_ticks_backfill(days, only))
    return {"status": "started", "days": days, "only": only or "all",
            "symbols": len(market_ticks.catalogue(only or None)),
            "hint": "poll this same URL for progress"}


# The signal replay in flight. Same one-at-a-time rule as the ticks backfill:
# two concurrent runs would race the same delete-then-insert window.
_replay_task: Optional[asyncio.Task] = None
_replay_result: dict = {}


def _market_signals():
    """The engine's market_signals worker, imported by path on first use.

    The endpoint runs THE WORKER — not a copy of it. A second implementation of
    the Signal shape here is the one thing that must not exist: a different
    entity name, source_id or ref_id and the replayed history resolves to
    different entities than the daily job's, so the detector sees two shallow
    series instead of one deep one.
    """
    import sys as _sys
    engine_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sherrbyte")
    if engine_root not in _sys.path:
        _sys.path.insert(0, engine_root)
    from app.workers import market_signals
    return market_signals


async def _run_signal_replay(days: int) -> None:
    global _replay_result
    try:
        pool = await get_spie_pool()
        if pool is None:
            _replay_result = {"ok": False, "error": "engine Postgres not reachable"}
            return
        # The pool this app already holds, rather than opening the engine's own —
        # backfill_from_ticks takes any async context-manager factory.
        result = await _market_signals().backfill_from_ticks(
            days=days, conn_factory=pool.acquire)
        _replay_result = {"ok": True, **result}
        log.info("[REPLAY] market signals: %s", result)
    except Exception as e:
        _replay_result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        log.error("[REPLAY] market signal replay failed: %s", e, exc_info=True)


@app.get("/admin/replay-signals")
async def admin_replay_signals(
    x_admin_token: str = Header(""),
    token: str = Query(""),
    days: int = Query(90, ge=1, le=3650),
    restart: int = Query(0),
):
    """Replay sherrbyte_app.market_ticks into domain_signals as domain='market'.

    The endpoint form of `python -m app.workers.market_signals --from-ticks`,
    for the same reason /admin/backfill-ticks exists: there is no shell on the
    deployment, and GitHub Actions cannot reach the database — but Render's
    DATABASE_URL can, which is how the tick rows got there in the first place.

    Runs the worker's own backfill_from_ticks() over the pool this app already
    holds. Starts on the first call and returns immediately; poll the same URL.

        GET /admin/replay-signals?token=...            -> {"status": "started"}
        GET /admin/replay-signals?token=...            -> {"status": "running", ...}
        GET /admin/replay-signals?token=...            -> {"status": "complete", ...}
        GET /admin/replay-signals?token=...&restart=1  -> run again
        GET /admin/replay-signals?token=...&days=30

    Idempotent: every replayed day is deleted and re-inserted by ref_id, so a
    second run over the same window replaces rather than duplicates.
    """
    _check_admin(x_admin_token or token)          # ?token= for the address bar
    global _replay_task, _replay_result

    ms = _market_signals()
    running = _replay_task is not None and not _replay_task.done()

    if running and not restart:
        return {"status": "running", "progress": ms.progress(),
                "hint": "poll this URL; the result appears here when it finishes"}

    if _replay_task is not None and not running and not restart:
        return {"status": "complete", "progress": ms.progress(),
                "result": _replay_result}

    if not SHERR_I_DATABASE_URL:
        return {"status": "unconfigured",
                "detail": "neither DATABASE_URL nor SHERR_I_DATABASE_URL is set."}

    if running and restart:
        _replay_task.cancel()
    _replay_result = {}
    _replay_task = asyncio.create_task(_run_signal_replay(days))
    return {"status": "started", "days": days,
            "instruments": len(ms._TICK_SOURCES),
            "hint": "poll this same URL for progress"}


# The entity cleanup in flight.
_clean_task: Optional[asyncio.Task] = None
_clean_result: dict = {}


def _entity_cleanup():
    """The engine's entity_cleanup worker, imported by path on first use."""
    import sys as _sys
    engine_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sherrbyte")
    if engine_root not in _sys.path:
        _sys.path.insert(0, engine_root)
    from app.workers import entity_cleanup
    return entity_cleanup


async def _run_entity_cleanup(dry_run: bool, days: int) -> None:
    global _clean_result
    try:
        pool = await get_spie_pool()
        if pool is None:
            _clean_result = {"ok": False, "error": "engine Postgres not reachable"}
            return
        async with pool.acquire() as conn:
            result = await _entity_cleanup().run(conn, dry_run=dry_run, days=days)
        _clean_result = {"ok": True, **result}
        log.info("[ENTITIES] cleanup: %s",
                 {k: v for k, v in result.items() if not k.endswith("examples")})
    except Exception as e:
        _clean_result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        log.error("[ENTITIES] cleanup failed: %s", e, exc_info=True)


@app.get("/admin/clean-entities")
async def admin_clean_entities(
    x_admin_token: str = Header(""),
    token: str = Query(""),
    dry_run: int = Query(1),
    days: int = Query(90, ge=1, le=3650),
    restart: int = Query(0),
):
    """Re-apply the current resolver to entities already in the graph.

    Tightening extraction only affects NEW mentions — everything already stored
    keeps whatever the old rules produced, which is why /patterns still shows
    "India's" beside "India" and "Moreover" as an entity. This merges the
    duplicates, drops the junk, rebuilds co-occurrence from the corrected
    signals, and prunes the insights that collapse to a single entity.

    DRY RUN BY DEFAULT — it reports what would merge and what would be dropped
    without touching anything. Pass &dry_run=0 to apply, once the lists look
    right. Applying is destructive: merged and junk entities are deleted, and
    the insights that referenced only those are pruned.

        GET /admin/clean-entities?token=...              -> what would change
        GET /admin/clean-entities?token=...&dry_run=0    -> apply
        GET /admin/clean-entities?token=...&restart=1    -> run again

    Idempotent: a second apply finds nothing to merge and nothing to drop.
    """
    _check_admin(x_admin_token or token)
    global _clean_task, _clean_result

    ec = _entity_cleanup()
    running = _clean_task is not None and not _clean_task.done()

    if running and not restart:
        return {"status": "running", "progress": ec.progress()}

    if _clean_task is not None and not running and not restart:
        return {"status": "complete", "progress": ec.progress(),
                "result": _clean_result}

    if not SHERR_I_DATABASE_URL:
        return {"status": "unconfigured",
                "detail": "neither DATABASE_URL nor SHERR_I_DATABASE_URL is set."}

    if running and restart:
        _clean_task.cancel()
    _clean_result = {}
    _clean_task = asyncio.create_task(_run_entity_cleanup(bool(dry_run), days))
    return {"status": "started", "dry_run": bool(dry_run), "days": days,
            "hint": "poll this same URL; dry_run=0 applies"}


# ─── body reprocess: replace stubs and source text with our own words ────────
_body_task: Optional[asyncio.Task] = None
_body_result: dict = {}
_body_progress: dict = {}
# Survives the run so /admin/body-audit can report why nothing happened. A job
# that reports "complete, rewritten 0" with no error anywhere is the exact
# failure mode this exists to make visible.
_body_last: dict = {"ran_at": None, "fetched": 0, "candidates": 0, "sent_to_ai": 0,
                    "ai_returned": 0, "written": 0, "failed": 0,
                    "last_error": None, "errors": []}


def _provider_summary() -> str:
    try:
        import ai_processor                                    # noqa: PLC0415
        return ", ".join(f"{p}:{ai_processor.KEYS.get(p).size}"
                         for p in ai_processor.KEYS.configured()) or "NONE CONFIGURED"
    except Exception as e:                                     # noqa: BLE001
        return f"unavailable ({e})"


def _body_reason(reason: str, article_id=None) -> None:
    """A rejection that is not an exception but still means no row was written."""
    entry = {"stage": "rewrite", "type": reason, "error": reason,
             "article_id": article_id}
    _body_last["last_error"] = entry
    _body_last["errors"] = ([entry] + _body_last.get("errors", []))[:10]
    log.warning("[BODY] article %s not written: %s", article_id, reason)


def _body_err(stage: str, exc: Exception, article_id=None) -> None:
    """Record a real exception — type, text, and where it happened."""
    entry = {"stage": stage, "type": type(exc).__name__,
             "error": str(exc)[:400], "article_id": article_id}
    _body_last["last_error"] = entry
    _body_last["errors"] = ([entry] + _body_last.get("errors", []))[:10]
    log.error("[BODY] %s failed%s: %s: %s", stage,
              f" (article {article_id})" if article_id else "",
              type(exc).__name__, str(exc)[:400])


def body_progress() -> dict:
    p = dict(_body_progress)
    if p.get("started_at"):
        p["elapsed_s"] = round((p.get("finished_at") or time.time()) - p["started_at"], 1)
    return p


def _reprocess_bodies_sync(limit: int, batch: int) -> dict:
    """Rewrite published rows whose body is a stub, the publisher's text, or empty.

    Runs the SAME AI pass the pipeline uses, over the surviving source text — not
    over full_body, which on a drained row is the stub itself.

    Synchronous because get_db()/pgcompat is a sync driver; the caller runs it in
    an executor so the event loop keeps serving.
    """
    for k in ("fetched", "candidates", "sent_to_ai", "ai_returned", "written", "failed"):
        _body_last[k] = 0
    _body_last.update({"ran_at": datetime.now(timezone.utc).isoformat(),
                       "last_error": None, "errors": []})
    log.info("[BODY] run starting: limit=%d batch=%d", limit, batch)
    conn = get_db()
    try:
        before = body_state.audit(conn)
        _body_progress.update({"phase": "auditing", "before": before["published_by_state"]})
        log.info("[BODY] before: %s | needs_rewrite=%d",
                 before["published_by_state"], before["needs_rewrite"])

        providers = available_providers()
        if providers["primary"] == "rule-based":
            log.error("[BODY] REFUSING: no AI provider. %d published article(s) "
                      "are on a placeholder and cannot be rewritten until "
                      "GEMINI_API_KEY or GROQ_API_KEY is set.",
                      before["needs_rewrite"])
            return {"ok": False, "before": before["published_by_state"],
                    "needs_rewrite": before["needs_rewrite"], "ai": providers,
                    "BLOCKED": "NO AI PROVIDER CONFIGURED — set GEMINI_API_KEY "
                               "or GROQ_API_KEY. Rewriting without one would "
                               "write the same placeholder back."}

        done = failed = skipped = 0
        # Every id this run has already tried. A row that fails the originality
        # gate keeps reprocessed=0 ON PURPOSE — so the next run retries it — but
        # that means the candidate query keeps returning it, and without this the
        # loop re-selects the same failing batch forever. Progress cannot be
        # measured by done+skipped alone for the same reason.
        attempted: set = set()
        _body_progress.update({"phase": "rewriting", "done": 0, "target": limit})
        while len(attempted) < limit:
            take = min(batch, limit - len(attempted))
            try:
                fetched = conn.execute(
                    body_state.SELECT_NEEDING_REWRITE, (take + len(attempted),)
                ).fetchall()
            except Exception as e:
                _body_err("select", e)
                break
            rows = [r for r in fetched if r["id"] not in attempted][:take]
            _body_last["fetched"] += len(fetched)
            log.info("[BODY] fetched %d candidate row(s), %d new this batch",
                     len(fetched), len(rows))
            if not rows:
                # The single most important log line here: the audit says work
                # exists but the selector found none, which is a selector bug,
                # not an empty backlog.
                if before["needs_rewrite"] and not attempted:
                    log.error("[BODY] SELECTOR RETURNED NOTHING while the audit "
                              "reports %d article(s) needing a rewrite — the "
                              "candidate query and the classifier disagree.",
                              before["needs_rewrite"])
                    _body_err("select", RuntimeError(
                        f"selector found 0 rows but audit says "
                        f"{before['needs_rewrite']} need rewriting"))
                break
            attempted.update(r["id"] for r in rows)

            # reprocessed=0 is only the candidate marker; the body itself decides.
            # EITHER column failing is work. A row whose body was rewritten but
            # whose summary_60 is still the stub renders that stub on the Home
            # card, which is the first and often only text a reader sees.
            unhealthy = [r for r in rows if not body_state.row_is_healthy(r)]

            # THE ORDER HERE MATTERS. Rows that are already original get flagged
            # reprocessed=1 so the selector stops returning them. Rows that are
            # merely STARVED of source text must NOT be flagged: they are not
            # done, and marking them done is how work disappears silently — the
            # exact failure this whole pass was built to end.
            unhealthy_ids = {r["id"] for r in unhealthy}
            for r in rows:
                if r["id"] not in unhealthy_ids:
                    conn.execute("UPDATE articles SET reprocessed=1 WHERE id=?",
                                 (r["id"],))
                    skipped += 1

            # A row with no surviving publisher text cannot be rewritten, only
            # invented. Skip it and say so, rather than spending a provider call
            # to receive a fabrication or a placeholder. Left unflagged, so it
            # comes back the moment the ingest gives it real text.
            starved = [r for r in unhealthy
                       if not body_state.has_usable_source(
                           r["summary_60"], r["source_summary"], r["full_body"])]
            for r in starved:
                _body_reason("no_source_material", r["id"])
            failed += len(starved)
            starved_ids = {r["id"] for r in starved}
            work = [r for r in unhealthy if r["id"] not in starved_ids]
            if not work:
                conn.commit()
                continue

            batch_input = [{
                "title": r["headline"],
                "body": body_state.source_material(
                    r["headline"], r["summary_60"], r["source_summary"], r["full_body"]),
                "fallback_category": PILLARS.get(r["pillar_id"], PILLARS[3])["slug"],
            } for r in work]

            _body_last["candidates"] += len(work)
            _body_last["sent_to_ai"] += len(batch_input)
            log.info("[BODY] sent %d article(s) to AI (providers: %s)",
                     len(batch_input), _provider_summary())
            try:
                results = asyncio.run(process_batch(batch_input, concurrency=AI_CONCURRENCY))
            except Exception as e:
                # The whole batch died. This is where a bad key, an exhausted
                # quota, or a network refusal surfaces — record it verbatim
                # instead of losing it to a one-line warning.
                _body_err("ai_batch", e)
                failed += len(work)
                break
            _body_last["ai_returned"] += len(results)
            log.info("[BODY] AI returned %d result(s) for %d sent",
                     len(results), len(batch_input))

            for row, result in zip(work, results):
                try:
                    # A rewrite that came back as the stub is not a rewrite. Leave
                    # the row unflagged so the next run tries it again rather than
                    # marking it done with the same placeholder.
                    if (body_state.is_stub(result.get("full_body", ""))
                            or body_state.is_stub(result.get("summary", ""))):
                        # Every provider failed and the rule-based fallback
                        # handed back the placeholder. Counted as a failure so
                        # it cannot masquerade as a completed rewrite.
                        _body_reason("ai_returned_stub", row["id"])
                        failed += 1
                        continue
                    src_head = row["source_headline"] or row["headline"] or ""
                    source = body_state.source_material(
                        row["headline"], row["summary_60"], row["source_summary"], "")

                    # THE BODY AND THE HEADLINE ARE GATED SEPARATELY HERE.
                    #
                    # _gate_article answers "may this article be published at
                    # all", and it parks the row when EITHER half fails. That is
                    # right at publish time. It is wrong for this pass: the row is
                    # already published, and refusing to replace a stub body
                    # because the rewritten headline happened to resemble the
                    # publisher's leaves the reader looking at the placeholder —
                    # the exact failure this exists to clear.
                    #
                    # So: the body is written whenever it passes the originality
                    # check, and the new headline is taken only if it is itself
                    # original. Neither is ever the publisher's text.
                    body_ok, body_m = originality_check(result["full_body"], source)
                    head_ok, head_m = headline_is_original(
                        result["refined_title"], src_head)
                    audit = {"pass": "body_reprocess", "body": body_m,
                             "headline": head_m, "headline_replaced": bool(head_ok),
                             "at": datetime.now(timezone.utc).isoformat()}
                    if not body_ok:
                        # The rewrite reproduced the source. Left unflagged so the
                        # next run tries again; never written.
                        audit["status"] = "rejected_body_overlap"
                        _body_reason("rejected_body_overlap", row["id"])
                        failed += 1
                        continue
                    headline = ((result.get("refined_title") or "").strip()
                                if head_ok else "") \
                        or (row["headline"] or "").strip() \
                        or (row["source_headline"] or "").strip()
                    audit["status"] = "published"
                    conn.execute("""
                        UPDATE articles SET headline=?, summary_60=?, full_body=?,
                            ai_processed=1, reprocessed=1, status='published',
                            originality_json=?, originality_overlap=?,
                            originality_run=?, originality_checked_at=?
                        WHERE id=?""", (
                        headline, result["summary"], result["full_body"],
                        json.dumps(audit), body_m["overlap"], body_m["longest_run"],
                        datetime.now(timezone.utc).isoformat(), row["id"]))
                    done += 1
                    _body_last["written"] += 1
                except Exception as e:
                    _body_err("write", e, row["id"])
                    failed += 1
            conn.commit()
            _body_progress.update({"done": done, "failed": failed,
                                   "skipped": skipped, "attempted": len(attempted)})

        after = body_state.audit(conn)
        _body_last["failed"] = failed
        log.info("[BODY] run finished: fetched=%d candidates=%d sent=%d returned=%d "
                 "wrote=%d failed=%d already-original=%d | after: %s | last error: %s",
                 _body_last["fetched"], _body_last["candidates"],
                 _body_last["sent_to_ai"], _body_last["ai_returned"],
                 done, failed, skipped, after["published_by_state"],
                 _body_last["last_error"])
        return {"ok": True, "rewritten": done, "failed": failed,
                "already_original": skipped, "attempted": len(attempted),
                "before": before["published_by_state"],
                "after": after["published_by_state"],
                "remaining": after["needs_rewrite"]}
    finally:
        conn.close()


async def _run_body_reprocess(limit: int, batch: int) -> None:
    global _body_result
    _body_progress.clear()
    _body_progress.update({"running": True, "started_at": time.time(),
                           "finished_at": None, "phase": "starting"})
    try:
        _body_result = await asyncio.get_event_loop().run_in_executor(
            None, _reprocess_bodies_sync, limit, batch)
    except Exception as e:
        # The executor call itself died — the run never reached its own
        # error handling, so record it here or it is lost with the task.
        _body_err("task", e)
        _body_result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        log.error("[BODY] reprocess failed: %s", e, exc_info=True)
    finally:
        _body_progress["running"] = False
        _body_progress["finished_at"] = time.time()


async def body_reprocess_job() -> None:
    """Scheduled sweep. Bounded per run so a free-tier instance is not spending
    its whole night on AI calls, and so the free AI tiers are not blown."""
    if _body_task is not None and not _body_task.done():
        return
    try:
        await _run_body_reprocess(BODY_REPROCESS_NIGHTLY, AI_BATCH_SIZE)
    except Exception as e:
        log.error("[BODY] nightly job failed: %s", e, exc_info=True)


@app.get("/admin/why-empty")
async def admin_why_empty(x_admin_token: str = Header(""), token: str = Query("")):
    """One call that answers "why does the app look broken".

    Four independent things make a card look wrong, and they have four different
    fixes. Guessing between them from a screenshot is what has cost the most
    time here, so each is counted separately with the action that clears it.
    """
    _check_admin(x_admin_token or token)
    conn = get_db()
    try:
        one = lambda q, p=(): conn.execute(q, p).fetchone()["c"]
        total = one("SELECT COUNT(*) c FROM articles WHERE status='published'")
        body = body_state.audit(conn)
        no_img = one("SELECT COUNT(*) c FROM articles WHERE status='published' "
                     "AND COALESCE(image_url,'')='' AND COALESCE(source_image_url,'')=''")
        with_img = total - no_img
        newest = conn.execute(
            "SELECT MAX(published_at) AS m FROM articles").fetchone()["m"]
        newest_dt = timestamps.parse(newest)
        age_h = ((datetime.now(timezone.utc) - newest_dt).total_seconds() / 3600
                 if newest_dt else None)
        mega = one("SELECT COUNT(*) c FROM (SELECT story_id FROM articles "
                   "WHERE story_id<>0 GROUP BY story_id HAVING COUNT(*) > ?) t",
                   (MAX_THREAD_SIZE,))
        providers = available_providers()

        problems = []
        if body["needs_rewrite"]:
            problems.append({
                "problem": "articles open on a placeholder",
                "count": body["needs_rewrite"],
                "fix": ("no AI key configured — set GEMINI_API_KEY"
                        if providers.get("primary") == "rule-based"
                        else "GET /admin/reprocess-bodies?token=...&limit=3000")})
        if no_img:
            problems.append({
                "problem": "articles with no image at all (neither ours nor the publisher's)",
                "count": no_img,
                "fix": ("the RSS entries carried no image. Newly ingested rows "
                        "store source_image_url; older rows cannot be recovered "
                        "without re-crawling.")})
        if age_h is not None and age_h > 6:
            problems.append({
                "problem": "nothing new is arriving",
                "newest_age_hours": round(age_h, 1),
                "fix": "ingest is stalled — check the collect_news job and the feed list"})
        if mega:
            problems.append({
                "problem": "over-merged story threads (the String shows unrelated articles)",
                "count": mega,
                "fix": (f"threads larger than {MAX_THREAD_SIZE} are now ignored at "
                        f"read time; POST /admin/relink rebuilds them")})

        return {
            "published_total": total,
            "bodies": body["published_by_state"],
            "summaries": body["summary_by_state"],
            "healthy_articles": body["healthy"],
            "with_image": with_img, "without_image": no_img,
            "newest_published_at": timestamps.to_iso(newest) or str(newest or ""),
            "newest_age_hours": round(age_h, 2) if age_h is not None else None,
            "over_merged_threads": mega,
            "ai": providers,
            "problems": problems or [{"problem": "none found", "fix": ""}],
        }
    finally:
        conn.close()


@app.get("/admin/published-at")
async def admin_published_at(x_admin_token: str = Header(""), token: str = Query("")):
    """Is ingest stalled, or was the feed just mis-sorted?

    The newest published_at answers that directly: a stamp from minutes ago
    means ingest is running and the ordering was the problem; a stamp from days
    ago means nothing new is arriving and no amount of sorting will help.

    Also reports the format census, because a non-zero `non_canonical` after a
    boot means the normalisation did not run.
    """
    _check_admin(x_admin_token or token)
    conn = get_db()
    try:
        out = {"column_type": None, "total": 0, "newest": None, "oldest": None,
               "non_canonical": 0, "distinct_shapes": []}
        try:
            row = conn.execute(
                "SELECT data_type FROM information_schema.columns "
                "WHERE table_name='articles' AND column_name='published_at'").fetchone()
            out["column_type"] = (row or {}).get("data_type") if row else "text"
        except Exception:
            out["column_type"] = "text"

        agg = conn.execute(
            "SELECT COUNT(*) AS c, MAX(published_at) AS newest, "
            "MIN(published_at) AS oldest FROM articles").fetchone()
        out["total"] = agg["c"]
        out["newest"] = timestamps.to_iso(agg["newest"]) or str(agg["newest"] or "")
        out["oldest"] = timestamps.to_iso(agg["oldest"]) or str(agg["oldest"] or "")

        newest = timestamps.parse(agg["newest"])
        if newest:
            age_h = (datetime.now(timezone.utc) - newest).total_seconds() / 3600
            out["newest_age_hours"] = round(age_h, 2)
            out["verdict"] = (
                "Ingest is running — the ordering was the problem."
                if age_h < 6 else
                f"Newest article is {age_h:.1f}h old. Ingest looks STALLED; "
                f"sorting cannot fix an empty pipeline.")

        # Only meaningful while the column is still text.
        if (out["column_type"] or "text").lower() == "text":
            rows = conn.execute(
                "SELECT published_at FROM articles WHERE published_at IS NOT NULL "
                "ORDER BY id DESC LIMIT 2000").fetchall()
            shapes = {}
            for r in rows:
                v = r["published_at"]
                if not timestamps.is_canonical(v):
                    out["non_canonical"] += 1
                key = ("T" if "T" in str(v) else "space") + (
                    "+offset" if ("+" in str(v)[10:] or str(v).endswith("Z")) else "+naive")
                shapes[key] = shapes.get(key, 0) + 1
            out["distinct_shapes"] = sorted(shapes.items(), key=lambda kv: -kv[1])
        return out
    finally:
        conn.close()


@app.get("/admin/body-audit")
async def admin_body_audit(x_admin_token: str = Header(""), token: str = Query("")):
    """What is actually in every article body, counted by state.

    original    — AI-written, ours
    stub        — "Sherr AI is preparing…", the startup drain's placeholder
    source_text — the publisher's own prose (a live copyright exposure)
    empty       — nothing, or too short to be a summary

    Read-only. This is the number to watch while /admin/reprocess-bodies runs.
    """
    _check_admin(x_admin_token or token)
    conn = get_db()
    try:
        out = body_state.audit(conn)
    finally:
        conn.close()

    # WHICH AI, IF ANY. Without this the audit could report a corpus full of
    # stubs and give no hint that the reason nothing improves is that no key is
    # configured — the rewrite refuses to run, correctly, but silently.
    providers = available_providers()
    out["ai"] = providers

    # A MODEL ID THAT NO LONGER EXISTS LOOKS EXACTLY LIKE A WORKING ONE FROM
    # HERE — until the provider 404s and every article silently stays on its
    # placeholder. gemini-2.5-flash and grok-2-latest both went that way. So the
    # audit names the model each provider is actually using, and flags the ones
    # still on a built-in default, because those are the ids that go stale
    # without anyone editing anything.
    defaulted = [p for p, m in (providers.get("models") or {}).items()
                 if m.get("source") == "built-in default"]
    if defaulted:
        out["model_note"] = (
            "%s using a built-in default model id (%s). Defaults go stale when a "
            "provider retires a model; set the matching env var to pin one. If "
            "provider_errors below shows 404/400, the id is the reason."
            % (", ".join(defaulted),
               ", ".join(providers["models"][p]["model"] for p in defaulted)))

    # WHY THE LAST RUN DID WHAT IT DID.
    #
    # The counts alone cannot distinguish "nothing needed rewriting" from
    # "the selector matched nothing", "every provider returned 401", or "the
    # rewrite came back as the placeholder again". Each of those leaves
    # original at 0 and each needs a different fix, so the run's own stage
    # counters and the last real exception are reported here — no redeploy and
    # no log access required.
    out["last_run"] = dict(_body_last)
    out["running"] = bool(_body_progress.get("running"))
    try:
        import ai_processor                                    # noqa: PLC0415
        out["provider_errors"] = ai_processor.last_provider_errors(5)
    except Exception:                                          # noqa: BLE001
        out["provider_errors"] = []

    # A selector that disagrees with the classifier is the silent no-op that
    # made this pass look successful while rewriting nothing. Say so outright.
    lr = out["last_run"]
    if lr.get("ran_at") and out["needs_rewrite"] and not lr.get("candidates"):
        out["DIAGNOSIS"] = (
            "The last run fetched %d row(s) and found %d to work on, yet the "
            "audit says %d article(s) still need rewriting. The candidate query "
            "and the classifier disagree — that is a selector bug, not an empty "
            "backlog." % (lr.get("fetched", 0), lr.get("candidates", 0),
                          out["needs_rewrite"]))
    elif lr.get("sent_to_ai") and not lr.get("written"):
        out["DIAGNOSIS"] = (
            "The last run sent %d article(s) to the AI and wrote none. See "
            "provider_errors and last_run.last_error for the actual failure."
            % lr["sent_to_ai"])
    if providers.get("primary") == "rule-based":
        out["BLOCKED"] = (
            "NO AI PROVIDER CONFIGURED. Set GEMINI_API_KEY or GROQ_API_KEY on "
            "the service. Until then /admin/reprocess-bodies will refuse to run "
            "and every one of the %d article(s) below stays on a placeholder — "
            "rewriting without a provider would only write the placeholder again."
            % out["needs_rewrite"])
    elif out["needs_rewrite"]:
        out["next"] = ("GET /admin/reprocess-bodies?token=...&limit=%d"
                       % min(out["needs_rewrite"], 3000))
    return out


@app.get("/admin/reprocess-bodies")
async def admin_reprocess_bodies(
    x_admin_token: str = Header(""),
    token: str = Query(""),
    limit: int = Query(500, ge=1, le=20000),
    batch: int = Query(0, ge=0, le=200),
    restart: int = Query(0),
):
    """Rewrite published bodies that are still a stub or the publisher's text.

    The startup drain released the corpus with a placeholder body AND set
    ai_processed=1 — the very column run_ai_batch filters on — so those rows
    became permanently invisible to the pass meant to rewrite them. This is that
    pass, selecting on what the body CONTAINS rather than on the flag.

        GET /admin/reprocess-bodies?token=...            -> {"status": "started"}
        GET /admin/reprocess-bodies?token=...            -> progress
        GET /admin/reprocess-bodies?token=...&limit=500  -> a bigger sweep
        GET /admin/body-audit?token=...                  -> counts, any time

    Bounded per call: the free AI tiers are rate limited, so a full corpus pass
    is several runs rather than one. Also runs nightly on its own.
    """
    _check_admin(x_admin_token or token)
    global _body_task, _body_result

    running = _body_task is not None and not _body_task.done()
    if running and not restart:
        return {"status": "running", "progress": body_progress()}
    if _body_task is not None and not running and not restart:
        return {"status": "complete", "progress": body_progress(),
                "result": _body_result, "last_run": dict(_body_last)}

    if running and restart:
        _body_task.cancel()
    _body_result = {}
    _body_task = asyncio.create_task(
        _run_body_reprocess(limit, batch or AI_BATCH_SIZE))
    return {"status": "started", "limit": limit,
            "providers": _provider_summary(),
            "hint": "poll this URL; /admin/body-audit shows the counts and, "
                    "under last_run and provider_errors, why a run wrote nothing"}


# ─── detectors on demand ─────────────────────────────────────────────────────
_detect_task: Optional[asyncio.Task] = None
_detect_result: dict = {}


async def _run_detectors(only: str) -> None:
    global _detect_result
    try:
        pool = await get_spie_pool()
        if pool is None:
            _detect_result = {"ok": False, "error": "engine Postgres not reachable"}
            return
        import sys as _sys
        engine_root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sherrbyte")
        if engine_root not in _sys.path:
            _sys.path.insert(0, engine_root)
        # app.workers.detectors imports app.db (pydantic-settings, not shipped
        # here), so the REGISTRY is driven directly — the detectors themselves
        # take a connection and depend on nothing but stdlib.
        from app.spie.discovery import REGISTRY
        from app.spie.graph import cooccurrence
        from app.spie.decision import rules as decision_rules

        out: dict = {}
        async with pool.acquire() as conn:
            try:
                await cooccurrence.compute_npmi(conn)
            except Exception as e:
                log.warning("[DETECT] npmi refresh failed: %s", e)
            for name, fn in REGISTRY.items():
                if only and name != only:
                    continue
                try:
                    out[name] = await fn(conn)
                except Exception as e:
                    log.error("[DETECT] %s failed: %s", name, e, exc_info=True)
                    out[name] = -1
            if not only or only == "cross_domain_chain":
                try:
                    out["cross_domain_chain"] = await decision_rules.run(conn)
                except Exception as e:
                    log.error("[DETECT] chain rules failed: %s", e)
                    out["cross_domain_chain"] = -1
            if not only or only == "reasoned":
                try:
                    from app.spie.reasoning import engine as reasoning_engine
                    out["reasoned"] = await reasoning_engine.run(conn)
                except Exception as e:
                    log.error("[DETECT] reasoning failed: %s", e)
                    out["reasoned"] = -1
            out["insights_total"] = int(
                await conn.fetchval("SELECT COUNT(*) FROM insights") or 0)
        _detect_result = {"ok": True, **out}
        log.info("[DETECT] %s", out)
    except Exception as e:
        _detect_result = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        log.error("[DETECT] run failed: %s", e, exc_info=True)


async def detectors_job() -> None:
    """Nightly detector pass, in-process.

    The engine's own scheduler lives in sherrbyte/app/main.py, which render.yaml
    does not start, and the GitHub Actions cron cannot reach the database. So the
    detectors had never run in production: every insight was dated the day the
    corpus was seeded.
    """
    if _detect_task is not None and not _detect_task.done():
        return
    try:
        await _run_detectors("")
    except Exception as e:
        log.error("[DETECT] nightly job failed: %s", e, exc_info=True)


@app.get("/admin/run-detectors")
async def admin_run_detectors(
    x_admin_token: str = Header(""),
    token: str = Query(""),
    only: str = Query(""),
    restart: int = Query(0),
):
    """Run the Sherr-I detector pass now, and report what each one wrote.

        GET /admin/run-detectors?token=...                    -> started
        GET /admin/run-detectors?token=...                    -> progress/result
        GET /admin/run-detectors?token=...&only=market_reaction
        GET /admin/run-detectors?token=...&restart=1

    Also runs nightly at 02:10 UTC from this app's scheduler.
    """
    _check_admin(x_admin_token or token)
    global _detect_task, _detect_result

    running = _detect_task is not None and not _detect_task.done()
    if running and not restart:
        return {"status": "running"}
    if _detect_task is not None and not running and not restart:
        return {"status": "complete", "result": _detect_result}
    if not SHERR_I_DATABASE_URL:
        return {"status": "unconfigured",
                "detail": "neither DATABASE_URL nor SHERR_I_DATABASE_URL is set."}
    if running and restart:
        _detect_task.cancel()
    _detect_result = {}
    _detect_task = asyncio.create_task(_run_detectors(only))
    return {"status": "started", "only": only or "all",
            "hint": "poll this same URL for the per-detector counts"}


# ─── Sherr-I cards ───────────────────────────────────────────────────────────
# The read path is cached because the pipeline calls an LLM: an uncached
# endpoint would re-run the cascade for every reader, and the same card would
# cost a provider call per request.
_SHERR_I_CACHE_S = int(os.getenv("SHERR_I_CACHE_SECONDS", "900"))   # 15 minutes
_sherr_i_cache: dict = {"at": 0.0, "payload": None}


def _sherr_i_engine():
    """The spie package, imported by path. app/spie/* needs only stdlib +
    pydantic; app.config and app.db are NOT touched, so this works in the root
    service, which ships neither pydantic-settings nor pgvector."""
    import sys as _sys
    root = os.path.join(os.path.dirname(os.path.abspath(__file__)), "sherrbyte")
    if root not in _sys.path:
        _sys.path.insert(0, root)
    from app.spie.discovery import pipeline
    return pipeline


async def _sherr_i_cards(force: bool = False) -> dict:
    now = time.time()
    cached = _sherr_i_cache["payload"]
    if cached is not None and not force and (now - _sherr_i_cache["at"]) < _SHERR_I_CACHE_S:
        return {**cached, "cached": True,
                "age_seconds": round(now - _sherr_i_cache["at"], 1)}

    pool = await get_spie_pool()
    if pool is None:
        return {"cards": [], "funnel": {}, "source": "unavailable",
                "detail": "engine Postgres not reachable"}
    async with pool.acquire() as conn:
        out = await _sherr_i_engine().run(conn)
    payload = {**out, "source": "engine",
               "generated_at": datetime.now(timezone.utc).isoformat()}
    _sherr_i_cache.update({"at": now, "payload": payload})
    return {**payload, "cached": False, "age_seconds": 0}


@app.get("/api/sherr-i/patterns")
async def sherr_i_patterns(force: int = Query(0)):
    """Today's decision cards, newest first. Cached for 15 minutes.

    AN EMPTY LIST IS A VALID RESPONSE, and the common one. A card exists only
    where a move cleared the statistical test AND at least two real articles
    corroborated it; markets have quiet days, and the honest answer then is
    nothing. `funnel` says which stage stopped, so an empty page is explainable
    rather than mysterious.

    Nothing older than 72 hours is returned — same window as /patterns.
    """
    out = await _sherr_i_cards(force=bool(force))
    cutoff = datetime.now(timezone.utc) - timedelta(hours=PATTERN_MAX_AGE_HOURS)
    fresh = []
    for c in out.get("cards", []):
        ts = (c.get("anomaly") or {}).get("ts")
        try:
            when = datetime.fromisoformat(str(ts)) if ts else None
        except ValueError:
            when = None
        if when is None:
            continue
        if when.tzinfo is None:
            when = when.replace(tzinfo=timezone.utc)
        if when >= cutoff:
            fresh.append(c)
    fresh.sort(key=lambda c: (c.get("anomaly") or {}).get("ts") or "", reverse=True)
    return {**out, "cards": fresh, "count": len(fresh),
            "max_age_hours": PATTERN_MAX_AGE_HOURS}


@app.get("/admin/sherr-i-status")
async def admin_sherr_i_status(x_admin_token: str = Header(""),
                               token: str = Query("")):
    """Is the engine able to say anything, and if not, which stage stopped it.

    Reads through the pipeline's own funnel rather than re-deriving it, so this
    reports what actually ran rather than what should have.
    """
    _check_admin(x_admin_token or token)
    pool = await get_spie_pool()
    if pool is None:
        return {"ok": False, "detail": "engine Postgres not reachable"}

    engine = _sherr_i_engine()
    import sys as _sys
    from app.spie.discovery import tick_anomaly
    from app.spie.reasoning import card as card_mod

    async with pool.acquire() as conn:
        cov = await tick_anomaly.coverage(conn)
        anomalies = await tick_anomaly.scan(conn)
        try:
            edges_n = int(await conn.fetchval("SELECT COUNT(*) FROM entity_edges") or 0)
        except Exception:
            edges_n = 0
        try:
            ticks = int(await conn.fetchval(
                "SELECT COUNT(*) FROM sherrbyte_app.market_ticks") or 0)
        except Exception:
            ticks = 0

    return {
        "ok": True,
        "ticks_rows": ticks,
        "symbols_total": cov["symbols_total"],
        "symbols_scoreable": cov["symbols_scoreable"],
        "min_observations": cov["min_observations"],
        "too_short": cov["too_short"],
        "anomalies_today": len(anomalies),
        "anomalies": [a.as_dict() for a in anomalies[:20]],
        "entity_edges": edges_n,
        "cards_last_run": engine.LAST_RUN.get("cards"),
        "funnel_last_run": dict(engine.LAST_RUN),
        "llm_calls": dict(card_mod.LLM_CALLS),
        "signal_strength": card_mod.explain_signal_strength(),
        "thresholds": {"z": tick_anomaly.Z_THRESHOLD,
                       "window": tick_anomaly.WINDOW,
                       "min_observations": tick_anomaly.MIN_OBSERVATIONS},
    }


@app.get("/admin/sherr-i-doctor")
async def admin_sherr_i_doctor(
    x_admin_token: str = Header(""),
    token: str = Query(""),
    run_detector: int = Query(0),
):
    """Why is /patterns returning 'seed' or 'unavailable' instead of 'engine'?

    Answers the whole chain in one call — DSN and pool, migrations applied here,
    row counts for every detector input, and what is scheduled — then names the
    FIRST broken link, because fixing anything downstream of it changes nothing.

    Read-only unless &run_detector=1, which runs market_reaction for real (it
    seeds instrument_keywords and persists anything it finds).
    """
    _check_admin(x_admin_token or token)
    import sherr_i_doctor
    which = ("SHERR_I_DATABASE_URL" if os.getenv("SHERR_I_DATABASE_URL")
             else "DATABASE_URL" if os.getenv("DATABASE_URL") else "unset")
    return await sherr_i_doctor.diagnose(
        SHERR_I_DATABASE_URL, which, get_spie_pool,
        scheduler=scheduler, run_detector=bool(run_detector))


@app.get("/admin/feed-doctor")
async def admin_feed_doctor(x_admin_token: str = Header(""), token: str = Query("")):
    """Why is the home feed empty?

    /feed serves `ai_processed=1 AND status='published'`, so a feed can be empty
    for four unrelated reasons that all look identical from the app. This answers
    which one it is in a single call, and names the fix.
    """
    _check_admin(x_admin_token or token)       # ?token= for the address bar
    conn = get_db()
    try:
        one = lambda q, p=(): conn.execute(q, p).fetchone()["c"]
        total     = one("SELECT COUNT(*) c FROM articles")
        servable  = one("SELECT COUNT(*) c FROM articles "
                        "WHERE ai_processed=1 AND status='published'")
        pending   = one("SELECT COUNT(*) c FROM articles WHERE status='pending_rewrite'")
        blocked   = one("SELECT COUNT(*) c FROM articles WHERE status='blocked_originality'")
        unproc    = one("SELECT COUNT(*) c FROM articles WHERE COALESCE(ai_processed,0)=0")
        newest    = conn.execute(
            "SELECT MAX(published_at) AS t FROM articles "
            "WHERE ai_processed=1 AND status='published'").fetchone()["t"]

        # Ordered most-specific first: the first matching cause is the one to act on.
        if total == 0:
            diagnosis, fix = ("no articles ingested at all",
                              "the collector has never run — check COLLECT_INTERVAL_MIN "
                              "and the scheduler logs")
        elif servable == 0 and pending > 0:
            diagnosis, fix = (f"{pending} articles are held in pending_rewrite",
                              "POST /admin/publish-pending?dry_run=false")
        elif servable == 0 and blocked > 0:
            diagnosis, fix = (f"{blocked} articles failed the originality gate",
                              "they are verbatim copies — they need the AI rewrite, "
                              "not a republish")
        elif servable == 0 and unproc > 0:
            diagnosis, fix = (f"{unproc} articles are ingested but never AI-processed",
                              "POST /admin/reprocess — the AI cycle is not completing")
        elif servable == 0:
            diagnosis, fix = ("articles exist but none are servable",
                              "check status and ai_processed on a sample row")
        else:
            diagnosis, fix = (f"{servable} articles are servable — the feed is not empty",
                              "if the app still shows nothing, the problem is "
                              "client-side or CORS, not the corpus")

        return {"total": total, "servable": servable, "newest_published_at": newest,
                "pending_rewrite": pending, "blocked_originality": blocked,
                "not_ai_processed": unproc,
                "diagnosis": diagnosis, "fix": fix}
    finally:
        conn.close()


@app.post("/admin/publish-pending")
async def admin_publish_pending(
    dry_run: bool = Query(True),
    limit: int = Query(0, ge=0),
    x_admin_token: str = Header(""),
    token: str = Query(""),
):
    """Release pending_rewrite articles into the feed, aggregator-style.

    Same logic as scripts/publish_pending.py --mode aggregator, exposed over HTTP
    because the deployment has no shell and its sqlite file is not reachable from
    anywhere else. Only the aggregator posture is available here: the publisher's
    headline is kept WITH credit and an outbound link, and the body is replaced
    with our own stub. `--mode force` republishes somebody else's prose, so it
    stays a deliberate command-line act and is deliberately not routable.

    dry_run defaults to true — a corpus-wide write is never one accidental URL away.
    Accepts ?token= as well as the header, for the same reason feed-doctor does.
    """
    _check_admin(x_admin_token or token)
    sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), "scripts"))
    return await asyncio.get_event_loop().run_in_executor(
        None, lambda: _run_drain_for_backend(dry_run=dry_run, limit=limit or None))


@app.post("/admin/relink")
async def admin_relink(x_admin_token: str = Header("")):
    """Recompute all story threads on demand."""
    _check_admin(x_admin_token)
    conn = get_db()
    threads = link_stories(conn)
    conn.close()
    return {"threads": threads, "window_days": STORY_WINDOW_DAYS}


@app.post("/admin/rescope")
async def admin_rescope(x_admin_token: str = Header("")):
    """Re-bucket every article into local / national / global with the current
    classifier, so the Explore region tabs work on existing rows immediately.
    Cheap and AI-free — recomputes from the headline + body text."""
    _check_admin(x_admin_token)
    conn = get_db()
    rows = conn.execute("SELECT id, headline, summary_60, full_body FROM articles").fetchall()
    counts = {"local": 0, "national": 0, "global": 0}
    for r in rows:
        body = r["full_body"] or r["summary_60"] or ""
        sc = classify_scope(r["headline"] or "", body)
        counts[sc] = counts.get(sc, 0) + 1
        conn.execute("UPDATE articles SET scope=? WHERE id=?", (sc, r["id"]))
    conn.commit()
    conn.close()
    log.info("[RESCOPE] %d rows → %s", len(rows), counts)
    return {"rescoped": len(rows), "distribution": counts}


def _live_thread_ids(conn, article_id: int, window_days: int = STORY_WINDOW_DAYS,
                     limit: int = 8) -> list:
    """On-demand thread: find same-category articles that share ≥2 significant
    terms with this one, so the "String" works even before link_stories has run.
    Returns article ids (current first, strongest matches next)."""
    base = conn.execute(
        "SELECT id, headline, micro_tags, pillar_id FROM articles "
        "WHERE id=? AND ai_processed=1 AND status='published'", (article_id,)
    ).fetchone()
    if not base:
        return []
    try:
        base_tags = json.loads(base["micro_tags"] or "[]")
    except Exception:
        base_tags = []
    base_terms = _story_terms(base["headline"], base_tags)
    if len(base_terms) < 2:
        return []

    cands = conn.execute(
        "SELECT id, headline, micro_tags FROM articles "
        "WHERE ai_processed=1 AND status='published' AND pillar_id=? AND id!=? "
        "AND published_at >= datetime('now', ?)",
        (base["pillar_id"], article_id, f"-{int(window_days)} days")
    ).fetchall()

    scored = []
    for c in cands:
        try:
            tags = json.loads(c["micro_tags"] or "[]")
        except Exception:
            tags = []
        shared = len(base_terms & _story_terms(c["headline"], tags))
        if shared >= 2:
            scored.append((shared, c["id"]))
    if not scored:
        return []
    scored.sort(key=lambda x: (-x[0], x[1]))
    return [article_id] + [cid for _, cid in scored[:limit]]


@app.get("/story/{article_id}")
async def get_story(article_id: int):
    """Return the chronological story thread ("string") an article belongs to.

    Prefers a precomputed thread (story_id from link_stories); if none exists
    yet, computes a matching thread live so the feature works immediately."""
    conn = get_db()
    art = conn.execute("SELECT story_id FROM articles WHERE id=?", (article_id,)).fetchone()
    if not art:
        conn.close()
        raise HTTPException(404, "Article not found")

    # A REAL story thread is a handful of articles about one event. When
    # link_stories over-merges — which it does when bodies are placeholders and
    # the only signal left is generic tags — every article in a pillar can land
    # on one story_id, and the reader gets "2319 UPDATES" of unrelated stories
    # under a headline they were reading. A thread that large is not a thread,
    # so it is discarded and the live term-overlap path is used instead.
    sid = (art["story_id"] or 0) if "story_id" in art.keys() else 0
    if sid:
        n = conn.execute(
            "SELECT COUNT(*) AS c FROM articles WHERE story_id=? AND "
            "ai_processed=1 AND status='published'", (sid,)).fetchone()["c"]
        if n > MAX_THREAD_SIZE:
            log.info("[STORY] story_id=%s has %d members — over-merged, "
                     "falling back to live matching", sid, n)
            sid = 0
    if sid:
        rows = conn.execute(
            "SELECT * FROM articles WHERE story_id=? AND ai_processed=1 AND status='published' "
            "ORDER BY published_at ASC, id ASC",
            (sid,)
        ).fetchall()
    else:
        # No precomputed thread — build one on the fly.
        ids = _live_thread_ids(conn, article_id)
        if len(ids) < 2:
            conn.close()
            return {"story_id": 0, "count": 0, "thread": []}
        placeholders = ",".join("?" for _ in ids)
        rows = conn.execute(
            f"SELECT * FROM articles WHERE id IN ({placeholders}) AND ai_processed=1 AND status='published' "
            "ORDER BY published_at ASC, id ASC",
            ids
        ).fetchall()
    conn.close()

    thread = []
    for r in rows:
        d = article_row_to_dict(r)
        thread.append({
            "id": d["id"],
            "headline": d.get("headline", ""),
            "summary": d.get("summary_60", ""),
            "published_at": d.get("published_at", ""),
            "pillar_slug": d.get("pillar_slug", ""),
            "pillar_name": d.get("pillar_name", ""),
            "pillar_color": d.get("pillar_color", "#1E88E5"),
            "image_url": d.get("image_url", ""),
            "source": "SherrByte News",
            "is_current": d["id"] == article_id,
        })

    return {"story_id": sid, "count": len(thread), "thread": thread}


@app.get("/health")
async def health():
    """LIVENESS ONLY. No database, no upstream, no I/O of any kind.

    This used to run thirteen sequential COUNT(*) queries — four totals plus one
    per pillar — each a round trip over the Supabase pooler, on the event loop.
    Under load that exceeded Render's 5s health-check timeout, Render killed the
    instance, and the restart put it straight back into the same collection cycle:
    a permanent boot loop caused entirely by the health check itself.

    A liveness probe answers one question — is this process able to serve? — and
    anything that can be slow or can fail belongs somewhere a probe will not
    reach. The counts moved to /admin/stats.
    """
    return {"status": "ok", "version": "5.0.0",
            "pillars": 9, "micro_topics": len(MICRO_TOPICS)}


@app.get("/admin/stats")
async def admin_stats(x_admin_token: str = Header(""), token: str = Query("")):
    """The corpus counts /health used to carry.

    Three queries rather than thirteen: conditional aggregation for the totals
    and a GROUP BY for the pillars. The old shape cost one pooler round trip per
    pillar to compute something one scan answers. Run off the loop so a slow
    database cannot block request handling here either.

    Accepts ?token= as well as the header, because a browser address bar cannot
    set headers and this is the endpoint you reach for when the app is down.
    """
    _check_admin(x_admin_token or token)

    def _gather() -> dict:
        conn = get_db()
        try:
            agg = conn.execute(
                "SELECT COUNT(*) AS total, "
                "SUM(CASE WHEN is_trending=1 THEN 1 ELSE 0 END) AS trending, "
                "SUM(CASE WHEN ai_processed=1 AND status='published' "
                "         THEN 1 ELSE 0 END) AS servable "
                "FROM articles").fetchone()
            by_pillar = {r["pillar_id"]: r["c"] for r in conn.execute(
                "SELECT pillar_id, COUNT(*) AS c FROM articles GROUP BY pillar_id")}
            users = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()["c"]
        finally:
            conn.close()
        return {
            "articles": agg["total"] or 0,
            "users": users,
            "ai_processed": agg["servable"] or 0,
            "trending": agg["trending"] or 0,
            "pillar_counts": {PILLARS[pid]["slug"]: by_pillar.get(pid, 0)
                              for pid in range(1, 10)},
            "ai": available_providers(),
        }

    return await asyncio.get_event_loop().run_in_executor(None, _gather)


@app.get("/pillars")
async def get_pillars():
    return _pillars_payload()


@app.get("/topics/search")
async def search_topics(q: str = Query("")):
    q_lower = q.lower()
    matches = [
        {"name": t, "pillar_id": pid, "color": PILLARS[pid]["color"],
         "emoji": PILLARS[pid]["emoji"], "pillar_name": PILLARS[pid]["name"]}
        for t, pid in MICRO_TOPICS.items() if q_lower in t.lower()
    ]
    return {"topics": matches[:40]}


# ─── RUN ─────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    import os
    port = int(os.environ.get("PORT", 10000))
    uvicorn.run("main:app", host="0.0.0.0", port=port)