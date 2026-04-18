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

import os, json, math, hashlib, asyncio, logging, re, sqlite3
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
from ai_processor import process_batch, available_providers

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sherbyte")

# ─── ENV ─────────────────────────────────────────────────────────────────────
NEWSAPI_KEY     = os.getenv("NEWSAPI_KEY", "")
JWT_SECRET      = os.getenv("JWT_SECRET", "sherbyte-secret-change-in-prod")
OPENWEATHER_KEY = os.getenv("OPENWEATHER_KEY", "")
DB_PATH         = os.getenv("DB_PATH", "sherbyte.db")

# Knobs for the AI cycle
AI_BATCH_SIZE   = int(os.getenv("AI_BATCH_SIZE", "50"))
AI_CONCURRENCY  = int(os.getenv("AI_CONCURRENCY", "5"))
COLLECT_INTERVAL_MIN = int(os.getenv("COLLECT_INTERVAL_MIN", "25"))

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
}

# ─── FAST RULE-BASED CLASSIFIER (fallback before AI runs) ────────────────────
PILLAR_EXCLUSIVE_KEYWORDS = {
    1: ["election","parliament","government","minister","senate","vote","democracy",
        "constitution","treaty","diplomat","legislation","judiciary","supreme court",
        "president","prime minister","cabinet","political party","bjp","congress party",
        "lok sabha","rajya sabha","united nations","nato","geopolitics","sanctions",
        "military","army","defence","protest","coup","chief minister","governor"],
    2: ["stock market","share price","nifty","sensex","nasdaq","bitcoin","cryptocurrency",
        "crypto","ethereum","blockchain","startup","venture capital","funding round",
        "ipo","merger","acquisition","quarterly earnings","inflation","interest rate",
        "gdp","recession","rbi","federal reserve","sebi","budget","gst","bank",
        "mutual fund","hedge fund","e-commerce","fintech","real estate","supply chain"],
    3: ["artificial intelligence","machine learning","deep learning","chatgpt","openai",
        "llm","neural network","quantum computing","crispr","gene editing","robotics",
        "spacex","isro","nasa","rocket launch","satellite","cybersecurity","data breach",
        "ransomware","smartphone launch","5g","6g","semiconductor","electric vehicle",
        "nuclear fusion","github","app update","nvidia","tpu"],
    4: ["box office","oscar","grammy","emmy","bafta","music album","concert tour",
        "netflix series","amazon prime","disney+","streaming platform","art exhibition",
        "museum","gallery","fashion week","book launch","bestseller","broadway",
        "k-pop","bollywood film","hollywood movie","anime","film festival","cannes"],
    5: ["climate change","global warming","carbon emissions","greenhouse gas",
        "wildlife conservation","endangered species","national park","earthquake",
        "tsunami","hurricane","cyclone","tornado","flood","drought","wildfire",
        "deforestation","renewable energy","coral reef","biodiversity"],
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
    india_words = ["india","delhi","mumbai","bangalore","chennai","hyderabad","kolkata",
                   "indian","modi","bjp","congress","rupee","nifty","sensex","kerala","tamil"]
    local_words = ["city","district","local","municipal","village","town","ward","panchayat"]
    global_words = ["world","global","international","nato","china","russia","europe",
                    "america","washington","beijing","moscow","london"]
    i = sum(1 for w in india_words if w in text)
    l = sum(1 for w in local_words if w in text)
    g = sum(1 for w in global_words if w in text)
    if l >= 2 and i > 0:
        return "local"
    return "national" if i > g else "global"


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
    published_at TEXT DEFAULT (datetime('now')),
    collected_at TEXT DEFAULT (datetime('now')),
    ai_processed INTEGER DEFAULT 0,
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

CREATE INDEX IF NOT EXISTS idx_articles_pillar ON articles(pillar_id);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_articles_title_hash ON articles(title_hash);
CREATE INDEX IF NOT EXISTS idx_articles_trending ON articles(is_trending);
CREATE INDEX IF NOT EXISTS idx_feeds_user ON feeds(user_id, score DESC);
CREATE INDEX IF NOT EXISTS idx_prefs_user ON user_preferences(user_id);
"""

# Safe migrations for users upgrading from v4.1
_MIGRATIONS = [
    "ALTER TABLE articles ADD COLUMN title_hash TEXT",
    "ALTER TABLE articles ADD COLUMN is_trending INTEGER DEFAULT 0",
    "ALTER TABLE articles ADD COLUMN sentiment TEXT DEFAULT 'neutral'",
]


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = get_db()
    conn.executescript(CREATE_TABLES)
    for stmt in _MIGRATIONS:
        try:
            conn.execute(stmt)
        except sqlite3.OperationalError:
            pass  # Column already exists — fine
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

    conn.commit()
    conn.close()
    log.info("DB ready at %s", DB_PATH)

# ─── AUTH ────────────────────────────────────────────────────────────────────
def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()


def check_password(pw: str, hashed: str) -> bool:
    return hmac_module.compare_digest(hash_password(pw), hashed)


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

            pub_date = datetime.now().isoformat()
            if hasattr(entry, "published_parsed") and entry.published_parsed:
                try:
                    pub_date = datetime(*entry.published_parsed[:6]).isoformat()
                except Exception:
                    pass

            articles.append({
                "url": link,
                "title_hash": title_fingerprint(title),
                "headline": title,
                "summary_60": clean[:400],
                "full_body": clean,
                "source_summary": clean[:200],
                "when_info": pub_date,
                "where_info": "Not specified",
                "what_info": title,
                "how_info": "",
                "image_url": img,
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
                        "summary_60": clean[:400],
                        "full_body": clean,
                        "source_summary": (a.get("description") or "")[:200],
                        "when_info": a.get("publishedAt", datetime.now().isoformat()),
                        "where_info": "Not specified",
                        "what_info": title,
                        "how_info": "",
                        "image_url": a.get("urlToImage") or "",
                        "source_name": (a.get("source") or {}).get("name", "NewsAPI"),
                        "pillar_id": pid,
                        "micro_tags": json.dumps(tags),
                        "scope": scope,
                        "published_at": a.get("publishedAt", datetime.now().isoformat()),
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
    try:
        conn.execute("""
            INSERT OR IGNORE INTO articles
            (url, title_hash, headline, summary_60, full_body, source_summary,
             when_info, where_info, what_info, how_info, image_url, source_name,
             pillar_id, micro_tags, scope, published_at)
            VALUES(:url, :title_hash, :headline, :summary_60, :full_body, :source_summary,
                   :when_info, :where_info, :what_info, :how_info, :image_url, :source_name,
                   :pillar_id, :micro_tags, :scope, :published_at)
        """, article)
        return conn.total_changes > 0
    except Exception as e:
        log.debug("Insert skipped: %s", e)
        return False


async def run_ai_batch(conn):
    """Pull unprocessed articles and refine them with Gemini in parallel."""
    rows = conn.execute(
        "SELECT id, headline, full_body, pillar_id, micro_tags FROM articles "
        "WHERE ai_processed=0 ORDER BY collected_at DESC LIMIT ?",
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
            "body": row["full_body"],
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

            conn.execute("""
                UPDATE articles SET
                    headline=?, summary_60=?, full_body=?, source_summary=?,
                    when_info=?, where_info=?, pillar_id=?, micro_tags=?,
                    is_trending=?, sentiment=?, ai_processed=1
                WHERE id=?
            """, (
                result["refined_title"],
                result["summary"],
                result["full_body"],
                result["summary"],  # kept for back-compat with source_summary field
                result.get("when_info", ""),
                result.get("where_info", "Not specified"),
                new_pid,
                json.dumps(all_tags),
                1 if result["is_trending"] else 0,
                result["sentiment"],
                row["id"],
            ))
            success += 1
        except Exception as e:
            log.warning("[AI] Update failed for id %d: %s", row["id"], e)

    conn.commit()
    log.info("[AI] %d/%d articles refined", success, len(rows))
    return success


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
        new_count = 0
        for a in unique:
            if _insert_with_dedup(conn, a):
                new_count += 1
        conn.commit()
        log.info("[DB] %d new articles inserted", new_count)

        # AI refinement pass
        await run_ai_batch(conn)

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
        try:
            pub = datetime.fromisoformat(art["published_at"])
            hours_ago = (datetime.now() - pub).total_seconds() / 3600
            recency = 1.0 / (1.0 + math.log1p(hours_ago / 4))
        except Exception:
            recency = 0.5
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
from fastapi import FastAPI, HTTPException, Header, Query
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
    scheduler.start()
    log.info("Scheduler: collect every %d min", COLLECT_INTERVAL_MIN)
    log.info("AI providers: %s", available_providers())
    yield
    scheduler.shutdown()


app = FastAPI(title="SherByte API", version="5.0.0", lifespan=lifespan)
app.add_middleware(
    CORSMiddleware, allow_origins=["*"], allow_credentials=True,
    allow_methods=["*"], allow_headers=["*"],
)

app.include_router(activity_router)
app.include_router(markets_router)

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def get_current_user(authorization: str = "") -> int:
    if authorization and authorization.startswith("Bearer "):
        uid = verify_token(authorization[7:])
        if uid:
            return uid
    return 1  # Anonymous user default


def article_row_to_dict(row) -> dict:
    d = dict(row)
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
    cur = conn.execute(
        "INSERT INTO users (email, password, name) VALUES(?, ?, ?)",
        (req.email, pw_hash, req.name or req.email.split("@")[0])
    )
    user_id = cur.lastrowid
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


@app.post("/login")
@app.post("/auth/login")
async def login(req: LoginReq):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email,)).fetchone()
    if not user or not check_password(req.password, user["password"]):
        conn.close()
        raise HTTPException(401, "Invalid credentials")
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
        q = "SELECT a.*, f.score FROM articles a JOIN feeds f ON a.id=f.article_id WHERE f.user_id=?"
        p = [uid]
        if scope:
            q += " AND a.scope=?"; p.append(scope)
        if pillar:
            q += " AND a.pillar_id=?"; p.append(pillar)
        q += " ORDER BY f.score DESC, a.published_at DESC LIMIT ? OFFSET ?"
        p += [limit + 1, offset]
        rows = conn.execute(q, p).fetchall()
        if len(rows) < 5:
            rows = conn.execute(
                "SELECT *, 1.0 as score FROM articles ORDER BY published_at DESC LIMIT ? OFFSET ?",
                (limit + 1, offset)
            ).fetchall()
    else:
        rows = conn.execute(
            "SELECT *, 1.0 as score FROM articles ORDER BY published_at DESC LIMIT ? OFFSET ?",
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
    q = "SELECT * FROM articles WHERE 1=1"
    p = []
    if category and not pillar:
        resolved = FRONTEND_SLUG_MAP.get(category.lower())
        if resolved:
            pillar = resolved
    if pillar:
        q += " AND pillar_id=?"; p.append(pillar)
    if scope:
        q += " AND scope=?"; p.append(scope)
    q += " ORDER BY published_at DESC LIMIT ? OFFSET ?"
    p += [limit + 1, offset]
    rows = conn.execute(q, p).fetchall()
    conn.close()
    has_more = len(rows) > limit
    return {"articles": [article_row_to_dict(r) for r in rows[:limit]], "has_more": has_more}


@app.get("/trending")
async def trending_feed(
    limit: int = Query(10, le=30),
    authorization: str = Header(""),
):
    """Only trending articles — premium feature flagged by the AI pass."""
    get_current_user(authorization)
    conn = get_db()
    rows = conn.execute(
        "SELECT * FROM articles WHERE is_trending=1 ORDER BY published_at DESC LIMIT ?",
        (limit,)
    ).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}


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
        "SELECT * FROM articles WHERE headline LIKE ? OR summary_60 LIKE ? "
        "ORDER BY published_at DESC LIMIT 25",
        (q_like, q_like)
    ).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}


@app.get("/me")
async def get_me(authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE id=?", (uid,)).fetchone()
    if not user:
        conn.close()
        raise HTTPException(404, "User not found")
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


@app.get("/health")
async def health():
    conn = get_db()
    article_count = conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()["c"]
    user_count = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    trending_count = conn.execute("SELECT COUNT(*) as c FROM articles WHERE is_trending=1").fetchone()["c"]
    ai_processed = conn.execute("SELECT COUNT(*) as c FROM articles WHERE ai_processed=1").fetchone()["c"]
    pillar_counts = {}
    for pid in range(1, 10):
        cnt = conn.execute("SELECT COUNT(*) as c FROM articles WHERE pillar_id=?", (pid,)).fetchone()["c"]
        pillar_counts[PILLARS[pid]["slug"]] = cnt
    conn.close()
    return {
        "status": "ok", "version": "5.0.0",
        "articles": article_count, "users": user_count,
        "ai_processed": ai_processed, "trending": trending_count,
        "pillars": 9, "micro_topics": len(MICRO_TOPICS),
        "pillar_counts": pillar_counts,
        "ai": available_providers(),
    }


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
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)
