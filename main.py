"""
SherByte Backend — Pro-Prototype v4.0
9-Pillar Taxonomy | WWWW Articles | Personalized Feed | JWT Auth
Run: python main.py
"""

import os, json, time, math, hashlib, asyncio, logging, re
from datetime import datetime, timedelta, timezone
from contextlib import asynccontextmanager
from typing import Optional, List
import httpx
import feedparser
from dotenv import load_dotenv

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("sherbyte")

# ─── ENV ─────────────────────────────────────────────────────────────────────
GROK_API_KEY     = os.getenv("GROK_API_KEY", "")
NEWSAPI_KEY      = os.getenv("NEWSAPI_KEY", "")
NEWSDATA_KEY     = os.getenv("NEWSDATA_KEY", "")
JWT_SECRET       = os.getenv("JWT_SECRET", "sherbyte-secret-change-in-prod")
OPENWEATHER_KEY  = os.getenv("OPENWEATHER_KEY", "")

# ─── TAXONOMY: 9 PILLARS ─────────────────────────────────────────────────────
PILLARS = {
    1: {"name": "Society & Governance",  "color": "#1E88E5", "emoji": "🏛️",  "slug": "society"},
    2: {"name": "Business & Economy",    "color": "#FBC02D", "emoji": "💼",  "slug": "business"},
    3: {"name": "Science & Technology",  "color": "#3949AB", "emoji": "🔬",  "slug": "science"},
    4: {"name": "Arts & Culture",        "color": "#E53935", "emoji": "🎭",  "slug": "arts"},
    5: {"name": "Natural World",         "color": "#43A047", "emoji": "🌿",  "slug": "nature"},
    6: {"name": "Self & Well-being",     "color": "#FB8C00", "emoji": "🧘",  "slug": "wellbeing"},
    7: {"name": "Philosophy & Belief",   "color": "#8E24AA", "emoji": "🔮",  "slug": "philosophy"},
    8: {"name": "Society & Lifestyle",   "color": "#00ACC1", "emoji": "✨",  "slug": "lifestyle"},
    9: {"name": "Sports & Gaming",       "color": "#546E7A", "emoji": "⚽",  "slug": "sports"},
}

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

# Full micro-topic → pillar_id mapping
MICRO_TOPICS: dict[str, int] = {
    # Pillar 1 — Society & Governance
    "Elections2024":1,"UCC":1,"Lok Sabha":1,"Federalism":1,"NATO":1,"G20 Summit":1,
    "Soft Power":1,"Supreme Court":1,"Human Rights":1,"Intellectual Property":1,
    "Cyber Law":1,"Smart Cities":1,"Public Transport":1,"Zoning Laws":1,"NEP 2020":1,
    "Ivy League":1,"STEM Education":1,"Montessori":1,"Feminism":1,"LGBTQ+ Rights":1,
    "Anti-Corruption":1,"Sociology":1,"Anthropology":1,"Criminology":1,"Demography":1,
    "Diplomacy":1,"United Nations":1,"EU":1,"Brexit":1,"Constitutional Law":1,
    "Public Policy":1,"Urban Planning":1,"Immigration Law":1,"Asylum Policy":1,
    "Trade Unions":1,"Local Government":1,"Geopolitics":1,"Military Strategy":1,
    "Nuclear Policy":1,"Voter Behavior":1,"Judiciary":1,"Elections":1,"Parliament":1,
    "Government":1,"Politics":1,"Law":1,"Education":1,"Justice":1,
    # Pillar 2 — Business & Economy
    "Stock Market":2,"Nifty 50":2,"NASDAQ":2,"Mutual Funds":2,"Options Trading":2,
    "Forex":2,"Cryptocurrency":2,"Bitcoin":2,"Ethereum":2,"Mergers & Acquisitions":2,
    "Corporate Law":2,"Supply Chain":2,"Venture Capital":2,"Seed Funding":2,
    "Unicorns":2,"SaaS":2,"Bootstrapping":2,"Inflation":2,"Interest Rates":2,
    "GDP":2,"Recession":2,"SEO":2,"Affiliate Marketing":2,"Neuromarketing":2,
    "Real Estate":2,"PropTech":2,"REITs":2,"FinTech":2,"E-Commerce":2,
    "Payment Gateways":2,"Business Intelligence":2,"Actuarial Science":2,
    "Personal Finance":2,"Wealth Management":2,"Passive Income":2,"Tax Planning":2,
    "Gig Economy":2,"Labor Markets":2,"Consumer Behavior":2,"Startup":2,"IPO":2,
    "Economy":2,"Finance":2,"Market":2,"Trade":2,"Business":2,"Investment":2,
    # Pillar 3 — Science & Technology
    "Artificial Intelligence":3,"LLMs":3,"ChatGPT":3,"Neural Networks":3,
    "Generative AI":3,"Computer Vision":3,"AI Ethics":3,"Full Stack":3,"DevOps":3,
    "Cloud Computing":3,"AWS":3,"Cybersecurity":3,"Hacking":3,"Arduino":3,
    "Raspberry Pi":3,"Robotics":3,"Drones":3,"3D Printing":3,"Digital Minimalism":3,
    "Dark Web":3,"Quantum Computing":3,"Nuclear Fusion":3,"CRISPR":3,
    "Gene Editing":3,"Astrophysics":3,"Black Holes":3,"Dark Matter":3,"SpaceX":3,
    "ISRO":3,"NASA":3,"Game Theory":3,"Cryptography":3,"Chaos Theory":3,
    "Nanotechnology":3,"Aerospace Engineering":3,"Web3":3,"Metaverse":3,
    "Semiconductors":3,"Battery Tech":3,"Telecommunications":3,"AI":3,"Tech":3,
    "Science":3,"Research":3,"Space":3,"Innovation":3,"Software":3,"Programming":3,
    # Pillar 4 — Arts & Culture
    "Game of Thrones":4,"Marvel Cinematic Universe":4,"Anime":4,"Studio Ghibli":4,
    "Dark Academia":4,"Cyberpunk":4,"Cottagecore":4,"Y2K Aesthetic":4,"Old Money":4,
    "Pottery":4,"Calligraphy":4,"Streetwear":4,"Sustainable Fashion":4,
    "Sneaker Culture":4,"Filmmaking":4,"Cinematography":4,"Graphic Design":4,
    "UX Design":4,"Photography":4,"Color Theory":4,"K-Pop":4,"Lo-Fi":4,
    "Vinyl Collecting":4,"Podcasting":4,"Fanfiction":4,"Poetry":4,"Sci-Fi":4,
    "Classic Novels":4,"Goth Subculture":4,"Punk":4,"Cosplay":4,"Typography":4,
    "Modern Art":4,"Architecture":4,"Sculpture":4,"Digital Art":4,"NFTs":4,
    "Broadway":4,"Jazz":4,"Electronic Music":4,"Music":4,"Film":4,"TV":4,
    "Entertainment":4,"Culture":4,"Art":4,"Books":4,"Literature":4,
    # Pillar 5 — Natural World
    "Climate Change":5,"Global Warming":5,"Renewable Energy":5,"Solar Power":5,
    "Wind Energy":5,"Zero Waste":5,"Conservation":5,"Houseplants":5,
    "Hydroponics":5,"Permaculture":5,"Bonsai":5,"Urban Farming":5,
    "Birdwatching":5,"Herpetology":5,"Marine Biology":5,"Zoology":5,
    "Entomology":5,"Animal Behavior":5,"Genetics":5,"Evolution":5,
    "Microbiology":5,"Botany":5,"Cartography":5,"Van Life":5,"Ecotourism":5,
    "National Parks":5,"Hiking":5,"Auroras":5,"Eclipses":5,"Volcanoes":5,
    "Meteorology":5,"Geology":5,"Oceanography":5,"Rewilding":5,
    "Circular Economy":5,"Wildlife Photography":5,"Endangered Species":5,
    "Forestry":5,"Agriculture":5,"Sustainable Living":5,"Nature":5,
    "Environment":5,"Wildlife":5,"Ecology":5,"Weather":5,"Animals":5,
    # Pillar 6 — Self & Well-being
    "Mental Health":6,"Mindfulness":6,"Meditation":6,"Therapy":6,"Shadow Work":6,
    "Dopamine Detox":6,"Fitness":6,"Yoga":6,"CrossFit":6,"Calisthenics":6,
    "Pilates":6,"Marathon Training":6,"Nutrition":6,"Veganism":6,"Keto":6,
    "Sourdough Baking":6,"Minimalism":6,"KonMari":6,"Interior Design":6,
    "Gentle Parenting":6,"Eldercare":6,"Attachment Theory":6,"Dating Advice":6,
    "Social Skills":6,"Productivity":6,"Atomic Habits":6,"Time Blocking":6,
    "Pomodoro":6,"Notion Setups":6,"Sleep Hygiene":6,"Biohacking":6,
    "Coffee Roasting":6,"Meal Prepping":6,"Journaling":6,"Self-Care":6,
    "Career Coaching":6,"Public Speaking":6,"Longevity":6,"Stress Management":6,
    "Personal Growth":6,"Health":6,"Wellness":6,"Diet":6,"Exercise":6,
    # Pillar 7 — Philosophy & Belief
    "Philosophy":7,"Stoicism":7,"Nihilism":7,"Absurdism":7,"Existentialism":7,
    "Utilitarianism":7,"Ethics":7,"Bioethics":7,"Hinduism":7,"Buddhism":7,
    "Christianity":7,"Islam":7,"Sikhism":7,"Theology":7,"Mysticism":7,
    "Sufism":7,"Tarot":7,"Astrology":7,"Crystals":7,"Lucid Dreaming":7,
    "Consciousness":7,"Panpsychism":7,"Free Will":7,"Mythology":7,
    "Greek Mythology":7,"Norse Mythology":7,"Hindu Epics":7,"Urban Legends":7,
    "Alchemy":7,"Occult":7,"Secret Societies":7,"Epistemology":7,"Metaphysics":7,
    "Meditation Techniques":7,"Zen":7,"Taoism":7,"Shamanism":7,"Kabbalah":7,
    "Religion":7,"Spirituality":7,"Belief":7,"Mindset":7,
    # Pillar 8 — Society & Lifestyle
    "Digital Nomad":8,"Co-Living":8,"Micro-Apartments":8,"Social Media Trends":8,
    "Privacy":8,"Big Data":8,"Subscription Economy":8,"Ethical Consumerism":8,
    "Fast Fashion":8,"Travel Hacking":8,"Glamping":8,"Backpacking":8,
    "Street Food":8,"Fusion Cuisine":8,"Coffee Culture":8,"Craft Beer":8,
    "Mixology":8,"Modern Etiquette":8,"Networking":8,"Digital Boundaries":8,
    "Work-Life Balance":8,"Future of Work":8,"Remote Work":8,"Smart Home":8,
    "Vlogging":8,"TikTok Trends":8,"Influencer Marketing":8,"Pop Culture":8,
    "Celebrity News":8,"Nightlife":8,"Festivals":8,"Burning Man":8,
    "Gifting Culture":8,"Decluttering":8,"Sustainable Travel":8,"Luxury Travel":8,
    "Solo Travel":8,"Pet Culture":8,"Cat Cafes":8,"Communal Living":8,
    "Travel":8,"Food":8,"Lifestyle":8,"Social Media":8,"Fashion":8,
    # Pillar 9 — Sports & Gaming
    "Cricket":9,"IPL":9,"Football":9,"Premier League":9,"F1":9,"Olympics":9,
    "NBA":9,"Tennis":9,"Wimbledon":9,"Esports":9,"League of Legends":9,
    "Valorant":9,"CS:GO":9,"Dota 2":9,"Game Development":9,"Unreal Engine":9,
    "Unity":9,"Indie Dev":9,"Retro Gaming":9,"Speedrunning":9,"Nintendo":9,
    "PlayStation":9,"Xbox":9,"Tabletop Games":9,"Chess":9,"Poker":9,
    "Board Games":9,"Sports Science":9,"Biomechanics":9,"Recovery Tech":9,
    "Fantasy Sports":9,"Sports Betting":9,"Gymnastics":9,"Swimming":9,
    "Athletics":9,"MMA":9,"UFC":9,"Wrestling":9,"E-cycling":9,
    "Sports Analytics":9,"Coaching Science":9,"Sports":9,"Gaming":9,
    "Game":9,"Match":9,"Tournament":9,"Player":9,"Team":9,
}

# Keywords for auto-classification
PILLAR_KEYWORDS = {
    1: ["election","parliament","government","court","law","policy","politics","minister",
        "senate","congress","vote","democracy","constitution","military","treaty","diplomat"],
    2: ["stock","market","economy","bitcoin","crypto","startup","revenue","GDP","inflation",
        "company","billion","million","investment","bank","fund","trade","finance","IPO"],
    3: ["AI","artificial intelligence","robot","space","NASA","quantum","cyber","hack",
        "software","tech","science","research","climate model","algorithm","data","machine learning"],
    4: ["film","movie","music","art","culture","fashion","design","book","novel","series",
        "celebrity","award","oscar","grammy","concert","album","exhibition","gallery"],
    5: ["climate","environment","wildlife","forest","ocean","species","ecology","nature",
        "renewable","solar","conservation","weather","earthquake","flood","biodiversity"],
    6: ["health","fitness","mental","yoga","diet","nutrition","wellness","therapy","exercise",
        "sleep","stress","meditation","productivity","habit","career","self-care"],
    7: ["philosophy","religion","faith","god","spiritual","meditation","ethics","consciousness",
        "mythology","belief","astrology","metaphysics","hinduism","buddhism","islam"],
    8: ["lifestyle","travel","food","fashion","social media","influencer","nomad","remote work",
        "restaurant","cuisine","luxury","trend","culture","festival","community"],
    9: ["cricket","football","soccer","tennis","F1","Olympics","NBA","IPL","esports","gaming",
        "game","match","tournament","player","team","championship","league","score"],
}

# RSS feeds — 24 sources
RSS_FEEDS = [
    ("https://feeds.feedburner.com/ndtvnews-top-stories",         "NDTV"),
    ("https://timesofindia.indiatimes.com/rssfeedstopstories.cms","Times of India"),
    ("https://www.thehindu.com/feeder/default.rss",               "The Hindu"),
    ("https://feeds.feedburner.com/gadgets360-latest",            "Gadgets 360"),
    ("https://economictimes.indiatimes.com/rssfeedsdefault.cms",  "Economic Times"),
    ("https://www.moneycontrol.com/rss/latestnews.xml",           "MoneyControl"),
    ("https://feeds.bbci.co.uk/news/rss.xml",                     "BBC News"),
    ("https://feeds.bbci.co.uk/news/technology/rss.xml",          "BBC Tech"),
    ("https://feeds.bbci.co.uk/news/science_and_environment/rss.xml","BBC Science"),
    ("https://techcrunch.com/feed/",                              "TechCrunch"),
    ("https://www.wired.com/feed/rss",                            "Wired"),
    ("https://feeds.arstechnica.com/arstechnica/index",           "Ars Technica"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/World.xml",    "NYT World"),
    ("https://rss.nytimes.com/services/xml/rss/nyt/Technology.xml","NYT Tech"),
    ("https://www.theguardian.com/world/rss",                     "The Guardian"),
    ("https://www.aljazeera.com/xml/rss/all.xml",                 "Al Jazeera"),
    ("https://feeds.skynews.com/feeds/rss/world.xml",             "Sky News"),
    ("https://www.espn.com/espn/rss/news",                        "ESPN"),
    ("https://feeds.feedburner.com/ign/games-all",                "IGN"),
    ("https://www.sciencedaily.com/rss/top.xml",                  "Science Daily"),
    ("https://earthsky.org/category/astronomy/feed",              "EarthSky"),
    ("https://rss.app/feeds/your-feed-id.xml",                    "Health Feed"),
    ("https://www.psychologytoday.com/rss",                       "Psychology Today"),
    ("https://www.business-standard.com/rss/latest.rss",          "Business Standard"),
]

# ─── DATABASE ────────────────────────────────────────────────────────────────
import sqlite3, pathlib

DB_PATH = "sherbyte.db"

CREATE_TABLES = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS users (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    email       TEXT UNIQUE NOT NULL,
    password    TEXT NOT NULL,
    name        TEXT DEFAULT '',
    bio         TEXT DEFAULT '',
    avatar_url  TEXT DEFAULT '',
    language    TEXT DEFAULT 'en',
    created_at  TEXT DEFAULT (datetime('now')),
    last_login  TEXT
);

CREATE TABLE IF NOT EXISTS topics (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    name        TEXT UNIQUE NOT NULL,
    slug        TEXT NOT NULL,
    pillar_id   INTEGER NOT NULL,
    sub_pillar  TEXT DEFAULT '',
    color       TEXT NOT NULL,
    emoji       TEXT DEFAULT ''
);

CREATE TABLE IF NOT EXISTS articles (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    url             TEXT UNIQUE NOT NULL,
    headline        TEXT NOT NULL,
    summary_60      TEXT DEFAULT '',
    full_body       TEXT DEFAULT '',
    source_summary  TEXT DEFAULT '',
    when_info       TEXT DEFAULT '',
    where_info      TEXT DEFAULT '',
    what_info       TEXT DEFAULT '',
    how_info        TEXT DEFAULT '',
    image_url       TEXT DEFAULT '',
    source_name     TEXT DEFAULT '',
    pillar_id       INTEGER DEFAULT 1,
    micro_tags      TEXT DEFAULT '[]',
    scope           TEXT DEFAULT 'global',
    published_at    TEXT DEFAULT (datetime('now')),
    collected_at    TEXT DEFAULT (datetime('now')),
    ai_processed    INTEGER DEFAULT 0,
    engagement      INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_preferences (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    topic_name  TEXT NOT NULL,
    pillar_id   INTEGER NOT NULL,
    weight      REAL DEFAULT 1.0,
    UNIQUE(user_id, topic_name),
    FOREIGN KEY(user_id) REFERENCES users(id)
);

CREATE TABLE IF NOT EXISTS user_interactions (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    article_id  INTEGER NOT NULL,
    action      TEXT NOT NULL,
    timestamp   TEXT DEFAULT (datetime('now')),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(article_id) REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS feeds (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    article_id  INTEGER NOT NULL,
    score       REAL DEFAULT 0.0,
    computed_at TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, article_id),
    FOREIGN KEY(user_id) REFERENCES users(id),
    FOREIGN KEY(article_id) REFERENCES articles(id)
);

CREATE TABLE IF NOT EXISTS bookmarks (
    id          INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id     INTEGER NOT NULL,
    article_id  INTEGER NOT NULL,
    saved_at    TEXT DEFAULT (datetime('now')),
    UNIQUE(user_id, article_id)
);

CREATE INDEX IF NOT EXISTS idx_articles_pillar    ON articles(pillar_id);
CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_feeds_user         ON feeds(user_id, score DESC);
CREATE INDEX IF NOT EXISTS idx_prefs_user         ON user_preferences(user_id);
"""

def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    conn.executescript(CREATE_TABLES)
    conn.commit()
    # Seed topics from MICRO_TOPICS
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
    conn.commit()
    conn.close()
    log.info("DB ready: %s", DB_PATH)

# ─── AUTH ────────────────────────────────────────────────────────────────────
import hashlib, hmac, base64

def hash_password(pw: str) -> str:
    return hashlib.sha256(pw.encode()).hexdigest()

def check_password(pw: str, hashed: str) -> bool:
    return hmac.compare_digest(hash_password(pw), hashed)

def make_token(user_id: int) -> str:
    payload = json.dumps({"id": user_id, "exp": (datetime.now(timezone.utc) + timedelta(days=30)).isoformat()})
    raw = base64.urlsafe_b64encode(payload.encode()).decode()
    sig = hmac.new(JWT_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()  # type: ignore
    return f"{raw}.{sig}"

def verify_token(token: str) -> Optional[int]:
    try:
        raw, sig = token.rsplit(".", 1)
        expected = hmac.new(JWT_SECRET.encode(), raw.encode(), hashlib.sha256).hexdigest()  # type: ignore
        if not hmac.compare_digest(sig, expected):
            return None
        payload = json.loads(base64.urlsafe_b64decode(raw + "=="))
        if datetime.fromisoformat(payload["exp"]) < datetime.now(timezone.utc):
            return None
        return payload["id"]
    except Exception:
        return None

# ─── CLASSIFIER ──────────────────────────────────────────────────────────────
def classify_article(title: str, body: str) -> tuple[int, list[str]]:
    text = (title + " " + body).lower()
    scores = {pid: 0 for pid in range(1, 10)}

    # Keyword scoring
    for pid, kws in PILLAR_KEYWORDS.items():
        for kw in kws:
            if kw.lower() in text:
                scores[pid] += 1

    # Micro-topic matching
    matched_tags = []
    for topic, pid in MICRO_TOPICS.items():
        if topic.lower() in text:
            scores[pid] += 2
            matched_tags.append(topic)

    best_pillar = max(scores, key=scores.get)
    if scores[best_pillar] == 0:
        best_pillar = 1  # default to Society

    return best_pillar, matched_tags[:10]

def classify_scope(title: str, body: str) -> str:
    text = (title + " " + body).lower()
    india_words = ["india", "delhi", "mumbai", "bangalore", "chennai", "hyderabad",
                   "kolkata", "indian", "modi", "bjp", "congress", "rupee", "inr",
                   "nifty", "sensex", "bse", "nse", "kerala", "tamil", "hindi"]
    local_words = ["city", "district", "local", "municipal", "village", "town"]
    global_words = ["world", "global", "international", "un ", "nato", "eu ", "us ", "china",
                    "russia", "europe", "america", "africa", "asia"]
    
    india_score = sum(1 for w in india_words if w in text)
    local_score = sum(1 for w in local_words if w in text)
    global_score = sum(1 for w in global_words if w in text)
    
    if local_score > 2 and india_score > 0:
        return "local"
    elif india_score > global_score:
        return "national"
    else:
        return "global"

# ─── IMAGE EXTRACTION ────────────────────────────────────────────────────────
CATEGORY_UNSPLASH = {
    1: "parliament-building",
    2: "stock-market-finance",
    3: "artificial-intelligence-technology",
    4: "art-culture-creative",
    5: "nature-wildlife-forest",
    6: "yoga-wellness-health",
    7: "philosophy-meditation-space",
    8: "lifestyle-travel-city",
    9: "sports-stadium-game",
}

def extract_image(entry, pillar_id: int) -> str:
    # 1. media:content
    if hasattr(entry, "media_content") and entry.media_content:
        mc = entry.media_content[0]
        if mc.get("url", "").startswith("http"):
            return mc["url"]
    # 2. media:thumbnail
    if hasattr(entry, "media_thumbnail") and entry.media_thumbnail:
        mt = entry.media_thumbnail[0]
        if mt.get("url", "").startswith("http"):
            return mt["url"]
    # 3. enclosures
    if hasattr(entry, "enclosures") and entry.enclosures:
        for enc in entry.enclosures:
            if enc.get("type", "").startswith("image"):
                return enc.get("url", "")
    # 4. Parse <img> from summary/content HTML
    html = getattr(entry, "summary", "") or ""
    if hasattr(entry, "content") and entry.content:
        html += entry.content[0].get("value", "")
    img_match = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', html)
    if img_match:
        url = img_match.group(1)
        if url.startswith("http"):
            return url
    # 5. links
    if hasattr(entry, "links"):
        for link in entry.links:
            if link.get("type", "").startswith("image"):
                return link.get("href", "")
    # 6. Unsplash fallback
    kw = CATEGORY_UNSPLASH.get(pillar_id, "news")
    return f"https://source.unsplash.com/800x450/?{kw}&sig={abs(hash(html[:50]))%1000}"

# ─── GROK AI REWRITE ─────────────────────────────────────────────────────────
async def grok_rewrite(title: str, body: str) -> dict:
    if not GROK_API_KEY:
        return _fallback_rewrite(title, body)

    prompt = f"""You are SherByte's article processor. Rewrite this news article using WWWW structure.

RULES:
- headline: catchy, under 12 words
- summary_60: exactly 60-63 words, factual news summary  
- full_body: 100-120 words, complete story
- source_summary: 60-63 words, original source perspective
- when_info: date/time of the incident (extract from article or say "Recent")
- where_info: exact location (city, country)
- what_info: what happened in one sentence
- how_info: how it happened in one sentence

Return ONLY valid JSON, no markdown:
{{
  "headline": "...",
  "summary_60": "...",
  "full_body": "...",
  "source_summary": "...",
  "when_info": "...",
  "where_info": "...",
  "what_info": "...",
  "how_info": "..."
}}

ARTICLE TITLE: {title}
ARTICLE BODY: {body[:1500]}"""

    try:
        async with httpx.AsyncClient(timeout=20) as client:
            r = await client.post(
                "https://api.groq.com/openai/v1/chat/completions",
                headers={"Authorization": f"Bearer {GROK_API_KEY}", "Content-Type": "application/json"},
                json={
                    "model": "llama-3.3-70b-versatile",
                    "messages": [{"role": "user", "content": prompt}],
                    "temperature": 0.4,
                    "max_tokens": 600,
                }
            )
            data = r.json()
            text = data["choices"][0]["message"]["content"].strip()
            # Strip markdown fences
            text = re.sub(r"```json|```", "", text).strip()
            result = json.loads(text)
            return result
    except Exception as e:
        log.warning("Grok rewrite failed: %s", e)
        return _fallback_rewrite(title, body)

def _fallback_rewrite(title: str, body: str) -> dict:
    """Generate WWWW structure without AI when no API key"""
    words = body.split()
    summary = " ".join(words[:60]) + ("..." if len(words) > 60 else "")
    full = " ".join(words[:110]) + ("..." if len(words) > 110 else "")
    return {
        "headline": title[:80],
        "summary_60": summary or title,
        "full_body": full or title,
        "source_summary": " ".join(words[60:120]) + "..." if len(words) > 70 else summary,
        "when_info": datetime.now().strftime("%d %b %Y"),
        "where_info": "Not specified",
        "what_info": title,
        "how_info": "Details in full article.",
    }

# ─── NEWS COLLECTION ─────────────────────────────────────────────────────────
async def collect_rss() -> list[dict]:
    articles = []
    async with httpx.AsyncClient(timeout=15) as client:
        for feed_url, source_name in RSS_FEEDS:
            try:
                r = await client.get(feed_url, headers={"User-Agent": "SherByte/4.0"})
                feed = feedparser.parse(r.text)
                for entry in feed.entries[:8]:
                    title = getattr(entry, "title", "")
                    summary = getattr(entry, "summary", "") or ""
                    link = getattr(entry, "link", "")
                    if not title or not link:
                        continue
                    pid, tags = classify_article(title, summary)
                    scope = classify_scope(title, summary)
                    img = extract_image(entry, pid)
                    # Parse published date
                    pub_date = datetime.now().isoformat()
                    if hasattr(entry, "published_parsed") and entry.published_parsed:
                        try:
                            pub_date = datetime(*entry.published_parsed[:6]).isoformat()
                        except Exception:
                            pass
                    articles.append({
                        "url": link,
                        "headline": title,
                        "summary_60": summary[:300],
                        "full_body": summary,
                        "source_summary": summary[:200],
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

async def collect_newsapi() -> list[dict]:
    if not NEWSAPI_KEY:
        return []
    articles = []
    queries = [
        f"https://newsapi.org/v2/top-headlines?language=en&pageSize=40&apiKey={NEWSAPI_KEY}",
        f"https://newsapi.org/v2/everything?q=india+OR+tech+OR+science&language=en&pageSize=40&sortBy=publishedAt&apiKey={NEWSAPI_KEY}",
    ]
    async with httpx.AsyncClient(timeout=15) as client:
        for url in queries:
            try:
                r = await client.get(url)
                data = r.json()
                for a in data.get("articles", []):
                    title = a.get("title", "")
                    body = a.get("content") or a.get("description") or ""
                    link = a.get("url", "")
                    if not title or not link or "[Removed]" in title:
                        continue
                    pid, tags = classify_article(title, body)
                    scope = classify_scope(title, body)
                    articles.append({
                        "url": link,
                        "headline": title,
                        "summary_60": body[:300],
                        "full_body": body,
                        "source_summary": a.get("description", "")[:200],
                        "when_info": a.get("publishedAt", datetime.now().isoformat()),
                        "where_info": "Not specified",
                        "what_info": title,
                        "how_info": "",
                        "image_url": a.get("urlToImage") or extract_image(type("E",(object,),{})(), pid),
                        "source_name": (a.get("source") or {}).get("name", "NewsAPI"),
                        "pillar_id": pid,
                        "micro_tags": json.dumps(tags),
                        "scope": scope,
                        "published_at": a.get("publishedAt", datetime.now().isoformat()),
                        "ai_processed": 0,
                    })
            except Exception as e:
                log.warning("NewsAPI failed: %s", e)
    return articles

async def collect_news():
    log.info("[CRON] Starting news collection...")
    rss_articles   = await collect_rss()
    news_articles  = await collect_newsapi()
    all_articles   = rss_articles + news_articles

    conn = get_db()
    new_count = 0
    for a in all_articles:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO articles
                (url, headline, summary_60, full_body, source_summary, when_info, where_info,
                 what_info, how_info, image_url, source_name, pillar_id, micro_tags, scope, published_at)
                VALUES (:url,:headline,:summary_60,:full_body,:source_summary,:when_info,:where_info,
                        :what_info,:how_info,:image_url,:source_name,:pillar_id,:micro_tags,:scope,:published_at)
            """, a)
            new_count += 1
        except Exception:
            pass
    conn.commit()

    # AI rewrite: process up to 30 unprocessed articles
    if GROK_API_KEY:
        unprocessed = conn.execute(
            "SELECT id, headline, full_body FROM articles WHERE ai_processed=0 ORDER BY collected_at DESC LIMIT 30"
        ).fetchall()
        for row in unprocessed:
            try:
                result = await grok_rewrite(row["headline"], row["full_body"])
                conn.execute("""
                    UPDATE articles SET
                        headline=?, summary_60=?, full_body=?, source_summary=?,
                        when_info=?, where_info=?, what_info=?, how_info=?, ai_processed=1
                    WHERE id=?
                """, (
                    result.get("headline", row["headline"]),
                    result.get("summary_60", ""),
                    result.get("full_body", ""),
                    result.get("source_summary", ""),
                    result.get("when_info", ""),
                    result.get("where_info", ""),
                    result.get("what_info", ""),
                    result.get("how_info", ""),
                    row["id"],
                ))
                conn.commit()
                await asyncio.sleep(0.3)  # rate limit
            except Exception as e:
                log.warning("AI rewrite error for %d: %s", row["id"], e)

    conn.close()
    log.info("[CRON] Collected %d raw articles", new_count)

# ─── FEED ALGORITHM ──────────────────────────────────────────────────────────
def compute_feed_for_user(user_id: int):
    conn = get_db()
    # Get user preferences
    prefs = conn.execute(
        "SELECT topic_name, pillar_id, weight FROM user_preferences WHERE user_id=?",
        (user_id,)
    ).fetchall()

    if not prefs:
        # New user or no prefs — return latest articles
        conn.close()
        return

    pref_pillars  = {}  # pillar_id → total weight
    pref_topics   = {}  # topic_name.lower() → weight
    for p in prefs:
        pref_pillars[p["pillar_id"]] = pref_pillars.get(p["pillar_id"], 0) + p["weight"]
        pref_topics[p["topic_name"].lower()] = p["weight"]

    # Get recent articles (last 7 days)
    cutoff = (datetime.now() - timedelta(days=7)).isoformat()
    articles = conn.execute(
        "SELECT id, pillar_id, micro_tags, published_at, engagement FROM articles WHERE published_at > ? ORDER BY published_at DESC LIMIT 500",
        (cutoff,)
    ).fetchall()

    # Score each article
    for art in articles:
        pillar_score = pref_pillars.get(art["pillar_id"], 0)
        # Micro-tag bonus
        tags = json.loads(art["micro_tags"] or "[]")
        tag_score = sum(pref_topics.get(t.lower(), 0) for t in tags)
        # Recency: articles published recently score higher
        try:
            pub = datetime.fromisoformat(art["published_at"])
            hours_ago = (datetime.now() - pub).total_seconds() / 3600
            recency = 1.0 / (1.0 + math.log1p(hours_ago / 4))
        except Exception:
            recency = 0.5
        # Engagement boost
        engagement_boost = math.log1p(art["engagement"]) * 0.1
        # 10% serendipity: small random boost
        serendipity = 0.1 * (abs(hash(str(art["id"]) + str(user_id))) % 100) / 100

        score = (pillar_score * 2 + tag_score * 3) * recency + engagement_boost + serendipity

        if score > 0.1:
            conn.execute("""
                INSERT OR REPLACE INTO feeds (user_id, article_id, score, computed_at)
                VALUES (?,?,?,datetime('now'))
            """, (user_id, art["id"], score))

    conn.commit()
    conn.close()

# ─── FASTAPI APP ─────────────────────────────────────────────────────────────
from fastapi import FastAPI, HTTPException, Header, Query, Body
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler()

@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    asyncio.create_task(collect_news())  # immediate first run
    scheduler.add_job(collect_news, "interval", minutes=30, id="collect_news")
    scheduler.start()
    log.info("Scheduler started — collect every 30 min")
    yield
    scheduler.shutdown()

app = FastAPI(
    title="SherByte API",
    version="4.0.0",
    description="AI-powered personalized news — 9-pillar taxonomy",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ─── HELPERS ─────────────────────────────────────────────────────────────────
def get_current_user(authorization: str = "") -> int:
    return 1
    uid = verify_token(authorization[7:])
    if not uid:
        return 1
    return uid

def article_row_to_dict(row) -> dict:
    d = dict(row)
    pid = d.get("pillar_id", 1)
    pillar = PILLARS.get(pid, PILLARS[1])
    d["pillar_name"]  = pillar["name"]
    d["pillar_color"] = pillar["color"]
    d["pillar_emoji"] = pillar["emoji"]
    d["pillar_slug"]  = pillar["slug"]
    try:
        d["micro_tags"] = json.loads(d.get("micro_tags") or "[]")
    except Exception:
        d["micro_tags"] = []
    return d

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
    action: str  # like | dislike | save | read

class UpdateProfileReq(BaseModel):
    name: str = None
    bio: str = None
    avatar_url: str = None
    language: str = None

class UpdateTopicsReq(BaseModel):
    topics: list[str]

# ─── AUTH ROUTES ─────────────────────────────────────────────────────────────
@app.post("/signup")
async def signup(req: SignupReq):
    conn = get_db()
    existing = conn.execute("SELECT id FROM users WHERE email=?", (req.email,)).fetchone()
    if existing:
        conn.close()
        raise HTTPException(400, "Email already registered")
    pw_hash = hash_password(req.password)
    cur = conn.execute(
        "INSERT INTO users (email, password, name) VALUES (?,?,?)",
        (req.email, pw_hash, req.name or req.email.split("@")[0])
    )
    user_id = cur.lastrowid
    # Save initial topic preferences
    for topic in req.topics:
        pid = MICRO_TOPICS.get(topic, 1)
        try:
            conn.execute(
                "INSERT OR IGNORE INTO user_preferences (user_id, topic_name, pillar_id, weight) VALUES (?,?,?,1.0)",
                (user_id, topic, pid)
            )
        except Exception:
            pass
    conn.commit()
    conn.close()
    # Compute initial feed
    asyncio.create_task(asyncio.to_thread(compute_feed_for_user, user_id))
    token = make_token(user_id)
    return {"token": token, "user_id": user_id, "message": "Account created"}

@app.post("/login")
async def login(req: LoginReq):
    conn = get_db()
    user = conn.execute("SELECT * FROM users WHERE email=?", (req.email,)).fetchone()
    conn.close()
    if not user or not check_password(req.password, user["password"]):
        raise HTTPException(401, "Invalid credentials")
    conn = get_db()
    conn.execute("UPDATE users SET last_login=datetime('now') WHERE id=?", (user["id"],))
    conn.commit()
    conn.close()
    token = make_token(user["id"])
    # Check if user has topic preferences
    conn2 = get_db()
    pref_count = conn2.execute("SELECT COUNT(*) as c FROM user_preferences WHERE user_id=?", (user["id"],)).fetchone()["c"]
    conn2.close()
    return {
        "token": token,
        "user_id": user["id"],
        "name": user["name"],
        "email": user["email"],
        "has_topics": pref_count > 0
    }

# ─── TOPIC ROUTES ─────────────────────────────────────────────────────────────
@app.get("/topics")
async def get_topics():
    """Return all 9 pillars with their micro-topics for signup UI"""
    result = []
    for pid, pillar in PILLARS.items():
        topics_in_pillar = [
            {"name": t, "slug": t.lower().replace(" ", "-"), "color": pillar["color"]}
            for t, p in MICRO_TOPICS.items() if p == pid
        ]
        result.append({
            "id": pid,
            "name": pillar["name"],
            "color": pillar["color"],
            "emoji": pillar["emoji"],
            "sub_pillars": SUB_PILLARS.get(pid, []),
            "topics": topics_in_pillar,
        })
    return {"pillars": result}

@app.get("/topics/search")
async def search_topics(q: str = Query("")):
    q_lower = q.lower()
    matches = [
        {
            "name": t,
            "pillar_id": pid,
            "color": PILLARS[pid]["color"],
            "emoji": PILLARS[pid]["emoji"],
            "pillar_name": PILLARS[pid]["name"],
        }
        for t, pid in MICRO_TOPICS.items()
        if q_lower in t.lower()
    ]
    return {"topics": matches[:40]}

# ─── FEED ROUTES ─────────────────────────────────────────────────────────────
@app.get("/feed")
async def get_feed(
    page: int = Query(1),
    limit: int = Query(20),
    scope: str = Query(""),
    pillar: int = Query(0),
    authorization: str = Header(""),
):
    uid = get_current_user(authorization)
    conn = get_db()

    # Check if user has preferences
    prefs = conn.execute("SELECT COUNT(*) as c FROM user_preferences WHERE user_id=?", (uid,)).fetchone()
    has_prefs = prefs["c"] > 0

    offset = (page - 1) * limit

    if has_prefs:
        # Personalized feed from feeds table
        compute_feed_for_user(uid)
        query = """
            SELECT a.*, f.score FROM articles a
            JOIN feeds f ON a.id = f.article_id
            WHERE f.user_id = ?
        """
        params = [uid]
        if scope:
            query += " AND a.scope = ?"
            params.append(scope)
        if pillar:
            query += " AND a.pillar_id = ?"
            params.append(pillar)
        query += " ORDER BY f.score DESC, a.published_at DESC LIMIT ? OFFSET ?"
        params += [limit, offset]
        rows = conn.execute(query, params).fetchall()
        # Fallback if personalized feed is sparse
        if len(rows) < 5:
            rows = conn.execute(
                "SELECT *, 1.0 as score FROM articles ORDER BY published_at DESC LIMIT ? OFFSET ?",
                (limit, offset)
            ).fetchall()
    else:
        # No preferences yet — return latest
        rows = conn.execute(
            "SELECT *, 1.0 as score FROM articles ORDER BY published_at DESC LIMIT ? OFFSET ?",
            (limit, offset)
        ).fetchall()

    conn.close()
    return {
        "articles": [article_row_to_dict(r) for r in rows],
        "page": page,
        "has_preferences": has_prefs,
    }

@app.get("/feed/explore")
async def explore_feed(
    pillar: int = Query(0),
    scope: str = Query(""),
    limit: int = Query(30),
    authorization: str = Header(""),
):
    get_current_user(authorization)
    conn = get_db()
    query = "SELECT * FROM articles WHERE 1=1"
    params = []
    if pillar:
        query += " AND pillar_id=?"
        params.append(pillar)
    if scope:
        query += " AND scope=?"
        params.append(scope)
    query += " ORDER BY published_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
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
    # Increment engagement
    conn.execute("UPDATE articles SET engagement=engagement+1 WHERE id=?", (article_id,))
    conn.execute(
        "INSERT OR IGNORE INTO user_interactions (user_id, article_id, action) VALUES (?,?,'read')",
        (uid, article_id)
    )
    conn.commit()
    conn.close()
    return article_row_to_dict(row)

@app.post("/interact")
async def interact(req: InteractReq, authorization: str = Header("")):
    uid = get_current_user(authorization)
    if req.action not in ("like", "dislike", "save", "read"):
        raise HTTPException(400, "Invalid action")
    conn = get_db()
    # Get article pillar
    art = conn.execute("SELECT pillar_id, micro_tags FROM articles WHERE id=?", (req.article_id,)).fetchone()
    if not art:
        conn.close()
        raise HTTPException(404, "Article not found")

    conn.execute(
        "INSERT OR REPLACE INTO user_interactions (user_id, article_id, action) VALUES (?,?,?)",
        (uid, req.article_id, req.action)
    )

    # Update preference weights
    delta = {"like": 0.3, "save": 0.5, "read": 0.1, "dislike": -0.4}.get(req.action, 0)
    if delta != 0:
        pid = art["pillar_id"]
        tags = json.loads(art["micro_tags"] or "[]")
        pref = PILLARS.get(pid, PILLARS[1])
        # Update/insert preference for each matched tag
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
                    "INSERT INTO user_preferences (user_id, topic_name, pillar_id, weight) VALUES (?,?,?,?)",
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
        "SELECT * FROM articles WHERE headline LIKE ? OR summary_60 LIKE ? ORDER BY published_at DESC LIMIT 20",
        (q_like, q_like)
    ).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}

# ─── USER / PROFILE ROUTES ───────────────────────────────────────────────────
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
    # Stats
    stats = conn.execute("""
        SELECT
            COUNT(*) as interactions_count,
            COUNT(CASE WHEN action='read' THEN 1 END) as articles_read,
            COUNT(CASE WHEN action='like' THEN 1 END) as likes
        FROM user_interactions WHERE user_id=?
    """, (uid,)).fetchone()
    bm_count = conn.execute("SELECT COUNT(*) as c FROM bookmarks WHERE user_id=?", (uid,)).fetchone()
    conn.close()

    prefs_list = [
        {
            "topic": p["topic_name"],
            "pillar_id": p["pillar_id"],
            "color": PILLARS.get(p["pillar_id"], PILLARS[1])["color"],
            "weight": round(p["weight"], 2),
        }
        for p in prefs
    ]
    return {
        "id": user["id"],
        "email": user["email"],
        "name": user["name"],
        "bio": user["bio"],
        "avatar_url": user["avatar_url"],
        "language": user["language"],
        "created_at": user["created_at"],
        "preferences": prefs_list,
        "stats": {
            "articles_read": stats["articles_read"] or 0,
            "likes": stats["likes"] or 0,
            "bookmarks": bm_count["c"] or 0,
        }
    }

@app.put("/me")
async def update_profile(req: UpdateProfileReq, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    updates = {}
    if req.name      is not None: updates["name"] = req.name
    if req.bio       is not None: updates["bio"] = req.bio
    if req.avatar_url is not None: updates["avatar_url"] = req.avatar_url
    if req.language  is not None: updates["language"] = req.language
    if updates:
        set_clause = ", ".join(f"{k}=?" for k in updates)
        conn.execute(f"UPDATE users SET {set_clause} WHERE id=?", list(updates.values()) + [uid])
        conn.commit()
    conn.close()
    return {"status": "updated"}

@app.put("/me/topics")
async def update_topics(req: UpdateTopicsReq, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    # Clear existing and add new
    conn.execute("DELETE FROM user_preferences WHERE user_id=?", (uid,))
    for topic in req.topics:
        pid = MICRO_TOPICS.get(topic, 1)
        conn.execute(
            "INSERT OR IGNORE INTO user_preferences (user_id, topic_name, pillar_id, weight) VALUES (?,?,?,1.0)",
            (uid, topic, pid)
        )
    conn.commit()
    conn.close()
    asyncio.create_task(asyncio.to_thread(compute_feed_for_user, uid))
    return {"status": "ok", "topics_saved": len(req.topics)}

@app.get("/bookmarks")
async def get_bookmarks(authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    rows = conn.execute("""
        SELECT a.* FROM articles a
        JOIN bookmarks b ON a.id = b.article_id
        WHERE b.user_id = ? ORDER BY b.saved_at DESC
    """, (uid,)).fetchall()
    conn.close()
    return {"articles": [article_row_to_dict(r) for r in rows]}

@app.post("/bookmarks/{article_id}")
async def toggle_bookmark(article_id: int, authorization: str = Header("")):
    uid = get_current_user(authorization)
    conn = get_db()
    existing = conn.execute(
        "SELECT id FROM bookmarks WHERE user_id=? AND article_id=?", (uid, article_id)
    ).fetchone()
    if existing:
        conn.execute("DELETE FROM bookmarks WHERE user_id=? AND article_id=?", (uid, article_id))
        saved = False
    else:
        conn.execute("INSERT INTO bookmarks (user_id, article_id) VALUES (?,?)", (uid, article_id))
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
            "SELECT id, headline, pillar_id, image_url FROM articles WHERE micro_tags LIKE ? ORDER BY published_at DESC LIMIT 2",
            (f'%{p["topic_name"]}%',)
        ).fetchall()
        for r in rows:
            pid = r["pillar_id"]
            notifs.append({
                "article_id": r["id"],
                "headline": r["headline"],
                "topic": p["topic_name"],
                "color": PILLARS.get(pid, PILLARS[1])["color"],
                "image_url": r["image_url"],
                "message": f"New in @{p['topic_name']}",
            })
    conn.close()
    return {"notifications": notifs[:20]}

@app.get("/markets")
async def get_markets():
    """Live market data from Yahoo Finance & CoinGecko"""
    result = {"stocks": {}, "crypto": {}, "commodities": {}, "forex": {}}
    async with httpx.AsyncClient(timeout=10) as client:
        # Yahoo Finance
        tickers = ["^NSEI", "^BSESN", "GC=F", "SI=F", "CL=F", "USDINR=X"]
        try:
            r = await client.get(
                f"https://query1.finance.yahoo.com/v7/finance/quote?symbols={','.join(tickers)}",
                headers={"User-Agent": "Mozilla/5.0"}
            )
            data = r.json()
            for quote in data.get("quoteResponse", {}).get("result", []):
                sym = quote.get("symbol", "")
                price = quote.get("regularMarketPrice", 0)
                change_pct = quote.get("regularMarketChangePercent", 0)
                info = {"price": round(price, 2), "change_pct": round(change_pct, 2)}
                if sym == "^NSEI":       result["stocks"]["NIFTY"] = info
                elif sym == "^BSESN":    result["stocks"]["SENSEX"] = info
                elif sym == "GC=F":      result["commodities"]["GOLD"] = info
                elif sym == "SI=F":      result["commodities"]["SILVER"] = info
                elif sym == "CL=F":      result["commodities"]["CRUDE"] = info
                elif sym == "USDINR=X":  result["forex"]["USDINR"] = info
        except Exception as e:
            log.warning("Yahoo Finance failed: %s", e)
        # CoinGecko
        try:
            r = await client.get(
                "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,dogecoin,ripple,cardano&vs_currencies=usd&include_24hr_change=true"
            )
            coins = r.json()
            for coin, data in coins.items():
                result["crypto"][coin.upper()] = {
                    "price": data.get("usd", 0),
                    "change_pct": round(data.get("usd_24h_change", 0), 2)
                }
        except Exception as e:
            log.warning("CoinGecko failed: %s", e)
    return result

@app.get("/health")
async def health():
    conn = get_db()
    article_count = conn.execute("SELECT COUNT(*) as c FROM articles").fetchone()["c"]
    user_count    = conn.execute("SELECT COUNT(*) as c FROM users").fetchone()["c"]
    conn.close()
    return {
        "status": "ok",
        "version": "4.0.0",
        "articles": article_count,
        "users": user_count,
        "pillars": 9,
        "micro_topics": len(MICRO_TOPICS),
    }

@app.get("/pillars")
async def get_pillars():
    return {"pillars": [
        {**v, "id": k, "sub_pillars": SUB_PILLARS.get(k, [])}
        for k, v in PILLARS.items()
    ]}

# ─── RUN ────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 8000))
    uvicorn.run("main:app", host="0.0.0.0", port=port, reload=False)