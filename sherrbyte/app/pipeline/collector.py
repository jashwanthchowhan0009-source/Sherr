"""
pipeline/collector.py — Stage 01: COLLECT.

Pulls raw items from 50+ RSS feeds and (optionally) NewsAPI, normalizes them
into ArticleIn objects, and stamps each with the three dedup signatures
(url_hash, title_hash, simhash) so the deduplicator can act before any
expensive understanding runs.

This stage does NOT classify or summarize — that's the Understand stage's job.
The collector only turns "the internet" into clean, fingerprinted articles.
"""

from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime

import httpx

from app.config import settings
from app.models.article import ArticleIn
from app.text_utils import (
    clean_html_fragments,
    simhash,
    title_fingerprint,
    url_fingerprint,
)

log = logging.getLogger("sherbyte.collector")

# ─── Source registry: 50+ RSS feeds ───────────────────────────────────────────
RSS_FEEDS: list[tuple[str, str]] = [
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


def _extract_image(entry) -> str:
    if getattr(entry, "media_content", None):
        mc = entry.media_content[0]
        if isinstance(mc, dict) and mc.get("url", "").startswith("http"):
            return mc["url"]
    if getattr(entry, "media_thumbnail", None):
        mt = entry.media_thumbnail[0]
        if isinstance(mt, dict) and mt.get("url", "").startswith("http"):
            return mt["url"]
    for enc in getattr(entry, "enclosures", []) or []:
        if isinstance(enc, dict) and enc.get("type", "").startswith("image"):
            if enc.get("url", "").startswith("http"):
                return enc["url"]
    html = getattr(entry, "summary", "") or ""
    if getattr(entry, "content", None):
        html += entry.content[0].get("value", "")
    m = re.search(r'<img[^>]+src=["\']([^"\']+)["\']', html)
    if m and m.group(1).startswith("http"):
        return m.group(1)
    return ""


def _extract_video(entry) -> str:
    """Direct video file (mp4/webm) from media:content or enclosures, if present."""
    def _vid(d):
        if not isinstance(d, dict):
            return ""
        t = (d.get("type") or "").lower()
        u = d.get("url") or ""
        if u.startswith("http") and (t.startswith("video") or u.lower().split("?")[0].endswith((".mp4", ".webm", ".m4v"))):
            return u
        return ""
    for mc in (getattr(entry, "media_content", None) or []):
        u = _vid(mc)
        if u:
            return u
    for enc in (getattr(entry, "enclosures", None) or []):
        u = _vid(enc)
        if u:
            return u
    return ""


def _best_body(entry) -> str:
    """Prefer the feed's full article HTML (content:encoded) over the short
    summary teaser, so the reader view has real depth instead of two lines.
    Falls back to the summary when no full content is present; capped to keep
    DB rows and LLM prompts bounded."""
    best = getattr(entry, "summary", "") or ""
    for c in (getattr(entry, "content", None) or []):
        if isinstance(c, dict):
            val = c.get("value") or ""
            if len(val) > len(best):
                best = val
    return best[:12000]


def _build_article(title: str, body: str, link: str, source: str,
                   image: str, published: datetime, video: str = "") -> ArticleIn:
    clean = clean_html_fragments(body)
    return ArticleIn(
        url=link,
        url_hash=url_fingerprint(link),
        title=title,
        title_hash=title_fingerprint(title),
        simhash=simhash(clean or title),
        body=clean,
        image_url=image,
        video_url=video,
        source_name=source,
        published_at=published,
    )


async def _fetch_feed(url: str, source: str, client: httpx.AsyncClient) -> list[ArticleIn]:
    import feedparser  # lazy: only needed at collection time

    out: list[ArticleIn] = []
    timeout = settings.feed_fetch_timeout_sec
    try:
        # Hard total-time cap: httpx's read timeout resets per received byte, so
        # a slow-trickling feed can stall indefinitely. asyncio.wait_for bounds
        # the whole request by wall-clock time and cancels it on expiry.
        r = await asyncio.wait_for(
            client.get(
                url,
                headers={"User-Agent": f"SherByte/{settings.app_version} (+https://sherbyte.in)"},
            ),
            timeout=timeout,
        )
        if r.status_code != 200:
            return out
        feed = await asyncio.get_event_loop().run_in_executor(None, feedparser.parse, r.text)
        # Latest few only (feed.entries is newest-first) — skips stale history.
        for entry in feed.entries[: settings.max_entries_per_feed]:
            title = (getattr(entry, "title", "") or "").strip()
            link = (getattr(entry, "link", "") or "").strip()
            if not title or not link:
                continue
            published = datetime.utcnow()
            if getattr(entry, "published_parsed", None):
                try:
                    published = datetime(*entry.published_parsed[:6])
                except Exception:
                    pass
            out.append(_build_article(
                title=title,
                body=_best_body(entry),
                link=link,
                source=source,
                image=_extract_image(entry),
                published=published,
                video=_extract_video(entry),
            ))
    except (asyncio.TimeoutError, httpx.TimeoutException):
        log.warning("RSS %s timed out after %.0fs — skipping", source, timeout)
    except Exception as e:
        log.warning("RSS %s failed: %s", source, e)
    return out


async def collect_rss() -> list[ArticleIn]:
    """Fetch every RSS feed concurrently (batched to be polite)."""
    results: list[ArticleIn] = []
    batch = settings.collect_concurrency
    async with httpx.AsyncClient(
        follow_redirects=True,
        timeout=httpx.Timeout(settings.feed_fetch_timeout_sec),
    ) as client:
        for i in range(0, len(RSS_FEEDS), batch):
            chunk = RSS_FEEDS[i:i + batch]
            tasks = [_fetch_feed(u, s, client) for u, s in chunk]
            for res in await asyncio.gather(*tasks, return_exceptions=True):
                if isinstance(res, list):
                    results.extend(res)
            await asyncio.sleep(0.3)
    log.info("[COLLECT] %d raw items from %d feeds", len(results), len(RSS_FEEDS))
    return results


async def collect_newsapi() -> list[ArticleIn]:
    """Optional NewsAPI top-headlines pull (only if NEWSAPI_KEY is set)."""
    if not settings.newsapi_key:
        return []
    key = settings.newsapi_key
    queries = [
        f"https://newsapi.org/v2/top-headlines?language=en&pageSize=40&apiKey={key}",
        f"https://newsapi.org/v2/top-headlines?country=in&pageSize=40&apiKey={key}",
    ]
    out: list[ArticleIn] = []
    async with httpx.AsyncClient(timeout=15) as client:
        for q in queries:
            try:
                data = (await client.get(q)).json()
                for a in data.get("articles", []):
                    title = (a.get("title") or "").strip()
                    link = (a.get("url") or "").strip()
                    if not title or not link or "[Removed]" in title:
                        continue
                    published = datetime.utcnow()
                    try:
                        published = datetime.fromisoformat(
                            (a.get("publishedAt") or "").replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                    except Exception:
                        pass
                    out.append(_build_article(
                        title=title,
                        body=a.get("content") or a.get("description") or "",
                        link=link,
                        source=(a.get("source") or {}).get("name", "NewsAPI"),
                        image=a.get("urlToImage") or "",
                        published=published,
                    ))
                await asyncio.sleep(0.3)
            except Exception as e:
                log.warning("NewsAPI query failed: %s", e)
    log.info("[COLLECT] %d items from NewsAPI", len(out))
    return out


async def _fetch_og_image(url: str, client: httpx.AsyncClient) -> str:
    """Best-effort: pull og:image / twitter:image from an article page so cards
    show a real photo instead of a gradient placeholder."""
    try:
        r = await asyncio.wait_for(
            client.get(url, headers={"User-Agent": f"SherByte/{settings.app_version} (+https://sherbyte.in)"}),
            timeout=settings.feed_fetch_timeout_sec,
        )
        if r.status_code != 200:
            return ""
        head = r.text[:120000]   # the og tags live in <head>
        for pat in (
            r'<meta[^>]+(?:property|name)=["\'](?:og:image(?::url)?|twitter:image)["\'][^>]+content=["\']([^"\']+)["\']',
            r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+(?:property|name)=["\'](?:og:image(?::url)?|twitter:image)["\']',
        ):
            m = re.search(pat, head, re.I)
            if m and m.group(1).strip().startswith("http"):
                return m.group(1).strip()
    except Exception:
        pass
    return ""


async def _enrich_images(articles: list[ArticleIn], max_fetch: int = 80) -> None:
    """Fill missing images by scraping og:image — bounded so it never dominates a
    cycle (capped count, small concurrency, short timeout, best-effort)."""
    targets = [a for a in articles if not a.image_url][:max_fetch]
    if not targets:
        return
    sem = asyncio.Semaphore(8)
    async with httpx.AsyncClient(
        follow_redirects=True, timeout=httpx.Timeout(settings.feed_fetch_timeout_sec)
    ) as client:
        async def _one(a: ArticleIn):
            async with sem:
                img = await _fetch_og_image(a.url, client)
                if img:
                    a.image_url = img
        await asyncio.gather(*[_one(a) for a in targets], return_exceptions=True)
    log.info("[COLLECT] og:image filled %d/%d missing images",
             sum(1 for a in targets if a.image_url), len(targets))


async def collect_all() -> list[ArticleIn]:
    rss, newsapi = await asyncio.gather(collect_rss(), collect_newsapi())
    articles = rss + newsapi
    try:
        await _enrich_images(articles)
    except Exception as e:
        log.warning("image enrichment skipped: %s", e)
    return articles
