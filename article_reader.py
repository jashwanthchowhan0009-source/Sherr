"""article_reader.py — read the publisher's full article so we can REWRITE it.

WHY THIS EXISTS
───────────────
Ingest keeps only the RSS blurb: `source_summary` is clean[:200], usually 30-40
words. CLAUDE.md records the consequence: "a single 200-character blurb cannot
be turned into an original 60-80 word body". The rewrite either invented facts
or paraphrased the blurb, the originality gate rejected it, and the reader was
left looking at the placeholder ("SherrByte has not yet published its own
write-up…") on story after story.

A journalist does not rewrite a headline. They READ the report, pull out the
facts — what happened, where and when, why, how — and write it again in their
own words. This module is the "read the report" half.

WHAT IT GUARANTEES
──────────────────
* The text is used as INPUT ONLY. Nothing here is stored in the database or
  served to a reader: the publisher's prose never leaves this process. That is
  both the copyright line and the Supabase-quota line.
* It is also the originality REFERENCE: the rewrite is checked against the full
  article, not just the blurb, so a copied sentence from paragraph six is still
  caught.
* Never raises. A blocked, paywalled or unparseable page returns "" and the
  caller falls back to the old blurb path.

Stdlib + httpx only (html.parser, json), so it adds no dependency.
"""
from __future__ import annotations

import asyncio
import html as html_lib
import json
import logging
import re
from html.parser import HTMLParser
from typing import Optional

import httpx

log = logging.getLogger("sherbyte.reader")

MAX_BYTES = 1_500_000          # a news page larger than this is not an article
MAX_CHARS = 7000               # ~1,100 words: enough facts, bounded prompt cost
MIN_PARA_CHARS = 45            # shorter <p> are captions, bylines, buttons
MIN_ARTICLE_WORDS = 120        # below this, the page gave us no more than the blurb
FETCH_TIMEOUT = 12.0
CONCURRENCY = 6

_UA = ("Mozilla/5.0 (Linux; Android 13; Pixel 7) AppleWebKit/537.36 "
       "(KHTML, like Gecko) Chrome/124.0 Mobile Safari/537.36 SherrByteReader/1.0")

# Blocks whose text is never article prose.
_SKIP_TAGS = {"script", "style", "noscript", "nav", "footer", "header", "aside",
              "form", "button", "svg", "figure", "figcaption", "select", "iframe"}

_BOILERPLATE = re.compile(
    r"subscribe|sign up|sign in|log in|newsletter|cookie|all rights reserved|"
    r"advertisement|click here|follow us|download the app|read more:|also read|"
    r"terms of (use|service)|privacy policy|share this|copyright ©|"
    r"get the latest|breaking news alerts|whatsapp channel|join our",
    re.I)


class _ParagraphParser(HTMLParser):
    """Collect <p> text, remembering whether each sat inside an <article>."""

    def __init__(self):
        super().__init__(convert_charrefs=True)
        self.skip_depth = 0
        self.article_depth = 0
        self.in_p = 0
        self.buf: list[str] = []
        self.paras: list[tuple[str, bool]] = []
        self.ld_json: list[str] = []
        self._in_ld = False

    def handle_starttag(self, tag, attrs):
        if tag == "script":
            if dict(attrs).get("type", "").lower() == "application/ld+json":
                self._in_ld = True
                self.ld_json.append("")
                return
        if tag in _SKIP_TAGS:
            self.skip_depth += 1
            return
        if tag == "article":
            self.article_depth += 1
        elif tag == "p" and not self.skip_depth:
            self.in_p += 1
            self.buf = []
        elif tag == "br" and self.in_p:
            self.buf.append(" ")

    def handle_endtag(self, tag):
        if tag == "script" and self._in_ld:
            self._in_ld = False
            return
        if tag in _SKIP_TAGS:
            self.skip_depth = max(0, self.skip_depth - 1)
            return
        if tag == "article":
            self.article_depth = max(0, self.article_depth - 1)
        elif tag == "p" and self.in_p:
            self.in_p -= 1
            text = re.sub(r"\s+", " ", "".join(self.buf)).strip()
            if text:
                self.paras.append((text, self.article_depth > 0))
            self.buf = []

    def handle_data(self, data):
        if self._in_ld:
            self.ld_json[-1] += data
        elif self.in_p and not self.skip_depth:
            self.buf.append(data)


def _ld_article_body(blobs: list[str]) -> str:
    """schema.org NewsArticle.articleBody, when the publisher ships it."""
    def walk(node):
        if isinstance(node, dict):
            body = node.get("articleBody")
            if isinstance(body, str) and len(body.split()) >= MIN_ARTICLE_WORDS:
                return body
            for v in node.values():
                found = walk(v)
                if found:
                    return found
        elif isinstance(node, list):
            for v in node:
                found = walk(v)
                if found:
                    return found
        return ""

    for raw in blobs:
        try:
            found = walk(json.loads(raw.strip()))
        except Exception:                                         # noqa: BLE001
            continue
        if found:
            return found
    return ""


def _clean(text: str) -> str:
    text = html_lib.unescape(text or "")
    text = re.sub(r"<[^>]+>", " ", text)
    return re.sub(r"\s+", " ", text).strip()


def extract_text(page: str) -> str:
    """Main article prose from an HTML page, or "" if there is none worth using."""
    if not page:
        return ""
    parser = _ParagraphParser()
    try:
        parser.feed(page)
        parser.close()
    except Exception:                                             # noqa: BLE001
        pass

    ld = _clean(_ld_article_body(parser.ld_json))
    if len(ld.split()) >= MIN_ARTICLE_WORDS:
        return ld[:MAX_CHARS]

    paras = [(p, inside) for p, inside in parser.paras
             if len(p) >= MIN_PARA_CHARS and not _BOILERPLATE.search(p)]
    inside = [p for p, flag in paras if flag]
    chosen = inside if len(inside) >= 3 else [p for p, _ in paras]

    seen, out = set(), []
    for p in chosen:
        key = p.lower()[:80]
        if key in seen:
            continue
        seen.add(key)
        out.append(_clean(p))
    text = "\n\n".join(out)
    if len(text.split()) < MIN_ARTICLE_WORDS:
        return ""
    return text[:MAX_CHARS]


async def fetch_article(url: str, client: Optional[httpx.AsyncClient] = None) -> str:
    """Fetch one URL and return its article text, or "" on any failure."""
    if not url or not url.startswith(("http://", "https://")):
        return ""
    own = client is None
    client = client or httpx.AsyncClient(follow_redirects=True)
    try:
        r = await client.get(url, timeout=FETCH_TIMEOUT, follow_redirects=True,
                             headers={"User-Agent": _UA,
                                      "Accept": "text/html,application/xhtml+xml",
                                      "Accept-Language": "en-IN,en;q=0.9"})
        if r.status_code != 200:
            log.info("[READER] %s -> HTTP %d", url[:90], r.status_code)
            return ""
        ctype = r.headers.get("content-type", "")
        if "html" not in ctype and "xml" not in ctype:
            return ""
        if len(r.content) > MAX_BYTES:
            return ""
        return extract_text(r.text)
    except Exception as e:                                        # noqa: BLE001
        log.info("[READER] %s failed: %s", url[:90], type(e).__name__)
        return ""
    finally:
        if own:
            await client.aclose()


async def fetch_many(urls: dict) -> dict:
    """{key: url} -> {key: text}. Only keys whose page yielded an article appear."""
    out: dict = {}
    if not urls:
        return out
    sem = asyncio.Semaphore(CONCURRENCY)
    async with httpx.AsyncClient(follow_redirects=True) as client:
        async def one(key, url):
            async with sem:
                text = await fetch_article(url, client)
                if text:
                    out[key] = text
        await asyncio.gather(*[one(k, u) for k, u in urls.items()])
    log.info("[READER] read %d of %d article page(s)", len(out), len(urls))
    return out


def is_full_article(text: str) -> bool:
    return len((text or "").split()) >= MIN_ARTICLE_WORDS
