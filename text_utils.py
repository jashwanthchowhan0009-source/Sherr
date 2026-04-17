"""
text_utils.py — Text cleaning, deduplication, and content utilities
Shared helpers for main.py and ai_processor.py.
"""

import re
import hashlib
from typing import Optional

# ─── Junk patterns commonly found in RSS feed bodies ─────────────────────
_JUNK_PATTERNS = [
    re.compile(r'<!--.*?-->', re.DOTALL),
    re.compile(r'<script[^>]*>.*?</script>', re.DOTALL | re.IGNORECASE),
    re.compile(r'<style[^>]*>.*?</style>', re.DOTALL | re.IGNORECASE),
    re.compile(r'<[^>]+>'),                                            # any remaining HTML tag
    re.compile(r'read more at\s+[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'continue reading[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'click here to\s+[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'copyright\s*©?\s*\d{4}[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'all rights reserved[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'follow us on\s+[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'subscribe to\s+[^.\n]*\.?', re.IGNORECASE),
    re.compile(r'the post .+? appeared first on .+?\.', re.IGNORECASE),
    re.compile(r'this article (?:originally|first) appeared on [^.\n]+\.?', re.IGNORECASE),
    re.compile(r'\[image[^\]]*\]', re.IGNORECASE),
    re.compile(r'\[video[^\]]*\]', re.IGNORECASE),
    re.compile(r'\[\+\d+\s*chars?\]', re.IGNORECASE),                   # NewsAPI's "[+1234 chars]"
    re.compile(r'\[+\s*\]+'),                                           # empty brackets
    re.compile(r'\(reuters\)\s*[-–]?\s*', re.IGNORECASE),
    re.compile(r'\(ap\)\s*[-–]?\s*', re.IGNORECASE),
    re.compile(r'\(afp\)\s*[-–]?\s*', re.IGNORECASE),
    re.compile(r'\(pti\)\s*[-–]?\s*', re.IGNORECASE),
    re.compile(r'\(ians\)\s*[-–]?\s*', re.IGNORECASE),
]

_HTML_ENTITIES = {
    '&nbsp;': ' ', '&amp;': '&', '&quot;': '"', '&lt;': '<', '&gt;': '>',
    '&#39;': "'", '&apos;': "'", '&rsquo;': "'", '&lsquo;': "'",
    '&rdquo;': '"', '&ldquo;': '"', '&ndash;': '-', '&mdash;': '—',
    '&hellip;': '...', '&#8217;': "'", '&#8216;': "'", '&#8220;': '"',
    '&#8221;': '"', '&#8211;': '-', '&#8212;': '—',
}


def decode_entities(text: str) -> str:
    for entity, char in _HTML_ENTITIES.items():
        text = text.replace(entity, char)
    # Numeric entities we haven't mapped explicitly
    text = re.sub(r'&#\d+;', ' ', text)
    return text


def clean_html_fragments(text: str) -> str:
    """Strip HTML, decode entities, remove feed boilerplate, collapse whitespace."""
    if not text:
        return ""
    text = decode_entities(text)
    for pattern in _JUNK_PATTERNS:
        text = pattern.sub(' ', text)
    text = re.sub(r'\s+', ' ', text).strip()
    text = re.sub(r'^[\s\-\–\—\•\*]+', '', text)
    text = re.sub(r'[\s\-\–\—\•\*]+$', '', text)
    return text.strip()


def normalize_title(title: str) -> str:
    """Normalize a title for fingerprinting/similarity checks."""
    if not title:
        return ""
    t = title.lower()
    t = re.sub(r'^(breaking|update|updated|watch|exclusive|live|just in|news):\s*', '', t)
    t = re.sub(r'[^a-z0-9\s]', '', t)
    t = re.sub(r'\s+', ' ', t).strip()
    return t


def title_fingerprint(title: str) -> str:
    """16-char hash for fast dedup comparison."""
    return hashlib.md5(normalize_title(title).encode('utf-8')).hexdigest()[:16]


def jaccard_similarity(a: str, b: str) -> float:
    """Word-set Jaccard similarity — fast duplicate heuristic."""
    na = set(normalize_title(a).split())
    nb = set(normalize_title(b).split())
    if not na or not nb:
        return 0.0
    return len(na & nb) / len(na | nb)


def is_similar_title(a: str, b: str, threshold: float = 0.80) -> bool:
    return jaccard_similarity(a, b) >= threshold


def word_count(text: str) -> int:
    return len(text.split()) if text else 0


def truncate_to_words(text: str, max_words: int) -> str:
    if not text:
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text
    return ' '.join(words[:max_words]).rstrip(',.;:') + '…'


def extract_sentences(text: str, n: int = 2) -> str:
    """Grab the first N sentences as a fallback summary."""
    if not text:
        return ""
    sentences = re.split(r'(?<=[.!?])\s+', text.strip())
    sentences = [s.strip() for s in sentences if len(s.strip()) > 10]
    return ' '.join(sentences[:n])


def summary_conflicts_with_title(summary: str, title: str, threshold: float = 0.75) -> bool:
    """True if summary is basically a restatement of the title."""
    if not summary or not title:
        return False
    s_norm = normalize_title(summary)
    t_norm = normalize_title(title)
    if len(t_norm) < 8:
        return False
    # Summary literally starts with the title
    if s_norm.startswith(t_norm):
        return True
    # Or heavy word overlap
    t_words = set(t_norm.split())
    s_words = set(s_norm.split())
    if not t_words or not s_words:
        return False
    return len(s_words & t_words) / len(t_words) >= threshold
