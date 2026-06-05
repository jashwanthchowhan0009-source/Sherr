"""
text_utils.py — Shared text cleaning, fingerprinting & similarity helpers.

Ported from the MVP and extended with a 64-bit SimHash for near-duplicate
body detection (dedup layer 3). Used across the pipeline.
"""

from __future__ import annotations

import hashlib
import re

# ─── Junk patterns commonly found in RSS feed bodies ──────────────────────────
_JUNK_PATTERNS = [
    re.compile(r"<!--.*?-->", re.DOTALL),
    re.compile(r"<script[^>]*>.*?</script>", re.DOTALL | re.IGNORECASE),
    re.compile(r"<style[^>]*>.*?</style>", re.DOTALL | re.IGNORECASE),
    re.compile(r"<[^>]+>"),
    re.compile(r"read more at\s+[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"continue reading[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"click here to\s+[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"copyright\s*©?\s*\d{4}[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"all rights reserved[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"follow us on\s+[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"subscribe to\s+[^.\n]*\.?", re.IGNORECASE),
    re.compile(r"the post .+? appeared first on .+?\.", re.IGNORECASE),
    re.compile(r"this article (?:originally|first) appeared on [^.\n]+\.?", re.IGNORECASE),
    re.compile(r"\[image[^\]]*\]", re.IGNORECASE),
    re.compile(r"\[video[^\]]*\]", re.IGNORECASE),
    re.compile(r"\[\+\d+\s*chars?\]", re.IGNORECASE),
    re.compile(r"\[+\s*\]+"),
    re.compile(r"\((?:reuters|ap|afp|pti|ians)\)\s*[-–]?\s*", re.IGNORECASE),
]

_HTML_ENTITIES = {
    "&nbsp;": " ", "&amp;": "&", "&quot;": '"', "&lt;": "<", "&gt;": ">",
    "&#39;": "'", "&apos;": "'", "&rsquo;": "'", "&lsquo;": "'",
    "&rdquo;": '"', "&ldquo;": '"', "&ndash;": "-", "&mdash;": "—",
    "&hellip;": "...", "&#8217;": "'", "&#8216;": "'", "&#8220;": '"',
    "&#8221;": '"', "&#8211;": "-", "&#8212;": "—",
}


def decode_entities(text: str) -> str:
    for entity, char in _HTML_ENTITIES.items():
        text = text.replace(entity, char)
    return re.sub(r"&#\d+;", " ", text)


def clean_html_fragments(text: str) -> str:
    """Strip HTML, decode entities, remove feed boilerplate, collapse whitespace."""
    if not text:
        return ""
    text = decode_entities(text)
    for pattern in _JUNK_PATTERNS:
        text = pattern.sub(" ", text)
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"^[\s\-\–\—\•\*]+", "", text)
    text = re.sub(r"[\s\-\–\—\•\*]+$", "", text)
    return text.strip()


def normalize_title(title: str) -> str:
    if not title:
        return ""
    t = title.lower()
    t = re.sub(r"^(breaking|update|updated|watch|exclusive|live|just in|news):\s*", "", t)
    t = re.sub(r"[^a-z0-9\s]", "", t)
    return re.sub(r"\s+", " ", t).strip()


def title_fingerprint(title: str) -> str:
    """16-char md5 over the normalized title — dedup layer 2."""
    return hashlib.md5(normalize_title(title).encode("utf-8")).hexdigest()[:16]


def url_fingerprint(url: str) -> str:
    """Stable hash of the URL (sans query/fragment) — dedup layer 1."""
    base = re.sub(r"[?#].*$", "", (url or "").strip().lower().rstrip("/"))
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


# ─── SimHash (dedup layer 3) ──────────────────────────────────────────────────
def _tokens(text: str) -> list[str]:
    return re.findall(r"[a-z0-9]+", (text or "").lower())


def simhash(text: str, bits: int = 64) -> int:
    """Charikar SimHash over word tokens. Near-identical bodies → close hashes."""
    tokens = _tokens(text)
    if not tokens:
        return 0
    vector = [0] * bits
    for tok in tokens:
        h = int(hashlib.md5(tok.encode("utf-8")).hexdigest(), 16)
        for i in range(bits):
            vector[i] += 1 if (h >> i) & 1 else -1
    out = 0
    for i in range(bits):
        if vector[i] > 0:
            out |= 1 << i
    # Mask to 63 bits so the value always fits PostgreSQL's signed BIGINT
    # (max 2**63-1). The raw 64-bit form can set bit 63 and overflow on INSERT
    # ("value out of int64 range"). Masking here — at the source — keeps the
    # in-memory and stored simhash identical, so near-duplicate Hamming
    # comparisons stay consistent.
    return out & 0x7FFFFFFFFFFFFFFF


def hamming_distance(a: int, b: int) -> int:
    return bin(a ^ b).count("1")


def simhash_similar(a: int, b: int, max_distance: int = 3) -> bool:
    if not a or not b:
        return False
    return hamming_distance(a, b) <= max_distance


def jaccard_similarity(a: str, b: str) -> float:
    na, nb = set(normalize_title(a).split()), set(normalize_title(b).split())
    if not na or not nb:
        return 0.0
    return len(na & nb) / len(na | nb)


# ─── Small content helpers ────────────────────────────────────────────────────
def word_count(text: str) -> int:
    return len(text.split()) if text else 0


def truncate_to_words(text: str, max_words: int) -> str:
    if not text:
        return ""
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words]).rstrip(",.;:") + "…"


def extract_sentences(text: str, n: int = 2) -> str:
    if not text:
        return ""
    sentences = [s.strip() for s in re.split(r"(?<=[.!?])\s+", text.strip()) if len(s.strip()) > 10]
    return " ".join(sentences[:n])
