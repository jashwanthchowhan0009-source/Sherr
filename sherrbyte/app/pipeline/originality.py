"""
pipeline/originality.py — the originality gate (P0, legal blocker).

Nothing reaches the feed without passing through here. An article that renders the
publisher's sentences is copyright infringement regardless of how the pipeline got
there, so this is a hard gate on the publish path, not a lint.

    originality_check(generated, source_text) -> (passed, metrics)

Pure and stdlib-only: no DB, no network, no model. That makes it unit-testable, cheap
enough to run on every article, and impossible to "fail open" because a service was
down.

TWO MEASURES, because either one alone has a blind spot:

  • CONTAINMENT — what fraction of the GENERATED text's 7-grams also appear in the
    source. This is the plagiarism question: "how much of what we published did we
    take?"
  • JACCARD — symmetric overlap of the two 7-gram sets.

The spec asked for Jaccard, and Jaccard alone is not safe here. It divides by the
UNION, so copying a paragraph verbatim out of a long article scores low: 60 shared
7-grams against a union of 900 is 0.067, under the 0.08 threshold, and a verbatim
excerpt sails through. Containment catches exactly that case (it would score 1.0), so
the gate fails on EITHER measure and both are persisted for the audit trail.

QUOTED SPANS. A short attributed quote is legitimate. Text inside quotation marks is
exempt from the contiguous-run check, but only up to MAX_QUOTE_TOKENS words — a span
longer than that is itself a failure, which is what stops the whole article being
wrapped in one pair of quotes.
"""

from __future__ import annotations

import re
import unicodedata

# 7-gram overlap above this fails. Deliberately low: seven consecutive words shared
# with the source is not a coincidence of phrasing.
MAX_NGRAM_OVERLAP = 0.08
# Longest contiguous shared run, in tokens, outside a quoted span.
MAX_CONTIGUOUS_RUN = 25
# A single quoted span may not exceed this. Same number, different rule: 25 words is
# the most we will ever reproduce verbatim, and only with quotation marks around it.
MAX_QUOTE_TOKENS = 25
NGRAM_N = 7

_WORD_RE = re.compile(r"[a-z0-9]+")
# Straight and curly double quotes, plus the guillemets some wires use.
_QUOTE_CHARS = '"“”«»'
_QUOTE_RE = re.compile(f"[{re.escape(_QUOTE_CHARS)}]([^{re.escape(_QUOTE_CHARS)}]+)"
                       f"[{re.escape(_QUOTE_CHARS)}]")


def normalize(text: str) -> str:
    """Lowercase, strip accents and punctuation, collapse whitespace."""
    if not text:
        return ""
    text = unicodedata.normalize("NFKD", text)
    text = "".join(c for c in text if not unicodedata.combining(c))
    return " ".join(_WORD_RE.findall(text.lower()))


def tokenize(text: str) -> list[str]:
    return normalize(text).split()


def ngrams(tokens: list[str], n: int = NGRAM_N) -> set[tuple]:
    if len(tokens) < n:
        return set()
    return {tuple(tokens[i:i + n]) for i in range(len(tokens) - n + 1)}


def quoted_spans(text: str) -> list[dict]:
    """Quoted passages in the ORIGINAL text, with their token lengths.

    Returned as {text, tokens, over_limit} so the caller — and the audit row — can see
    not just that a quote existed but whether it was within the limit.
    """
    spans = []
    for m in _QUOTE_RE.finditer(text or ""):
        toks = tokenize(m.group(1))
        if not toks:
            continue
        spans.append({"text": m.group(1).strip()[:200], "tokens": len(toks),
                      "over_limit": len(toks) > MAX_QUOTE_TOKENS})
    return spans


def _quoted_token_positions(text: str) -> set[int]:
    """Indices, in the normalized token stream, that fall inside a legitimate quote.

    Only spans within MAX_QUOTE_TOKENS are exempted. An over-long quote stays subject
    to the run check AND fails on its own, so wrapping a stolen paragraph in quotes
    buys nothing.
    """
    exempt: set[int] = set()
    cursor = 0
    full = tokenize(text)
    for m in _QUOTE_RE.finditer(text or ""):
        before = tokenize((text or "")[:m.start()])
        inside = tokenize(m.group(1))
        if not inside or len(inside) > MAX_QUOTE_TOKENS:
            continue
        start = len(before)
        # Guard against a normalization mismatch shifting the offset.
        if full[start:start + len(inside)] == inside:
            exempt.update(range(start, start + len(inside)))
        cursor = start + len(inside)
    return exempt


def longest_common_run(a: list[str], b: list[str], skip: set[int] | None = None) -> int:
    """Longest contiguous token run shared by `a` and `b`.

    `skip` holds indices in `a` (the generated text) that sit inside a legitimate
    quote; a run is not credited while it is inside one. Space-optimised DP — one row
    at a time, so an article-length pair costs a few hundred KB, not megabytes.
    """
    if not a or not b:
        return 0
    skip = skip or set()
    prev = [0] * (len(b) + 1)
    best = 0
    for i, tok in enumerate(a, 1):
        cur = [0] * (len(b) + 1)
        if (i - 1) not in skip:
            for j, other in enumerate(b, 1):
                if tok == other:
                    cur[j] = prev[j - 1] + 1
                    if cur[j] > best:
                        best = cur[j]
        prev = cur
    return best


def originality_check(generated: str, source_text: str) -> tuple[bool, dict]:
    """Decide whether `generated` is original enough to publish.

    Returns (passed, metrics). `metrics` is persisted on the article row — it is the
    audit trail, so it records the numbers whether or not the article passed.
    """
    gen_tokens = tokenize(generated)
    src_tokens = tokenize(source_text)

    gen_ng = ngrams(gen_tokens)
    src_ng = ngrams(src_tokens)
    shared = gen_ng & src_ng
    union = gen_ng | src_ng

    overlap = round(len(shared) / len(union), 4) if union else 0.0
    containment = round(len(shared) / len(gen_ng), 4) if gen_ng else 0.0

    spans = quoted_spans(generated)
    over_long_quote = any(s["over_limit"] for s in spans)
    longest_run = longest_common_run(gen_tokens, src_tokens,
                                     skip=_quoted_token_positions(generated))

    reasons = []
    if overlap > MAX_NGRAM_OVERLAP:
        reasons.append(f"7-gram overlap {overlap} exceeds {MAX_NGRAM_OVERLAP}")
    if containment > MAX_NGRAM_OVERLAP:
        reasons.append(f"7-gram containment {containment} exceeds {MAX_NGRAM_OVERLAP}")
    if longest_run > MAX_CONTIGUOUS_RUN:
        reasons.append(f"contiguous run of {longest_run} tokens exceeds "
                       f"{MAX_CONTIGUOUS_RUN}")
    if over_long_quote:
        longest_q = max(s["tokens"] for s in spans if s["over_limit"])
        reasons.append(f"quoted span of {longest_q} words exceeds {MAX_QUOTE_TOKENS}")

    metrics = {
        "overlap": overlap,
        "containment": containment,
        "longest_run": longest_run,
        "quoted_spans": spans,
        "generated_tokens": len(gen_tokens),
        "source_tokens": len(src_tokens),
        "reasons": reasons,
        "thresholds": {"overlap": MAX_NGRAM_OVERLAP,
                       "longest_run": MAX_CONTIGUOUS_RUN,
                       "quote_tokens": MAX_QUOTE_TOKENS},
    }
    return (not reasons), metrics


# ─── 0.2 — our headline must be ours ──────────────────────────────────────────
# A rewrite that merely reorders or truncates the source headline is not a rewrite.
# Two independent rules: it may not be contained in the source, and it may not share
# any 5-word contiguous run with it.
MAX_HEADLINE_RUN = 4          # 5-word run == fail, so the ceiling is 4


def headline_is_original(ours: str, source: str) -> tuple[bool, dict]:
    """Check a generated headline against the publisher's.

    Returns (passed, metrics). An empty or whitespace-only headline fails: the caller
    must park the row, never fall back to the source headline.
    """
    a, b = tokenize(ours), tokenize(source)
    if not a:
        return False, {"reason": "empty headline", "longest_run": 0, "substring": False}

    # Substring test on the normalized token stream, so punctuation and casing
    # differences cannot disguise a verbatim lift.
    joined_src, joined_ours = " ".join(b), " ".join(a)
    substring = bool(joined_ours) and joined_ours in joined_src

    run = longest_common_run(a, b)
    reasons = []
    if substring:
        reasons.append("headline is contained in the source headline")
    if run > MAX_HEADLINE_RUN:
        reasons.append(f"shares a {run}-word run with the source headline")
    return (not reasons), {"longest_run": run, "substring": substring,
                           "reasons": reasons, "tokens": len(a)}
