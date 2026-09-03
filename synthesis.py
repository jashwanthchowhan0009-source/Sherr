"""synthesis.py — multi-source synthesis: the only honest way to write a body.

WHY THIS MODULE EXISTS
──────────────────────
A single 200-character blurb cannot become an original 60-80 word article. There
is not enough information in it: anything the rewrite adds beyond the blurb is
invented, and anything it does not add is a paraphrase. That is the whole reason
the single-article rewrite kept producing either placeholders or near-copies —
it was an impossible task, not a badly prompted one.

Three to five blurbs about the SAME event are a different problem. The facts
that appear in more than one of them are corroborated, the union of them is
genuinely more than any single source, and re-authoring from that union is
ordinary journalism rather than fabrication.

So the rewrite is a synthesis pass over CLUSTERS, and this module owns:

  * `cluster_articles` — the clustering itself (shared significant terms, same
    pillar, inside one time window). It is the same union-find `link_stories`
    has always used; that function now calls in here rather than keeping a
    second copy that could drift.
  * `SYNTHESIS_PROMPT` — the prompt, verbatim and unedited.
  * `render_sources` / `build_prompt` — turning a cluster into the prompt's
    {{SOURCE_ARTICLES}} block.
  * `parse_synthesis` — validating what comes back before anything reaches a row.

CLUSTER SIZE IS NOT ASSUMED. `MIN_CLUSTER = 2`, not 3. A corpus whose events
mostly carry one or two sources is the common case for a feed ingesting a wide
spread of publishers, and a pass that only fires at 3+ would sit idle on it. Two
sources still corroborate; the prompt is told how many it has and the word count
scales with the evidence. Singletons are not synthesised at all — they fall back
to the single-article rewrite, which is honest about being one source.

Nothing here imports main. It is pure, so the clustering and the parsing are
testable without a database, an app, or a provider.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone

# ─── term extraction ─────────────────────────────────────────────────────────
# Lifted from main.link_stories, which is where this clustering has always
# lived. It is here so both callers share one definition.
STOPWORDS = set((
    "the a an and or of to in on for with at by from as is are was were be been "
    "being this that these those it its he she they them his her their our your "
    "you we new say says said report reports amid over after before into out up "
    "down off than then when what which who whom how why will would can could may "
    "might must not no yes but if about first also more most other some such only "
    "just very now get got make made back two one year years day days week weeks"
).split())

# A term in more articles than this is describing the corpus, not an event, and
# unioning on it merges everything into one cluster.
#
# IT HAS TO SCALE WITH THE SAMPLE. A flat 40 was written for a sample of a few
# dozen; against a pool of 400 it is 10%, but against a pool of 50 a term in 40
# of them passes as "specific" while being in 80% of the sample. Publisher
# summaries are full of exactly that kind of vocabulary — "the company said",
# "on Monday", "according to a statement" — and a generic term that survives the
# filter chains unrelated rows together through union-find until the whole pool
# is one cluster. Measured: with shared boilerplate and a flat cap, a 115-row
# pool collapsed into a single 115-member "event".
MAX_TERM_DOCS = 40
MAX_TERM_DOC_FRACTION = 0.10
MIN_SHARED_TERMS = 2

# A cluster bigger than this is not an event, it is a clustering failure —
# transitive merging that chained through a term it should have discarded. It is
# REFUSED rather than truncated to the first five members: picking five of forty
# would merge four arbitrary rows out of the feed and leave the rest, which is a
# worse outcome than writing nothing and is impossible to explain afterwards.
MAX_EVENT_SIZE = 8

# ── Why event clustering reads the SUMMARY and story threads do not ──────────
# Two publishers covering one event write different headlines on purpose. Their
# shared vocabulary in a headline is often a single proper noun — "OPEC+ weighs
# deeper output cuts" and "Oil advances after OPEC+ signals restraint" share
# exactly one significant term — so a headline-only rule at 2 shared terms finds
# almost no real events, and dropping it to 1 merges everything that mentions the
# same company.
#
# The body text is where the agreement actually is. Clustering for SYNTHESIS
# therefore reads headline + the publisher's summary, which gives each article
# tens of terms instead of six, and pays for the larger vocabulary with a second
# threshold: an overlap RATIO, not just a count. Two articles about one event
# share a large fraction of each other's terms; two unrelated articles that both
# mention a common company share a handful out of forty.
#
# Story threads keep the old headline-only rule. They only set story_id — a
# false link there costs a wrong "related" card, while a false link HERE merges
# two rows and takes one of them out of the feed.
EVENT_MIN_SHARED = 4
EVENT_MIN_RATIO = 0.28

# Two sources corroborate. See the module docstring — this is deliberately not 3.
MIN_CLUSTER = 2
# The prompt gets at most this many sources; beyond it the marginal source adds
# tokens rather than facts, and the strongest-overlapping members are enough.
MAX_CLUSTER = 5
WINDOW_HOURS = 24


def significant_terms(headline: str, tags=None) -> set:
    """Significant terms deciding whether two articles are about one event."""
    terms = set()
    for t in (tags or []):
        t = str(t).strip().lower()
        if len(t) >= 3:
            terms.add(t)
    for w in re.findall(r"[a-z0-9]{4,}", (headline or "").lower()):
        if w not in STOPWORDS:
            terms.add(w)
    return terms


def _tags_of(row) -> list:
    try:
        raw = row["micro_tags"]
    except (KeyError, IndexError, TypeError):
        raw = None
    try:
        return json.loads(raw or "[]")
    except Exception:                                             # noqa: BLE001
        return []


def _get(row, key, default=None):
    try:
        v = row[key]
    except (KeyError, IndexError, TypeError):
        return default
    return default if v is None else v


def parse_ts(value):
    """A timestamp from either schema, or None.

    published_at is TEXT on sqlite and timestamptz on Postgres (migration 018),
    so this has to accept a datetime as readily as a string — the same split
    that took the analog engine's Phase 1 down on first contact with production.
    """
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    s = str(value).strip().replace("Z", "+00:00").replace(" ", "T", 1)
    try:
        dt = datetime.fromisoformat(s[:32])
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)


def _within(a, b, window_hours) -> bool:
    """Are two rows inside the same window?

    A MISSING TIMESTAMP DOES NOT BLOCK A PAIR. Rows predating the published_at
    normalisation carry an empty string, and refusing to cluster them would
    quietly exclude the oldest part of the backlog — which is most of it. The
    term overlap is doing the real work; the window only stops two unrelated
    events months apart sharing enough vocabulary to merge.
    """
    if window_hours is None:
        return True
    ta, tb = parse_ts(a), parse_ts(b)
    if ta is None or tb is None:
        return True
    return abs((ta - tb).total_seconds()) <= window_hours * 3600.0


def event_terms(row) -> set:
    """Terms for EVENT clustering: the headline plus the publisher's own text.

    Deliberately not the rewritten body — on a candidate row that is the
    placeholder, and every placeholder is word-for-word identical, so including
    it would make every un-rewritten article look like every other one.
    """
    text = " ".join(str(_get(row, k, "") or "") for k in
                    ("headline", "source_headline", "source_summary", "summary_60"))
    return significant_terms(text, _tags_of(row))


# Ratio buckets for the diagnostic. The threshold's own value is a boundary so
# "just under" and "just over" are never averaged into the same bar — that is
# the difference between "the threshold is slightly wrong" and "no pair in this
# sample is remotely the same story".
RATIO_BUCKETS = (0.05, 0.10, 0.20, EVENT_MIN_RATIO, 0.40, 0.60, 1.01)


def _bucket(ratio: float) -> str:
    lo = 0.0
    for hi in RATIO_BUCKETS:
        if ratio < hi:
            return f"{lo:.2f}-{hi:.2f}"
        lo = hi
    return f">={RATIO_BUCKETS[-1]:.2f}"


def cluster_articles(rows, *, window_hours: int = WINDOW_HOURS,
                     min_shared: int = MIN_SHARED_TERMS,
                     min_ratio: float = 0.0,
                     same_pillar: bool = True,
                     terms_of=None,
                     stats: dict = None) -> list:
    """Group rows into events. Returns a list of lists, singletons included.

    Union-find over an inverted term index: two articles are joined when they
    share at least `min_shared` non-generic terms, sit in the same pillar, and
    fall inside `window_hours` of each other. Every input row appears in exactly
    one output group, so a caller can branch on `len(group)` and never lose one.
    """
    rows = list(rows)
    if len(rows) < 2:
        return [[r] for r in rows]

    by_id = {}
    for r in rows:
        rid = _get(r, "id")
        if rid is not None:
            by_id[rid] = r
    if len(by_id) < 2:
        return [[r] for r in rows]

    terms_of = terms_of or (lambda r: significant_terms(
        _get(r, "headline", ""), _tags_of(r)))
    terms: dict = {rid: terms_of(r) for rid, r in by_id.items()}

    inverted: dict = {}
    for rid, tset in terms.items():
        for term in tset:
            inverted.setdefault(term, []).append(rid)

    parent = {rid: rid for rid in by_id}

    def find(x):
        while parent[x] != x:
            parent[x] = parent[parent[x]]
            x = parent[x]
        return x

    def union(a, b):
        ra, rb = find(a), find(b)
        if ra != rb:
            parent[rb] = ra

    pair_shared: dict = {}
    doc_cap = max(3, min(MAX_TERM_DOCS,
                         int(MAX_TERM_DOC_FRACTION * len(by_id)) or MAX_TERM_DOCS))
    if stats is not None:
        stats["term_doc_cap"] = doc_cap
        stats["terms_dropped_as_generic"] = sum(
            1 for ids in inverted.values() if len(ids) > doc_cap)
    for term, ids in inverted.items():
        if len(ids) < 2 or len(ids) > doc_cap:
            continue
        for i in range(len(ids)):
            for j in range(i + 1, len(ids)):
                a, b = ids[i], ids[j]
                key = (a, b) if a < b else (b, a)
                pair_shared[key] = pair_shared.get(key, 0) + 1

    # WHICH GATE STOPS A PAIR IS THE WHOLE DIAGNOSTIC.
    #
    # "No clusters" has four completely different causes and four different
    # fixes: the threshold is too strict, the corpus genuinely has no
    # same-event pairs, the pillar split is separating them, or — the one that
    # cost this pass its first production run — the sample handed in was too
    # small and too narrow in time to CONTAIN a pair at all. Counting the stop
    # reason tells them apart; counting only the clusters cannot.
    if stats is not None:
        stats.setdefault("pairs_examined", 0)
        stats.setdefault("stopped", {})
        stats.setdefault("ratio_histogram", {})
        stats.setdefault("shared_histogram", {})
        stats.setdefault("best_pairs", [])

    def _stop(reason):
        if stats is not None:
            stats["stopped"][reason] = stats["stopped"].get(reason, 0) + 1

    for (a, b), shared in sorted(pair_shared.items()):
        smaller = min(len(terms[a]), len(terms[b])) or 1
        ratio = shared / smaller
        ra, rb = by_id[a], by_id[b]
        if stats is not None:
            stats["pairs_examined"] += 1
            key = _bucket(ratio)
            stats["ratio_histogram"][key] = stats["ratio_histogram"].get(key, 0) + 1
            sk = str(min(shared, 12))
            stats["shared_histogram"][sk] = stats["shared_histogram"].get(sk, 0) + 1
            # Keep the closest near-misses, headlines included, so a human can
            # answer the only question a histogram cannot: are these actually
            # the same story?
            stats["best_pairs"].append({
                "ids": [a, b], "shared": shared, "ratio": round(ratio, 3),
                "same_pillar": _get(ra, "pillar_id") == _get(rb, "pillar_id"),
                "in_window": _within(_get(ra, "published_at"),
                                     _get(rb, "published_at"), window_hours),
                "headlines": [str(_get(ra, "headline", ""))[:90],
                              str(_get(rb, "headline", ""))[:90]],
            })
        if shared < min_shared:
            _stop("shared_below_min")
            continue
        if min_ratio and ratio < min_ratio:
            # Normalised against the SMALLER vocabulary. Against the union, a
            # thin two-line wire item could never reach the threshold against a
            # long piece about the same event, and the thin ones are exactly the
            # rows this pass exists to rescue.
            _stop("ratio_below_min")
            continue
        if same_pillar and _get(ra, "pillar_id") != _get(rb, "pillar_id"):
            _stop("different_pillar")
            continue
        if not _within(_get(ra, "published_at"), _get(rb, "published_at"), window_hours):
            _stop("outside_window")
            continue
        _stop("joined")
        union(a, b)

    if stats is not None:
        stats["best_pairs"] = sorted(
            stats["best_pairs"], key=lambda p: -p["ratio"])[:8]

    groups: dict = {}
    for rid in by_id:
        groups.setdefault(find(rid), []).append(rid)

    out = []
    oversized = 0
    for members in groups.values():
        members.sort()
        if len(members) > MAX_EVENT_SIZE:
            # Broken back into singletons, not truncated. Each row is then
            # handled honestly by the single-article path instead of four of
            # them silently leaving the feed as "merged".
            oversized += 1
            out.extend([[by_id[m]] for m in members])
            continue
        out.append([by_id[m] for m in members])
    if stats is not None:
        stats["oversized_clusters_refused"] = oversized
    # Biggest events first: a cluster is worth more provider calls than a single.
    out.sort(key=lambda g: (-len(g), _get(g[0], "id", 0)))
    return out


def cluster_events(rows, *, window_hours: int = WINDOW_HOURS,
                   stats: dict = None) -> list:
    """cluster_articles with the settings that identify one news EVENT.

    One entry point so the synthesis pass, the report script and the tests all
    cluster identically — three call sites with three sets of thresholds is how
    a measured number becomes three unmeasured ones.
    """
    return cluster_articles(rows, window_hours=window_hours,
                            min_shared=EVENT_MIN_SHARED,
                            min_ratio=EVENT_MIN_RATIO,
                            terms_of=event_terms, stats=stats)


def pool_report(rows, *, window_hours: int = WINDOW_HOURS) -> dict:
    """Is this sample even CAPABLE of containing a same-event pair?

    THE QUESTION THAT WAS NEVER ASKED. The clustering window is 24 hours, but
    the drain used to hand the clusterer whatever the tick's rate limit allowed
    — ten rows, ordered by published_at DESC, so ten CONSECUTIVE articles.
    A feed ingesting a hundred articles an hour puts ten consecutive rows inside
    about six minutes. Two publishers covering one event are commonly an hour
    apart. The clusterer was being asked to find pairs from a 24-hour window
    inside a six-minute sample, and it found none — which was the correct answer
    to the question it was actually being asked.

    `span_hours` against `window_hours` is the number that says so: a span far
    below the window means the sample, not the threshold, is the constraint.
    """
    stamps = [t for t in (parse_ts(_get(r, "published_at")) for r in rows) if t]
    pillars: dict = {}
    for r in rows:
        p = _get(r, "pillar_id")
        pillars[str(p)] = pillars.get(str(p), 0) + 1
    span = ((max(stamps) - min(stamps)).total_seconds() / 3600.0
            if len(stamps) >= 2 else 0.0)
    # Pairs available at all: same pillar, inside the window. If this is 0 the
    # threshold is irrelevant, because nothing ever reaches it.
    eligible = 0
    rl = list(rows)
    for i in range(len(rl)):
        for j in range(i + 1, len(rl)):
            if _get(rl[i], "pillar_id") != _get(rl[j], "pillar_id"):
                continue
            if _within(_get(rl[i], "published_at"),
                       _get(rl[j], "published_at"), window_hours):
                eligible += 1
    return {
        "rows": len(rl),
        "with_timestamp": len(stamps),
        "span_hours": round(span, 2),
        "window_hours": window_hours,
        "sample_covers_window": bool(span >= window_hours * 0.5),
        "oldest": min(stamps).isoformat() if stamps else None,
        "newest": max(stamps).isoformat() if stamps else None,
        "pillars": dict(sorted(pillars.items())),
        "eligible_pairs": eligible,
        "max_possible_pairs": len(rl) * (len(rl) - 1) // 2,
    }


def size_histogram(clusters) -> dict:
    """{cluster size: how many clusters} — the number that decides this design."""
    hist: dict = {}
    for c in clusters:
        hist[len(c)] = hist.get(len(c), 0) + 1
    return dict(sorted(hist.items()))


# ─── the prompt ──────────────────────────────────────────────────────────────
# VERBATIM. Do not reword, reformat, or "improve" this. It is the specification
# for what the model is allowed to write, and every clause in it is load-bearing:
# fact isolation is what keeps the output out of copyright, the prohibition on
# hallucination is what keeps it out of SEBI's way, and the JSON shape is what
# parse_synthesis validates against.
SYNTHESIS_PROMPT = """You are an objective news synthesis engine. Your job is to extract raw factual events from multiple provided source articles and author an original, publication-ready news briefing.
INPUT DATA:
Sources provided below: {{SOURCE_ARTICLES}}
OPERATIONAL RULES:

1. FACT ISOLATION:
   * Extract only verifiable data points: What happened, who was involved, where it occurred, when it happened, and why/how.
   * Do not copy phrasing, stylistic choices, rhetoric, or sentence structures from the input text.
2. SYNTHESIS & RE-AUTHORING:
   * Write a completely new article based solely on the isolated facts.
   * Keep the tone journalistic, neutral, and strictly informative.
   * Target word count: [Insert e.g., 60 to 80 words].
   * Write in the inverted pyramid style: lead with the core development, follow with secondary details, and close with official context.
3. STRICT PROHIBITIONS:
   * Do NOT produce a line-by-line paraphrase.
   * Do NOT include editorial commentary, personal opinions, or adjectives not supported directly by the facts.
   * Do NOT hallucinate names, dates, or numbers. If the sources conflict, mention the discrepancy explicitly.

OUTPUT FORMAT:
Output ONLY valid JSON with this exact structure: { "headline": "Punchy, factual headline under 12 words", "content": "The generated original summary text.", "extracted_entities": ["Key Person", "Organization", "Location"], "primary_source_attribution": "Name of the primary publication reporting this" }"""

# The JSON contract above, restated for Gemini's responseSchema so the provider
# enforces the shape rather than us discovering a missing key at parse time.
SYNTHESIS_SCHEMA = {
    "type": "OBJECT",
    "properties": {
        "headline": {"type": "STRING"},
        "content": {"type": "STRING"},
        "extracted_entities": {"type": "ARRAY", "items": {"type": "STRING"}},
        "primary_source_attribution": {"type": "STRING"},
    },
    "required": ["headline", "content", "extracted_entities",
                 "primary_source_attribution"],
}

# Per-source text budget. Five sources at 700 characters is well inside a free
# tier request and leaves the model room to answer.
SOURCE_CHARS = 700


def render_sources(rows, source_text_of=None) -> str:
    """The {{SOURCE_ARTICLES}} block: one numbered entry per cluster member.

    `source_text_of(row)` supplies the publisher's surviving text — in the app
    that is body_state.source_material, which is passed in rather than imported
    so this module stays free of the app's dependencies.
    """
    parts = []
    for i, r in enumerate(rows, 1):
        if source_text_of is not None:
            text = source_text_of(r) or ""
        else:
            text = _get(r, "source_summary", "") or _get(r, "summary_60", "") or ""
        parts.append(
            f"[SOURCE {i}]\n"
            f"Publication: {_get(r, 'source_name', '') or 'Unknown'}\n"
            f"Headline: {_get(r, 'source_headline', '') or _get(r, 'headline', '')}\n"
            f"Text: {str(text).strip()[:SOURCE_CHARS]}"
        )
    return "\n\n".join(parts)


def build_prompt(rows, source_text_of=None) -> str:
    return SYNTHESIS_PROMPT.replace("{{SOURCE_ARTICLES}}",
                                    "\n\n" + render_sources(rows, source_text_of))


# ─── validating the answer ───────────────────────────────────────────────────
MIN_CONTENT_WORDS = 25
MAX_HEADLINE_WORDS = 12


class SynthesisRejected(Exception):
    """The model's answer cannot be written to a row, and why."""


def parse_synthesis(raw, *, n_sources: int = 0) -> dict:
    """Validate the model's JSON, or raise SynthesisRejected.

    NOTHING HALF-VALID IS ACCEPTED. A synthesis that came back short, empty, or
    shaped wrong is a failed call, not a partial result — writing it would put a
    fragment on a published row, and the row is better left as a placeholder
    that the next tick retries.
    """
    if isinstance(raw, (str, bytes)):
        text = raw.decode() if isinstance(raw, bytes) else raw
        text = text.strip()
        if text.startswith("```"):
            text = re.sub(r"^```[a-z]*\s*|\s*```$", "", text, flags=re.I | re.S)
        try:
            raw = json.loads(text)
        except json.JSONDecodeError as e:
            raise SynthesisRejected(f"not JSON: {e}") from e
    if not isinstance(raw, dict):
        raise SynthesisRejected(f"expected an object, got {type(raw).__name__}")

    headline = str(raw.get("headline") or "").strip()
    content = str(raw.get("content") or "").strip()
    if not headline:
        raise SynthesisRejected("empty headline")
    if len(headline.split()) > MAX_HEADLINE_WORDS + 4:
        headline = " ".join(headline.split()[:MAX_HEADLINE_WORDS])
    words = len(content.split())
    if words < MIN_CONTENT_WORDS:
        raise SynthesisRejected(
            f"content is {words} words, under the {MIN_CONTENT_WORDS}-word floor")

    ents = raw.get("extracted_entities") or []
    if isinstance(ents, str):
        ents = [ents]
    entities = [str(e).strip() for e in ents if str(e).strip()][:10]

    return {
        "headline": headline,
        "content": content,
        "extracted_entities": entities,
        "primary_source_attribution": str(
            raw.get("primary_source_attribution") or "").strip(),
        "n_sources": n_sources,
        "words": words,
    }
