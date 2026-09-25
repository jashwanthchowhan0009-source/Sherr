# SHERR_WRITING_SPEC.md

Authoritative spec for how SherrByte writes article cards.

**Status:** v1, Sept 2026. Covers Section 1 (NEWS) only as implemented.
Sections 2 and 3 (STRINGS, DOTS) are specified here but **not to be built yet**.

**Scope note for implementers:** Section 1 and Section 4 are binding and must
be implemented verbatim. Do not paraphrase the prompt, do not soften a gate
rule. If something is ambiguous, stop and ask rather than deciding.

**Reconciled with the existing gate (2026-09-25).** When this spec was
implemented, `writer_gate.py` already carried an earlier gate wired into the
multi-source synthesis path (throat-clearing, first-six-words, `length`,
open-loop, passive-nominalisation, entity-presence). By the owner's decision
those rules were KEPT, running alongside G1-G7 rather than replaced, and where
the two disagreed the code won and this file was updated to match — see §4.5.
So "do not add rules that aren't here" governs the G1-G7 gate on the
single-source NEWS writer; it does not require removing the synthesis-card
checks that predate it.

---

## Section 0 — Non-negotiables

These apply to every writer in every section.

1. **No verbatim reproduction.** No sentence, distinctive phrase, or clause
   may be lifted from the source article. This is a copyright boundary, not
   a style preference.
2. **No invented facts.** Every name, number, date, place and quantity in the
   output must be traceable to the source text. If it isn't in the source, it
   does not go in the card.
3. **No prediction language.** SherrByte detects and describes; it does not
   forecast. See the banned list in Section 4.
4. **No editorial voice.** No adjectives of judgement, no sensationalism, no
   implied stance on any party.
5. **Source conflicts are stated, not resolved.** If two sources disagree on
   a fact, say so plainly in the body rather than picking one.

---

## Section 1 — The NEWS writer

### 1.1 What NEWS is

NEWS is the factual layer of a card: what happened, reconstructed from source
material into SherrByte's own words. It is not a summary of an article — it is
an independent restatement of the verifiable facts that article reports.

### 1.2 Output contract

The writer returns a single JSON object. No prose outside the JSON, no
markdown fences, no commentary.

```json
{
  "headline": "",
  "body": "",
  "why_it_matters": "",
  "numbers_used": [],
  "entities": {
    "subject": "",
    "affected": []
  },
  "primary_source_attribution": ""
}
```

Field rules:

- **`headline`** — 6 to 14 words. States the event. Not a question, not a
  tease, no colon-subtitle construction. Must not reuse more than 4
  consecutive words from any source headline.
- **`body`** — 60 to 80 words, three sentences minimum. Structure:
  - Sentence 1: what happened, who did it, when and where.
  - Sentence 2: the mechanism — how it happened or how it takes effect.
  - Sentence 3: context or scale marker — the figure, comparison or
    timeframe that tells the reader how big this is.
- **`why_it_matters`** — one sentence, 12 to 28 words. **This is sentence 3
  of the body restated as significance**, not a new claim. It must not
  introduce any fact absent from the body. It must not speculate about
  consequences.
- **`numbers_used`** — verification array. Every numeric value that appears
  anywhere in `headline`, `body` or `why_it_matters`, listed as objects:
  ```json
  {"value": "647 million", "unit": "USD", "source_span": "<exact substring from the source text where this number appears>"}
  ```
  The `source_span` must be a literal substring of the input. This array is
  used by the gate to verify numbers, and is never shown to the reader.
  If the card contains no numbers, this is an empty array.
- **`entities.subject`** — the primary actor, as named in the source.
- **`entities.affected`** — other named parties directly affected. Named
  entities only, no categories.
- **`primary_source_attribution`** — the publication the facts came from.

### 1.3 The prompt

This is the prompt text. Use it as-is.

```
You are the SherrByte NEWS writer. You receive raw source text about a
real-world event. You produce an original, factual restatement of that event
in SherrByte's own words.

ABSOLUTE RULES

1. Never reproduce a sentence, clause or distinctive phrase from the source.
   Read for facts, then write from scratch. Matching wording is a failure
   even when the meaning is correct.
2. Never state a fact the source does not contain. No background you know
   from elsewhere, no inferred figures, no rounded-up numbers, no names the
   source does not name.
3. Never predict, forecast, advise or evaluate. Describe what happened and
   what is verifiably in effect. Do not say what will happen next, and do not
   characterise anything as good, bad, strong or weak.
4. Never use editorial adjectives. Write as a wire service writes: flat,
   specific, unhurried.
5. If the source contains conflicting figures or accounts, state the conflict
   in the body rather than choosing one.

WHAT TO WRITE

headline: 6-14 words stating the event. No questions, no teases, no colons.
Do not reuse more than four consecutive words from the source headline.

body: 60-80 words, at least three sentences.
  Sentence 1 - what happened, who, when, where.
  Sentence 2 - the mechanism: how it happened, or how it takes effect.
  Sentence 3 - context or scale: the figure, comparison or timeframe that
  tells a reader how large this is. This sentence must contain something
  concrete, not a generality.

why_it_matters: one sentence, 12-28 words, restating sentence 3 of the body
as significance. Introduce nothing new. Do not speculate about consequences.

numbers_used: for every number appearing in your headline, body or
why_it_matters, give an object with the value, its unit, and source_span -
an exact substring copied from the source text where that number appears.
This is the only place you may copy source text, and it is never published.
If you used no numbers, return an empty array.

entities: the subject (primary actor) and affected (other named parties).
Named entities only.

primary_source_attribution: the publication these facts came from.

OUTPUT

Return one JSON object and nothing else. No markdown fences, no explanation.

{
  "headline": "",
  "body": "",
  "why_it_matters": "",
  "numbers_used": [],
  "entities": {"subject": "", "affected": []},
  "primary_source_attribution": ""
}

SOURCE TEXT:
```

### 1.4 Writer identity

Every generated card records which writer produced it:
`writer_id` = model name plus prompt version, e.g. `gemini-flash/news-v1`.
This is what `/admin/writer-doctor` buckets failures by.

---

## Section 2 — STRINGS (specified, not to be built)

The causal thread. Past events that led to this one, as a timeline of
Origin → Escalation → Spark.

**Blocked on:** real semantic embeddings. Retrieval over the current
md5-hash vectors returns lexically-similar but semantically unrelated
articles, which would produce confidently wrong history. Do not implement
STRINGS until MiniLM vectors are live and the retrieval quality has been
measured.

---

## Section 3 — DOTS (specified, not to be built)

Cross-asset contagion. How this event transmits to markets, policy, supply
chains and consumer behaviour, plus the asymmetric catch.

**Rule when built:** no forced analogies. A dot is only written where there
is an actual balance-sheet, regulatory or behavioural transmission path, and
the mechanism must be stated. Absence of dots is an acceptable output.

---

## Section 4 — The quality gate

Runs on every card before publish. Chains after the existing originality
check, which it does not replace.

### 4.1 Rules

Each rule returns pass or fail with a reason string. A card fails the gate if
any rule fails.

| ID | Rule | Fails when |
|----|------|-----------|
| `G1_ORIGINALITY` | Existing 7-gram Jaccard + containment check | The existing `originality_check` returns a fail. Thresholds unchanged. |
| `G2_NUMBERS` | Every number in the output is verified | Any numeric token in headline/body/why_it_matters is absent from `numbers_used`, **or** a `source_span` is not a literal substring of the input text. |
| `G3_BANNED_TERMS` | SEBI language block | Any banned term (4.2) appears in headline, body or why_it_matters, case-insensitive, whole-word. |
| `G4_LENGTH` | Field lengths within contract | headline outside 6-14 words; body outside 60-80 words or under 3 sentences; why_it_matters outside 12-28 words. |
| `G5_WHY_GROUNDED` | why_it_matters introduces nothing new | Any number or named entity in why_it_matters is absent from the body. |
| `G6_HEADLINE_ECHO` | Headline is not lifted | Any run of 5+ consecutive words shared with a source headline. |
| `G7_SHAPE` | Output is well-formed | JSON invalid, a required field missing, or any field empty except `numbers_used` and `entities.affected`. |

### 4.2 Banned terms

Extends the existing runtime blocklist. Whole-word, case-insensitive, and
matched across the listed inflections.

Already blocked, retained:
`will`, `buy`, `sell`, `predict`, `bullish`, `bearish`

Added by this spec:

- **Forecast verbs:** `forecast`, `expect`, `anticipate`, `project` (as verb),
  `estimate` (as verb), `poised`, `set to`, `on track to`, `likely to`,
  `could see`, `is expected to`
- **Advice framing:** `should`, `recommend`, `investors should`,
  `worth watching`, `one to watch`, `opportunity`, `risk to consider`
- **Valuation judgement:** `undervalued`, `overvalued`, `cheap`, `expensive`,
  `attractive`, `compelling`, `strong buy`, `outperform`, `underperform`
- **Sensational framing:** `soar`, `plunge`, `crash`, `skyrocket`, `collapse`
  (of a price), `explode`, `devastating`, `stunning`, `shocking`
- **Certainty overreach:** `guaranteed`, `certain to`, `inevitable`,
  `no doubt`, `clearly shows`

Exemption: a banned term inside a directly attributed statement of fact about
what a named party themselves said is permitted, provided the attribution is
explicit. `G3` must check for the attribution pattern before failing.

### 4.3 Failure handling

1. Card fails the gate → regenerate **once**, passing the failed rule IDs and
   their reasons back to the writer as corrective context.
2. Second attempt fails → fall back to `_SAFE_SUMMARY`. Do not publish the
   failed body.
3. Every attempt, pass or fail, is recorded for `/admin/writer-doctor`.

### 4.4 Rate limiting

Regenerate-once doubles worst-case LLM calls. The gate must therefore:

- Share a single rate limiter with the rest of the ingest pipeline, sized to
  the Gemini free tier (15 requests/minute).
- Apply exponential backoff with jitter on 429, honouring `Retry-After`.
- Treat a regeneration as lower priority than a first-pass generation, so a
  backlog of retries cannot starve new ingestion.
- Skip regeneration entirely and go straight to `_SAFE_SUMMARY` when the
  limiter is saturated. A safe fallback is better than a stalled queue.

### 4.5 Reconciliation with the existing synthesis gate

The G1-G7 gate above runs on the single-source NEWS writer's output
(`ai_processor.news_writer`). A separate, older checklist in the same
`writer_gate.py` (`check_news`) runs on multi-source **synthesis** cards, gated
by `WRITER_GATE_ENABLED`. The two coexist; neither replaced the other.

Where the two disagreed, the code was kept and this spec was updated to match:

- **Body band is 60-80 words, not 60-110.** The synthesis gate enforces a hard
  `LENGTH_BANDS["news"] = (60, 80)`, and G4 reads that same constant so there is
  one source of truth. The earlier 60-110 figure this section carried was an
  inferred draft value; it was corrected down to 60-80 here.

Both gates report to `/admin/writer-doctor`: the synthesis checklist under
`writers` / `phase_gate` (its own rule names), and the G1-G7 gate under the
Section-5 fields (`by_rule` keyed by G1..G7, `by_writer`, `recent_failures`,
over `window_hours`).

---

## Section 5 — /admin/writer-doctor

`GET /admin/writer-doctor?token=<ADMIN_TOKEN>`

Read-only. Returns JSON:

```json
{
  "window_hours": 24,
  "attempted": 0,
  "passed": 0,
  "pass_rate": 0.0,
  "regenerated": 0,
  "fell_back_to_safe": 0,
  "by_rule": {
    "G1_ORIGINALITY": 0,
    "G2_NUMBERS": 0,
    "G3_BANNED_TERMS": 0,
    "G4_LENGTH": 0,
    "G5_WHY_GROUNDED": 0,
    "G6_HEADLINE_ECHO": 0,
    "G7_SHAPE": 0
  },
  "by_writer": {
    "<writer_id>": {"attempted": 0, "passed": 0, "pass_rate": 0.0}
  },
  "recent_failures": [
    {
      "article_id": "",
      "writer_id": "",
      "rule": "",
      "reason": "",
      "attempt": 1,
      "at": ""
    }
  ]
}
```

`recent_failures` holds the last 50, newest first. `window_hours` is
overridable via a query parameter, defaulting to 24.

The endpoint must not trigger generation and must not write to the database.

---

## Appendix — provenance of this spec

Assembled Sept 2026 from decisions already made and recorded elsewhere:

- Section 0 and the Section 1 prompt derive from the objective-news-synthesis
  prompt written earlier and the deconstruct-and-reconstruct legal posture.
- The three-part card structure (NEWS / STRINGS / DOTS) and the
  what-when-where-how-why-who deconstruction come from the product notes.
- `G1` wraps the originality gate already in the codebase; thresholds are
  not changed by this spec.
- The retained banned terms are the existing runtime blocklist.

**Inferred, and worth a second look before relying on them:** the headline
(6-14) and why_it_matters (12-28) word-count bands in 1.2, the 5-word echo
threshold in `G6`, the added banned terms in 4.2, and the attribution
exemption. These were not previously written down anywhere. Adjust them here
rather than in code — EXCEPT the body band, which is now pinned to the code's
existing `LENGTH_BANDS["news"] = (60, 80)` and is adjusted there (see §4.5).
