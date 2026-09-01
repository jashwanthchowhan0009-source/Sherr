# CLAUDE.md

Notes for whoever picks this repo up next — human or agent. Decisions recorded
here are settled; re-deriving them costs a session.

For the Sherr-I engine's build state and funnel diagnostics, see
`PROGRESS_SHERR_I.md`.

---

## Two schemas, two `insights` — read this before writing any query

The deployed app (root `main.py`, started by `render.yaml`) runs its SQL through
`pgcompat` with `search_path=sherrbyte_app`. The SPIE engine (`sherrbyte/app/`,
run from `.github/workflows/cron_detectors.yml`, **never started by Render**)
writes to `public` over asyncpg.

Both talk to the same physical Supabase database.

| Table | Schema | Written by | Read by |
|---|---|---|---|
| `articles` | `sherrbyte_app` | root app ingest | root app |
| `insights`, `entities`, `domain_signals` | `public` | SPIE detectors (cron) | `_spie_patterns` in main.py |
| `demo_insights` | `sherrbyte_app` | root app boot seed | `/patterns` seed tier only |

`demo_insights` was called `insights` until 2026-09-01. That created a second
table of the same name in a different schema, filled with `_SAMPLE_INSIGHTS`
demo rows on every production boot, shadowing the engine's real one. Nothing
read the shadow, so nothing ever failed — it simply waited for someone to join
the wrong table. It is renamed so the name is unambiguous in both directions.

**An unqualified `insights` in an asyncpg query means `public.insights`.** That
is intentional. Do not "fix" it by adding a search_path.

---

## Sherr-I Historical Analog Engine (SHAE) — decisions

### The vector term is dropped from the analog matcher

The Phase 2 matcher ranks on:

```
0.45 * entity_jaccard
0.35 * class_match
0.20 * npmi_strength      # normalised 0-1 against the existing engine threshold
```

**No embedding/pgvector term.** This is not a judgement that vectors are
unwanted — it is that the embeddings currently in the database are not real
ones. `sherrbyte/app/pipeline/embedder.py` loads MiniLM only if
`sentence-transformers` is importable; that package lives in
`requirements-ml.txt`, which the detector cron does not install (it installs
`requirements.txt`). So `info_objects.embedding` is populated by the module's
deterministic **md5 hash fallback**. Cosine similarity over a hash embedding is
lexical collision, not semantic similarity — weighting a matcher 60% on it
would produce plausible-looking analogs that mean nothing, which is the most
expensive kind of wrong.

Revisit only as a later phase, and only after someone **measures** whether real
vectors beat this baseline. Do not scaffold for it now.

### Recency belongs to Phase 3 only

`recency_weight` is a term in the Phase 3 `signal_strength` formula. It is
deliberately **not** a matcher term — having it in both places double-counts it.

### Migration numbers

`021_entity_edges.sql` already exists. SHAE takes:
- `022_event_library.sql`
- `023_analog_reactions.sql`

### The Phase 3 coverage gate is set after measurement, not before

Do not set the "N symbols with ≥5 years" threshold from the detector funnel's
`with_enough_history`. That number comes from `domain_signals` (~10 headline
instruments), not from `market_ticks`, and is not evidence about tick coverage.

Correct sequence: run `scripts/backfill_ticks.py`, run `verify_ticks.py`, then
set the gate from the real per-symbol numbers.

### Out of scope, permanently

FinBERT or any second sentiment model; probability/percentage outputs and price
targets; Granger causality, GARCH, cointegration, intraday lead-lag; any agentic
loop or LLM that reads prices; a separate personalisation engine (it is a
watchlist filter over existing output).

---

## Compliance posture (SEBI)

Detection, not prediction. Past tense and conditional only. The score is
`signal_strength`, an integer 0–100 — never `confidence`, never rendered as a
percentage or a probability. Runtime-blocked words: `will`, `buy`, `sell`,
`predict`, `bullish`, `bearish`, `forecast`, `target price`, `recommend`.

Frequency-of-past-occurrence is the compliant way to say what a probability
would have said: *"in 11 comparable past events, crude moved beyond its normal
daily range within 3 sessions in 8 of them."*

The math decides significance; the LLM only writes prose from a fixed evidence
payload, never sees raw prices, and its output is schema-validated before it can
reach a card. If the math is silent, nothing is rendered. Silence is a valid
output.
