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

The orphan `sherrbyte_app.insights` left behind in production held exactly 3
rows, all of them known `_SAMPLE_INSIGHTS` signatures, 0 unexpected — verified
before removal, against `public.insights`'s 295 real rows. It is dropped by
`scripts/oneoff_drop_legacy_insights.sql`, which is a **one-time manual
cleanup, deliberately not a migration**: a migration would run forever on fresh
environments where that table never existed, and 022/023 are reserved for the
analog engine.

**An unqualified `insights` in an asyncpg query means `public.insights`,
permanently.** That is intentional. Do not "fix" it by adding a search_path,
and do not reintroduce any table named `insights` in `sherrbyte_app`.

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

### The Phase 3 coverage gate is MET, and there is no `--years` flag

Measured, not assumed. `backfill_ticks.py --days 400` (2026-09-01) returned:

```
18,830 rows | 51 symbols | 2025-01-08 -> 2026-09-01 | 46 symbols_ok
stocks 7,157/18 · energy_stocks 2,800/7 · forex 2,711/7 · metals 1,654/5
commodities 1,653/5 · rates 1,600/4 · crypto 455/5
```

**The "5 years / 25 symbols" gate is cancelled.** The analog engine can only
match against our own news corpus, which is ~368 days deep. Price history
therefore only has to cover about 400 days — 46 symbols already exceed that, so
the gate is met. Depth beyond the corpus buys nothing an analog could use.

**Do not build the `--years` flag.** It was scoped to reach 10 years of prices
for a matcher that can never look further back than the news. `--days` is
sufficient and already exists.

Crypto is the one class that stays short: CoinGecko's keyless public tier caps
history at 365 days, and adding a key or a paid tier is refused (it is on the
non-commercial licensing audit list). Treat thin crypto analogs as a known
limitation, not a bug to solve.

### CoinGecko's public tier: two failure modes, both now handled

A `--days 400` run lost all 11 crypto symbols — 5 with HTTP **401**, 6 with
**429**. Neither was an auth problem:

- **401** is how the public tier refuses a window wider than 365 days. It reads
  as a credential failure and is not one. `coingecko_daily` now clamps to
  `COINGECKO_MAX_DAYS` and logs the clamp per symbol, so a short crypto series
  is never mistaken for missing data.
- **429** is the rate limit. The old 1.5s gap was 40 calls a minute against a
  documented ~5-15 ceiling. The gap is now 6s, with retry and exponential
  backoff that honours `Retry-After`.

The 401/429 split is explained by ordering: the rate-limited requests never
reached range validation.

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
