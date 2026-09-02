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

## The publisher's text lives in `source_summary`, and nothing may overwrite it

Ingest writes `clean[:200]` there. **It is the only copy of the source this
schema keeps**, and two things depend on it:

- `body_state.classify` uses it as the ORIGINALITY REFERENCE. Overwrite it with
  our own summary and the gate compares our body against our own text.
- `source_material()` rewrites FROM it. Overwrite it and a retry has nothing to
  work with, so it regenerates the placeholder forever.

Both AI write paths used to set `source_summary = result["summary"]` under a
comment reading "kept for back-compat". Rows now reported `no_source_material`
are the ones that line destroyed; for those the publisher text is gone and only
re-ingest recovers it.

`tests/test_source_summary_preserved.py` asserts no AI UPDATE names the column,
and drives a full `run_ai_batch` to prove the value survives.

Fixed in the same pass, both found by counting rather than reading:
`/admin/reprocess` read `row` where the loop variable is `r` (NameError every
iteration) and supplied 11 values for 16 placeholders. Both were swallowed by a
bare `except` that logged "update failed", so that endpoint had never updated a
row. It duplicates `/admin/reprocess-bodies` and is a candidate for deletion.

## The rewrite is rate-limited, so it runs continuously and slowly

Gemini's free tier allows 15 requests a minute and **one article is one
request**. The backlog is ~25,000 articles, so the constraint is the request
rate, not compute — and a nightly sweep would need about 35 nights.

`body_drain_job` is therefore an APScheduler **interval** job, not a cron:
`BODY_DRAIN_RPM` (12) articles every `BODY_DRAIN_INTERVAL_S` (60s), ~720 an
hour, so a 25,000-row backlog is roughly 35 hours of unattended running. That is
the honest number; there is no faster path on a free tier that does not break
the terms.

Four properties make it safe to leave running, and each has a test:

- **12, not 15.** The published ceiling is 15/min. The ingest pass, the nightly
  sweep and any manual run share the same quota, and three callers each
  believing they are inside the limit is how a limit gets breached.
- **The tick is the quota window.** A tick takes at most `BODY_DRAIN_RPM`
  articles, so the rate is bounded by the schedule rather than by hoping a
  batch finishes in time. Concurrency is forced to 1 for the same reason —
  five requests in flight is five against the same per-minute quota.
- **No cursor.** The selector asks for rows that are still placeholders, so
  whatever is left IS the state. A restart resumes with nothing to reset.
- **Real backoff.** A 429 is read from `ai_processor.PROVIDER_ERRORS`, not
  inferred from a failure count, and each consecutive one doubles the wait up
  to `BODY_DRAIN_MAX_BACKOFF`. Failures that are not rate limits do not trigger
  it — backing off for those would slow the drain for no reason.

It yields to a manual or nightly run rather than doubling the rate, refuses to
run with no provider configured (that only rewrites the placeholder again), and
`BODY_DRAIN_ENABLED=0` switches it off.

`/admin/body-audit` reports it under `drain`: ticks, cumulative rewritten and
failed, remaining, per_hour, hours_to_clear, and the backoff reason.

## Branching: one branch per unit of work

Adopted 2026-09-02, after work was stranded behind merged PRs three times
(#189, #200, #201). A PR merges at whatever the branch head was; anything
pushed to the same branch afterwards lands behind it and is silently left out.

So: **branch fresh from `main` for each unit of work, push, open the PR, and do
not push to that branch again.** The next unit gets a new branch.

## Tests: two traps this repo has already fallen into

- **Do not `importlib.reload`** a module other tests hold references to. It
  passes in isolation and breaks the suite, because other modules keep the old
  objects. Read configuration at call time instead — that removes the need.
- **Patch the seam the code under test actually calls.** Several test modules
  reload `main`, and under the full suite `main.process_batch is
  ai_processor.process_batch` is False — so patching `ai_processor` never
  reaches what `run_ai_batch` invokes, and the test silently measures the
  fallback. It passed alone and failed only in the suite.

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

### The symbol universe is ~13 instruments, and that is deliberate for now

`linked_symbols` needs a ticker Phase 3 can join to `market_ticks`, and the only
entity→ticker bridge is `instrument_map.SEED` (display names) ∩
`market_signals.INSTRUMENTS`+`CRYPTO` (name→ticker). That intersection is ~13
instruments, not the 57 in `market_ticks`. An article about a mid-cap stock
resolves to entities fine, reaches no priced instrument, and gets no event row.

Widening it is name→ticker data entry, not engineering. It is a **known
post-Phase-3 task**, deliberately deferred: proving the loop end to end on 13
symbols is worth more than debugging a wide pipeline that has never run.

### No simhash, and no near-duplicate pass — clustering supersedes it

Near-duplicates in this corpus are republished versions of one story. The
matcher's 48h same-symbol cluster collapse already removes them: two rows for
the same story land in the same window on the same symbol and only the
higher-scoring one survives. Do not add a simhash column and do not compute one
on the fly — that would be a second mechanism for a problem the first already
solves.

### hist_events.article_id has no foreign key — orphans are expected

`sherrbyte_app.articles` is sqlite-shaped through pgcompat and its lifecycle
belongs to the deployed app, so a cross-schema FK would let an article cleanup
there fail or cascade into engine data. The consequence is accepted: **deleted
articles leave orphan event rows.** No cleanup is built for this. Handle it at
read time if it ever matters.

### Horizon scaling is sqrt(h), and the noise floor is a measured number

    z = r_h / (MAD_1day * sqrt(h))

The sqrt(h) term is not optional. An h-day return accumulates h days of
variance; dividing it by a one-day volatility makes long horizons look violent
for free. Measured on 200 random-walk corpora with no relationship anywhere in
the data, the unscaled version scored **42 at h=10 against 3 at h=1** — forty-two
points of confidence manufactured from nothing. With sqrt(h) the same data
stays at 13.

**The measured noise floor** (`app/spie/analog/calibration.py`, 200 seeds ×
140 events, 2026-09-01):

| horizon | mean | p50 | p95 | p99 | max | NOISE_FLOOR | NOISE_CEILING |
|---|---|---|---|---|---|---|---|
| 1  | 3.3 | 3 | 6  | 6  | 7  | **6**  | 11 |
| 3  | 6.9 | 7 | 11 | 13 | 15 | **11** | 19 |
| 5  | 7.6 | 8 | 12 | 16 | 17 | **12** | 21 |
| 10 | 7.3 | 7 | 13 | 18 | 21 | **13** | 25 |

`NOISE_FLOOR` is p95 — the reader-facing bar, stored NOT NULL on every
`analog_reactions` row so a card can never render its score without it. A card
scoring 11 against a floor of 11 is nothing; 60 against 11 is something.

`NOISE_CEILING` is max + headroom — the CI bar.
`tests/test_calibration_noise_floor.py` runs the null on every CI run and fails
if any horizon climbs back above it. Verified to fire: deleting the sqrt(h)
term makes h=3/5/10 breach at 22/31/46.

Re-derive both with `python -m app.spie.analog.calibration --seeds 200`. If the
generator's parameters change, the published numbers must be re-measured — a
floor that does not match what noise actually reaches is a lie to the reader.

### Phase 1 gates on the SUMMARY, not the body

`build()` requires `classify_summary(...) == ORIGINAL` and does **not** call
`row_is_healthy`. The body requirement was self-imposed: nothing in Phase 1
reads `full_body` except that check. Extraction runs on headline + `summary_60`
— `linked_symbols()`, `_entities_for()` and `classify()` all take exactly those
two — so a healthy body adds no information to any field the library stores,
and demanding one excluded the entire corpus while the bodies were placeholders.

The summary gate stays, and it is the one that matters: an event built on a
placeholder summary would match on our own words rather than the story's.

### `published_at` is TEXT *or* timestamptz — always cast before matching

Migration 018 converts the column, so the same query has to work against both.
`published_at ~ '...'` raises `operator does not exist: timestamp with time
zone ~ unknown` and takes the whole pass down — that is how Phase 1 crashed on
first contact with production. **`published_at::text ~ ...`** is a no-op on TEXT
and always valid on timestamptz.

Two modules carried the bug: `analog/event_library.py` and
`discovery/news_match.py` — so the detector's news matching had been failing the
same way, silently. `main.py`'s backfill is fine: it checks
`information_schema` and returns early once the column is no longer text.

`tests/test_analog_sql_executes.py` prepares every module-level SQL string in
the analog package against a real server on each CI run, which is the only
place this class of bug is visible before runtime.

### Two card types, and why the second never suppresses

`AnalogCard` is aggregate evidence and is often silent: below 5 analogs it does
not exist, and at or below the measured noise floor it is labelled context
rather than evidence. `ObservationCard` is one article, one instrument, one
measured move — no sample floor, no suppression — so the surface is never blank
while the library accumulates.

They are different claims and the wording keeps them apart: an analog says
"this happened before and here is how often"; an observation says only "here is
what happened after this one article". Every generated string is a template,
never a model, and every one passes `narrative.violates_language_rules` before
it can reach a reader — the engine's own blocklist, not a second copy.

### 019_watchlist.sql is NOT a user watchlist

Its columns are `(entity_a, entity_b, kind, score, npmi)` — the emergence
detector's parked entity pairs, connections it saw but judged not novel enough
to publish. There is no `user_id`, so there is no per-user join to make.

`cards.watchlist_symbols()` therefore returns "instruments connected to
something the engine already flagged", which is a real filter but is not
personalisation. Per-user personalisation needs a user→symbol table that does
not exist yet; the endpoint's explicit `symbol=` parameter is the seam it plugs
into.

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
