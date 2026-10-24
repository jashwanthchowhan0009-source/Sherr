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
- **Never bind to `asyncio.get_event_loop()`.** On 3.13 it RAISES in the main
  thread once any earlier test has closed the loop, so a test using it passes
  alone and fails in the suite. `asyncio.run` always.
- **Do not locate a thing by its position.** Two tests found a workflow step as
  `steps[-1]` and a function body as "the next 7200 characters"; appending a
  step and growing the function broke both while what they assert stayed true.
  Match on what the step runs and brace-match the function.

## CI runs the suite, and it only ever ran where somebody remembered to

`.github/workflows/tests.yml`, on every pull request and every push to `main`.
Before it, `.github/workflows` held only schedules and the sole check on a PR
was a Vercel preview deploy — which builds the static tree against no backend,
so it could not fail on anything in `main.py`. That is how **six** failures sat
on `main` long enough to be quoted in PR bodies as "pre-existing", and they are
all fixed in the same pass that added this file.

**Python 3.12, and that is load-bearing.** `requirements.txt` pins
`psycopg[binary]==3.2.1`, which has no wheel for 3.13, so a clean install on
3.13 fails outright — the version matches the ingest and detector crons, which
are the proof the shape installs. `tests/test_ci_workflow.py` pins it, along
with the trigger, that the suite runs whole (no `-x`, no `--deselect`) and that
the job takes no secrets, so it can run on a fork.

**Five of the failures were only visible on a clean install**, which is the
argument for the workflow in one line: the system environment here carries
newer packages than `requirements.txt` pins, and under the PINNED set
`scheduler.start()` raised `Event loop is closed` on the second `TestClient`
lifespan in a run. `AsyncIOScheduler` caches the loop it starts on and
`scheduler` is module-level, so the second app instance found the first test's
closed loop. `main.py` now calls `scheduler.configure(event_loop=...)` with the
running loop first; in production, where lifespan runs once, it is a no-op.

**`requirements-dev.txt` is the test-only set** — pytest, PyYAML,
pydantic-settings. They are deliberately not in `requirements.txt`: Render
installs that file to serve the app and has no reason to carry a test runner.

## Provider model ids go stale, and a stale one fails silently

`llama-3.3-70b-versatile` (the root app) and `llama-3.1-8b-instant` (the
engine's `config.py`, both its model and its failover) were BOTH shut down for
Groq's free and developer tiers on **2026-08-16**; `gemini-2.5-flash`, still the
engine's default, retires 2026-10-20. A retired id does not fail loudly: the
call 404s, the cascade drops to the next provider, and the only symptom is a
backlog that stops clearing. The replacements Groq named are now the defaults
(`openai/gpt-oss-120b` for the big model, `openai/gpt-oss-20b` for the small).

`tests/test_feed_doctor.py` carries `GROQ_RETIRED` — a dated set of ids the
provider has shut down — and asserts the configured model is not in it. **Add
to that set, do not rewrite the pin.** The test it replaced asserted one
hardcoded id and the id it asserted was itself retired on the same day as the
one it was guarding against, so it failed for months while agreeing with
nothing.

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
history at 365 days. A free **demo** key (`COINGECKO_CRYPTO_API_KEY`, sent as the
`x-cg-demo-api-key` header) is now used to lift the rate limit — the earlier note
that "adding a key is refused" predated having one, and referred to a *paid* tier
(on the non-commercial licensing audit list). A demo key is free and only raises
the call ceiling; it does NOT extend history, so the 365-day cap and thin crypto
analogs remain a known limitation, not a bug to solve.

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

---

## The rewrite's unit of work is an EVENT, not an article

Adopted 2026-09-03. A single 200-character publisher blurb cannot be turned into
an original 60-80 word body. There is no such transformation: everything added
beyond the blurb is invented and everything kept is a paraphrase — which is
exactly what the originality gate then rejected, run after run, while the rows
sat on the placeholder. The single-article rewrite was not badly prompted, it
was asked to do something impossible.

Several blurbs about one event are solvable. `synthesis.py` clusters the drain's
candidates and `main._synthesise_clusters` sends each cluster to
`ai_processor.synthesize` with the specified prompt, verbatim.

**`MIN_CLUSTER` is 2, not 3.** A feed ingesting a wide spread of publishers
produces mostly 1- and 2-source events; a pass that only fired at 3+ would idle.
Two publishers still corroborate. Singletons never reach synthesis — they fall
back to the single-article rewrite, which is at least honest about being one
source.

**One cluster produces ONE article.** The synthesised body goes to a single
primary row (the member with the most surviving publisher text, because that row
is the originality reference) and every other member is set to `status='merged'`,
which removes it from every served query. Writing the same body to five rows
would put five identical cards in the feed. The merge is reversible in one
statement — `UPDATE articles SET status='published' WHERE status='merged'` — and
`SYNTHESIS_MERGE=0` turns it off.

`synthesis_sources` on the primary is a JSON array of every member id and is
**the only attribution trail there is**: a synthesised body is not traceable to
any publisher by inspection. Merged rows point back via `story_id`.

### Event clustering reads the summary; story threads read the headline

Two publishers covering one event write different headlines deliberately.
"Crude climbs as OPEC+ weighs deeper output cuts" and "Oil advances after OPEC+
signals further restraint" share exactly ONE significant term, so the
headline-only rule at 2 shared terms finds almost no real events — and dropping
it to 1 merges everything that mentions the same company.

`event_terms` therefore reads headline + the publisher's summary (never
`full_body`: on a candidate row that is the placeholder, and every placeholder is
identical). The larger vocabulary is paid for with a second threshold —
`EVENT_MIN_RATIO = 0.28`, shared over the SMALLER term set, so a thin wire item
can still match a long piece about the same event.

`link_stories` keeps the old headline-only rule and now calls
`synthesis.cluster_articles` rather than carrying a second copy of the union-find.
A false link there costs a wrong "related" card; a false link in synthesis
removes a row from the feed, which is why the two are tuned differently.

### The clustering pool is NOT the request budget

First production run: `clusters_seen 10`, `size_histogram {"1": 10}`, nothing
written. The threshold was not the cause.

The drain's tick is bounded by the free tier's REQUEST rate — 12 articles a
minute — and the first version clustered exactly those 12 rows. `SELECT_NEEDING_REWRITE`
orders by `published_at DESC`, so those were twelve CONSECUTIVE articles: about
seven minutes of a feed ingesting ~100/hour, against a 24-hour clustering
window. Two publishers covering one event are routinely an hour apart. The
clusterer was asked to find day-window pairs inside a seven-minute sample and
correctly answered that there were none.

**Lowering `EVENT_MIN_RATIO` would have "fixed" it by merging unrelated
stories** — strictly worse than producing nothing, because a merge takes rows
out of the feed.

`SYNTHESIS_POOL` (400) is fetched separately and clustered; the request budget
still governs how many clusters are written, and the leftovers handed to the
single-article path are trimmed by what the clusters spent, so a tick can never
exceed its rate. Clustering 400 rows costs one extra SELECT a minute and no
provider calls. Measured on a simulated 24h corpus of 60 events (40
multi-source): pool 12 found 3, pool 200 found all 40, none impure.

### Two hazards a large pool introduces, both guarded

- **`MAX_TERM_DOC_FRACTION = 0.10`.** The generic-term cap has to scale with the
  sample. A flat 40 was written for a few dozen rows; against a pool of 50, a
  term in 40 of them passed as "specific". Publisher boilerplate — "the company
  said", "on Monday" — then chains unrelated rows through union-find. Measured:
  with a flat cap, a 115-row boilerplate-heavy pool collapsed into a **single
  115-member cluster**.
- **`MAX_EVENT_SIZE = 8`.** Union-find is transitive, so a~b, b~c, c~d joins all
  four even where a and d share nothing. A cluster past this size is a
  clustering failure, and it is **refused — broken back into singletons — never
  truncated to the first five**: truncating would merge four arbitrary rows out
  of the feed and leave the rest.

### The diagnosis is instrumentation, not archaeology

"No clusters" has four causes with four different fixes. `cluster_articles`
records which gate stopped every pair (`shared_below_min`, `ratio_below_min`,
`different_pillar`, `outside_window`, `joined`), the ratio and shared-term
histograms, and the closest near-misses **with their headlines** — the one
question a histogram cannot answer is "are these actually the same story".
`pool_report` adds the sample's span against the window, which is what
distinguishes a threshold problem from a sample problem.

All of it is under `synthesis` in `/admin/body-audit`, with a one-line
`DIAGNOSIS`. `scripts/cluster_report.py --pool N` reproduces it against the
database (`--pool 12` shows what a pre-fix tick saw).

**Synthesis spends FEWER requests, not more.** A cluster of five is one provider
call where the single-article path was five, so the drain's per-tick rate ceiling
is still honoured by construction. `/admin/body-audit` reports it under
`synthesis`, and `articles_per_request` is the number that says whether
clustering is earning its place — 1.0 means every "cluster" was a singleton.

### The News writer's quality gate (writing spec, Phase 1)

`writer_gate.py` is the checklist the writing spec runs before a News card is
published: banned tone, verified numbers, an informative opening, the length band,
no open loop, at least one entity. It is **pure** — no DB, no model, no network —
so it is cheap to run on every card and cannot fail open.

Decisions worth not re-deriving:

- **One compliance blocklist, still.** The SEBI / forward-looking words are the
  engine's `narrative.violates_language_rules`, INJECTED as `sebi_check`, never
  re-listed. What `writer_gate` adds are the spec's three NEW tone categories the
  engine list does not cover — throat-clearing, empty intensifiers (banned only
  when there is no number in the same clause), and the `there was … by …` passive
  nominalisation.
- **Numbers are verified against `numbers_used` OR the source.** The spec's hook is
  the model self-reporting each figure (now emitted by synthesis, alongside
  `fact_conflicts`). A figure that also appears in the source text is accepted even
  if the model forgot to list it — otherwise a model under-populating a brand-new
  field would stall every numeric story on the rate-limited drain, which is worse
  than the hallucination the rule guards against.
- **Overlap has one owner.** `_synthesise_clusters` already runs
  `originality_check` to produce the metrics stored on the row, so the gate does
  NOT recompute the 7-gram overlap — it is passed `overlap_passed=True` and the
  existing rejection records the same `ngram_overlap` rule to the writer-doctor.
- **The gate is always MEASURED; enforcement has a switch.** `check_news` runs on
  every synthesised card and its verdict is always recorded, so the News **pass
  rate** is a number. `WRITER_GATE_ENABLED` (default 1, the spec's posture) decides
  whether a failing card is WITHHELD — it keeps its safe placeholder for the next
  tick, never blanked. Set it to 0 to measure the pass rate on live traffic before
  switching enforcement on. A withheld card is the spec's "regenerate once, then
  fall back to the safe summary": the next drain tick is the regeneration.
- **`/admin/writer-doctor`** buckets every failure by writer and rule, most-fought
  first, with a sample of the offending text. Its `phase_gate` reads the spec's
  rule: **do not build Strings or Dots until News passes at ≥90%** on the live
  corpus. That is the one number that decision turns on — read it there, do not
  guess it.

The News length band is a **hard 60–80 words** (Strings 120–180, Dots 130–200). A
synthesis that lands outside it is withheld under enforcement rather than trimmed —
the writer-doctor's `length` bucket is where you see the prompt needs tuning, not
the gate.

---

## myFeed is the old Bytes tab, renamed and rebuilt into a dossier surface

The bottom nav is Home · myFeed · Explore · Profile. "Bytes" was renamed to
myFeed and moved to slot 2 (Explore fell to slot 3). This was a **complete**
rename in `index.html`: the route/state key `bytes`, the view id `v-bytes`, the
nav id `nb-bytes`, the feed container, the `.byte*`/`.bytes-*` CSS namespace
(now `.mf-*` / `.myfeed-*`), the JS identifiers, and every visible label. The
brand (`SherrByte`, the "Article Byte" share card, `Sherr<span>Byte</span>`) was
deliberately left untouched — `byte` is a substring of the brand, so the rename
was done by explicit token, never a blanket `byte`→ replacement.

**`/bytes/<slug>` 301-redirects to `/myfeed/<slug>`** (and `/bytes` → `/myfeed`)
so links shared before the rename still resolve; the canonical route the app,
the sitemap and the og:url all write is `/myfeed`. The client also accepts a
legacy `/bytes/<slug>` on boot in case a redirect is ever missed.

The dossier is rendered **INLINE ON EACH CARD**, not behind a tap (the earlier
tap-to-open overlay was removed). The card is **one vertical scroll** (`.mf-card`
is `overflow-y:auto`), Inshorts-style, top→bottom: a large **image** with the
**segmented tab row** (labelled exactly **News · Strings · Dots**) and the brand
**logo** overlaid on its lower edge; then the **headline + `SB-<id>` chip** (which
scroll WITH the content, not pinned); then the active pane; then a **sticky
action bar** (an "Add a comment" pill + heart / bookmark / share) glued to the
card's bottom. Reading scrolls the image up out of view rather than keeping it
fixed. Only the active pane shows (`.mfd-pane.on`, display-toggle with a slide-in);
`mfBindCard` switches it on tab tap or a **sideways swipe on the text**, while a
vertical gesture is left to the card's own scroll (axis decided per-gesture).
myFeed also **hides the global `#hdr`** (`body[data-view="myfeed"]`, set in
`navTo`) to reclaim the top, and the satellite FAB `#nb-scan` is hidden app-wide
for now. The tab-row logo is the White Tiger mark served at `/tiger-logo.png`
(`SHERR_LOGO_URL`); the action icons are inline SVG (no Font-Awesome dependency).
Fonts: body **Inter**, headline **Poppins** (`--f`/`--fd`), sized down to
Inshorts scale (headline ~1.18rem, body ~0.95rem). Per-card lazy hydration: an
`IntersectionObserver` (`myfeedObserver`) fetches `/article/<id>/dossier` the
first time a card nears the viewport, so the deck stays cheap (one fetch per card
actually seen). `buildMyfeedCard` builds the card; `renderStringsPane` /
`renderDotsPane` fill the panes (each returns the inner content, and hydrate
swaps it into `.mfd-pane.<name> .mfd-scroll`).
- **News** — the lead paragraph (TL;DR/hook) led by a sector "colour box" badge,
  then optional "What happened" and "Who & Where" (chips + when/where). Built from
  The Node, which is assembled from columns the row already carries. It ALWAYS
  renders.
- **Strings** — a **VERTICAL** timeline (`.mfd-tl`): dots connected top→bottom, a
  stage label + title + description per node, matching the design sketch. It is
  **seeded with a truthful "Present" node** built from the article itself so the
  thread is never blank; synthesis adds prior/later milestones. (This reverses the
  earlier horizontal-`.mfd-timeline-h` decision — the design PDF the owner
  supplied on 2026-09-16 draws it vertical, which is now the spec.)
- **Dots** — labelled rows "Dot: Markets / Governance / Tech/Social" plus a
  closing "The Catch", each with a red/green/amber impact pill and live tickers
  where an instrument is named (read from Explore's `window._lastMkt`). Ripples
  are only shown when synthesis wrote them — never fabricated; the empty state is a
  compact on-brand `.mfd-empty` card, not a blank slate.
Sector chips are colourful ticket-shaped badges (`.mf-badge`, per-sector colour
via `--bd`) — the ONE place colour lives; the rest of the card stays near-
monochrome (Twitter-minimal), leaning on space and curvature. `openDossier`
survives as the deep-link entry (`/myfeed/<slug>`): it prepends that story as the
first card rather than opening an overlay.

### The minimized nav pill is the only way out of myFeed

Adopted 2026-10-17. Scrolling the deck compacts the bottom nav, and
`nav.compact .nb:not(.on)` gives every tab but the active one zero width and
`pointer-events: none` — so on myFeed the minimized pill is the ONLY tappable
control on the screen, and a tap on it is the reader's single route back to the
full bar and from there to any other tab. **That tap was dead, which made a
scrolled myFeed a navigation dead end.** Two causes, both fixed:

- myFeed's scroll handler sets `compact` from `scrollTop` alone, where every
  other section's handler compacts on a DOWNWARD scroll. So anything that
  expanded the bar mid-deck was undone by the next scroll event — the settle
  events the tap itself produces included — and the bar re-collapsed within a
  frame. `_mfNavHold` records where the deck was when the reader asked for the
  bar, survives that settle, and clears once they move `MF_NAV_HOLD_SLACK` (24px)
  away. It is a DISTANCE, not a timer: a timer either outlives a slow reader or
  expires during the settle it exists to absorb.
- The re-tap branch called `loadMyfeed(true)` unconditionally, refetching and
  re-rendering the deck out from under the reader. Home has carried the
  `_reTap && _wasCompact` → inert rule for exactly this; myFeed now does too,
  and the refresh stays on a tap of the FULL bar, which is what it was for.

Not a regression from the laptop column — reproduced identically at 390x844 and
1440x900. `tests/test_myfeed_nav_pill.py` pins all of it, including the
`pointer-events: none` rule the rest of it rests on.

`GET /article/<id>/dossier` returns node + strings + dots in one payload, cached
through `cache.py` at `DOSSIER_CACHE_SECONDS` (raised to 600s) and it **serves
stale on a DB error**, which is what stops `/myfeed` hanging on
"Loading dossier…" when Postgres is refusing connections.

**The degraded path is the normal case, not an edge case.** Most rows have never
been through synthesis, so `strings`/`dots` are the `'[]'`/`'{}'` defaults; the
endpoint marks those panes `pending` and the surface renders The Node anyway.
Malformed JSON in either column degrades to pending too — it never 500s.

`strings` and `dots` are `TEXT` columns holding JSON, NOT a `jsonb` type. That
is deliberate and matches this schema's existing structured fields
(`who_affected`, `synthesis_sources`, `originality_json`): JSON-in-TEXT is the
one shape that works unchanged over both pgcompat/Postgres and the local sqlite
backend. Both are written ONLY by the synthesis pass, alongside the News node,
and both are emptyable. The synthesis prompt/schema/parser emit them under the
same compliance blocklist as the hook — a timeline step or a sector whose prose
trips the banlist is DROPPED, never raised, so a bad pane never costs the body.

---

## Staying up under a blown Supabase quota (egress, size, cache)

The database blew its free-tier caps (egress ~487%, size ~248%) and started
returning 402. Four things address it; the code half is landed, the DB half is
scripted for you to run.

### The egress leak was embeddings SELECTed into Python

`info_objects.embedding` is `vector(384)` (~1.5 KB/row). Three paths pulled it
into the app and compared there instead of in the database:

- `recommender/hybrid_scorer.score_feed` selected `embedding` for **all** up to
  `CANDIDATE_LIMIT` (500) rows on **every** feed call, then MMR did cosine in
  numpy. It now scores WITHOUT embeddings and fetches vectors only for the top
  `EMBED_POOL` (150) by relevance, in one `= ANY($1::uuid[])` query, degrading to
  score-only order if that fetch fails. ~70% fewer embedding rows per call.
- `pipeline/connector.connect` and `sherr/rag.retrieve_consensus` fetched the
  subject vector then shipped it back as a bind parameter. Both now reference it
  by id through an in-DB subquery (`(SELECT embedding FROM info_objects WHERE
  id=$1)`) — the operand never leaves Postgres, matching the `<=>` operator that
  already ran there.

**Rule: never SELECT an embedding column into application code.** Compare in the
database, or reference the vector by id in a subquery.

### The read cache raises TTLs and serves stale on a DB error

`cache.get_or_set_stale(key, ttl, producer, stale_ttl=86400)` stores a long-lived
last-good copy under `stale:<key>` and, when the producer RAISES (a 402/5xx
reaching through), returns that instead of propagating. This is NOT caching an
error (still forbidden) — it serves the last SUCCESSFUL answer so the site stays
readable while the DB is down. Wired on `/explore`, `/explore/pillars`,
`/explore/snapshot`, `/patterns` (which returns an `unavailable` payload rather
than raising, so it serves stale on that) and the dossier. TTLs raised to 600s
(`PATTERNS_/EXPLORE_/DOSSIER_CACHE_SECONDS`) while restricted.

### RSS ingestion: a semaphore, not a batch, plus DNS backoff

`collect_rss` caps concurrency with an `asyncio.Semaphore(RSS_CONCURRENCY=10)` —
10 fetches in flight continuously, which is what actually bounds the DNS pressure
that made Render's resolver fail ("No address associated with hostname").
`_get_with_dns_backoff` retries ONLY connect/DNS-class errors (`RSS_DNS_RETRIES`,
1s→2s→4s), never a real HTTP status. NASA APOD now needs a real `NASA_API_KEY`
and is skipped cleanly when unset (DEMO_KEY dropped) — matching FRED and
data.gov.in, which already raised-to-skip.

### The DB size half is scripted, not run from here

- `scripts/db_cleanup.py` — **dry-run by default**: measures sizes and prints how
  many rows each step would touch. `--apply` prunes `{schema}.articles` +
  `public.info_objects` older than `--days` (60), rolls the engine's bigger
  `public.articles` to `--public-articles-days` (30), nulls embeddings older than
  `--embedding-days` (90), then `VACUUM FULL`s to return space to the OS.
- `scripts/migrate_to_neon.py` — **prints the plan by default**, `--run` executes:
  `pg_dump` the `sherrbyte_app` + `public` schemas → restore into Neon → enable
  pgvector → rebuild the HNSW indexes. Run `db_cleanup --apply` first. The env
  swaps (Render `DATABASE_URL`/`SHERR_I_DATABASE_URL`, the GitHub Actions
  `DATABASE_URL` secret) are documented in the script header; no code changes.

---

## Card imagery is dynamic Pexels stock now, not the publisher's image

`IMAGE_MODE` defaults to **`stock`** (was `thumbnail`). Hotlinking the
publisher's own image is bandwidth theft and a copyright exposure — a publisher
can block it or turn on hotlink protection — so cards serve licensed Pexels
imagery keyed on each article's subject instead. Headlines stay as ingested; a
`Source:` credit carries the attribution. (Q chosen by the product owner: the
risk is the image, not the text.)

**`image_service.resolve_image` was DEAD CODE** — it existed but nothing called
it, so `stock` mode never did anything and a card could only ever show the
publisher image or the generated-art placeholder. `main._apply_stock_images`
now wires it into every card endpoint (`/feed`, `/explore`, `/explore/pillars`,
`/trending`, `/search`, `/bookmarks`, `/article/<id>`): it resolves each
article's image from Pexels (cached BY QUERY inside image_service, so shared
subjects cost one call), replaces any publisher hotlink, and on a miss leaves
`image_url` empty so the client shows art — imagery never blocks a response.

Two guards keep it safe:
- **No-op without `PEXELS_API_KEY`.** A mis-set `stock` mode with no key must not
  blank every card, so `_apply_stock_images` returns early and leaves whatever
  `article_row_to_dict` set.
- **Our own hosted images are kept** (`image_source == 'own'`); only publisher
  hotlinks / empties are resolved to stock.

To make it live: set `IMAGE_MODE=stock` (now the render.yaml default) **and**
`PEXELS_API_KEY` on Render. Without the key, stock falls back to generated art,
never a publisher hotlink. The frontend already renders `stock` (Pexels) hosts
with attribution via `imagePlan()`; no client change was needed.

---

## The client session: one generation counter, one way out

Adopted 2026-09-25, after "log out doesn't log me out". Three confirmed bugs,
all in the same place, all invisible to a string-matching test:

- **`signOut()` wrote the credentials back on its way out.** It called
  `switchUser('anon')`, and `switchUser`'s first act was `persist()` — which
  serialised the still-populated `ST` into the partition being LEFT. A 180-day
  refresh token therefore survived every logout under `sb21:u:<uid>`.
  `switchUser` no longer flushes at all: **the caller persists what should
  survive, before switching.** That is the whole fix and it must stay that way.
- **A refresh already in flight landed after the logout.** `tryRefresh` wrote
  into whatever `ST` was current, and `switchUser` REASSIGNS `ST`, so the answer
  went into the anonymous state: signed out, then silently signed back in.
- **A rejected refresh token was never cleared.** `!res.ok` returned false and
  left `ST.refresh` in place, so the client re-offered a token the server would
  never accept, forever, and never dropped to sign-in.

`_authGen` is the fix for the whole class. Every transition — a sign-in attempt,
a logout — bumps it; anything async carries the value it started under and
discards itself if that value has moved. `api()` pairs it with a token snapshot
so a response whose credentials are no longer active is thrown to the caller as
`e.sessionChanged` rather than handed over to be written into the new state. A
plain failed login therefore does not poison in-flight reads (gen moved, token
did not), which is why the check is `gen changed AND token changed`, not either.

**A network error and a 401 are not the same answer.** Only a 401/403 FROM
`/auth/refresh` clears the session; a timeout, a 5xx or a cold Render instance
leaves it alone. Getting this backwards logs everyone out every cold start.

`renderAuthState()` is the only thing that decides what the sidebar, the auth
modal and the onboarding screen show. It used to be set ad hoc — `updateSidebarAuth`
had only a signed-OUT branch, so the sidebar header kept opening the sign-in
modal after a successful login, and the previous reader's name/avatar/bio/stats
stayed painted behind the logged-out screen. `showSignIn:true` is what reveals
the onboarding screen; a routine repaint must NOT, or it ambushes guests who
chose "browse without an account".

### Revocation is `users.token_version`, and the check fails OPEN

Built 2026-09-25, the follow-up the entry above asked for. `make_token`/
`make_refresh_token` are stateless HMACs, so before this, logout could only
delete the client's copy: the access token stayed valid 30 days and the refresh
token 180, on every device. Now every token carries `"v"`, the version it was
minted under, `POST /auth/logout` bumps the column, and `verify_token` rejects
anything minted under an older one — so one logout ends the session everywhere.
The wording is "Log Out All Devices" again because it is finally true.

Three properties, each with a test in `tests/test_token_revocation.py`:

- **A missing `"v"` reads as 0, which is the column default.** Tokens minted
  before the column existed keep working, so the deploy that adds revocation
  does not sign the entire userbase out. Do not "tighten" this into requiring
  the claim.
- **An unreadable database FAILS OPEN.** `_token_version` returns None when the
  read raises and `verify_token` then accepts. Failing closed would log every
  reader out the moment Supabase refused a connection — a far worse outage than
  an unrevoked token living out its expiry, and the same trap `bootstrapSession`
  already avoids on the client.
- **The version is cached per account for `TOKEN_VERSION_TTL_S` (60s).**
  `verify_token` runs on every authenticated request and the pooler caps
  connections in the low tens (see the read-cache note), so an uncached read per
  request is the connection-exhaustion failure mode, not a safety improvement.
  The logout bump writes the new value straight into that cache, so revocation
  is immediate on the instance that served it. **With more than one instance and
  no shared cache, another instance can still accept a revoked token for up to
  the TTL** — that is the known bound; move the cache into `cache.py`'s Redis
  layer before scaling out, not after.

`auth_pair` reads the version **fresh** — minting under a cached, pre-logout
value hands back a pair the next request rejects.

The client fires `revokeSessionOnServer(ST.token)` as the FIRST step of
`signOut()`, because it is the only step that needs the token. It is never
awaited and its failure changes nothing: an offline logout still clears the
device, it just cannot revoke. A logout caused by a *rejected* refresh skips it
— that token is already dead server-side.

`tests/test_auth_session.py` slices the `SESSION CORE` / `SESSION ENTRY` blocks
out of index.html and runs them under node against a fake localStorage and
fetch — the real code, not a copy — so the races above are actually executed
(logout during a refresh, logout during a request, a superseded login response,
two 401s sharing one refresh, reload after logout as a second process sharing
one on-disk store). If you move code out of those markers, it stops being
tested. `scripts/verify_auth_browser.py` is the manual other half: the same
lifecycle in Chromium against a stubbed backend, for the DOM wiring.

## Real URLs, and the one route that must stay last

The app had a single URL: every screen lived behind a JS view switch, so a
reader could not link to a story, a refresh lost their place, and a shared link
previewed as the generic app card because **unfurlers do not run JavaScript**.

Both halves are required and they are in different files:

- `main.py` serves index.html at `/`, `/explore`, `/bytes`, `/bytes/<slug>` and
  `/profile`, and injects the og:/twitter: block **immediately after `<head>`** —
  a scraper takes the FIRST value it finds for a property, and index.html already
  carries a generic `og:title`, so appending would be silently ignored.
- `index.html`'s `routeFromPath` / `pathForView` / `articleSlug` push the same
  paths and resolve a deep link on boot.

`tests/test_desktop_layout.py` compares the two sides, because nothing else does.

**The catch-all `/{full_path:path}` is registered LAST and nothing may go below
it.** FastAPI matches in registration order, so a catch-all above `/feed` returns
560KB of HTML with a 200 and every client reports a JSON parse error instead of a
404. It also refuses `/api`, `/admin`, `/docs` outright — one belt for today's
routes, one for the route somebody adds in the wrong place.

`/explore`, `/feed`, `/search` and `/bookmarks` are **both** an API endpoint and
a linkable screen. The API came first and the shipped frontend calls those exact
paths, so they answer by content negotiation: `text/html` listed explicitly in
`Accept` (a browser navigating) gets the app, anything else (fetch's `*/*`) gets
JSON. The check must never match a wildcard.

`article_slug` puts the **id last and that is what resolves the row** — the words
in front are for readers and crawlers, so a headline rewritten by a later
synthesis pass never breaks a link already shared.

## The read cache is two layers, and the local one is not optional

`cache.py`. Supabase's pooler and Render's free tier cap connections in the low
tens. A feed that queries once per reader does not degrade under load — it
exhausts the pool and every request fails at once, including the admin endpoints
you would use to find out why.

- Redis (`UPSTASH_REDIS_URL` / `REDIS_URL`) is the shared layer.
- A per-process TTL dict is the second, and it is the one that saves the database
  **when Redis has just gone down** — which is exactly when a stampede arrives. A
  cache whose only layer is remote fails open onto the thing it protects.

`/feed` 30s, `/patterns` 60s, `/api/sherr-i/analogs` 120s, rendered og heads and
the sitemap 600s. A producer failure is never cached: storing an error under a
30-second TTL turns one bad query into thirty seconds of wrong answers. Neither
is a `source: "unavailable"` payload — that is a transient reachability
condition, and a cached one outlives the outage.

Pools are capped: `PG_POOL_MAX` 8 (pgcompat), `SPIE_POOL_MAX` 4 (asyncpg).

## A laptop shows the Android app, not a desktop website

Adopted 2026-10-17. The 1024px block used to build a desktop site out of the
same DOM: the bottom nav became a left sidebar, the reading column widened to
720px, Home became a two- and three-column portal and myFeed lost its
full-screen snap for a card grid. The owner's line is that **the Android app is
the product and there is no desktop look to design** — and `sherrbyte.vercel.app`
is also the TWA's origin, so a browser and the app should show the same screen.

So the laptop layout is now: cap the app at the phone's own width, centre it,
darken what is left. The whole `@media (min-width: 1024px)` block is about ten
rules.

**`--appw` (440px) is the app column and `--deskbg` is what surrounds it.**
`--sbw`/`--col` are gone with the sidebar. Every theme that redefines `--bg`
redefines `--deskbg` too, or the backdrop paints white behind a dark app.

**Nothing inside `.phone` needs a desktop rule, and that is structural.** The
500px rule gives `.phone` `transform: translateX(-50%)`, and a transform makes
an element the containing block for its `position: fixed` descendants — so the
header, the nav, every overlay and every sheet inside `.phone` already resolve
against the column at every width above 500px. The three overlays that sit
outside `.phone` (`#xp-page`, `#xp-sheet`, `#cmt-sheet`) are the only things the
laptop places by hand, and they are centred with `margin-left`, **never a
transform**: both sheets animate their own `translateY`, and the open state
would overwrite a horizontal transform and send the sheet back to the screen
edge.

**The 500px rule was also miscentering the column.** It kept the base rule's
`right: 0`, so the box could only be laid out in the half of the viewport right
of `left: 50%` and `margin: 0 auto` centred it inside THAT half — 140px right of
centre on a 1440px window. Nobody saw it because the desktop block overrode the
rule entirely. It now carries `right: auto` and an explicit width.

Still one breakpoint, and still the whole guarantee: every laptop rule lives in
that one block, so a laptop fix cannot reach a phone.
`tests/test_desktop_layout.py` asserts both halves — that the block frames the
app (no `grid-template-columns`, no `flex-direction: column`, no
`scroll-snap-type`, under a dozen rules) and that the phone's own rules are
untouched. The phone render was verified byte-identical before and after at
390x844.

1024px, not 768px: below it the 500px rule already gives a tablet the same
centred column with no border and no backdrop, which is right at that size.

## Auth identity is normalised, and every auth route the client calls exists

Adopted 2026-09-27. Emails are stored `strip().lower()` and looked up with
`LOWER(email)=?`, so accounts created before normalisation still sign in (and if
two legacy rows differ only by case, the one whose password matches wins). Phone
keyboards capitalise the first letter of a field — exact-match lookup was the
most common "can't log in" cause.

`/auth/check-username`, `/forgot-password` and `/reset-password` were called by
index.html but never existed. Reset codes are 6 digits, stored only as an HMAC,
expire after `RESET_CODE_TTL_MIN` (15) and die after 5 wrong guesses. Email goes
over the Resend HTTPS API because Render's free tier blocks outbound SMTP; with
no provider, prod answers 503 and dev returns `debug_otp`.

## The personalised feed is replaced, never accumulated

`compute_feed_for_user` DELETEs the reader's `feeds` rows before inserting. On
Postgres `INSERT OR REPLACE` is translated to `ON CONFLICT DO NOTHING`, so a
score written once was frozen forever — week-old stories kept the recency they
had when fresh and outranked today's for every signed-in reader.

## The news cycle needs an outside clock

A free Render instance sleeps and runs no APScheduler jobs while asleep.
`/cron/collect?token=<CRON_TOKEN>` (or ADMIN_TOKEN) wakes it and starts a cycle
unless one started within `COLLECT_MIN_GAP_MIN`; `collect_news` holds a lock so
the scheduler and the trigger never overlap. `/status/freshness` (public) reports
the newest servable story's age — the number that answers "is news updating".

## The rewrite READS the article first (article_reader), then writes WWWH

Adopted 2026-09-27. The blurb-only rewrite could never produce an original body
(see "The rewrite's unit of work is an EVENT"), so readers kept seeing the
placeholder. Both AI paths (`run_ai_batch` and the drain) now fetch the
publisher's page at `url` and extract the article prose (`article_reader.py`:
JSON-LD `articleBody` first, else `<article>` paragraphs, boilerplate dropped).

- **Input only, never stored.** The fetched text is not written to any column
  and never served — copyright line and Supabase-quota line in one.
- **It is the originality reference.** The gate compares our body against the
  full article, so a sentence lifted from paragraph six is still rejected.
- **Full text ⇒ the WWWH prompt** (`ai_processor.SYSTEM_INSTRUCTION_FULL`, from
  the owner's sbb.pdf): extract what happened (subject→object), where & when,
  why, how, then write a 110-170 word body from that skeleton with a factual
  human hook. Fewer than `FULL_TEXT_MIN_WORDS` (120) ⇒ the old blurb prompt.
  what/why/how/who are written with `COALESCE(NULLIF(?,''), col)` so an empty
  answer never blanks what ingest had.
- **Read rows skip synthesis.** Only blurb-only rows are clustered; they get the
  budget left after the read rows, so a tick still cannot exceed its rate.
- **Rewrite-on-open.** `/article/<id>` and `/article/<id>/full` queue a placeholder
  story for an immediate read-and-rewrite (`ONDEMAND_REWRITES_PER_MIN`, default
  4, one in flight per id) and return `rewrite_pending`; the client polls `/full`
  and swaps the body in. `/full` never returns the placeholder as a body.
- `ARTICLE_READER_ENABLED=0` turns reading off (back to blurb-only).

## Significance decides what gets written and what reaches myFeed

Adopted 2026-10-08. Ingest brings ~1,700 articles a day; the free-tier writer
covers a few hundred. The drain took them newest-first, so the budget went to
city crime, match reports, celebrity casting and gaming guides, and in one 48h
window **3,093 of 3,429 published cards were publish_pending aggregator rows** —
publisher headline kept, placeholder body, no Strings/Dots, the headline echoed
in every pane. 1,973 of those still carried the publisher's prose in
`summary_60`, which is what the card renders.

`significance.py` scores each row 0–100 at ingest (`articles.significance`,
-1 = unscored): source tier + topic (geopolitics, macro, markets,
energy/commodities, AI/tech, space/science, climate policy) + big entities −
junk (crime/accidents/local civic, celebrity, sport results, stock-tip spam,
gaming, product launches). SEBI/exchange micro-filings are capped at 20 — they
belong to the filings pipeline, not myFeed. Pure, no DB/model, and explained:
`/admin/significance` shows the distribution, per-source pass counts and
samples with reasons. Calibrated on 450 real rows; the cases are pinned in
`tests/test_significance.py`.

- **Writer budget:** `SELECT_NEEDING_REWRITE` skips rows scored below
  `SIG_REWRITE_MIN` (defaults to `SIG_FEED_MIN`, 42); unscored rows queue after
  every significant one. Low scorers stay in Explore under the aggregator posture.
- **myFeed (`/feed`) serves WRITTEN + SIGNIFICANT only**: our headline differs
  from the publisher's, the summary is not a placeholder, score ≥ `SIG_FEED_MIN`.
  `_myfeed_rows` relaxes (written-only, then everything) only when a page would
  come back with fewer than 5 rows, so a writer outage degrades the deck rather
  than blanking it. Explore is untouched.
- **publish_pending now replaces `summary_60` too** in aggregator mode;
  `source_summary` keeps the reference copy, `--mode force` leaves it alone.
- **The dossier never echoes the headline:** `node.what` is blanked when it is
  the headline, `node.where` when it is "Not specified"; the client's
  `mfSummaryOf` / `mfSameText` drop the same echoes from the lead, Key Points and
  the Strings "Present" node, which shows the empty state instead.
- `significance_backfill_job` scores old rows `SIG_BACKFILL_BATCH` (800) every
  `SIG_BACKFILL_INTERVAL_S` (180s), newest first, no cursor.

Tune the weights in `significance.py`, not in SQL, and re-run the test file:
the pinned headlines are the contract for what "important" means here.
---

## Account deletion was a button that deleted nothing

Adopted 2026-10-10, found while auditing for a Play Store submission.

`index.html` has shipped a Delete Account row, a type-DELETE-to-confirm modal and
an "Account deleted" toast since launch. It called `POST /account/delete`. **That
route did not exist.** The 404 landed in `catch(e){}`, the toast fired anyway,
and the email, password hash, bookmarks, comments and push tokens all stayed in
the database. The reader was told their data was gone while it was not.

Three things were wrong and all three are fixed:

- **The endpoint now exists** and deletes every user-owned table —
  `_USER_OWNED_TABLES` — before `users`. That order matters: the child tables
  identify their owner by `users.id`, so removing the parent first would strand
  rows that can no longer be matched to anyone, undeletable by this endpoint or
  any other.
- **The client no longer reports success it did not get.** A failed delete keeps
  the session and shows the real error; only a confirmed delete clears the
  device. The bare `catch` is exactly why a missing route went unnoticed for
  this long.
- **`_token_version` had to learn the difference between "row missing" and
  "cannot read".** It returned 0 for an absent row, which is the value most
  tokens are minted under, so every token a deleted account still held kept
  verifying — the account was gone and the session was not. A missing row now
  returns `_TOKEN_VERSION_DELETED` (-1, which no real `"v"` can equal). A read
  that *raises* still returns None and still FAILS OPEN; that property is
  unchanged and `tests/test_account_deletion.py` asserts both halves in one test
  so they cannot be collapsed into each other.

There is no `uid == 1` guard. An early version had one, on the theory that uid 1
is the anonymous system account — it is not. `get_current_user` returns a bare
`1` for anonymous readers but nothing ever seeds a `users` row with that id, so
uid 1 is simply whoever signed up first, and the guard would have stranded that
one real person with no way to delete their account.

## The Play Store surface: three URLs, each failing differently

`PLAY_STORE.md` is the submission runbook. The code half:

- **`/.well-known/assetlinks.json` 404'd on Render.** The file was committed and
  `vercel.json` set a header for it, but no route served it — `.well-known` is
  neither a static file nor an SPA route, so the catch-all 404'd it. Served
  explicitly now, above the catch-all. **But the origin that has to serve it is
  the Vercel one**: the APK opens `https://sherrbyte.vercel.app` (read out of
  its `resources.arsc`), where the file is static and `vercel.json` already sets
  its type. The Render route is correct to have and is not what the TWA
  verifies against. Without a verifying copy on the APK's own origin, Digital
  Asset Links fails and the TWA renders **with a browser address bar**, which is
  the "website in a shell" Play rejects.
- **The fingerprint in that file is the UPLOAD key, not the release key.** Play
  App Signing re-signs every upload, so the installed app presents a different
  certificate and verification fails silently — build, upload and install all
  succeed. After the first upload, add the SHA-256 from Play Console → App
  signing → *App signing key certificate*, keeping the upload fingerprint
  alongside it so local installs keep verifying.
- **`/privacy`, `/terms` and `/delete-account` did not exist.** Play's reviewer
  loads the privacy URL from a browser, signed out, with no app installed, and an
  app with sign-up needs a deletion route reachable by someone who has already
  uninstalled. `legal.py` serves all three.

**The app is served from TWO origins and that decides every URL above.** The
TWA and the browser load the STATIC deploy (`sherrbyte.vercel.app`); every
endpoint, these three pages included, is FastAPI on
`sherrbyte-api.onrender.com`, which `index.html` hardcodes as the fallback for
any non-localhost host. So the in-app Privacy/Terms rows build their href from
`API_URL` — a relative `/privacy` resolves against the static host, where no
such file exists, and the first version of these links 404'd for every reader.
The Play Console privacy and deletion URLs are the **Render** host for the same
reason. Putting one custom domain in front of both is the real fix and is worth
doing before launch.

`legal.py` takes the **data** claims from the schema and the call sites, never a
template — a policy that lists data the app does not collect is as wrong as one
that omits data it does. The **company** claims (entity, address, jurisdiction)
are environment variables, because a policy naming the wrong legal entity is
worse than no policy; unset values print a visible marker rather than a confident
blank, and `legal_config_gaps()` reports them.

`tests/test_play_store_readiness.py` asserts every one of these URLs answers, that
each manifest icon is the size it claims to be, that the catch-all is still the
last route, and that no `/admin` route is unguarded — two were
(`POST /admin/explore/refresh`, `GET /admin/originality`); the first was a free
cache-buster against the upstream feeds for anyone who found it.

**The committed `sherrbyte.apk` cannot be shipped and cannot be rebuilt here.**
Play takes an `.aab`, not an `.apk`, and there is no TWA source project in this
repo — no `twa-manifest.json`, no Gradle, no keystore — so `versionCode` cannot
be bumped and the target SDK cannot be raised. Rebuild with Bubblewrap against
the live `manifest.json`, keeping the package name `app.vercel.sherrbyte.twa`,
and back the keystore up off-machine: losing it means never updating the listing.

### No branch but `main` gets a Vercel deployment

`vercel.json`'s `git.deploymentEnabled` turns automatic deployments off for
every branch, so `main` still deploys to production — which is the origin the
TWA and every browser load, and the one `assetlinks.json` has to be served
from.

It started as `claude/*` only (PR #266), which left `feat/*` and everything
else gating PRs on the same useless red check — #269 is where that showed.
The two patterns are `*/**` (any branch whose name contains a slash; a
minimatch `*` does NOT cross a `/`, which is why a bare `*` would have missed
`feat/top10-video-studio`) and `!(main)` (any single-segment branch that is not
`main`). **Neither can match `main`, and that is the point**: a `"main": true`
entry would depend on Vercel preferring an exact key over a glob, which its
documentation does not promise, whereas "any unspecified branch defaults to
true" is documented. A bare `"deploymentEnabled": false` is the trap — it stops
production too. `tests/test_vercel_previews.py` pins all of it.

The preview was gating PRs on a deploy that could not tell anyone anything. It
builds the STATIC tree only: `vercel.json` carries headers and no backend, so
a preview serves `index.html` against no API and the deck renders as skeletons.
Judge a change on the Render deploy or on a phone, never on a preview.

It was also failing for a reason no diff could reach — `BUILD_FAILED /
"Resource provisioning failed"` under a second, with no build container
allocated, so not one file in the tree was read. That is account-side and
intermittent: on 2026-10-15 `main` deployed READY and a branch cut from it
failed twice, including as an explicit redeploy of the same commit. Do not
debug a red Vercel check by changing code.

Deleting the `git` block brings the previews back in one line.

---

## The myFeed card's TL;DR, and why it is not a model call

Adopted 2026-10-11. The control sat at the BOTTOM of the News pane — the last
thing a reader reaches, for a button whose whole purpose is "give me the short
version first". It now sits in a cue row **between the headline and the body**,
with the sector badge beside it, and a tap is what produces the panel.

**Nothing is generated until it is tapped.** `hydrateDossier` used to build the
Key Points block for every card in the deck and merely unhide it, so the deck
paid for summaries nobody asked to see. Hydration now only stashes The Node on
`el._mfdNode` as the offline fallback.

**The tap calls `/article/<id>/tldr`, which spends no provider quota.** Gemini's
free tier is 15 requests a minute and `body_drain_job` already budgets 12 of
them, so a reader-triggered model call would compete with the one pass that is
actually writing the corpus — one tap could cost a story. The endpoint DERIVES
bullets and key points from what the writer already produced (hook → summary_60
→ full_body, never `source_summary`, which is the publisher's text), splits them
with `_sentences()` (abbreviation-guarded, or "the U.S. said" becomes two
bullets), and answers `ready:false` with a reason when the story is still a
placeholder. Silence is a valid output here, as it is on an analog card.

`index.html` had been calling `POST /sherr/tldr` for the article overlay's
Summarize button. **That route has never existed in main.py**, so the button
answered "Summarize failed — try again" on every tap it ever received, and
retrying could not help. It now calls the same derived endpoint.

## Share was wired to a name collision

`shareArticleCard` called `buildMyfeedCard(a)` expecting a PNG Blob. Two
functions had that name, declared ~4,500 lines apart — the canvas card builder,
and the myFeed DOM-card builder — and **the later declaration wins**. So share
wrapped an `HTMLDivElement` in a `File`, which stringifies to
`"[object HTMLDivElement]"`: a 25-byte "PNG" the share sheet either refused or
passed on as a corrupt image. The canvas builder is now
`buildArticleByteBlob`, and `tests/test_myfeed_card_controls.py` asserts the
name appears once.

The share payload also **always carries the link** (`articleShareUrl`, the same
`/myfeed/<slug>` main.py serves, so it unfurls with the story's own og: card).
The old call passed `text` only alongside the image, so a shared story arrived
as a picture with no way back. Fallbacks in order: share sheet with image →
share sheet with text+link → clipboard → `execCommand('copy')`.

## Feed preferences were static server-side, whatever the reader tapped

Three defects, all of which made a correct save look ignored:

- **Categories were discarded.** `UpdateTopicsReq` has always carried
  `categories`; `update_topics` only read `topics`. `compute_feed_for_user`
  weights `pillar_score` at 2x, so the entire category half of the screen
  ranked nothing.
- **An unknown topic became pillar 1.** `MICRO_TOPICS.get(topic, 1)` is a silent
  default and the screen offered ~20 topics the map had never heard of
  ("Markets", "Future Tech", "Natural Phenomena", "Productivity"). Picking any
  of them filed a Society vote, so a reader who chose markets and engineering
  got a feed tilted at politics. Those topics are now IN `MICRO_TOPICS`, and
  `resolve_topic_pillar` falls back to `PILLAR_UNSCOPED` (0) — a pillar no
  article has, so an unplaceable topic still earns `tag_score` and contributes
  no pillar bias it did not earn. Do not restore the `, 1)` default.
- **The reader's cached feed outlived the save.** `/feed` is cached per
  `(reader, page, filter)`; `cache.delete_prefix("feed:<uid>:")` now drops them
  all on save, and the client reloads myFeed.

**The topic list is served, not hardcoded.** It lived in index.html as two
literal arrays that had already drifted apart (55 entries vs 49) and matched
neither each other nor `MICRO_TOPICS`. `GET /topics/catalogue` serves one list
with live counts over the corpus, stale-served so the screen opens even while
the database is refusing connections — an empty preferences screen is
indistinguishable from a broken one. `FEED_TOPICS_FALLBACK` in the client is
only for a cold start. **It is `/topics/catalogue`, not `/topics`**: that path
already serves the Explore pillar tree, and a second `@app.get("/topics")` would
never be reached, silently.

`loadUserPrefs` reads `GET /me/topics`, which returns the selection already
split the way it was stored. Inferring the split from `/me`'s one flat list
guessed wrong in the common case: every topic carries a `pillar_id`, so each one
switched its whole category on too.

## The page and the service worker were two different Firebase projects

Adopted 2026-10-14. `index.html` and `firebase-messaging-sw.js` each call
`firebase.initializeApp({...})`, and **every field matched except `apiKey`** — which
is why nothing looked wrong.

The page's key belonged to Google Cloud project **719632752358**. Every other field in
that same object names **485011285385** — `messagingSenderId` IS the Firebase project
number, and `appId` is `1:<projectNumber>:web:<id>`. It was also restricted to Android
apps, so a browser was blocked from it twice over: wrong project, wrong platform.
`getToken(..., { vapidKey })` therefore could not mint a web push token for
`sherrbyte`, and foreground registration from the page was dead while the service
worker's background handler (which had the right key) worked fine.

Determined without the Firebase Console, and the method is worth keeping: **an API that
is disabled on a project names that project in its error**, so
`GET https://translate.googleapis.com/language/translate/v2?target=es&q=hi&key=<KEY>`
returns `projects/<number>` for any Google API key. A control matters too — a bogus key
answers `INVALID_ARGUMENT` / "API key not valid", whereas the correct key answers
`CONFIGURATION_NOT_FOUND` (valid key, project has no Identity Platform config, which is
expected here: this app runs its own HMAC auth and uses Firebase only for FCM) and
`API_KEY_SERVICE_BLOCKED` (correctly scoped to the services it needs). Do not read
either of those as "the key is broken".

Both are Web API keys, which Google publishes as public identifiers — a wrong value,
not a leaked secret. Nothing was rotated.

`tests/test_notifications.py` compares the two configs **field for field** rather than
matching the key string, so the next field to drift is caught too, and separately
asserts `messagingSenderId` equals the project number inside `appId` — the invariant
that catches a credential pasted in from another project.

---

## Sherr Research: the cards read like a wire copy because of the INPUT

Adopted 2026-10-15. The writer's entire input was the publisher's own page
(`article_reader`) or ~700 characters per source in a cluster
(`synthesis.build_prompt`). Everything it could add beyond that page was either a
paraphrase — rejected by `originality_check` — or an invention, rejected by
`writer_gate`. **No prompt can fix a missing input**, which is why the previous
rounds of prompt work moved nothing.

`research.py` is the missing input, and it spends **zero provider requests**:
every leg is a join over data this repo already produced and then discarded.

- **Scope is the owner's line, not a new score.** Geopolitics and markets
  (stocks, crypto, commodities) only — `SCOPE_TOPICS` intersected with
  `significance.score_article`'s own `reasons["topics"]`, plus
  `RESEARCH_MIN_SIGNIFICANCE` (55). There is deliberately **no second keyword
  vocabulary**: two lists drift and the calibrated one is in `significance.py`.
  `ai_tech` scores higher than everything in scope and is still excluded, which
  is the test that proves the gate is the owner's line and not the score.
- **An instrument exists only if `markets.SYMBOLS` names it.** So a ticker a card
  prints is always a ticker `market_ticks` can price. `_ALIASES` is how a human
  writes the same instrument ("the rupee", "bullion"); unqualified "crude"
  resolves to **Brent**, the benchmark an Indian import bill is priced off.
- **A move is measured against the instrument's own normal day** —
  `z = r_h / (MAD * sqrt(h))`, the same formula as the analog engine, so two
  surfaces cannot disagree about "beyond its normal range". The sqrt(h) term is
  not optional here either. No series, no claim: `measure_move` returns **None**
  rather than a zero on a thin or flat series.
- **Prior coverage applies its window in Python, never in SQL.**
  `articles.published_at` is TEXT on some deployments and timestamptz on others
  (migration 018); comparing it to a bind parameter is the bug that took Phase 1
  down on first contact with production. The scan is bounded by `id` and filtered
  through `synthesis.parse_ts`.
- **A brief with no legs is not sent.** It would be the same input the plain
  rewrite sends, under a longer prompt — strictly worse. The row keeps the
  ordinary path.

### A string node is a ROW, and a dot carries an evidence leg

`strings` was prose the model inferred from one source: steps a reader cannot
open. `build_thread` makes every node an `articles` row — id, date, headline,
tappable — and demotes the model's answer to the **labels** on those nodes. It
can never add a node. `_labels_up_to_present` drops the model's "What's next"
step first, or a speculative future label would land on the present node.

`_parse_dots` accepted whatever ticker the model typed and nothing ever checked
it, so the one field that could carry a hard number carried an adjective.
`verify_dots` runs on **every** row, Research or not: a ticker survives only if
`markets.SYMBOLS` names it, the measured move is attached when the brief has one,
and the dot declares `evidence: "measured"` or `evidence: "context"`. Context
dots still render — the surface would otherwise be blank on most stories — but
labelled, the same separation `AnalogCard` already makes.

### The compliance corset is real, and the prompt has to teach it

The engine's blocklist is the ONE blocklist (`narrative.violates_language_rules`,
injected as `sebi_check`) and it was written for generated market cards, so it
forbids words ordinary market journalism reaches for: `hold`, `position`,
`rally`, `correction`, `opportunity`, `target`, `upside`/`downside`, and — via
`PRICE_TARGET_RE` — **any figure next to a currency symbol or code**. A natural
first draft ("Brent held above $80", "anyone buying diesel") fails twice over.

So `SYSTEM_INSTRUCTION_RESEARCH` enumerates those words with substitutes and
requires prices in words ("about 90 dollars a barrel"). Two tests keep the prompt
and the blocklist from drifting apart: every word the prompt forbids must really
be in the engine's list, and the words a crude-or-rupee story actually trips must
all be named in the prompt. The matching is on whole words, so `held` and
`buying` are not hits while `hold` and `buy` are — the prompt is written to match
that and explicitly told not to lean on it.

### Budget, blast radius and the switch

Research costs the **same one request** a plain rewrite costs — the brief is
free — so `RESEARCH_MAX_PER_TICK` (4) is not a quota bound but a blast-radius
bound. The gate is measured on every card under the writer `research`
(`LENGTH_BANDS["research"]` is 120-200, because this writer was asked for three
paragraphs; measuring it against the 60-80 News band would withhold every card
for being the length it was asked to be). A card that fails is withheld, keeps
its placeholder, and **its id is remembered for the life of the process so the
next tick writes it the ordinary way** — otherwise a story the model cannot write
to the band would retry as Research forever while the plain rewrite, which would
have published it, never ran. `RESEARCH_ENABLED=0` switches the stage off.

`/admin/research` is the diagnosis: the counters (considered / in_scope /
briefed / no_evidence / written), the legs histogram, a one-line DIAGNOSIS, and
`?article_id=N` builds that row's brief and returns the exact text appended to
the prompt — read-only and model-free, so it is safe against production.

### Dot to dot is a COUNT over our own stories, and the links never wait

The cross-story leg (`instrument_history`) answers the one question a single
card cannot: has this happened before. It is built the compliant way CLAUDE.md
already names — **frequency of past occurrence**, never a probability, never a
forecast: a count over stories we actually published, each one openable on the
card.

- **The sample floor is AnalogCard's, not a new one.** `HISTORY_MIN_EVENTS` (5)
  gates the SENTENCE only. Below it the earlier stories still render as tappable
  rows and **no number is stated** — "3 of 4" dresses an anecdote as a base rate.
  The links never wait for the floor; only the count does.
- **"Beyond normal" is the same bar as everywhere else** (`Z_NOTABLE` against the
  instrument's own MAD), so a count cannot be inflated by calling an ordinary day
  a move. A publishable frequency therefore makes a dot `evidence: "measured"`
  **with no live move at all** — it is a measurement, taken against the same bar.
- **The measured session is the one AFTER the story.** A story published at 20:00
  IST lands after the Indian close and before the US one, so "the day it was
  published" is a different day per exchange. The next bar is unambiguous
  everywhere, and it is also the honest question.
- **The tag match is `LOWER(micro_tags) LIKE`.** Patterns come from the resolver's
  own `_ALIASES` (lowercased, regex-only aliases skipped) so the two cannot
  disagree about what "Brent" means. `LIKE` is case-sensitive on Postgres, and
  `'%"brent"%'` against a stored `"Brent"` matches nothing — a silent miss that
  reads as "this instrument has no history". `.strip("\b")` is also why
  `_history_patterns` is a function with a test: it once turned BRENT into
  `rent`.

## Telugu is a TRANSLATION in the same request, and the gate is honest about it

Adopted 2026-10-15, the item Sherr Research left open. English stays the
default; Telugu rides along.

**It is one more FIELD, never one more request.** Gemini's free tier is 15
requests a minute and `body_drain_job` already spends 12, so a second call per
article to translate would halve the throughput of the one pass actually writing
the corpus. `ai_processor._TELUGU_RULE` asks for a `telugu`
object — {refined_title, summary, full_body} — inside the request that already
writes the story, and `_out_tokens` raises the output ceiling with the field,
because a response cut off mid-JSON loses the **English** card too.

`with_telugu()` appends the rule to whichever base instruction was chosen
(blurb / full-article / Research) instead of being numbered into any of them:
the WWWH rules are inserted at a different point in each, so a numbered rule
would collide in one of the three. It reads `TELUGU_ENABLED` at **call** time —
module-level configuration is what forces `importlib.reload`, which this repo
has already been burnt by.

**The compliance gate can only read English, and the design says so out loud.**
`writer_gate`'s tone rules and the engine blocklist are English. So Telugu is
only ever published as a translation of a body that has ALREADY passed the
originality check and, for a Research card, the writer gate — it carries no
claim the English card did not make and clear. `telugu.check` then verifies what
IS checkable in any language:

- **Telugu script**, by letter ratio (digits and Latin tickers excluded, or
  every numeric card would read as untranslated). A model that cannot translate
  answers in English rather than refusing, and that string in the column would
  render as a duplicate card with no error anywhere.
- **No invented figure.** A translation may drop a number; it may never add one.
  The tolerance is by VALUE, not digit count — `2026` is a date and `7500` is a
  claim, and both are four digits.
- **One blocklist, still.** The Latin half is the engine's
  `violates_language_rules`, unchanged, so a leaked "buy" or "$90" is caught
  exactly as in English. `telugu.FORBIDDEN_TE` adds only the Telugu-script
  equivalents, and **every entry carries the English word it mirrors**, with a
  test asserting that word is really in the engine's list — otherwise the two
  lists drift and nobody finds out from either side.
- **Proportionate length**, 0.40–1.25 of the English word count: Telugu is
  agglutinative, so the same content lands shorter, and anything outside that is
  a truncation or an addition.

**What it deliberately does NOT check is forward-looking tense.** Telugu marks
the future with an inflectional suffix, not a separate word, so there is no
"will" to find and a suffix match would also hit the ordinary present. Do not
add one believing it closes that gap: it would reject correct prose and still
miss the construction. The ordering above is the control.

The word list is deliberately strict — `అంచనా` (forecast *and* plain estimate)
is on it — because **a failing translation costs nothing**: the column stays
empty, the card renders in English, and the verdict is recorded so the Telugu
pass rate is a number under `telugu` in `/admin/writer-doctor`. A false positive
drops a translation; a false negative publishes an advice word.

`articles.telugu` is JSON-in-TEXT like `strings`/`dots`/`who_affected`. The
surface shows one pill in the card's cue row (`mfBindLang`), only when the row
actually has a translation — an empty language toggle reads as broken, a missing
one reads as English — and it swaps the **headline and lead only**, because
Strings, Dots and the TL;DR panel are still English. The choice is remembered in
`localStorage` behind try/catch, so the whole deck follows the reader.

Still open, deliberately: Strings/Dots/TL;DR in Telugu (each would need its own
field and its own output budget).

---

## The 512MB ceiling: the corpus grew, nothing was deployed

Adopted 2026-10-17, after `sherrbyte-api` was killed repeatedly — "Ran out of
memory (used over 512MB) while running your code", then "Service recovered",
then the same thing two hours later. **No deploy caused it.** The reads that
blew the limit had always been unbounded; the corpus finally got big enough for
one of them to not fit.

`link_stories` is the one that did it. It clusters every `ai_processed=1 AND
status='published'` row inside `STORY_WINDOW_DAYS` (45) at the end of **every**
collection cycle, and `synthesis.cluster_articles` is superlinear in the row
count — a term set per row, an inverted index over them, and a counter for
every pair sharing a term. Measured on a news-shaped corpus:

| rows | clustering peak |
|---|---|
| 3,000 | 18 MB |
| 6,000 | 45 MB |
| 12,000 | 94 MB |
| 25,000 | 189 MB |

That is before the fetched rows themselves and on top of a resident app; at
25,000 the benchmark process alone reached 513MB RSS. At ~1,700 published rows
a day, 45 days is well past that.

**The window is the semantic bound; `STORY_LINK_MAX_ROWS` (6,000) is the memory
one, and it wins.** The newest rows in the window are clustered and anything
older keeps the `story_id` it already had — the same kind of boundary the
window has always been, except it no longer moves with how much the feed
happened to ingest this month. The cap is read at CALL time, so it can be
lowered on a thrashing instance without a deploy, and the `[STORY]` log line
says when it binds. `MAX_TERM_DOC_FRACTION` was not the protection here: the
generic-term cap is `min(MAX_TERM_DOCS=40, …)`, so it is 40 at any corpus size
and bounds pairs per TERM, never the number of terms.

Three more reads had the same shape and are bounded the same way:

- **`_live_thread_ids` runs on a READER'S request**, scanning a pillar's slice
  of the same 45-day window with no LIMIT, so its cost multiplied by however
  many people were scrolling. `LIVE_THREAD_MAX_ROWS` (2,000).
- **`/admin/rescope` selected the whole corpus WITH its bodies** into one list.
  It now pages by id, `RESCOPE_BATCH` (500) at a time. One admin tap should not
  be able to kill the process.
- **Two fetches checked their size limit after buffering the whole response**,
  which is a check that cannot fire in the case it exists for. `article_reader`
  called `client.get` and then measured `r.content` against `MAX_BYTES`; httpx
  imposes no ceiling of its own, so a 300MB page — or a URL that had quietly
  become a video — was resident in full, six at a time under `fetch_many`'s
  semaphore. Both it and the RSS fetch (`_get_capped`, `RSS_MAX_BYTES`, 8MB,
  `RSS_CONCURRENCY` in flight) now stream and abandon the response the moment
  it passes the cap, and refuse outright on an over-cap `Content-Length`.

**`/admin/memory` is where you look.** RSS against the limit, the used
percentage, every corpus-scaled cap and what the in-process ledgers are
holding. It touches **no database**, deliberately: the moment you want it is
the moment a query would time out. Render's own Metrics graph shows the climb
but never what is on the heap.

**The rule this leaves behind: a query whose row count is set by how much news
there happens to be needs a LIMIT, and a fetch whose size is set by a remote
server needs the cap applied while it streams.** On a 512MB instance neither is
a tuning preference. `tests/test_memory_ceilings.py` asserts both halves.

---

## You.com is a SECOND attempt at a page, never a first — and it is metered

Adopted 2026-10-19. `article_reader` returns `""` for a paywalled page, a bot
wall or a body that only exists after JavaScript runs. The caller then falls
back to the 40-word blurb, which this file already records as impossible to
rewrite — so that row keeps its placeholder, and it keeps it **forever**: the
next drain tick fetches the same URL and fails the same way. The failure is not
transient, which is what makes a second route worth buying.

`you_contents.py` is that route. It is wired **inside `article_reader.fetch_many`**,
so every caller (the drain, the on-demand rewrite, Research) is covered without
a call-site change, and it is offered **only the URLs our own fetch returned
nothing for**. Our fetch is free and this one is not; a fallback that ran on the
whole batch would pay for pages we already had.

**The $100 of credit is a finite free tier, not a utility.** Contents is billed
per PAGE FETCHED at $1.00/1,000 ($0.001 a page). The drain reads ~17,000
articles a day, so "retry every failure" is tens of dollars a week and the
credit is gone before anyone opens a card. `YOU_PAGE_BUDGET_PER_DAY` (200) is a
**hard ceiling**, not a rate hint: $0.20 a day, so the credit outlives a year of
unattended running. Nothing queues — a page skipped today is offered again
tomorrow by the same selector that offered it today — and a failed call refunds
the pages it never fetched, or a run of timeouts would spend the day's ceiling
having read nothing.

**The ledger is per-process, and that is a stated bound.** It is in memory for
the reason `/admin/memory` touches no database: the moment the budget matters is
the moment a DB write is one more thing that can fail, and a failed write that
silently resets a spend counter is worse than a counter honest about its scope.
With N instances the real ceiling is N × the budget. Move it into `cache.py`'s
Redis layer **before** scaling out, not after — the same note `_token_version`'s
TTL cache carries.

**The host is an environment variable because the documentation disagrees with
itself.** You.com's reference gives `https://ydc-index.io/v1/contents`; their
API catalogue lists a base of `https://api.you.com/v1`. Both hosts are
unreachable from CI and from the container this was written in, so **no call in
this module has ever been made against a live response** — every test drives an
`httpx.MockTransport`. `YOU_API_BASE` makes picking the right host a dashboard
edit rather than a deploy, and `_pages()` accepts a bare array or a
`results`/`data`/`contents` envelope so a wrong guess about the wrapper degrades
to "no text" instead of raising inside the rewrite pass.

`YOU_KEYS` is **plural in the dashboard, so it is read as a list** (comma,
whitespace or newline separated) and rotated through `key_pool.KeyPool` on
401/403/429 — the statuses that mean the KEY is the problem, the same rule the
model providers follow. `YOU_API_KEY` and `YDC_API_KEY` (the name You.com's own
tooling uses) are accepted as singles so a key set under either is not dead
weight. The count is reported; a key never is.

Recovered text faces **the same floor our own extraction does**
(`is_full_article`): a page of navigation furniture is not an article because
somebody else fetched it. It is INPUT ONLY, never stored and never served — the
copyright line and the Supabase-quota line that `article_reader` already holds.

`/admin/you` reports the budget, the counters and a one-line `DIAGNOSIS`;
`YOU_CONTENTS_ENABLED=0`, or simply no key, makes the whole path a no-op and the
reader behaves exactly as it did before.

**What was deliberately NOT built:** the Search API ($5/1,000 calls, five times
the price) as an evidence leg for Sherr Research. Research is built as
deterministic joins over data we already hold, and that is a recorded decision,
not an accident — a web-search leg is a different claim about where a card's
evidence comes from, and it should be a decision taken on its own, not smuggled
in behind a page-reading fix.

---

## Provider circuit breakers, and the status that must never trip one

Adopted 2026-10-24. `_call_provider` tried every key in a pool and the cascade
tried every provider, on **every article**. Correct when a call fails for a
reason the next key might not share; pure waste when the provider is simply not
answering — each article then pays the full connect timeout once per key, the
drain's 60-second tick is spent waiting rather than writing, and the only record
is a `PROVIDER_ERRORS` line saying the same thing for the hundredth time.

`circuit.py` remembers. After `CIRCUIT_THRESHOLD` (4) consecutive failures of
the kind that mean "not answering", the provider is skipped for
`CIRCUIT_COOLDOWN_S` (60) and the cascade moves on immediately. One probe is let
through when the cooldown expires: success closes it, failure re-opens it with a
**doubled** cooldown up to `CIRCUIT_MAX_COOLDOWN_S` (900).

### Which failures trip it is the entire design

| | |
|---|---|
| **trips** | status `0` (no response at all — DNS, connect, read timeout) and any 5xx |
| **never** | 401/403 — the KEY is the problem and `key_pool` already rotates on it; opening here would take a provider out over one bad key in a pool of good ones |
| | **429** — "slow down", not "down" |
| | 400/404 — a configuration error, not a transient one |

**429 is the one that matters.** The free tier bounces bursts routinely,
`body_drain_job` already backs off on exactly that signal (read from
`ai_processor.PROVIDER_ERRORS`), and a breaker opening on it would take a
**healthy** provider out of service for a cooldown — the opposite of the correct
response, and it would stall the one pass writing the corpus. The drain's rule
that a rate limit and a failure are different answers is the same rule one layer
down. A test drives 50 consecutive 429s and asserts the breaker never opens.

**404 is excluded because that is how a RETIRED MODEL ID presents** (see the
provider-model-ids entry: the call 404s, the cascade drops through, and the only
symptom is a backlog that stops clearing). Cooling down and retrying is the
wrong shape for something only a config change fixes, and `GROQ_RETIRED` in
`tests/test_feed_doctor.py` is its guard. It still lands in `PROVIDER_ERRORS`.

**Any HTTP answer closes the breaker**, 429 and 401 included: receiving a status
at all is proof the host is reachable and serving. Leaving it open on evidence
that contradicts it would keep skipping a provider that is up.

### Two ways a breaker fails worse than no breaker, both guarded

- **It never closes.** `allow()` marks a probe in flight so the rest of the
  batch is not sent at a provider that is down. If the caller then raises before
  recording an outcome, that flag would never clear and the provider would be
  skipped **forever**. So the outcome is recorded in a `finally`, *and* a probe
  is abandoned after one cooldown, *and* the doubling has a ceiling — a breaker
  that cannot close is a worse outage than the one it was protecting against.
- **It opens on the wrong thing.** Covered above. `CIRCUIT_ENABLED=0` measures
  without short-circuiting, the same posture `WRITER_GATE_ENABLED` has, so the
  trip rate is readable on a live instance before anything is skipped on it.

### `_try_pool` is one copy of a loop that was three

The card rewrite, `synthesize` and `news_writer` each carried their own
key-rotation loop, so a breaker added to one would have covered almost none of
the drain's actual spend. All three now go through `_try_pool`, which owns the
rotation policy, the breaker and the telemetry span. A **rejected synthesis** is
deliberately not a breaker failure: the provider answered, and the next provider
would be asked the same question with the same prompt.

**The breaker sits in front of the pool, not each key.** What it protects
against is a provider that is not answering, and then every key times out
identically — so the unit it must be able to skip is the provider.

Per process, the same stated bound `you_contents`' budget ledger and
`_token_version`'s TTL cache carry: with N instances each has its own view. That
is the right scope — a breaker is about what *this* process observed, and a
shared one would let an instance that has made no call refuse to make one.

## Telemetry is a structured line, and OpenTelemetry is refused for two reasons

`telemetry.py`. Every admin endpoint here answers one subsystem's question well;
none answers the one that spans them — *where did the last hour go*. A model
call, the page fetch before it and the query after it were three log lines in
three formats with no field in common, so "the drain is slow" could not be read
off anything.

One shape, one logger (`sherbyte.telemetry`), `grep | jq`-able on Render's log
tail, which is the only log tool this deployment has:

```json
{"ev":"llm.call","op":"synthesize","provider":"gemini","ms":812,"ok":true,"status":200}
```

**OpenTelemetry was asked for and is the wrong tool here**, for reasons about
this deployment rather than about OTel: there is nowhere to send a trace (an
exporter with no collector is a dependency, a thread and a queue in exchange for
nothing, and standing up a collector is a hosting decision), and the SDK plus an
exporter and its protobuf runtime is tens of megabytes resident on a 512MB
instance that has already been OOM-killed. The span shape is deliberately
OTel-shaped — a name, a duration, an ok flag, flat attributes — so that if a
collector ever exists this becomes an exporter and not a rewrite.

Three properties worth not re-deriving:

- **A span never swallows an exception.** It records `ok:false` with the
  exception type and re-raises; swallowing would turn an instrumented call site
  into a silently different one. An explicit `ok=False` in the attributes wins
  over "did not raise", because a call that returned a 500 without raising did
  not succeed and the caller is what knows that.
- **The series dict is bounded** (`MAX_SERIES`, 200) and every attribute is
  truncated to 200 chars. An op name is a string a caller supplies, and one
  built from reader input would grow the dict with traffic — the exact shape the
  512MB entry warns about. A span attribute is a label, not a payload.
- **The redaction list does not contain a bare `key`.** It did, and it redacted
  `keys` — the *number* of keys in the pool, a count the diagnosis wants that
  reveals nothing. Credential-shaped names match as substrings (`GEMINI_API_KEY`,
  `authorization`); the bare word matches exactly.

`TELEMETRY_LOG=0` silences the lines while keeping the counters, because the
counters are what `/admin` reads and cost nothing while the lines are what fills
a log quota.

`/admin/pipeline` is both ledgers plus a `DIAGNOSIS` each. `top_by_time` is
ordered by **total** time, not average: a 3-second call made once is not the
problem and a 90ms one made ten thousand times is. It touches no database,
deliberately — the moment you want it is the moment a query would time out.

**`tests/conftest.py` resets all three pieces of per-process state** (the cache's
local layer, the breakers, the telemetry counters) before *and* after every test.
Before as well as after, so a test is not affected by one that failed partway
through and never reached its teardown.
