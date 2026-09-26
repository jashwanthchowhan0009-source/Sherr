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

**There is no server-side revocation, and the client must not pretend there is.**
`make_token`/`make_refresh_token` are stateless HMACs with no store behind them
and there is no `/auth/logout` route. Logout ends the session on THIS device and
scrubs it from every partition on disk; a token already issued stays valid until
it expires (30d access, 180d refresh). The Security Center row says "Log Out On
This Device" for that reason — do not restore the "all devices" wording without
building revocation (a `token_version` column on `users`, bumped on logout and
checked in `verify_token`, is the smallest honest version).

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

## Desktop is one @media block, and that is the whole guarantee

`@media (min-width: 1024px)` in index.html, and **every** desktop rule is inside
it. That is what makes "mobile renders identically to before" a structural
property rather than a hope, and `tests/test_desktop_layout.py` asserts it: one
breakpoint, the restyled selectors inside, the phone's base rules untouched
outside. If you are adding a desktop rule somewhere else in that stylesheet:
don't.

1024px, not 768px — a tablet in portrait is a big phone and the bottom nav is
right for it. The sidebar is the SAME `<nav>` element re-laid out, not a second
nav; two navs would mean two active-tab states that disagree the first time
either changes.

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
