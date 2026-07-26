# SPRIE Intelligence Engine — Go-Live Runbook

How to take the pattern engine (`sherrbyte/app`, Supabase/pgvector) from code to
live pattern output in the app. **These steps run on your Supabase + host accounts
— they can't be executed from the coding sandbox (no DB access, egress blocked).**

The engine and the deployed app (`main.py`, sqlite, `sherrbyte-api.onrender.com`)
are two separate services. This runbook deploys the engine, fills it with real
data, runs the detectors, and then flips the app's `/patterns` to the live engine.

---

## 1. Provision Supabase (Postgres + pgvector)
1. Create a Supabase project.
2. Project Settings → Database → Connection string → **Transaction pooler** URI
   (host `aws-0-<region>.pooler.supabase.com`, port `6543`). Append
   `?sslmode=require` if absent. This is your `DATABASE_URL`.
   - Use the **pooler**, not the direct connection (matches
     `statement_cache_size=0` in `db/supabase.py`).

## 2. Deploy the engine service
Deploy `sherrbyte/app` (its `Dockerfile` / `fly.toml` / `DEPLOY.md`) to your host.
Set env vars:
- `DATABASE_URL` = the pooler URI from step 1
- `REDIS_URL` = your Redis/KV instance
- `RUN_SCHEDULER=true` (so ingestion + nightly detectors run in-process)
- `GEMINI_API_KEY` / `GROQ_API_KEY` (optional but recommended for real summaries)

On boot the app **auto-runs migrations** (`run_migrations()` in the lifespan),
creating `vector` + `pgcrypto` extensions and all engine tables:
`entities`, `entity_aliases`, `domain_signals`, `cooccurrence`, `insights`.

Confirm: `GET https://<engine-host>/health` → ok.

## 3. Backfill signals from existing articles (verify small first)
The engine's `domain_signals` starts empty; new ingestion fills it going forward,
but you can seed it from the `info_objects` already in the DB. Run in the host's
shell / a one-off job:

```bash
# small verifiable batch — inspect the summary it prints
python -m app.workers.signals_backfill --limit 100
```
The summary shows `domain_signals`, `entities`, `cooccurrence_rows`,
`top_entities`, and `top_pairs`. Sanity-check that the top entities/pairs look
real (e.g. "Reserve Bank of India", "Nifty 50"). Then the full pass:

```bash
python -m app.workers.signals_backfill          # all info_objects
```

## 4. Rebuild co-occurrence (idempotent full pass)
`persist_signals` already updates co-occurrence incrementally, but this recomputes
the trailing-90-day window cleanly:

```bash
python -m app.workers.cooccurrence_backfill --limit 100   # verify
python -m app.workers.cooccurrence_backfill               # full 90-day rebuild
```

## 5. Run the detectors on real data
```bash
python -m app.workers.detectors                 # emergence + temporal_correlation
# or one at a time:
python -m app.workers.detectors --only emergence
python -m app.workers.detectors --only temporal_correlation
```
Then inspect the first real insights:
```bash
curl https://<engine-host>/patterns | jq
```

> **Expectation setting:** *emergence* can surface within days of ingestion.
> *temporal_correlation* needs ≥ 8 daily buckets per entity across ≥ 2 periods,
> so it only starts firing after a couple of weeks of accumulated daily signals
> (backfill from history helps, but only as far back as your info_objects go).

## 6. Point the app's /patterns at the live engine
On the **app** service (`main.py`, `sherrbyte-api.onrender.com`) set:
```
ENGINE_URL = https://<engine-host>
```
`/patterns` now proxies to the engine and serves real insights; without it, the
app falls back to the sqlite sample seed. No frontend change needed — the SPRIE
section already reads `/patterns`.

---

## Nightly, hands-off
With `RUN_SCHEDULER=true`, the engine service ingests every ~25 min (each article
now emits a `domain_signal` → entity resolution → co-occurrence) and runs the
detectors nightly at 02:00 UTC. Insights refresh on their `signature`, so the
SPRIE feed stays current on its own.
