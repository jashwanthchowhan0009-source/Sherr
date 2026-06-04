# Deploying SherByte v6

The database is **your Supabase project** (Postgres + pgvector). Everything
else — the API and an optional Redis/worker — runs in Docker. The app applies
its own SQL migrations on boot (it creates the `vector` + `pgcrypto`
extensions and all tables in Supabase), so there is no separate migration step.

Two paths below:
- **Run it locally with Docker Compose** (fastest way to see it working).
- **Get a public live link** with the Render Blueprint.

---

## 0. Prerequisites — your Supabase connection string

1. Supabase → **Project Settings → Database → Connection string**.
2. Copy the **Transaction pooler** URI — *not* the direct connection:
   ```
   postgresql://postgres.<project-ref>:<password>@aws-0-<region>.pooler.supabase.com:6543/postgres?sslmode=require
   ```
   Why the pooler: it's the IPv4 endpoint (the direct `db.<ref>.supabase.co`
   host is IPv6-only and won't resolve from many Docker hosts / Render's free
   tier). The app sets `statement_cache_size=0` so asyncpg works correctly
   through the pooler's transaction mode.

That URI is your `DATABASE_URL`. (AI keys are optional — see §3.)

---

## 1. Run locally with Docker Compose

Prereq: Docker Desktop.

```bash
cd sherrbyte
cp .env.example .env
# edit .env → paste your Supabase transaction-pooler URI into DATABASE_URL
docker compose up --build
```

What happens:
- The **API** (port 8000) and **Redis** start. No Postgres container runs —
  the DB is your Supabase project.
- On boot the API connects to Supabase, applies migrations, and (because
  `RUN_SCHEDULER=true` in `.env`) starts ingesting from the 50+ RSS feeds.

Check it:
```bash
curl http://localhost:8000/health           # {"status":"ok","db":true,"counts":{...}}
open  http://localhost:8000/docs             # interactive API docs
curl "http://localhost:8000/feed/trending"   # fills within ~1 min of first ingest
```

`"db": true` in `/health` confirms the Supabase connection. Run a single
pipeline cycle by hand:
```bash
docker compose exec api python -m app.workers.ingest_worker
```

### Dedicated worker instead of the in-process loop (optional)
For a cleaner separation (API serves requests, a worker owns ingestion):
```bash
# in .env: RUN_SCHEDULER=false
docker compose --profile workers up --build
```
This starts an ARQ `worker` driven by the Redis queue + cron schedule in
`app/workers/__init__.py` (ingest every 15 min, embed every 5, signals twice
hourly, ALS retrain nightly).

### Fully offline (no Supabase) — optional
To develop without touching Supabase, run a local Postgres+pgvector container:
```bash
# in .env: DATABASE_URL=postgresql://postgres:postgres@db:5432/sherrbyte
docker compose --profile local-db up --build
```

---

## 2. Public live link — Render Blueprint

The repo root has `render.yaml`. It runs the API container on Render and points
it at **your Supabase** (Render only adds a free Redis/Key-Value store).

1. Push this branch (or merge to `main`).
2. **render.com → New → Blueprint** → connect this repo → pick the branch.
3. Render detects `render.yaml`. When prompted, paste your Supabase
   transaction-pooler URI into **`DATABASE_URL`** (it's `sync:false`, so it's
   never committed to git).
4. Click **Apply**. After ~3–5 min you get `https://sherrbyte-api.onrender.com`.

Verify:
```
https://sherrbyte-api.onrender.com/health      # status + counts (db:true)
https://sherrbyte-api.onrender.com/docs         # interactive API
https://sherrbyte-api.onrender.com/feed/trending
```

> Free Render web services sleep after ~15 min idle and cold-start (~30s) on the
> next request; ingestion only runs while awake. Fine for a demo — for always-on
> ingestion use the GitHub-Actions cron (§4) or a paid instance.

---

## 3. Optional services

### Redis
Used for caching, the ARQ queue, and the layer-1 dedup set. **Optional** — if
`REDIS_URL` is unreachable every helper degrades to a safe no-op (caching off,
dedup falls back to its DB-backed layers). Compose includes Redis already; in
production point `REDIS_URL` at an Upstash database (`redis://...`).

### AI keys (all optional)
With none set, the pipeline runs rule-based understanding + hash embeddings
(fully functional, lighter quality).
- `GEMINI_API_KEY` — aistudio.google.com (free tier) → real summaries.
- `GROQ_API_KEY` — console.groq.com (fallback).
- `NEWSAPI_KEY` — newsapi.org (RSS works without it).

Add them to `.env` (local) or the host's environment (Render dashboard → Environment).
For the high-quality semantic stack (MiniLM embeddings, true ALS, spaCy NER),
install `requirements-ml.txt` on a larger instance.

---

## 4. Scheduled ingestion — GitHub Actions

`.github/workflows/cron_ingest.yml` runs the pipeline every 15 min and retrains
ALS nightly against Supabase — ideal when the API host sleeps. In the repo's
**Settings → Secrets and variables → Actions**, add: `DATABASE_URL` (the
Supabase pooler URI), and optionally `REDIS_URL`, `SUPABASE_URL`,
`SUPABASE_KEY`, `GEMINI_API_KEY`, `GROQ_API_KEY`, `NEWSAPI_KEY`.

(`fly.toml` and `.github/workflows/deploy.yml` are also bundled for a Fly.io
deployment, which sets `RUN_SCHEDULER=false` and lets the cron own ingestion.)

---

## 5. Smoke test the API

```bash
BASE=http://localhost:8000          # or your Render URL

# register → returns access + refresh tokens
curl -s -X POST $BASE/auth/register \
  -H 'content-type: application/json' \
  -d '{"email":"me@example.com","password":"secret123","topics":["AI","Cricket"]}'

# personalized feed (use the access_token from above)
curl -s "$BASE/feed/personalized" -H "Authorization: Bearer <ACCESS_TOKEN>"

# Sherr brief for an info object
curl -s "$BASE/sherr/brief/<INFO_ID>"
```

---

## 6. Notes on migrating from the MVP

- This is a **fresh data model** (UUID ids, info_objects, story graph). Old
  SQLite articles are not carried over — the pipeline repopulates from feeds.
- The HTTP contract changed (namespaced routes, access/refresh tokens, UUIDs).
  The existing `index.html` frontend needs rewiring to the new endpoints — see
  `/docs` for the exact shapes.
