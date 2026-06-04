# Deploying SherByte v6

Two paths: **local** (Docker, free, for development) and **production**
(Supabase + Upstash + Fly.io). The app runs its own SQL migrations on boot, so
there's no separate migration step.

---

## 1. Local — one command

Prereqs: Docker Desktop.

```bash
cd sherrbyte
cp .env.example .env          # optionally add GEMINI_API_KEY / GROQ_API_KEY
docker compose up --build
```

What happens:
- Postgres (with pgvector) and Redis start.
- The API waits for them, applies migrations, and begins ingesting from the 50+
  RSS feeds (`RUN_SCHEDULER=true`).

Check it:
```bash
curl http://localhost:8000/health          # {"status":"ok", counts:{...}}
open http://localhost:8000/docs             # interactive API docs
curl "http://localhost:8000/feed/trending"  # fills within ~1 min of first ingest
```

> First build is large (~2–3 GB) because `sentence-transformers` pulls Torch and
> downloads the MiniLM model on first embed. Without AI keys the pipeline still
> works — it falls back to rule-based understanding and hash embeddings.

Run a single pipeline cycle manually:
```bash
docker compose exec api python -m app.workers.ingest_worker
```

---

## 2. Production

### a. Postgres + pgvector — Supabase
1. Create a project at supabase.com.
2. Project Settings → Database → copy the **connection string** (URI). That's
   `DATABASE_URL` (use the `postgresql://...` form, port 5432 or the pooler 6543).
3. Project Settings → API → copy `Project URL` (`SUPABASE_URL`) and the
   `service_role` key (`SUPABASE_KEY`).
4. pgvector + pgcrypto are enabled automatically by the migrations.

### b. Redis — Upstash
1. Create a database at upstash.com (pick the region nearest your users).
2. Copy the `redis://...` URL → `REDIS_URL`.

### c. AI keys
- `GEMINI_API_KEY` — aistudio.google.com (free tier).
- `GROQ_API_KEY` — console.groq.com (fallback; optional).
- `NEWSAPI_KEY` — newsapi.org (optional; RSS works without it).

### d. Host — Fly.io
```bash
cd sherrbyte
fly launch --no-deploy        # uses the bundled fly.toml; pick the bom region
fly secrets set \
  DATABASE_URL="postgresql://..." \
  SUPABASE_URL="https://...supabase.co" \
  SUPABASE_KEY="..." \
  REDIS_URL="redis://..." \
  JWT_SECRET="$(openssl rand -hex 32)" \
  WATERMARK_SECRET="$(openssl rand -hex 16)" \
  GEMINI_API_KEY="..." \
  GROQ_API_KEY="..."
fly deploy
fly open                      # hits /
```

`fly.toml` sets `RUN_SCHEDULER=false` in prod — ingestion is owned by the cron
workflow instead (next step).

### e. Scheduled ingestion — GitHub Actions
`.github/workflows/cron_ingest.yml` runs the pipeline every 15 min and retrains
ALS nightly. In your repo settings → Secrets → Actions, add the same secrets:
`DATABASE_URL`, `REDIS_URL`, `SUPABASE_URL`, `SUPABASE_KEY`, `GEMINI_API_KEY`,
`GROQ_API_KEY`, `NEWSAPI_KEY`.

### f. Auto-deploy
`.github/workflows/deploy.yml` runs tests and deploys to Fly on push to `main`.
Add a `FLY_API_TOKEN` secret (`fly tokens create deploy`).

---

## 3. Smoke test the live API

```bash
BASE=https://your-app.fly.dev

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

## 4. Notes on migrating from the MVP

- This is a **fresh data model** (UUID ids, info_objects, story graph). Old
  SQLite articles are not carried over — the pipeline repopulates from feeds.
- The HTTP contract changed (namespaced routes, access/refresh tokens, UUIDs).
  The existing `index.html` frontend needs rewiring to the new endpoints — see
  `/docs` for the exact shapes.
