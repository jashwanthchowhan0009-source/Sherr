# SherByte (v6) — TheWhiteTigers

An 8-stage news intelligence pipeline that collects, understands, structures,
threads, personalizes, and narrates the news — then learns from how you read.

```
Collect → Understand → Construct → Connect → Personalize → Narrate → Deliver → Learn
  01          02           03         04          05           06         07       08
```

## Architecture

```
app/
├── main.py            FastAPI entry, lifespan, routers
├── config.py          Pydantic settings + 9-pillar taxonomy + write modes
├── security.py        password hashing + JWT (access/refresh)
│
├── api/               HTTP layer
│   ├── auth.py        /auth/register · /auth/login · /auth/refresh
│   ├── feed.py        /feed/personalized · /feed/trending · /feed/explore
│   ├── article.py     /article/{id} · /article/{id}/full · /search · bookmarks
│   ├── signal.py      /signal/dwell · /signal/hide · /signal/mute
│   ├── sherr.py       /sherr/brief · /sherr/explain · /sherr/thread
│   ├── markets.py     /markets/*  (ported from MVP)
│   └── activity.py    /me · /me/analytics · /activity/heartbeat (ported)
│
├── pipeline/          THE CORE ENGINE
│   ├── collector.py     01 Collect    — RSS + NewsAPI ingestion
│   ├── deduplicator.py  01 Dedup      — 3-layer (URL / title / SimHash)
│   ├── understander.py  02 Understand — NER + WWWW + classify (Gemini + spaCy)
│   ├── constructor.py   03 Construct  — InfoObject + importance score
│   ├── embedder.py      03 Embed      — MiniLM 384-d → pgvector
│   └── connector.py     04 Connect    — story graph threading via pgvector
│
├── sherr/             THE AI WRITER (06 Narrate)
│   ├── core.py        orchestrator
│   ├── router.py      Gemini → Groq cascade (also used by Understand)
│   ├── rag.py         RAG-consensus retrieval (pgvector)
│   ├── prompts.py     5 write modes: ALERT · BRIEF · EXPLAINER · THREAD · DEEP
│   ├── writer.py      narrative generation
│   └── watermark.py   SGI labelling (IT Rules 2026)
│
├── recommender/       05 Personalize
│   ├── hybrid_scorer.py  score = α·content + β·collab + γ·freshness
│   ├── als.py            ALS collaborative filter (+ SVD fallback)
│   ├── mmr.py            Maximal Marginal Relevance re-ranker
│   └── temporal.py       freshness decay
│
├── models/            Pydantic schemas (article, info_object, user, story_node)
├── workers/           ARQ workers (ingest / embed / als / signal)
└── db/                supabase.py (asyncpg pool), redis.py, migrations/
```

## Quick start (local)

```bash
cd sherrbyte
cp .env.example .env                  # fill in DATABASE_URL, REDIS_URL, keys
pip install -r requirements.txt
uvicorn app.main:app --reload         # migrations run automatically on boot
```

Postgres needs the `vector` and `pgcrypto` extensions (Supabase has both).
Migrations in `app/db/migrations/` are idempotent and applied on every boot.

## Running the pipeline manually

```bash
python -m app.workers.ingest_worker   # one full Collect→…→Connect cycle
python -m app.workers.embed_worker    # embed + thread stragglers
python -m app.workers.als_worker      # retrain collaborative filter
python -m app.workers.signal_worker   # decay prefs, refresh trending, adapt weights
```

In production set `RUN_SCHEDULER=false` and let the GitHub-Actions cron
(`.github/workflows/cron_ingest.yml`) own ingestion.

## Tests

```bash
pytest            # pure-unit tests, no DB/network required
```

## Graceful degradation

Every external dependency has a fallback so the service never hard-crashes:

| Missing | Falls back to |
|---|---|
| Gemini & Groq keys | rule-based understanding; Sherr returns the summary |
| sentence-transformers | deterministic hash embedding |
| spaCy model | regex NER |
| `implicit` library | truncated-SVD ALS |
| Redis | DB-only dedup, no caching |
