-- 001_init.sql — Core schema: extensions, users, raw articles, info objects.
-- Safe to re-run: everything is IF NOT EXISTS.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- gen_random_uuid()
CREATE EXTENSION IF NOT EXISTS vector;        -- pgvector

-- ─── Users ────────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    name          TEXT DEFAULT '',
    bio           TEXT DEFAULT '',
    avatar_url    TEXT DEFAULT '',
    language      TEXT DEFAULT 'en',
    created_at    TIMESTAMPTZ DEFAULT now(),
    last_login    TIMESTAMPTZ
);

-- Per-user topic affinities (seeded at onboarding, nudged by signals).
CREATE TABLE IF NOT EXISTS user_preferences (
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    topic      TEXT NOT NULL,
    pillar_id  SMALLINT NOT NULL,
    weight     REAL DEFAULT 1.0,
    updated_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, topic)
);
CREATE INDEX IF NOT EXISTS idx_prefs_user ON user_preferences(user_id);

-- ─── Raw articles (Collect stage output) ──────────────────────────────────
CREATE TABLE IF NOT EXISTS articles (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url          TEXT UNIQUE NOT NULL,
    url_hash     TEXT NOT NULL,
    title        TEXT NOT NULL,
    title_hash   TEXT NOT NULL,        -- normalized-title fingerprint (dedup L2)
    simhash      BIGINT,               -- 64-bit body simhash (dedup L3)
    body         TEXT DEFAULT '',
    image_url    TEXT DEFAULT '',
    source_name  TEXT DEFAULT '',
    scope        TEXT DEFAULT 'global',-- local | national | global
    published_at TIMESTAMPTZ DEFAULT now(),
    collected_at TIMESTAMPTZ DEFAULT now(),
    processed    BOOLEAN DEFAULT FALSE -- has the Understand→Construct path run?
);
CREATE INDEX IF NOT EXISTS idx_articles_title_hash ON articles(title_hash);
CREATE INDEX IF NOT EXISTS idx_articles_simhash    ON articles(simhash);
CREATE INDEX IF NOT EXISTS idx_articles_unprocessed ON articles(processed) WHERE processed = FALSE;
CREATE INDEX IF NOT EXISTS idx_articles_published  ON articles(published_at DESC);

-- ─── Info objects (Construct stage output) ────────────────────────────────
-- One structured information unit per real-world event. Carries the WWWW,
-- normalized entities, taxonomy, importance, and the 384-d MiniLM embedding.
CREATE TABLE IF NOT EXISTS info_objects (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    article_id  UUID REFERENCES articles(id) ON DELETE SET NULL,
    headline    TEXT NOT NULL,
    summary     TEXT DEFAULT '',
    body        TEXT DEFAULT '',
    who         TEXT DEFAULT '',
    what        TEXT DEFAULT '',
    where_info  TEXT DEFAULT '',
    when_info   TEXT DEFAULT '',
    why_info    TEXT DEFAULT '',
    entities    JSONB DEFAULT '[]'::jsonb,   -- [{name, type, canonical}]
    topic       TEXT DEFAULT '',
    pillar_id   SMALLINT DEFAULT 1,
    micro_tags  JSONB DEFAULT '[]'::jsonb,
    scope       TEXT DEFAULT 'global',
    importance  REAL DEFAULT 0.0,            -- 0..1 newsworthiness
    sentiment   TEXT DEFAULT 'neutral',
    is_trending BOOLEAN DEFAULT FALSE,
    embedding   vector(384),                 -- MiniLM all-MiniLM-L6-v2
    source_name TEXT DEFAULT '',
    image_url   TEXT DEFAULT '',
    thread_id   UUID,                         -- FK added in 002
    published_at TIMESTAMPTZ DEFAULT now(),
    created_at   TIMESTAMPTZ DEFAULT now(),
    engagement   INTEGER DEFAULT 0
);
CREATE INDEX IF NOT EXISTS idx_info_pillar    ON info_objects(pillar_id);
CREATE INDEX IF NOT EXISTS idx_info_published ON info_objects(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_info_trending  ON info_objects(is_trending) WHERE is_trending = TRUE;

-- HNSW index for fast approximate cosine search over embeddings (Connect + RAG).
CREATE INDEX IF NOT EXISTS idx_info_embedding_hnsw
    ON info_objects USING hnsw (embedding vector_cosine_ops)
    WITH (m = 16, ef_construction = 64);
