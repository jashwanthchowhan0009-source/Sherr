-- 003_signals_recsys.sql — Personalize + Learn stages.
-- Implicit-feedback signals, ALS latent factors, cached feed scores,
-- bookmarks, and reading activity (ported from the MVP's activity module).

-- ─── Signals (Learn stage input) ──────────────────────────────────────────
-- Every implicit/explicit interaction. dwell_ms carries reading depth;
-- hide/mute carry negative intent that suppresses similar content.
CREATE TABLE IF NOT EXISTS signals (
    id             BIGSERIAL PRIMARY KEY,
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    info_object_id UUID REFERENCES info_objects(id) ON DELETE CASCADE,
    kind           TEXT NOT NULL,        -- dwell|read|like|dislike|save|hide|mute|share|quiz
    value          REAL DEFAULT 1.0,     -- implicit weight (dwell seconds, etc.)
    target         TEXT DEFAULT '',      -- for mute: a topic / source / entity
    created_at     TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_signals_user ON signals(user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_signals_obj  ON signals(info_object_id);
CREATE INDEX IF NOT EXISTS idx_signals_kind ON signals(kind);

-- Things a user has muted (topic / source / entity) → hard filter in the feed.
CREATE TABLE IF NOT EXISTS user_mutes (
    user_id    UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    mute_type  TEXT NOT NULL,            -- topic | source | entity
    value      TEXT NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, mute_type, value)
);

-- ─── ALS collaborative-filter factors (Learn stage output) ────────────────
CREATE TABLE IF NOT EXISTS als_user_factors (
    user_id    UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    factors    REAL[] NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT now()
);
CREATE TABLE IF NOT EXISTS als_item_factors (
    info_object_id UUID PRIMARY KEY REFERENCES info_objects(id) ON DELETE CASCADE,
    factors        REAL[] NOT NULL,
    updated_at     TIMESTAMPTZ DEFAULT now()
);

-- Per-user learned blend weights (α content, β collab, γ freshness).
CREATE TABLE IF NOT EXISTS user_weights (
    user_id    UUID PRIMARY KEY REFERENCES users(id) ON DELETE CASCADE,
    alpha      REAL DEFAULT 0.5,
    beta       REAL DEFAULT 0.3,
    gamma      REAL DEFAULT 0.2,
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- ─── Cached personalized feed (Personalize stage output) ──────────────────
CREATE TABLE IF NOT EXISTS feeds (
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    info_object_id UUID NOT NULL REFERENCES info_objects(id) ON DELETE CASCADE,
    score          REAL DEFAULT 0.0,
    computed_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, info_object_id)
);
CREATE INDEX IF NOT EXISTS idx_feeds_user ON feeds(user_id, score DESC);

-- ─── Bookmarks ────────────────────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS bookmarks (
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    info_object_id UUID NOT NULL REFERENCES info_objects(id) ON DELETE CASCADE,
    saved_at       TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, info_object_id)
);

-- ─── Reading activity (ported from MVP activity.py) ───────────────────────
CREATE TABLE IF NOT EXISTS sessions (
    user_id      UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    session_date DATE NOT NULL,
    duration_sec INTEGER DEFAULT 0,
    last_seen    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, session_date)
);

CREATE TABLE IF NOT EXISTS reading_progress (
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    info_object_id UUID NOT NULL REFERENCES info_objects(id) ON DELETE CASCADE,
    scroll_pct     INTEGER DEFAULT 0,
    duration_sec   INTEGER DEFAULT 0,
    completed      BOOLEAN DEFAULT FALSE,
    started_at     TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (user_id, info_object_id)
);
CREATE INDEX IF NOT EXISTS idx_progress_user ON reading_progress(user_id, updated_at DESC);
