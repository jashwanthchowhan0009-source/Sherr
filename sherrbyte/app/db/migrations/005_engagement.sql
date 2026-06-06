-- 005_engagement.sql — user comments on info objects.
--
-- NOTE on likes: likes are already first-class rows in `signals` (kind='like'),
-- which the ALS recommender + preference nudges consume. We aggregate like
-- counts from there (COUNT DISTINCT user_id) rather than add a competing
-- `likes` table that would fragment engagement data. This migration only adds
-- the comments table. Idempotent — safe to re-run on every boot.

CREATE TABLE IF NOT EXISTS comments (
    id             UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    info_object_id UUID NOT NULL REFERENCES info_objects(id) ON DELETE CASCADE,
    user_id        UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    body           TEXT NOT NULL,
    created_at     TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_comments_info ON comments(info_object_id, created_at DESC);
