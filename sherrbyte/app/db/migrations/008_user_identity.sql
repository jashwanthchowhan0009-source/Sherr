-- 008_user_identity.sql — unique @username + last-active tracking.
-- Additive and idempotent: safe to re-run on every boot. Does NOT touch the
-- existing auth columns (password_hash, etc.) — only adds identity/analytics.

ALTER TABLE users ADD COLUMN IF NOT EXISTS username    TEXT;
ALTER TABLE users ADD COLUMN IF NOT EXISTS last_active TIMESTAMPTZ;

-- Case-insensitive uniqueness for non-empty usernames.
CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_lower
    ON users (lower(username)) WHERE username IS NOT NULL AND username <> '';

-- Backfill a guaranteed-unique handle for any pre-existing user that lacks one:
-- email local-part (≤12 chars, sanitized) + 6 hex chars from the uuid.
UPDATE users SET username =
    COALESCE(NULLIF(substr(regexp_replace(lower(split_part(email, '@', 1)), '[^a-z0-9_]', '', 'g'), 1, 12), ''), 'user')
    || '_' || substr(replace(id::text, '-', ''), 1, 6)
WHERE username IS NULL OR username = '';
