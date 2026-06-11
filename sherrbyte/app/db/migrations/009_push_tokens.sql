-- FCM device tokens for web push (one row per browser/device per user).
CREATE TABLE IF NOT EXISTS push_tokens (
    id         BIGSERIAL PRIMARY KEY,
    user_id    UUID NOT NULL,
    fcm_token  TEXT UNIQUE NOT NULL,
    created_at TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_push_tokens_user ON push_tokens(user_id);
