-- Web-push subscriptions (one row per browser/device per user).
CREATE TABLE IF NOT EXISTS push_subscriptions (
    id           BIGSERIAL PRIMARY KEY,
    user_id      UUID NOT NULL,
    endpoint     TEXT UNIQUE NOT NULL,
    subscription JSONB NOT NULL,
    last_push_at TIMESTAMPTZ,
    created_at   TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_push_user ON push_subscriptions(user_id);
