-- 013_insights.sql — detector output (Intelligence Engine V1, Step 4).
--
-- Every detector (emergence, temporal_correlation, …) writes rows here. Reads are
-- cheap SELECTs by type / entity. `signature` is a natural key so re-running a
-- nightly detector refreshes an insight instead of duplicating it.

CREATE TABLE IF NOT EXISTS insights (
    id           UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    type         TEXT NOT NULL,                 -- emergence | temporal_correlation | ...
    entity_ids   UUID[] NOT NULL DEFAULT '{}',
    domains      TEXT[] NOT NULL DEFAULT '{}',
    score        REAL DEFAULT 0,
    explain_json JSONB NOT NULL DEFAULT '{}'::jsonb,
    signature    TEXT UNIQUE,                   -- idempotency key for re-runs
    created_at   TIMESTAMPTZ DEFAULT now(),
    updated_at   TIMESTAMPTZ DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_insights_type     ON insights(type, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_insights_entities ON insights USING GIN (entity_ids);
CREATE INDEX IF NOT EXISTS idx_insights_created  ON insights(created_at DESC);
