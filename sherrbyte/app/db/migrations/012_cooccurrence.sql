-- 012_cooccurrence.sql — materialized entity co-occurrence (Intelligence Engine V1, Step 3).
--
-- For every domain_signals row, each unordered pair of its entities is counted into
-- a daily bucket. This is the candidate-pair source for the detectors (emergence,
-- temporal correlation). It is ALWAYS written at ingest / backfill time and only
-- ever SELECTed at read time — never computed on the fly.
--
-- Pairs are stored canonically (entity_a < entity_b) so (X,Y) and (Y,X) are one row.

CREATE TABLE IF NOT EXISTS cooccurrence (
    entity_a     UUID NOT NULL,
    entity_b     UUID NOT NULL,
    window_start DATE NOT NULL,                 -- daily bucket (UTC date of the signal)
    count        INTEGER NOT NULL DEFAULT 0,
    last_seen    TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (entity_a, entity_b, window_start),
    CHECK (entity_a < entity_b)
);

-- Detectors look up a specific pair across buckets; and scan one entity's partners.
CREATE INDEX IF NOT EXISTS idx_cooc_pair   ON cooccurrence(entity_a, entity_b);
CREATE INDEX IF NOT EXISTS idx_cooc_a      ON cooccurrence(entity_a);
CREATE INDEX IF NOT EXISTS idx_cooc_b      ON cooccurrence(entity_b);
CREATE INDEX IF NOT EXISTS idx_cooc_window ON cooccurrence(window_start);
