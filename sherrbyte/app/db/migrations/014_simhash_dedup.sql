-- 014_simhash_dedup.sql — near-duplicate story clustering (SPIE Knowledge Engine, Task 2).
--
-- The same wire story (PTI/ANI) republished across 20-30 outlets would otherwise
-- inflate every count — fake bursts, corrupt co-occurrence. We fingerprint each
-- article with a 64-bit SimHash (Manku et al. operating point) over cleaned-text
-- shingles; Hamming distance <= 3 → same story cluster. Downstream, counts are per
-- CLUSTER (unique stories) and source_count is distinct outlets within a cluster —
-- so republication becomes the velocity signal instead of a corruption.

-- Monotonic cluster id generator (a new story that matches nothing starts a cluster).
CREATE SEQUENCE IF NOT EXISTS article_cluster_seq;

CREATE TABLE IF NOT EXISTS article_fingerprints (
    article_id  UUID PRIMARY KEY,             -- fingerprinted doc (article or info-object id)
    simhash     BIGINT NOT NULL,              -- 64-bit SimHash stored as signed bigint
    cluster_id  BIGINT NOT NULL,              -- story cluster this doc belongs to
    created_at  TIMESTAMPTZ DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_fp_created ON article_fingerprints(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_fp_cluster ON article_fingerprints(cluster_id);

-- Each domain signal remembers which story cluster it came from, so detectors can
-- count distinct clusters rather than raw (duplicated) rows.
ALTER TABLE domain_signals ADD COLUMN IF NOT EXISTS cluster_id BIGINT;
CREATE INDEX IF NOT EXISTS idx_dsignals_cluster ON domain_signals(cluster_id);

-- Dedup ledger for co-occurrence: one row per (pair, day, cluster). A pair-day only
-- increments cooccurrence.count the FIRST time a given cluster contributes it, so the
-- materialized count is "distinct stories", not "distinct articles".
CREATE TABLE IF NOT EXISTS cooccurrence_events (
    entity_a     UUID NOT NULL,
    entity_b     UUID NOT NULL,
    window_start DATE NOT NULL,
    cluster_id   BIGINT NOT NULL,
    PRIMARY KEY (entity_a, entity_b, window_start, cluster_id),
    CHECK (entity_a < entity_b)
);
