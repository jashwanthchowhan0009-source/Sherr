-- 015_npmi.sql — NPMI edge weighting for co-occurrence (SPIE Graph Engine, Task 3).
--
-- Raw co-occurrence count over-rewards hub entities (e.g. "Modi" co-occurs with
-- everything by sheer volume). NPMI (normalized pointwise mutual information) asks
-- "do these two co-occur MORE THAN CHANCE?", discounting popularity. Computed from
-- the cluster-deduped counts (Task 2), and only where the pair count is >= 3
-- (rare-pair PMI is unstable); otherwise NULL. Detectors rank by npmi over count.

ALTER TABLE cooccurrence ADD COLUMN IF NOT EXISTS npmi DOUBLE PRECISION;

-- Ranking index: high-association pairs first, unknown (NULL) last.
CREATE INDEX IF NOT EXISTS idx_cooc_npmi ON cooccurrence(npmi DESC NULLS LAST);
