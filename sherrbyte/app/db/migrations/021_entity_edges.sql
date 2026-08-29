-- 021_entity_edges.sql — hand-authored directed causal edges.
--
-- DISTINCT FROM cooccurrence (012). That table is LEARNED and UNDIRECTED: it
-- counts which entities appear together and weights them by NPMI. It answers
-- "these two are associated". It cannot answer "crude drives airlines, and not
-- the other way round", because direction is not in the data it reads.
--
-- These edges are asserted, not learned, and deliberately so: 90 days of daily
-- closes cannot establish causal direction, and anything inferred from that
-- window would be a spurious correlation dressed as a mechanism. Domain
-- knowledge is the honest source here.
--
-- `direction` is the SIGN of the relationship, not the arrow — the arrow is
-- source -> target. amplifies: source up pushes target up. dampens: source up
-- pushes target down. Crude -> Airlines is `dampens` (fuel is a cost).

CREATE TABLE IF NOT EXISTS entity_edges (
    source_entity TEXT NOT NULL,
    target_entity TEXT NOT NULL,
    relation      TEXT NOT NULL,              -- input_cost | currency | policy | ...
    direction     TEXT NOT NULL DEFAULT 'amplifies'
                  CHECK (direction IN ('amplifies', 'dampens')),
    note          TEXT DEFAULT '',            -- why, in one line, for the card
    created_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (source_entity, target_entity, relation)
);

CREATE INDEX IF NOT EXISTS idx_entity_edges_src ON entity_edges(source_entity);
CREATE INDEX IF NOT EXISTS idx_entity_edges_tgt ON entity_edges(target_entity);
