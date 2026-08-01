-- 019_watchlist.sql — emergence candidates that passed but did not make the cut.
--
-- Emergence was writing 74 insights per run out of 593 candidates. Everything that
-- passes the filters is real, but a feed of 74 "new connections" is not intelligence,
-- it is a dump — the reader cannot tell which three matter.
--
-- So the detector writes only the top N by composite score and parks the rest here.
-- Kept, queryable, never surfaced: a candidate that was genuinely interesting and
-- ranked 13th today can be found tomorrow rather than being silently discarded.

CREATE TABLE IF NOT EXISTS watchlist (
    entity_a   UUID NOT NULL,
    entity_b   UUID NOT NULL,
    kind       TEXT NOT NULL DEFAULT 'emergence',
    score      REAL NOT NULL DEFAULT 0,
    npmi       REAL,
    detail     JSONB DEFAULT '{}'::jsonb,
    seen_at    TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (entity_a, entity_b, kind)
);

CREATE INDEX IF NOT EXISTS idx_watchlist_score ON watchlist(kind, score DESC);
CREATE INDEX IF NOT EXISTS idx_watchlist_seen  ON watchlist(seen_at DESC);
