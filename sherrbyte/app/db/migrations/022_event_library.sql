-- 022_event_library.sql — the historical analog engine's event library.
--
-- One row per past article that is usable as an analog: it resolved to at least
-- one entity already in the graph, and that entity reaches at least one market
-- instrument. Everything else in the corpus is not an "event" for this purpose
-- and gets no row.
--
-- This is an ACCUMULATING ASSET. The corpus is ~368 days deep today, so the
-- library starts thin and gets stronger every day the ingest runs. Phase 3
-- suppresses any card built on fewer than 5 analogs precisely because of that.
--
--
-- THREE SCHEMA DECISIONS, EACH OF WHICH LOOKS WRONG UNTIL YOU KNOW WHY
-- ====================================================================
--
-- 1. NO embedding COLUMN.
--    The analog matcher deliberately carries no vector term. The embeddings in
--    this database come from pipeline/embedder.py's md5 hash fallback, not
--    MiniLM: sentence-transformers lives in requirements-ml.txt, which the
--    detector cron does not install. Cosine over a hash embedding is lexical
--    collision dressed as semantic similarity. The column is absent rather than
--    nullable so nothing can quietly start writing hashes into it. See CLAUDE.md.
--
-- 2. NO simhash COLUMN.
--    The engine's simhash lives on public.articles / article_fingerprints, which
--    is the v6 pipeline's corpus. This library is built from
--    sherrbyte_app.articles — the corpus a reader can actually open — and that
--    table has no simhash column. Phase 2's near-duplicate rejection needs
--    another mechanism; it is not scaffolded here.
--
-- 3. article_id IS BIGINT, NOT UUID, AND CARRIES NO FOREIGN KEY.
--    sherrbyte_app.articles is sqlite-shaped (INTEGER PRIMARY KEY AUTOINCREMENT)
--    and reaches Postgres through pgcompat. Its ids are integers, not uuids. A
--    cross-schema FK is also deliberately avoided: that table's lifecycle
--    belongs to the deployed app, and an FK would let a routine article cleanup
--    there fail or cascade into engine data. Orphans are handled at read time.

CREATE TABLE IF NOT EXISTS hist_events (
    event_id       UUID PRIMARY KEY DEFAULT gen_random_uuid(),

    -- sherrbyte_app.articles.id. Unique: one event per article, so a re-run
    -- updates rather than duplicating (the backfill is idempotent).
    article_id     BIGINT      NOT NULL UNIQUE,

    occurred_at    TIMESTAMPTZ NOT NULL,

    -- Resolved with create=False. An article mentioning something the graph has
    -- never seen contributes nothing rather than minting an evidence-free node.
    entity_ids     UUID[]      NOT NULL DEFAULT '{}',

    -- Closed taxonomy. Extend ONLY by migration — a free-text class would make
    -- class_match (35% of the matcher's weight) meaningless.
    event_class    TEXT        NOT NULL,

    -- Market instrument TICKERS (CL=F, ^NSEI, bitcoin), not display names.
    -- Phase 3 joins these straight to sherrbyte_app.market_ticks.symbol.
    linked_symbols TEXT[]      NOT NULL DEFAULT '{}',

    created_at     TIMESTAMPTZ DEFAULT now(),
    updated_at     TIMESTAMPTZ DEFAULT now(),

    CONSTRAINT hist_events_class_known CHECK (event_class IN (
        'earnings', 'guidance_change', 'regulatory_action', 'leadership_change',
        'm_and_a', 'supply_disruption', 'geopolitical_conflict',
        'central_bank_policy', 'commodity_shock', 'currency_move',
        'sanctions', 'default_credit', 'other')),

    -- A row with no entity or no symbol cannot be an analog for anything. The
    -- backfill already filters these out; this stops a future writer
    -- reintroducing rows that can only ever be dead weight.
    CONSTRAINT hist_events_is_usable CHECK (
        array_length(entity_ids, 1) >= 1 AND array_length(linked_symbols, 1) >= 1)
);

-- Phase 2's candidate query is "overlaps my symbols OR shares my class", so
-- both sides need to be cheap. GIN handles the array overlap operator (&&).
CREATE INDEX IF NOT EXISTS idx_hist_events_symbols
    ON hist_events USING GIN (linked_symbols);
CREATE INDEX IF NOT EXISTS idx_hist_events_entities
    ON hist_events USING GIN (entity_ids);

-- Class + time together: the matcher filters by class and Phase 3 then walks
-- forward from occurred_at.
CREATE INDEX IF NOT EXISTS idx_hist_events_class_time
    ON hist_events (event_class, occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_hist_events_time
    ON hist_events (occurred_at DESC);
