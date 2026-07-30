-- 017_instrument_keywords.sql — news-keyword ↔ market-instrument links.
--
-- WHY A NEW TABLE (and not entity_ticker_map):
--   entity_ticker_map is an IDENTITY map — PRIMARY KEY (symbol, domain) with a
--   single entity_id, i.e. "the symbol GC=F *is* the entity Gold". It answers
--   "which entity is this ticker?".
--   What the reasoning engine needs is a RELATION: "news about Iran, OPEC or
--   Hormuz is *relevant to* WTI Crude" — many keywords to many instruments, with
--   a weight. That does not fit a one-entity-per-symbol primary key, so it gets
--   its own table rather than being forced into the wrong shape.
--
-- Rows are keyed on the instrument's DISPLAY NAME (the same string
-- app/workers/market_signals.py INSTRUMENTS emits as the entity name), not on a
-- UUID, because entity ids are minted at runtime by the resolver and a static
-- seed cannot know them. Resolution to entity ids happens at read time and only
-- ever MATCHES existing entities — it never creates them, so an unmatched
-- keyword stays honestly unmatched instead of inventing a node in the graph.
--
-- Add mappings freely: INSERT a row with source='manual' and the engines pick it
-- up on the next run. sync_seeds() only ever touches source='seed' rows, so
-- hand-added rows are never clobbered.

CREATE TABLE IF NOT EXISTS instrument_keywords (
    instrument   TEXT NOT NULL,                 -- "WTI Crude", "NIFTY 50", "Gold"
    keyword      TEXT NOT NULL,                 -- news-side surface form: "OPEC", "Iran"
    norm_keyword TEXT,                          -- normalize_name(keyword); NULL → lower(keyword)
    weight       REAL NOT NULL DEFAULT 1.0,     -- 0..1 relevance, kept for future ranking
    source       TEXT NOT NULL DEFAULT 'seed',  -- seed | manual | learned
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (instrument, keyword)
);

CREATE INDEX IF NOT EXISTS idx_instrkw_instrument ON instrument_keywords(instrument);
CREATE INDEX IF NOT EXISTS idx_instrkw_norm       ON instrument_keywords(norm_keyword);
