-- 020_market_ticks.sql — the historical price store (Sherr-I).
--
-- Sherr-I correlates market anomalies with news events, and "anomaly" is a
-- statement about history: a 1% day is ordinary for crude and extraordinary for
-- USD/INR. markets.py only ever holds the LATEST quote (in-memory, reset on every
-- redeploy), so nothing in the codebase can currently say what normal looks like
-- for a symbol. This table is that baseline — one row per symbol per day, kept.
--
-- SCHEMA-QUALIFIED ON PURPOSE. This DB carries two apps: the engine's tables in
-- the search_path the engine connects with, and the deployed app's in
-- `sherrbyte_app` (see pgcompat.APP_SCHEMA — a dedicated schema is what stops the
-- engine's same-named `articles` from shadowing the app's). Naming the schema in
-- every statement means this file lands in the same place whichever pool applies
-- it, with no search_path assumption at all.
--
-- Idempotent: safe to re-run on every boot.

CREATE SCHEMA IF NOT EXISTS sherrbyte_app;

CREATE TABLE IF NOT EXISTS sherrbyte_app.market_ticks (
    id          SERIAL PRIMARY KEY,
    symbol      VARCHAR(32)   NOT NULL,   -- "^NSEI", "GC=F", "USDINR=X", "bitcoin"
    market_type VARCHAR(32)   NOT NULL,   -- stocks|metals|forex|commodities|rates|energy_stocks|crypto
    price       NUMERIC(16,4) NOT NULL,   -- that day's close, in the symbol's own currency
    change_24h  NUMERIC(8,4),             -- PERCENT vs the previous close; NULL when unknown
    ts          TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);

-- One row per symbol per day, enforced.
--
-- Written as a unique INDEX rather than the UNIQUE(symbol, ts::date) table
-- constraint the spec asked for, because Postgres does not accept an expression
-- in a UNIQUE constraint — only in an index. Same guarantee, and ON CONFLICT
-- targets it identically.
--
-- The cast is (ts AT TIME ZONE 'UTC')::date, not the bare ts::date: casting a
-- timestamptz straight to date depends on the session's TimeZone setting, which
-- makes it STABLE rather than IMMUTABLE and Postgres refuses to index it. Pinning
-- the zone also fixes what "a day" means — every backfilled bar and every
-- appended close is bucketed by UTC date, not by whatever zone the writer happens
-- to be connected from.
CREATE UNIQUE INDEX IF NOT EXISTS uq_market_ticks_symbol_day
    ON sherrbyte_app.market_ticks (symbol, ((ts AT TIME ZONE 'UTC')::date));

-- The detector read path: "the last N closes for this symbol, newest first".
CREATE INDEX IF NOT EXISTS idx_market_ticks_symbol_ts
    ON sherrbyte_app.market_ticks (symbol, ts DESC);

-- The report path and any per-class scan (earliest/latest per market_type).
CREATE INDEX IF NOT EXISTS idx_market_ticks_type_ts
    ON sherrbyte_app.market_ticks (market_type, ts DESC);
