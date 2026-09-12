-- 028_filings.sql — exchange & regulator FILINGS as a first-class evidence source.
--
-- WHY THIS TABLE EXISTS, AND WHY IT IS NOT `articles`
-- ===================================================
-- The analog engine had nothing to say because the corpus held ~38 financial
-- articles. The fix is not another detector — it is more financial evidence at
-- the source. BSE/NSE corporate announcements and RBI/SEBI press releases are
-- structured, free, and publish hundreds of filings a day: results, capacity
-- changes, management changes, regulatory orders — exactly what an analyst reads.
--
-- A filing is NOT a news article and must never enter the rewrite path:
--   • it already has a filing type, a company code and a date — nothing to
--     "originalise", and running it through the AI drain would (a) burn the
--     free-tier request budget the drain is rationing and (b) invent prose over
--     a structured fact. So filings live in their OWN table, resolved and
--     classified at INGEST with ZERO provider calls (event_class is a rule table
--     in app/spie/filings/classify.py, never an LLM).
--
-- SCHEMA-QUALIFIED sherrbyte_app, exactly like 020_market_ticks and 024_ril_proof:
-- the engine (asyncpg) writes these rows schema-named, and the deployed app
-- (pgcompat, search_path=sherrbyte_app) reads them unqualified for
-- GET /admin/filing-doctor. Naming the schema in every statement means the file
-- lands in the same place whichever pool applies it.
--
-- Idempotent: safe to re-run on every boot.

CREATE SCHEMA IF NOT EXISTS sherrbyte_app;

CREATE TABLE IF NOT EXISTS sherrbyte_app.filings (
    id            BIGSERIAL PRIMARY KEY,
    source        TEXT NOT NULL,                 -- BSE | NSE | RBI | SEBI
    external_id   TEXT NOT NULL,                 -- the source's own id — the dedup key
    company_code  TEXT,                          -- BSE scrip code / NSE symbol; NULL for regulators
    company_name  TEXT,
    filing_type   TEXT NOT NULL DEFAULT '',      -- the source's own category/subject label, verbatim
    -- Mapped from filing_type by RULES (app/spie/filings/classify.py), into the
    -- SAME closed taxonomy the analog matcher's class_match weight is built on.
    -- The CHECK is the schema-level guarantee that a rule can only ever emit a
    -- class the matcher understands; extending it means editing this list in a
    -- new migration, deliberately the same friction 022_event_library imposes.
    event_class   TEXT NOT NULL DEFAULT 'other'
                  CHECK (event_class IN (
                      'earnings', 'guidance_change', 'regulatory_action',
                      'leadership_change', 'm_and_a', 'supply_disruption',
                      'geopolitical_conflict', 'central_bank_policy',
                      'commodity_shock', 'currency_move', 'sanctions',
                      'default_credit', 'other')),
    subject       TEXT NOT NULL DEFAULT '',      -- the filing's subject/headline line
    filing_date   DATE,                          -- the filing's own date, not ingest time
    url           TEXT,                          -- attachment / release link, best-effort
    -- Resolved at ingest with create=False: a filing about a company we have
    -- never seen resolves to nothing and contributes nothing, exactly like
    -- instrument_map — we do not mint an entity from a filing alone.
    entity_id     UUID,
    symbol        TEXT,                          -- resolved instrument ticker (may be NULL)
    -- Always 'financial'. Kept as a column so a future reader joining filings to
    -- the news corpus inherits the same separation the analog engine gates on,
    -- rather than having to special-case this table.
    feed_class    TEXT NOT NULL DEFAULT 'financial',
    -- The parsed raw record, verbatim. This is what /admin/filing-doctor shows
    -- when a shape is wrong: the raw record, not a stack trace.
    raw           JSONB NOT NULL DEFAULT '{}'::jsonb,
    ingested_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One filing per (source, external_id): a re-run inserts nothing it already
-- holds. This is the idempotency guarantee that lets the cron re-ingest every
-- pass without duplicating a filing, the same discipline ril_proof_log uses.
CREATE UNIQUE INDEX IF NOT EXISTS uq_filings_source_extid
    ON sherrbyte_app.filings (source, external_id);

CREATE INDEX IF NOT EXISTS idx_filings_filing_date
    ON sherrbyte_app.filings (filing_date DESC);
CREATE INDEX IF NOT EXISTS idx_filings_event_class
    ON sherrbyte_app.filings (event_class);
CREATE INDEX IF NOT EXISTS idx_filings_symbol
    ON sherrbyte_app.filings (symbol) WHERE symbol IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_filings_entity
    ON sherrbyte_app.filings (entity_id) WHERE entity_id IS NOT NULL;
