-- 011_domain_signals.sql — the universal Signal spine (Intelligence Engine V1, Step 2).
--
-- Every input domain (news, stocks, commodities, metals, forex, weather) is
-- converted by a thin adapter into rows of THIS shape. All engine algorithms read
-- only from here — never from raw domain data. Adding a source later = one adapter,
-- schema untouched.
--
-- NOTE: named `domain_signals`, not `signals`, because `signals` already exists
-- (recsys implicit-feedback, migration 003). Different concept, kept separate.

CREATE TABLE IF NOT EXISTS domain_signals (
    id          BIGSERIAL PRIMARY KEY,
    entity_ids  UUID[] NOT NULL DEFAULT '{}',   -- resolved canonical entity ids
    domain      TEXT NOT NULL,                  -- news|stocks|commodities|metals|forex|weather
    ts          TIMESTAMPTZ NOT NULL,           -- when the event/observation happened
    location    TEXT,                           -- optional place
    magnitude   REAL DEFAULT 0,                 -- size: |price %|, rainfall mm, importance
    direction   SMALLINT DEFAULT 0,             -- +1 up | -1 down | 0 neutral
    sentiment   REAL,                           -- -1..1 where applicable, else NULL
    embedding   vector(384),                    -- semantic vector where applicable
    source_id   TEXT DEFAULT '',                -- feed/source identifier
    credibility REAL DEFAULT 0.5,               -- 0..1 source trust
    confidence  REAL DEFAULT 0.5,               -- 0..1 adapter confidence in this signal
    novelty     REAL DEFAULT 0,                 -- 0..1 first-time vs repeat
    ref_id      TEXT,                           -- provenance: info_object id / quote key
    created_at  TIMESTAMPTZ DEFAULT now()
);

-- Read paths filter by (recent) time + domain; detectors scan by entity membership.
CREATE INDEX IF NOT EXISTS idx_dsignals_ts_domain ON domain_signals(ts DESC, domain);
CREATE INDEX IF NOT EXISTS idx_dsignals_entities  ON domain_signals USING GIN (entity_ids);
CREATE INDEX IF NOT EXISTS idx_dsignals_domain_ts ON domain_signals(domain, ts DESC);

-- ─── Entity ↔ market symbol map ────────────────────────────────────────────
-- Links a canonical entity to its tradable symbol(s) so the stocks/commodities/
-- metals/forex adapters can attach price signals to the same entity the news
-- adapter uses (e.g. entity "Tata Motors" ↔ "TATAMOTORS.NS").
CREATE TABLE IF NOT EXISTS entity_ticker_map (
    symbol       TEXT NOT NULL,                 -- "TATAMOTORS.NS", "GC=F", "BTC-USD"
    domain       TEXT NOT NULL,                 -- stocks|commodities|metals|forex|crypto
    entity_id    UUID REFERENCES entities(id) ON DELETE CASCADE,
    display_name TEXT DEFAULT '',
    created_at   TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (symbol, domain)
);
CREATE INDEX IF NOT EXISTS idx_ticker_entity ON entity_ticker_map(entity_id);
