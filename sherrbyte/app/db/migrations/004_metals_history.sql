-- 004_metals_history.sql — daily metal price snapshots.
-- metals.dev (and most free metals APIs) paywall historical data, so we store
-- one row per metal per day and build our own history for the detail graphs.
-- Idempotent: safe to re-run on every boot.

CREATE TABLE IF NOT EXISTS metals_history (
    metal        TEXT NOT NULL,
    day          DATE NOT NULL,
    price_usd_oz NUMERIC NOT NULL,
    PRIMARY KEY (metal, day)
);
