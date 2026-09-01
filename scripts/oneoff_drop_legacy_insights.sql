-- oneoff_drop_legacy_insights.sql
--
-- RUN THIS ONCE, BY HAND, THEN NEVER AGAIN. It is deliberately NOT a migration.
--
-- WHAT IT DROPS
-- -------------
-- sherrbyte_app.insights — a table that should never have existed.
--
-- main.py's CREATE_TABLES used to declare a table called `insights`. That DDL
-- runs through pgcompat with search_path=sherrbyte_app, while the SPIE engine
-- writes — and _spie_patterns reads — public.insights over a separate asyncpg
-- pool using the default search_path. Two tables, the same name, two schemas,
-- one physical database.
--
-- The shadow was not inert: the boot seed wrote _SAMPLE_INSIGHTS demo rows into
-- it on every production start. Nothing ever read it there (the "seed" tier of
-- /patterns is only reachable when no DSN is configured, which never happens in
-- production), so nothing ever failed. It simply sat in the wrong schema holding
-- three fake rows, waiting for someone to join the wrong table.
--
-- The declaration was renamed to demo_insights on 2026-09-01, so the name is now
-- unambiguous in both directions and no new deployment can recreate this table.
-- This script removes the one that already exists in production.
--
-- WHY NOT A MIGRATION
-- -------------------
-- A migration runs forever, on every environment, including fresh databases
-- where this table was never created. That would make a one-time production
-- cleanup a permanent part of the schema history, for no benefit. Migration
-- numbers 022 and 023 are also reserved for the analog engine's own tables.
--
-- VERIFIED BEFORE WRITING THIS (production, 2026-09-01)
-- -----------------------------------------------------
--   sherrbyte_app.insights  COUNT(*) = 3, all three _SAMPLE_INSIGHTS
--                           signatures, 0 unexpected rows
--   public.insights         295 rows, untouched — this is the real one
--
-- RE-VERIFY BEFORE RUNNING. If the guard below reports anything other than the
-- three known seed signatures, STOP: something has written real data into the
-- wrong schema and dropping the table would destroy it.

-- ── 1. Guard. Read this output before running the DROP. ──────────────────────
SELECT COUNT(*)                                              AS total,
       COUNT(*) FILTER (WHERE signature IN (
           'temporal:monsoon-mumbai:veg-prices:3',
           'temporal:crude:usdinr:1',
           'emergence:rbi:fintech-lending'))                 AS known_seed_rows,
       COUNT(*) FILTER (WHERE signature IS NULL OR signature NOT IN (
           'temporal:monsoon-mumbai:veg-prices:3',
           'temporal:crude:usdinr:1',
           'emergence:rbi:fintech-lending'))                 AS unexpected_rows
  FROM sherrbyte_app.insights;

-- Expected: total = 3, known_seed_rows = 3, unexpected_rows = 0.
-- If unexpected_rows > 0, do not continue.

-- ── 2. The drop. IF EXISTS so a second run is a harmless no-op. ──────────────
DROP TABLE IF EXISTS sherrbyte_app.insights;

-- ── 3. Confirm the right table survived and the wrong one is gone. ───────────
SELECT table_schema, table_name
  FROM information_schema.tables
 WHERE table_name IN ('insights', 'demo_insights')
 ORDER BY table_schema, table_name;

-- Expected afterwards:
--   public.insights                 <- the engine's, still here
--   sherrbyte_app.demo_insights     <- the seed tier's, correctly named
-- and no sherrbyte_app.insights.

SELECT COUNT(*) AS engine_insights_still_present FROM public.insights;
