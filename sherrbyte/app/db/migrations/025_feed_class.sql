-- Corpus separation for the market engines.
--
-- market_reaction answers "what news accompanied this price move". Fed the whole
-- general-news corpus it links a silver move to a video-game guide that merely
-- contains the word "silver". So info_objects now carries which feed set the row
-- came from, and market_reaction reads WHERE io.feed_class = 'financial'.
--
-- The value is stamped at write time by pipeline/constructor.persist_info_object
-- from feeds_financial.feed_class(source_name) — the SAME registry the signal-path
-- query whitelist (financial_sources()) is bound from, so this persisted column
-- and that whitelist derive from one set and can never disagree. Two belts, one
-- truth: the column is the stored fact a query joins on, the whitelist is the
-- in-query guard, and a future news reader inherits the separation rather than
-- having to remember it.
--
-- Default 'general': existing rows came from the general feeds and are correctly
-- excluded. market_reaction operates on recent news windows, so it self-heals as
-- financial feeds ingest — no historical backfill of info_objects is needed here.
ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS feed_class TEXT NOT NULL DEFAULT 'general';

CREATE INDEX IF NOT EXISTS idx_info_objects_feed_class ON info_objects (feed_class);
