-- Corpus separation for the market engines.
--
-- market_reaction answers "what news accompanied this price move". Fed the whole
-- general-news corpus it links a silver move to a video-game guide that merely
-- contains the word "silver". So info_objects now carries which feed set the row
-- came from, and market_reaction reads WHERE io.feed_class = 'financial'.
--
-- The value is stamped at write time by pipeline/constructor.persist_info_object
-- from financial_feeds.feed_class(source_name) — the same registry the root app
-- uses, so a source is financial in exactly one place. Stored as a column, not a
-- source_name IN (...) filter, so a future news reader inherits the separation
-- instead of having to remember it.
--
-- Default 'general': existing rows came from the general feeds and are correctly
-- excluded. The market engines start fresh on the financial feeds' output rather
-- than inheriting a mixed history.
ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS feed_class TEXT NOT NULL DEFAULT 'general';

CREATE INDEX IF NOT EXISTS idx_info_objects_feed_class ON info_objects (feed_class);
