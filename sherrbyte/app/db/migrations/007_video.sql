-- Optional direct video URL (mp4/webm) extracted from RSS media/enclosures.
ALTER TABLE info_objects ADD COLUMN IF NOT EXISTS video_url TEXT DEFAULT '';
