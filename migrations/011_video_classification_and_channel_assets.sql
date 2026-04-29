-- Migration 011: video classification + channel assets
--
-- 1. videos.youtube_deleted    — set to 1 when YouTube reports the video has
--                                been deleted (the uploads-playlist stub
--                                still mentions the videoId, so we can
--                                cross-reference our DB rows to the stub
--                                and mark them).
-- 2. videos.youtube_kind       — coarse YouTube classification: 'video',
--                                'short', or 'live'. Independent of our
--                                internal item_type taxonomy (episode /
--                                segment / short / hook / standalone). NULL
--                                when not yet classified.
-- 3. projects.channel_thumbnail_url
--    projects.channel_banner_url
--                              — cached channel art from YouTube so the
--                                Home page project cards can render an
--                                icon / banner without refetching every
--                                page load.

ALTER TABLE videos ADD COLUMN youtube_deleted INTEGER NOT NULL DEFAULT 0;
ALTER TABLE videos ADD COLUMN youtube_kind TEXT;

ALTER TABLE projects ADD COLUMN channel_thumbnail_url TEXT;
ALTER TABLE projects ADD COLUMN channel_banner_url TEXT;
