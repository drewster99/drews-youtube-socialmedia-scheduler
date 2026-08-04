-- Whether WE liked a comment — the thumbs-up, as a form of having answered.
--
-- `comments.snippet.viewerRating` is the rating given by whoever authorized the
-- request. Every sweep is authorized as the channel owner, so for our rows it
-- means "the channel gave this a thumbs-up". Values are `like` or `none`;
-- YouTube deliberately reports a negative rating as `none` too, so this column
-- can say a comment was liked and can never say one was disliked.
--
-- NULL is unknown — a row stored before this column existed, or one YouTube did
-- not report a rating for. Never read it as `none`: "we did not ask" and "not
-- liked" would otherwise be the same answer, and the second one silently keeps
-- a thread marked as needing a reply.
--
-- The creator HEART (the channel avatar shown on a comment in the YouTube UI)
-- is a different gesture and is NOT in the Data API at all — no property on the
-- comments resource exposes it. A hearted comment is therefore indistinguishable
-- from an untouched one here; only the thumbs-up is visible to us.

ALTER TABLE youtube_comments ADD COLUMN viewer_rating TEXT;
