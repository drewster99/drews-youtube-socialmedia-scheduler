"""YouTube Data API v3 wrapper."""

from __future__ import annotations

import logging
from pathlib import Path

from googleapiclient.http import MediaFileUpload

from yt_scheduler.services.auth import get_youtube_service

logger = logging.getLogger(__name__)


class PrivateVideoError(Exception):
    """Raised by ``download_video_file`` when the target video is private.

    pytubefix can fetch unlisted/public videos anonymously, but private
    ones require either browser cookies (which we deliberately don't
    use — see the cookie-prompt UX problems we hit with yt-dlp) or a
    privacy flip. The route layer catches this and asks the user to
    confirm flipping the video to unlisted before retrying.
    """

    def __init__(self, video_id: str):
        super().__init__(
            f"Video {video_id} is private; flip to unlisted to download."
        )
        self.video_id = video_id


def upload_video(
    file_path: str | Path,
    title: str,
    description: str = "",
    tags: list[str] | None = None,
    category_id: str = "28",  # "Science & Technology" — default for this dev-focused channel
    privacy_status: str = "unlisted",
    publish_at: str | None = None,
    made_for_kids: bool = False,
) -> dict:
    """Upload a video to YouTube using resumable upload.

    Returns the YouTube API response including the video ID.
    """
    youtube = get_youtube_service()

    body = {
        "snippet": {
            "title": title,
            "description": description,
            "tags": tags or [],
            "categoryId": category_id,
        },
        "status": {
            "privacyStatus": privacy_status,
            "selfDeclaredMadeForKids": made_for_kids,
        },
    }

    if publish_at and privacy_status == "private":
        body["status"]["publishAt"] = publish_at

    media = MediaFileUpload(
        str(file_path),
        mimetype="video/*",
        resumable=True,
        chunksize=256 * 1024 * 10,  # 2.5 MB chunks
    )

    request = youtube.videos().insert(
        part="snippet,status",
        body=body,
        media_body=media,
    )

    response = None
    while response is None:
        status, response = request.next_chunk()

    return response


def update_video_metadata(
    video_id: str,
    title: str | None = None,
    description: str | None = None,
    tags: list[str] | None = None,
    category_id: str | None = None,
    privacy_status: str | None = None,
    publish_at: str | None = None,
) -> dict:
    """Update video metadata. Only provided fields are changed."""
    youtube = get_youtube_service()

    # Fetch current metadata first
    current = youtube.videos().list(part="snippet,status", id=video_id).execute()
    if not current.get("items"):
        raise ValueError(f"Video {video_id} not found")

    item = current["items"][0]
    # Only keep mutable snippet fields — sending read-only fields (thumbnails,
    # channelTitle, localized, etc.) causes the YouTube API to reject the update.
    allowed_snippet = {"title", "description", "tags", "categoryId", "defaultLanguage", "defaultAudioLanguage"}
    snippet = {k: v for k, v in item["snippet"].items() if k in allowed_snippet}
    status = item["status"]

    if title is not None:
        snippet["title"] = title
    if description is not None:
        snippet["description"] = description
    if tags is not None:
        snippet["tags"] = tags
    if category_id is not None:
        snippet["categoryId"] = category_id
    if privacy_status is not None:
        status["privacyStatus"] = privacy_status
    if publish_at is not None:
        status["publishAt"] = publish_at

    return youtube.videos().update(
        part="snippet,status",
        body={"id": video_id, "snippet": snippet, "status": status},
    ).execute()


def set_thumbnail(video_id: str, thumbnail_path: str | Path) -> dict:
    """Upload a custom thumbnail for a video."""
    youtube = get_youtube_service()

    media = MediaFileUpload(str(thumbnail_path), mimetype="image/jpeg")
    return youtube.thumbnails().set(videoId=video_id, media_body=media).execute()


def get_video(video_id: str) -> dict | None:
    """Get video details."""
    youtube = get_youtube_service()
    result = youtube.videos().list(
        part="snippet,status,statistics,contentDetails", id=video_id
    ).execute()
    items = result.get("items", [])
    return items[0] if items else None


def list_channel_videos(max_results: int = 25) -> list[dict]:
    """List videos from the authenticated user's channel."""
    youtube = get_youtube_service()

    # Get the uploads playlist
    channels = youtube.channels().list(part="contentDetails", mine=True).execute()
    if not channels.get("items"):
        return []

    uploads_playlist = channels["items"][0]["contentDetails"]["relatedPlaylists"]["uploads"]

    # List videos from uploads playlist
    items = []
    request = youtube.playlistItems().list(
        part="snippet,status",
        playlistId=uploads_playlist,
        maxResults=min(max_results, 50),
    )

    while request and len(items) < max_results:
        result = request.execute()
        items.extend(result.get("items", []))
        request = youtube.playlistItems().list_next(request, result)

    return items[:max_results]


# --- Captions ---


def list_captions(video_id: str) -> list[dict]:
    """List caption tracks for a video."""
    youtube = get_youtube_service()
    result = youtube.captions().list(part="snippet", videoId=video_id).execute()
    return result.get("items", [])


def download_caption(caption_id: str, fmt: str = "srt") -> str:
    """Download a caption track. Returns the caption text."""
    youtube = get_youtube_service()
    return youtube.captions().download(id=caption_id, tfmt=fmt).execute().decode("utf-8")


# YouTube auto-captions live on a track owned by YouTube (ASR) that we can't
# overwrite. Our own uploads create / update a separate user-owned track that
# we tag with this name so we can find it again on subsequent saves.
_OUR_CAPTION_TRACK_NAME = "Drew's YT Scheduler"


def upload_caption(
    video_id: str,
    srt_text: str,
    language: str = "en",
    name: str = _OUR_CAPTION_TRACK_NAME,
) -> dict:
    """Push a transcript to YouTube as a user-uploaded caption track.

    Looks for an existing track with our app's name on this video and
    updates it if found; otherwise inserts a new one. Body is the raw SRT
    we already store as the canonical transcript shape — no conversion
    needed.

    Returns the caption resource (with ``id``) on success.
    """
    from io import BytesIO

    from googleapiclient.http import MediaIoBaseUpload

    if not srt_text.strip():
        raise ValueError("Refusing to upload an empty caption body.")

    youtube = get_youtube_service()
    media = MediaIoBaseUpload(
        BytesIO(srt_text.encode("utf-8")),
        mimetype="application/octet-stream",
        resumable=False,
    )

    # Look for an existing track with our name so we update instead of
    # piling up duplicates on each save.
    existing = list_captions(video_id)
    ours = next(
        (c for c in existing if c.get("snippet", {}).get("name") == name),
        None,
    )

    if ours is not None:
        return youtube.captions().update(
            part="snippet",
            body={
                "id": ours["id"],
                "snippet": {
                    "videoId": video_id,
                    "language": language,
                    "name": name,
                },
            },
            media_body=media,
        ).execute()

    return youtube.captions().insert(
        part="snippet",
        body={
            "snippet": {
                "videoId": video_id,
                "language": language,
                "name": name,
                "isDraft": False,
            },
        },
        media_body=media,
    ).execute()


# --- Video file download (for re-transcribing imported videos) -------------


def download_video_file(video_id: str, target_dir: "Path | str") -> "Path":
    """Pull the video file off YouTube via pytubefix.

    Used when the user wants to transcribe (or describe-from-frames) an
    imported video locally — Apple Speech / Whisper / ffmpeg need a file
    on disk. Anonymous pytubefix happily fetches public and unlisted
    videos; for private videos it raises ``VideoPrivate``, which we
    re-raise as :class:`PrivateVideoError` so the route can ask the user
    to flip the video to unlisted before retrying.

    Picks the highest-resolution progressive mp4 stream (audio + video
    in one file). The transcribers only need audio, but pytubefix's
    ``get_highest_resolution`` is the cleanest one-liner and the file
    sizes for a typical YouTube video are still modest at 720p.

    Returns the absolute path to the downloaded file. Raises
    :class:`PrivateVideoError` for private videos and ``RuntimeError``
    for any other failure (network, age-gated, members-only, removed).
    """
    try:
        from pytubefix import YouTube
        from pytubefix.exceptions import LoginRequired, VideoPrivate
    except ImportError as exc:  # pragma: no cover — dependency probe
        raise RuntimeError(
            "pytubefix is not installed. Install with: "
            "pip install -e \".[youtube-download]\""
        ) from exc

    target_dir = Path(target_dir)
    target_dir.mkdir(parents=True, exist_ok=True)

    url = f"https://www.youtube.com/watch?v={video_id}"
    try:
        yt = YouTube(url)
        stream = yt.streams.get_highest_resolution()
        if stream is None:
            raise RuntimeError(
                f"pytubefix returned no usable progressive streams for {video_id}"
            )
        downloaded = stream.download(
            output_path=str(target_dir),
            filename=f"{video_id}.{stream.subtype or 'mp4'}",
        )
    except (VideoPrivate, LoginRequired) as exc:
        # pytubefix surfaces private videos as either ``VideoPrivate`` (when
        # the playability response says ``LOGIN_REQUIRED`` with the
        # PRIVATE_VIDEO subreason) or the broader ``LoginRequired`` (anything
        # else that requires a login — for our use case, also a private
        # video the caller doesn't have cookies for).
        raise PrivateVideoError(video_id) from exc
    except Exception as exc:
        # Last-ditch heuristic: some YouTube responses still surface as a
        # plain Exception with "Please sign in" / "requires login" text.
        # Treat those as private too so the route can offer the unlist flow.
        text = str(exc).lower()
        if "sign in" in text or "login" in text or "private" in text:
            raise PrivateVideoError(video_id) from exc
        raise RuntimeError(f"pytubefix could not download {video_id}: {exc}") from exc

    return Path(downloaded).resolve()


def set_video_privacy(video_id: str, privacy_status: str) -> dict:
    """Flip a video's privacy status (private / unlisted / public).

    Thin wrapper over :func:`update_video_metadata` so the route layer
    can express its intent ("flip to unlisted before download") without
    importing the broader metadata-update helper.
    """
    return update_video_metadata(video_id, privacy_status=privacy_status)


# --- Comments ---


def list_comment_threads(
    video_id: str,
    max_results: int = 100,
    moderation_status: str | None = None,
) -> list[dict]:
    """List top-level comment threads on a video."""
    youtube = get_youtube_service()

    kwargs = {
        "part": "snippet,replies",
        "videoId": video_id,
        "maxResults": min(max_results, 100),
        "textFormat": "plainText",
    }
    if moderation_status:
        kwargs["moderationStatus"] = moderation_status

    items = []
    request = youtube.commentThreads().list(**kwargs)

    while request and len(items) < max_results:
        result = request.execute()
        items.extend(result.get("items", []))
        request = youtube.commentThreads().list_next(request, result)

    return items[:max_results]


def reply_to_comment(parent_comment_id: str, text: str) -> dict:
    """Reply to a comment."""
    youtube = get_youtube_service()
    return youtube.comments().insert(
        part="snippet",
        body={
            "snippet": {
                "parentId": parent_comment_id,
                "textOriginal": text,
            }
        },
    ).execute()


def delete_comment(comment_id: str) -> None:
    """Delete a comment."""
    youtube = get_youtube_service()
    youtube.comments().delete(id=comment_id).execute()


def moderate_comment(comment_id: str, status: str = "rejected", ban_author: bool = False) -> None:
    """Set moderation status on a comment.

    status: 'published', 'heldForReview', 'rejected'
    """
    youtube = get_youtube_service()
    youtube.comments().setModerationStatus(
        id=comment_id,
        moderationStatus=status,
        banAuthor=ban_author,
    ).execute()


# --- Channel ---


def get_channel_info() -> dict | None:
    """Get the authenticated user's channel info."""
    youtube = get_youtube_service()
    result = youtube.channels().list(
        part="snippet,statistics,brandingSettings,contentDetails",
        mine=True,
    ).execute()
    items = result.get("items", [])
    return items[0] if items else None


def compose_channel_url(handle: str | None, channel_id: str | None) -> str | None:
    """Build the canonical channel URL for a YouTube channel.

    Prefers the ``@handle`` form (``snippet.customUrl``) — the prettier URL
    most channels surface publicly. Falls back to ``/channel/<id>`` when
    no ``customUrl`` is published.

    Audited the YouTube Data API v3 ``channels`` resource against the
    ``snippet | statistics | brandingSettings | contentDetails`` parts:
    no fully-formed URL field exists. Composing client-side is the only
    option, and both forms resolve to the same channel page.
    """
    handle = (handle or "").strip()
    if handle:
        # `customUrl` already includes the leading `@` for modern channels.
        # Older channels can have a slash-form like "user/foo" or "c/bar"
        # — pass whatever the API gave us through verbatim.
        if not handle.startswith(("@", "user/", "c/")):
            handle = "@" + handle
        return f"https://www.youtube.com/{handle}"
    if channel_id:
        return f"https://www.youtube.com/channel/{channel_id}"
    return None
